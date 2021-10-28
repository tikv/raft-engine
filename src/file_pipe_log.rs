// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::BufRead;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use fs2::FileExt;
use log::{debug, info, warn};
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use rayon::prelude::*;

use crate::codec::{self, NumberEncoder};
use crate::config::{Config, RecoveryMode};
use crate::event_listener::EventListener;
use crate::file_builder::FileBuilder;
use crate::log_batch::LogItemBatch;
use crate::log_file::{LogFd, LogFile};
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue, PipeLog};
use crate::reader::LogItemBatchFileReader;
use crate::util::InstantExt;
use crate::{Error, Result};

const LOG_NUM_LEN: usize = 16;
const LOG_APPEND_SUFFIX: &str = ".raftlog";
const LOG_REWRITE_SUFFIX: &str = ".rewrite";

const INIT_FILE_ID: u64 = 1;

const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

const LOG_FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
pub const LOG_FILE_HEADER_LEN: usize = LOG_FILE_MAGIC_HEADER.len() + std::mem::size_of::<Version>();

fn build_file_name(file_id: FileId) -> String {
    match file_id.queue {
        LogQueue::Append => format!(
            "{:0width$}{}",
            file_id.seq,
            LOG_APPEND_SUFFIX,
            width = LOG_NUM_LEN
        ),
        LogQueue::Rewrite => format!(
            "{:0width$}{}",
            file_id.seq,
            LOG_REWRITE_SUFFIX,
            width = LOG_NUM_LEN
        ),
    }
}

fn parse_file_name(file_name: &str) -> Option<FileId> {
    if file_name.len() > LOG_NUM_LEN {
        if let Ok(seq) = file_name[..LOG_NUM_LEN].parse::<u64>() {
            if file_name.ends_with(LOG_APPEND_SUFFIX) {
                return Some(FileId {
                    queue: LogQueue::Append,
                    seq,
                });
            } else if file_name.ends_with(LOG_REWRITE_SUFFIX) {
                return Some(FileId {
                    queue: LogQueue::Rewrite,
                    seq,
                });
            }
        }
    }
    None
}

fn build_file_path<P: AsRef<Path>>(dir: P, file_id: FileId) -> PathBuf {
    let mut path = PathBuf::from(dir.as_ref());
    path.push(build_file_name(file_id));
    path
}

#[derive(Clone, Copy, FromPrimitive, ToPrimitive)]
#[repr(u64)]
enum Version {
    V1 = 1,
}

pub struct LogFileHeader {
    version: Version,
}

impl LogFileHeader {
    pub fn new() -> Self {
        Self {
            version: Version::V1,
        }
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < LOG_FILE_HEADER_LEN {
            return Err(Error::Corruption("log file header too short".to_owned()));
        }
        if !buf.starts_with(LOG_FILE_MAGIC_HEADER) {
            return Err(Error::Corruption(
                "log file magic header mismatch".to_owned(),
            ));
        }
        buf.consume(LOG_FILE_MAGIC_HEADER.len());
        let v = codec::decode_u64(buf)?;
        if let Some(version) = Version::from_u64(v) {
            Ok(Self { version })
        } else {
            Err(Error::Corruption(format!(
                "unrecognized log file version: {}",
                v
            )))
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
        buf.encode_u64(self.version.to_u64().unwrap())?;
        Ok(())
    }
}

struct FileToRecover<R: Seek + Read> {
    seq: FileSeq,
    fd: Arc<LogFd>,
    reader: Option<R>,
}

struct ActiveFile<W: Seek + Write> {
    fd: Arc<LogFd>,
    writer: W,

    written: usize,
    capacity: usize,
    last_sync: usize,
}

impl<W: Seek + Write> ActiveFile<W> {
    fn open(fd: Arc<LogFd>, writer: W) -> Result<Self> {
        let file_size = fd.file_size()?;
        let mut f = Self {
            fd,
            writer,
            written: file_size,
            capacity: file_size,
            last_sync: file_size,
        };
        if file_size < LOG_FILE_HEADER_LEN {
            f.write_header()?;
        } else {
            f.writer.seek(std::io::SeekFrom::Start(file_size as u64))?;
        }
        Ok(f)
    }

    fn rotate(&mut self, fd: Arc<LogFd>, writer: W) -> Result<()> {
        self.writer = writer;
        self.written = 0;
        self.capacity = 0;
        self.fd = fd;
        self.last_sync = 0;
        self.write_header()
    }

    fn truncate(&mut self) -> Result<()> {
        if self.written < self.capacity {
            self.fd.truncate(self.written)?;
            self.capacity = self.written;
        }
        Ok(())
    }

    /// rockback to last synced position
    fn rollback(&mut self) -> Result<()> {
        self.writer
            .seek(std::io::SeekFrom::Start(self.last_sync as u64))?;
        self.written = self.last_sync;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.seek(std::io::SeekFrom::Start(0))?;
        self.written = 0;
        let mut buf = Vec::with_capacity(LOG_FILE_HEADER_LEN);
        LogFileHeader::new().encode(&mut buf)?;
        self.write(&buf, 0)
    }

    fn write(&mut self, buf: &[u8], target_file_size: usize) -> Result<()> {
        if self.written + buf.len() > self.capacity {
            // Use fallocate to pre-allocate disk space for active file.
            let alloc = std::cmp::max(
                self.written + buf.len() - self.capacity,
                std::cmp::min(
                    FILE_ALLOCATE_SIZE,
                    target_file_size.saturating_sub(self.capacity),
                ),
            );
            if alloc > 0 {
                self.fd.allocate(self.capacity, alloc)?;
                self.capacity += alloc;
            }
        }
        self.writer.write_all(buf)?;
        self.written += buf.len();
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        if self.last_sync < self.written {
            let start = Instant::now();
            self.fd.sync()?;
            self.last_sync = self.written;
            LOG_SYNC_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        }
        Ok(())
    }

    fn since_last_sync(&self) -> usize {
        self.written - self.last_sync
    }
}

struct LogManager<B: FileBuilder> {
    queue: LogQueue,
    dir: String,
    rotate_size: usize,
    bytes_per_sync: usize,
    file_builder: Arc<B>,
    listeners: Vec<Arc<dyn EventListener>>,

    pub first_file_seq: FileSeq,
    pub active_file_seq: FileSeq,

    all_files: VecDeque<Arc<LogFd>>,
    active_file: ActiveFile<B::Writer<LogFile>>,
}

impl<B: FileBuilder> LogManager<B> {
    fn open(
        cfg: &Config,
        file_builder: Arc<B>,
        listeners: Vec<Arc<dyn EventListener>>,
        queue: LogQueue,
        files: Vec<FileToRecover<B::Reader<LogFile>>>,
    ) -> Result<Self> {
        let mut first_file_seq = files.first().map(|f| f.seq).unwrap_or(0);
        let mut active_file_seq = files.last().map(|f| f.seq).unwrap_or(0);
        let mut all_files: VecDeque<Arc<LogFd>> = files
            .into_iter()
            .map(|f| {
                for listener in &listeners {
                    listener.post_new_log_file(FileId { queue, seq: f.seq });
                }
                f.fd
            })
            .collect();

        let mut create_file = false;
        if first_file_seq == 0 {
            first_file_seq = INIT_FILE_ID;
            active_file_seq = first_file_seq;
            create_file = true;
            let fd = Arc::new(LogFd::create(&build_file_path(
                &cfg.dir,
                FileId {
                    queue,
                    seq: first_file_seq,
                },
            ))?);
            all_files.push_back(fd);
            for listener in &listeners {
                listener.post_new_log_file(FileId {
                    queue,
                    seq: first_file_seq,
                });
            }
        }

        let active_fd = all_files.back().unwrap().clone();
        let active_file = ActiveFile::open(
            active_fd.clone(),
            file_builder.build_writer(
                &build_file_path(
                    &cfg.dir,
                    FileId {
                        queue,
                        seq: active_file_seq,
                    },
                ),
                LogFile::new(active_fd),
                create_file,
            )?,
        )?;

        let manager = Self {
            queue,
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            file_builder,
            listeners,

            first_file_seq,
            active_file_seq,

            all_files,
            active_file,
        };
        manager.update_metrics();
        Ok(manager)
    }

    fn new_log_file(&mut self) -> Result<()> {
        // Necessary to truncate extra zeros from fallocate().
        self.truncate_active_log()?;
        // ===== old log data is safe from here =====
        self.rotate()
    }

    fn truncate_and_sync(&mut self) -> Result<()> {
        debug_assert!(self.active_file_seq >= INIT_FILE_ID);
        self.truncate_active_log()?;
        self.active_file.sync()?;
        Ok(())
    }

    fn rotate(&mut self) -> Result<()> {
        debug_assert!(self.active_file_seq >= INIT_FILE_ID);
        self.active_file_seq += 1;
        let path = build_file_path(
            &self.dir,
            FileId {
                queue: self.queue,
                seq: self.active_file_seq,
            },
        );

        let fd = Arc::new(LogFd::create(&path)?);
        self.all_files.push_back(fd.clone());
        self.active_file.rotate(
            fd.clone(),
            self.file_builder
                .build_writer(&path, LogFile::new(fd), true /*create*/)?,
        )?;
        self.sync_dir()?;
        for listener in &self.listeners {
            listener.post_new_log_file(FileId {
                queue: self.queue,
                seq: self.active_file_seq,
            });
        }
        self.update_metrics();
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        self.active_file.sync()
    }

    fn sync_dir(&self) -> Result<()> {
        let path = PathBuf::from(&self.dir);
        std::fs::File::open(path).and_then(|d| d.sync_all())?;
        Ok(())
    }

    fn truncate_active_log(&mut self) -> Result<()> {
        self.active_file.truncate()
    }

    fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<LogFd>> {
        if file_seq < self.first_file_seq || file_seq > self.active_file_seq {
            return Err(Error::Io(IoError::new(
                IoErrorKind::NotFound,
                "file seqno out of range",
            )));
        }
        Ok(self.all_files[(file_seq - self.first_file_seq) as usize].clone())
    }

    fn get_active_fd(&self) -> Option<Arc<LogFd>> {
        self.all_files.back().cloned()
    }

    fn purge_to(&mut self, file_seq: FileSeq) -> Result<usize> {
        if file_seq > self.active_file_seq {
            return Err(box_err!("Purge active or newer files"));
        }
        let end_offset = (file_seq - self.first_file_seq) as usize;
        self.all_files.drain(..end_offset);
        self.first_file_seq = file_seq;
        self.update_metrics();
        Ok(end_offset)
    }

    fn append(
        &mut self,
        ctx: &mut FilePipeLogWriteContext,
        content: &[u8],
    ) -> Result<FileBlockHandle> {
        let offset = self.active_file.written as u64;
        self.active_file.write(content, self.rotate_size)?;
        let handle = FileBlockHandle {
            id: FileId {
                queue: self.queue,
                seq: self.active_file_seq,
            },
            offset,
            len: self.active_file.written - offset as usize,
        };
        for listener in &self.listeners {
            listener.on_append_log_file(handle);
        }
        if self.active_file.written >= self.rotate_size {
            ctx.syncable = Syncable::NeedRotate
        } else if self.active_file.since_last_sync() >= self.bytes_per_sync
            && ctx.syncable == Syncable::DontNeed
        {
            ctx.syncable = Syncable::NeedSync
        }
        Ok(handle)
    }

    fn update_metrics(&self) {
        match self.queue {
            LogQueue::Append => LOG_FILE_COUNT.append.set(self.all_files.len() as i64),
            LogQueue::Rewrite => LOG_FILE_COUNT.rewrite.set(self.all_files.len() as i64),
        }
    }

    fn size(&self) -> usize {
        (self.active_file_seq - self.first_file_seq) as usize * self.rotate_size
            + self.active_file.written
    }

    fn get_write_context(&self) -> FilePipeLogWriteContext {
        FilePipeLogWriteContext {
            synced: self.active_file.written == self.active_file.last_sync,
            syncable: Syncable::DontNeed,
        }
    }
}

pub trait ReplayMachine: Send + Default {
    fn replay(&mut self, item_batch: LogItemBatch, file_id: FileId) -> Result<()>;

    fn merge(&mut self, rhs: Self, queue: LogQueue) -> Result<()>;
}

pub struct FilePipeLog<B: FileBuilder> {
    dir: String,
    file_builder: Arc<B>,

    appender: Arc<RwLock<LogManager<B>>>,
    rewriter: Arc<RwLock<LogManager<B>>>,

    _lock_file: File,
}

impl<B: FileBuilder> FilePipeLog<B> {
    pub fn open<S: ReplayMachine>(
        cfg: &Config,
        file_builder: Arc<B>,
        listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<(FilePipeLog<B>, S, S)> {
        let path = Path::new(&cfg.dir);
        if !path.exists() {
            info!("Create raft log directory: {}", &cfg.dir);
            fs::create_dir(&cfg.dir)?;
        }
        if !path.is_dir() {
            return Err(box_err!("Not directory: {}", &cfg.dir));
        }

        // Add a LOCK File to lock the directory.
        let lock_path = path.join(Path::new("LOCK"));
        let lock_file = File::create(lock_path.as_path())?;
        lock_file.try_lock_exclusive().map_err(|e| {
            Error::Other(box_err!(
                "lock {} failed ({}), maybe another instance is using this directory.",
                lock_path.display(),
                e
            ))
        })?;

        let (mut min_append_id, mut max_append_id) = (u64::MAX, 0);
        let (mut min_rewrite_id, mut max_rewrite_id) = (u64::MAX, 0);
        fs::read_dir(path)?.for_each(|e| {
            if let Ok(e) = e {
                match parse_file_name(e.file_name().to_str().unwrap()) {
                    Some(FileId {
                        queue: LogQueue::Append,
                        seq,
                    }) => {
                        min_append_id = std::cmp::min(min_append_id, seq);
                        max_append_id = std::cmp::max(max_append_id, seq);
                    }
                    Some(FileId {
                        queue: LogQueue::Rewrite,
                        seq,
                    }) => {
                        min_rewrite_id = std::cmp::min(min_rewrite_id, seq);
                        max_rewrite_id = std::cmp::max(max_rewrite_id, seq);
                    }
                    _ => {}
                }
            }
        });

        let mut append_files = Vec::new();
        let mut rewrite_files = Vec::new();
        for (queue, min_id, max_id, files) in [
            (
                LogQueue::Append,
                min_append_id,
                max_append_id,
                &mut append_files,
            ),
            (
                LogQueue::Rewrite,
                min_rewrite_id,
                max_rewrite_id,
                &mut rewrite_files,
            ),
        ] {
            if max_id > 0 {
                for i in min_id..=max_id {
                    let seq = i;
                    let path = build_file_path(&cfg.dir, FileId { queue, seq });
                    let fd = Arc::new(LogFd::open(&path)?);
                    files.push(FileToRecover {
                        seq,
                        fd: fd.clone(),
                        reader: Some(file_builder.build_reader(&path, LogFile::new(fd))?),
                    })
                }
            }
        }

        let (append_sequential_replay_machine, rewrite_sequential_replay_machine) = Self::recover(
            cfg.recovery_mode,
            cfg.recovery_threads,
            cfg.recovery_read_block_size.0 as usize,
            &mut append_files,
            &mut rewrite_files,
        )?;

        let appender = Arc::new(RwLock::new(LogManager::open(
            cfg,
            file_builder.clone(),
            listeners.clone(),
            LogQueue::Append,
            append_files,
        )?));
        let rewriter = Arc::new(RwLock::new(LogManager::open(
            cfg,
            file_builder.clone(),
            listeners,
            LogQueue::Rewrite,
            rewrite_files,
        )?));

        Ok((
            FilePipeLog {
                dir: cfg.dir.clone(),
                file_builder,
                appender,
                rewriter,
                _lock_file: lock_file,
            },
            append_sequential_replay_machine,
            rewrite_sequential_replay_machine,
        ))
    }

    fn recover<S: ReplayMachine>(
        recovery_mode: RecoveryMode,
        threads: usize,
        read_block_size: usize,
        append_files: &mut [FileToRecover<B::Reader<LogFile>>],
        rewrite_files: &mut [FileToRecover<B::Reader<LogFile>>],
    ) -> Result<(S, S)> {
        let (append_concurrency, rewrite_concurrency) =
            match (append_files.len(), rewrite_files.len()) {
                (a, b) if a > 0 && b > 0 => {
                    let a_threads = std::cmp::max(1, threads * a / (a + b));
                    let b_threads = std::cmp::max(1, threads.saturating_sub(a_threads));
                    (a_threads, b_threads)
                }
                _ => (threads, threads),
            };

        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        let (append, rewrite) = pool.join(
            || {
                Self::recover_queue(
                    LogQueue::Append,
                    recovery_mode,
                    append_concurrency,
                    read_block_size,
                    append_files,
                )
            },
            || {
                Self::recover_queue(
                    LogQueue::Rewrite,
                    recovery_mode,
                    rewrite_concurrency,
                    read_block_size,
                    rewrite_files,
                )
            },
        );

        Ok((append?, rewrite?))
    }

    fn recover_queue<S: ReplayMachine>(
        queue: LogQueue,
        recovery_mode: RecoveryMode,
        concurrency: usize,
        read_block_size: usize,
        files: &mut [FileToRecover<B::Reader<LogFile>>],
    ) -> Result<S> {
        if concurrency == 0 || files.is_empty() {
            return Ok(S::default());
        }
        let max_chunk_size = std::cmp::max((files.len() + concurrency - 1) / concurrency, 1);
        let chunks = files.par_chunks_mut(max_chunk_size);
        let chunk_count = chunks.len();
        debug_assert!(chunk_count <= concurrency);
        let sequential_replay_machine = chunks
            .enumerate()
            .map(|(index, chunk)| {
                let mut reader = LogItemBatchFileReader::new(read_block_size);
                let mut sequential_replay_machine = S::default();
                let file_count = chunk.len();
                for (i, f) in chunk.iter_mut().enumerate() {
                    let is_last_file = index == chunk_count - 1 && i == file_count - 1;
                    reader.open(
                        FileId { queue, seq: f.seq },
                        f.reader.take().unwrap(),
                        f.fd.file_size()?,
                    )?;
                    loop {
                        match reader.next() {
                            Ok(Some(item_batch)) => {
                                sequential_replay_machine
                                    .replay(item_batch, FileId { queue, seq: f.seq })?;
                            }
                            Ok(None) => break,
                            Err(e)
                                if recovery_mode == RecoveryMode::TolerateTailCorruption
                                    && is_last_file =>
                            {
                                warn!("The tail of raft log is corrupted but ignored: {}", e);
                                f.fd.truncate(reader.valid_offset())?;
                                break;
                            }
                            Err(e) if recovery_mode == RecoveryMode::TolerateAnyCorruption => {
                                warn!("File is corrupted but ignored: {}", e);
                                f.fd.truncate(reader.valid_offset())?;
                                break;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                Ok(sequential_replay_machine)
            })
            .try_reduce(
                S::default,
                |mut sequential_replay_machine_left, sequential_replay_machine_right| {
                    sequential_replay_machine_left.merge(sequential_replay_machine_right, queue)?;
                    Ok(sequential_replay_machine_left)
                },
            )?;
        debug!("Recover queue:{:?} finish.", queue);
        Ok(sequential_replay_machine)
    }

    fn append_bytes(
        &self,
        queue: LogQueue,
        ctx: &mut FilePipeLogWriteContext,
        content: &[u8],
    ) -> Result<FileBlockHandle> {
        self.mut_queue(queue).append(ctx, content)
    }

    fn get_queue(&self, queue: LogQueue) -> RwLockReadGuard<LogManager<B>> {
        match queue {
            LogQueue::Append => self.appender.read(),
            LogQueue::Rewrite => self.rewriter.read(),
        }
    }

    fn mut_queue(&self, queue: LogQueue) -> RwLockWriteGuard<LogManager<B>> {
        match queue {
            LogQueue::Append => self.appender.write(),
            LogQueue::Rewrite => self.rewriter.write(),
        }
    }
}

#[derive(PartialEq)]
enum Syncable {
    DontNeed,
    NeedSync,
    NeedRotate,
}

/// invariant: non-active file must be successfully synced.
pub struct FilePipeLogWriteContext {
    synced: bool,
    syncable: Syncable,
}

impl<B: FileBuilder> PipeLog for FilePipeLog<B> {
    type WriteContext = FilePipeLogWriteContext;

    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let fd = self.get_queue(handle.id.queue).get_fd(handle.id.seq)?;
        let mut reader = self
            .file_builder
            .build_reader(&build_file_path(&self.dir, handle.id), LogFile::new(fd))?;
        reader.seek(std::io::SeekFrom::Start(handle.offset))?;
        let mut buf = vec![0; handle.len];
        let size = reader.read(&mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }

    fn pre_write(&self, queue: LogQueue) -> Self::WriteContext {
        self.get_queue(queue).get_write_context()
    }

    fn post_write(
        &self,
        queue: LogQueue,
        mut ctx: Self::WriteContext,
        force_sync: bool,
    ) -> Result<()> {
        if ctx.syncable == Syncable::DontNeed && force_sync {
            ctx.syncable = Syncable::NeedSync;
        }
        if let Err(e) = match ctx.syncable {
            Syncable::DontNeed => Ok(()),
            Syncable::NeedSync => self.mut_queue(queue).sync(),
            Syncable::NeedRotate => self.mut_queue(queue).truncate_and_sync(),
        } {
            self.mut_queue(queue).active_file.rollback()?;
            return Err(Error::Fsync(ctx.synced, e.to_string()));
        }
        if ctx.syncable == Syncable::NeedRotate {
            self.mut_queue(queue).rotate()?;
        }
        Ok(())
    }

    fn append(
        &self,
        queue: LogQueue,
        ctx: &mut Self::WriteContext,
        bytes: &[u8],
    ) -> Result<FileBlockHandle> {
        let start = Instant::now();
        let block_handle = self.append_bytes(queue, ctx, bytes)?;
        match queue {
            LogQueue::Rewrite => {
                LOG_APPEND_TIME_HISTOGRAM_VEC
                    .rewrite
                    .observe(start.saturating_elapsed().as_secs_f64());
            }
            LogQueue::Append => {
                LOG_APPEND_TIME_HISTOGRAM_VEC
                    .append
                    .observe(start.saturating_elapsed().as_secs_f64());
            }
        }
        Ok(block_handle)
    }

    fn sync(&self, queue: LogQueue) -> Result<()> {
        if let Some(fd) = self.get_queue(queue).get_active_fd() {
            fd.sync()?;
        }
        Ok(())
    }

    fn file_span(&self, queue: LogQueue) -> (FileSeq, FileSeq) {
        let queue = self.get_queue(queue);
        (queue.first_file_seq, queue.active_file_seq)
    }

    fn total_size(&self, queue: LogQueue) -> usize {
        self.get_queue(queue).size()
    }

    fn new_log_file(&self, queue: LogQueue) -> Result<()> {
        self.mut_queue(queue).new_log_file()
    }

    fn purge_to(&self, file_id: FileId) -> Result<usize> {
        let mut manager = match file_id.queue {
            LogQueue::Append => self.appender.write(),
            LogQueue::Rewrite => self.rewriter.write(),
        };
        let purge_count = manager.purge_to(file_id.seq)?;
        drop(manager);

        let mut seq = file_id.seq - purge_count as u64;
        for i in 0..purge_count {
            let path = build_file_path(
                &self.dir,
                FileId {
                    queue: file_id.queue,
                    seq,
                },
            );
            if let Err(e) = fs::remove_file(&path) {
                warn!("Remove purged log file {:?} failed: {}", path, e);
                return Ok(i);
            }
            seq += 1;
        }
        Ok(purge_count)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::file_builder::DefaultFileBuilder;
    use crate::log_batch::LogItemBatch;
    use crate::util::ReadableSize;

    #[derive(Default)]
    struct BlackholeReplayMachine {}
    impl ReplayMachine for BlackholeReplayMachine {
        fn replay(&mut self, _: LogItemBatch, _: FileId) -> Result<()> {
            Ok(())
        }

        fn merge(&mut self, _: Self, _: LogQueue) -> Result<()> {
            Ok(())
        }
    }

    fn new_test_pipe_log(
        path: &str,
        bytes_per_sync: usize,
        rotate_size: usize,
    ) -> FilePipeLog<DefaultFileBuilder> {
        let cfg = Config {
            dir: path.to_owned(),
            bytes_per_sync: ReadableSize(bytes_per_sync as u64),
            target_file_size: ReadableSize(rotate_size as u64),
            ..Default::default()
        };

        FilePipeLog::open::<BlackholeReplayMachine>(&cfg, Arc::new(DefaultFileBuilder {}), vec![])
            .unwrap()
            .0
    }

    #[test]
    fn test_file_name() {
        let file_name: &str = "0000000000000123.raftlog";
        let file_id = FileId {
            queue: LogQueue::Append,
            seq: 123,
        };
        assert_eq!(parse_file_name(file_name).unwrap(), file_id,);
        assert_eq!(build_file_name(file_id), file_name);

        let file_name: &str = "0000000000000123.rewrite";
        let file_id = FileId {
            queue: LogQueue::Rewrite,
            seq: 123,
        };
        assert_eq!(parse_file_name(file_name).unwrap(), file_id,);
        assert_eq!(build_file_name(file_id), file_name);

        let invalid_file_name: &str = "123.log";
        assert!(parse_file_name(invalid_file_name).is_none());
        assert!(parse_file_name(invalid_file_name).is_none());
    }

    #[test]
    fn test_dir_lock() {
        let dir = Builder::new().prefix("test_lock").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let cfg = Config {
            dir: path.to_owned(),
            ..Default::default()
        };

        let _r1 = FilePipeLog::open::<BlackholeReplayMachine>(
            &cfg,
            Arc::new(DefaultFileBuilder {}),
            vec![],
        )
        .unwrap();

        // Only one thread can hold file lock
        let r2 = FilePipeLog::open::<BlackholeReplayMachine>(
            &cfg,
            Arc::new(DefaultFileBuilder {}),
            vec![],
        );

        assert!(format!("{}", r2.err().unwrap())
            .contains("maybe another instance is using this directory"));
    }

    fn test_pipe_log_impl(queue: LogQueue) {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 1024;
        let bytes_per_sync = 32 * 1024;
        let pipe_log = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.file_span(queue), (INIT_FILE_ID, INIT_FILE_ID));

        let header_size = LOG_FILE_HEADER_LEN as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        let mut ctx = pipe_log.pre_write(queue);
        let file_handle = pipe_log.append_bytes(queue, &mut ctx, &content).unwrap();
        pipe_log.post_write(queue, ctx, false).unwrap();
        assert_eq!(file_handle.id.seq, 1);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 2);

        let mut ctx = pipe_log.pre_write(queue);
        let file_handle = pipe_log.append_bytes(queue, &mut ctx, &content).unwrap();
        pipe_log.post_write(queue, ctx, false).unwrap();
        assert_eq!(file_handle.id.seq, 2);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 3);

        // purge file 1
        assert_eq!(pipe_log.purge_to(FileId { queue, seq: 2 }).unwrap(), 1);
        assert_eq!(pipe_log.file_span(queue).0, 2);

        // cannot purge active file
        assert!(pipe_log.purge_to(FileId { queue, seq: 4 }).is_err());

        // append position
        let s_content = b"short content".to_vec();
        let mut ctx = pipe_log.pre_write(queue);
        let file_handle = pipe_log.append_bytes(queue, &mut ctx, &s_content).unwrap();
        pipe_log.post_write(queue, ctx, false).unwrap();
        assert_eq!(file_handle.id.seq, 3);
        assert_eq!(file_handle.offset, header_size);

        let mut ctx = pipe_log.pre_write(queue);
        let file_handle = pipe_log.append_bytes(queue, &mut ctx, &s_content).unwrap();
        pipe_log.post_write(queue, ctx, false).unwrap();
        assert_eq!(file_handle.id.seq, 3);
        assert_eq!(
            file_handle.offset,
            header_size as u64 + s_content.len() as u64
        );

        let content_readed = pipe_log
            .read_bytes(FileBlockHandle {
                id: FileId { queue, seq: 3 },
                offset: header_size as u64,
                len: s_content.len(),
            })
            .unwrap();
        assert_eq!(content_readed, s_content);

        // leave only 1 file to truncate
        assert!(pipe_log.purge_to(FileId { queue, seq: 3 }).is_ok());
        assert_eq!(pipe_log.file_span(queue), (3, 3));
    }

    #[test]
    fn test_pipe_log_append() {
        test_pipe_log_impl(LogQueue::Append)
    }

    #[test]
    fn test_pipe_log_rewrite() {
        test_pipe_log_impl(LogQueue::Rewrite)
    }
}
