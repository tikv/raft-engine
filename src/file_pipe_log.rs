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
use crate::log_batch::LogBatch;
use crate::log_file::{LogFd, LogFile};
use crate::metrics::*;
use crate::pipe_log::{FileId, LogQueue, PipeLog, SequentialReplayMachine};
use crate::reader::LogItemBatchFileReader;
use crate::util::InstantExt;
use crate::{Error, Result};

const LOG_NUM_LEN: usize = 16;
const LOG_APPEND_SUFFIX: &str = ".raftlog";
const LOG_REWRITE_SUFFIX: &str = ".rewrite";

const INIT_FILE_ID: u64 = 1;

const DEFAULT_FILES_COUNT: usize = 32;
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

fn build_file_name(queue: LogQueue, file_id: FileId) -> String {
    match queue {
        LogQueue::Append => format!(
            "{:0width$}{}",
            file_id,
            LOG_APPEND_SUFFIX,
            width = LOG_NUM_LEN
        ),
        LogQueue::Rewrite => format!(
            "{:0width$}{}",
            file_id,
            LOG_REWRITE_SUFFIX,
            width = LOG_NUM_LEN
        ),
    }
}

fn parse_file_name(file_name: &str) -> Result<(LogQueue, FileId)> {
    if file_name.len() > LOG_NUM_LEN {
        if let Ok(num) = file_name[..LOG_NUM_LEN].parse::<u64>() {
            if file_name.ends_with(LOG_APPEND_SUFFIX) {
                return Ok((LogQueue::Append, num.into()));
            } else if file_name.ends_with(LOG_REWRITE_SUFFIX) {
                return Ok((LogQueue::Rewrite, num.into()));
            }
        }
    }
    Err(Error::ParseFileName(file_name.to_owned()))
}

fn build_file_path<P: AsRef<Path>>(dir: P, queue: LogQueue, file_id: FileId) -> PathBuf {
    let mut path = PathBuf::from(dir.as_ref());
    path.push(build_file_name(queue, file_id));
    path
}

const LOG_FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
pub const LOG_FILE_HEADER_LEN: usize = LOG_FILE_MAGIC_HEADER.len() + Version::len();

#[derive(Clone, Copy, FromPrimitive, ToPrimitive)]
enum Version {
    V1 = 1,
}

impl Version {
    const fn current() -> Self {
        Self::V1
    }

    const fn len() -> usize {
        8
    }
}

pub struct LogFileHeader {}

impl LogFileHeader {
    pub fn new() -> Self {
        Self {}
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
        if Version::from_u64(v).is_none() {
            return Err(Error::Corruption(format!(
                "unrecognized log file version: {}",
                v
            )));
        }
        Ok(Self {})
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
        buf.encode_u64(Version::current().to_u64().unwrap())?;
        Ok(())
    }
}

struct FileToRecover<R: Seek + Read> {
    file_id: FileId,
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
            self.fd.sync()?;
            self.capacity = self.written;
        }
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.seek(std::io::SeekFrom::Start(0))?;
        self.written = 0;
        let mut buf = Vec::with_capacity(LOG_FILE_HEADER_LEN);
        LogFileHeader::new().encode(&mut buf)?;
        self.write(&buf, true, 0)
    }

    fn write(&mut self, buf: &[u8], sync: bool, target_file_size: usize) -> Result<()> {
        if self.written + buf.len() > self.capacity {
            // Use fallocate to pre-allocate disk space for active file.
            let alloc = std::cmp::max(
                self.written + buf.len() - self.capacity,
                std::cmp::min(
                    FILE_ALLOCATE_SIZE,
                    target_file_size.saturating_sub(self.capacity),
                ),
            );
            self.fd.allocate(self.capacity, alloc)?;
            self.capacity += alloc;
        }
        self.writer.write_all(buf)?;
        self.written += buf.len();
        if sync {
            self.last_sync = self.written;
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

    pub first_file_id: FileId,
    pub active_file_id: FileId,

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
        let mut first_file_id = FileId::default();
        let mut active_file_id = FileId::default();
        let mut all_files =
            VecDeque::with_capacity(std::cmp::max(DEFAULT_FILES_COUNT, files.len()));
        let mut create_file = false;
        for f in files.into_iter() {
            if !first_file_id.valid() {
                first_file_id = f.file_id;
            }
            all_files.push_back(f.fd);
            active_file_id = f.file_id;
            for listener in &listeners {
                listener.post_new_log_file(queue, f.file_id);
            }
        }
        if !first_file_id.valid() {
            first_file_id = INIT_FILE_ID.into();
            active_file_id = first_file_id;
            create_file = true;
            let fd = Arc::new(LogFd::create(&build_file_path(
                &cfg.dir,
                queue,
                first_file_id,
            ))?);
            all_files.push_back(fd);
            for listener in &listeners {
                listener.post_new_log_file(queue, first_file_id);
            }
        }
        let active_fd = all_files.back().unwrap().clone();
        let active_file = ActiveFile::open(
            active_fd.clone(),
            file_builder.build_writer(
                &build_file_path(&cfg.dir, queue, active_file_id),
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

            first_file_id,
            active_file_id,

            all_files,
            active_file,
        };
        manager.update_metrics();
        Ok(manager)
    }

    fn new_log_file(&mut self) -> Result<()> {
        if self.active_file_id.valid() {
            // Necessary to truncate extra zeros from fallocate().
            self.truncate_active_log()?;
        }
        self.active_file_id = if self.active_file_id.valid() {
            self.active_file_id.forward(1)
        } else {
            self.first_file_id
        };

        let path = build_file_path(&self.dir, self.queue, self.active_file_id);
        let fd = Arc::new(LogFd::create(&path)?);
        self.all_files.push_back(fd.clone());
        self.active_file.rotate(
            fd.clone(),
            self.file_builder
                .build_writer(&path, LogFile::new(fd), true /*create*/)?,
        )?;
        self.sync_dir()?;

        for listener in &self.listeners {
            listener.post_new_log_file(self.queue, self.active_file_id);
        }

        self.update_metrics();

        Ok(())
    }

    fn sync_dir(&self) -> Result<()> {
        let path = PathBuf::from(&self.dir);
        std::fs::File::open(path).and_then(|d| d.sync_all())?;
        Ok(())
    }

    fn truncate_active_log(&mut self) -> Result<()> {
        self.active_file.truncate()
    }

    fn get_fd(&self, file_id: FileId) -> Result<Arc<LogFd>> {
        if file_id < self.first_file_id || file_id > self.active_file_id {
            return Err(Error::Io(IoError::new(
                IoErrorKind::NotFound,
                "file_id out of range",
            )));
        }
        Ok(self.all_files[file_id.step_after(&self.first_file_id).unwrap()].clone())
    }

    fn get_active_fd(&self) -> Option<Arc<LogFd>> {
        self.all_files.back().cloned()
    }

    fn purge_to(&mut self, file_id: FileId) -> Result<usize> {
        if file_id > self.active_file_id {
            return Err(box_err!("Purge active or newer files"));
        }
        let end_offset = file_id.step_after(&self.first_file_id).unwrap();
        self.all_files.drain(..end_offset);
        self.first_file_id = file_id;
        self.update_metrics();
        Ok(end_offset)
    }

    fn append(&mut self, content: &[u8], sync: &mut bool) -> Result<(FileId, u64, Arc<LogFd>)> {
        if self.active_file.written >= self.rotate_size {
            self.new_log_file()?;
        }
        if self.active_file.since_last_sync() >= self.bytes_per_sync {
            *sync = true;
        }
        let offset = self.active_file.written as u64;
        self.active_file.write(content, *sync, self.rotate_size)?;
        Ok((self.active_file_id, offset, self.active_file.fd.clone()))
    }

    fn update_metrics(&self) {
        match self.queue {
            LogQueue::Append => LOG_FILE_COUNT.append.set(self.all_files.len() as i64),
            LogQueue::Rewrite => LOG_FILE_COUNT.rewrite.set(self.all_files.len() as i64),
        }
    }

    fn size(&self) -> usize {
        self.active_file_id.step_after(&self.first_file_id).unwrap() * self.rotate_size
            + self.active_file.written
    }
}

#[derive(Clone)]
pub struct FilePipeLog<B: FileBuilder> {
    dir: String,
    rotate_size: usize,
    compression_threshold: usize,

    appender: Arc<RwLock<LogManager<B>>>,
    rewriter: Arc<RwLock<LogManager<B>>>,
    file_builder: Arc<B>,
    listeners: Vec<Arc<dyn EventListener>>,

    db_file: Arc<File>,
}

impl<B: FileBuilder> FilePipeLog<B> {
    pub fn open<S: SequentialReplayMachine>(
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
        let f = File::create(lock_path.as_path());
        if f.is_err() {
            return Err(box_err!("failed to create lock at {}: {}", lock_path.display(), f.err().unwrap()));
        }

        let file = f.unwrap();
        if file.try_lock_exclusive().is_err() {
            return Err(box_err!("lock {} failed, maybe another instance is using this directory.",
                lock_path.display()));
        }


        let (mut min_append_id, mut max_append_id) = (Default::default(), Default::default());
        let (mut min_rewrite_id, mut max_rewrite_id) = (Default::default(), Default::default());
        fs::read_dir(path)?.for_each(|e| {
            if let Ok(e) = e {
                match parse_file_name(e.file_name().to_str().unwrap()) {
                    Ok((LogQueue::Append, file_id)) => {
                        min_append_id = FileId::min(min_append_id, file_id);
                        max_append_id = FileId::max(max_append_id, file_id);
                    }
                    Ok((LogQueue::Rewrite, file_id)) => {
                        min_rewrite_id = FileId::min(min_rewrite_id, file_id);
                        max_rewrite_id = FileId::max(max_rewrite_id, file_id);
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
            if min_id.valid() {
                for i in min_id.as_u64()..=max_id.as_u64() {
                    let file_id = i.into();
                    let path = build_file_path(&cfg.dir, queue, file_id);
                    let fd = Arc::new(LogFd::open(&path)?);
                    files.push(FileToRecover {
                        file_id,
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
            listeners.clone(),
            LogQueue::Rewrite,
            rewrite_files,
        )?));

        Ok((
            FilePipeLog {
                dir: cfg.dir.clone(),
                rotate_size: cfg.target_file_size.0 as usize,
                compression_threshold: cfg.batch_compression_threshold.0 as usize,
                appender,
                rewriter,
                file_builder,
                listeners,
                db_file: Arc::new(file)
            },
            append_sequential_replay_machine,
            rewrite_sequential_replay_machine,
        ))
    }

    fn recover<S: SequentialReplayMachine>(
        recovery_mode: RecoveryMode,
        threads: usize,
        read_block_size: usize,
        append_files: &mut [FileToRecover<B::Reader<LogFile>>],
        rewrite_files: &mut [FileToRecover<B::Reader<LogFile>>],
    ) -> Result<(S, S)> {
        let (append_concurrency, rewrite_concurrency) =
            match (append_files.len(), rewrite_files.len()) {
                (0, 0) => (0, 0),
                (0, _) => (0, threads),
                (_, 0) => (threads, 0),
                (a, b) => {
                    let a_threads = std::cmp::max(1, threads * a / (a + b));
                    let b_threads = std::cmp::max(1, threads.saturating_sub(a_threads));
                    (a_threads, b_threads)
                }
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

    fn recover_queue<S: SequentialReplayMachine>(
        queue: LogQueue,
        recovery_mode: RecoveryMode,
        concurrency: usize,
        read_block_size: usize,
        files: &mut [FileToRecover<B::Reader<LogFile>>],
    ) -> Result<S> {
        if concurrency == 0 {
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
                    let is_last = index == chunk_count - 1 && i == file_count - 1;
                    reader.open(f.reader.take().unwrap(), f.fd.file_size()?)?;
                    loop {
                        match reader.next() {
                            Ok(Some(mut item_batch)) => {
                                item_batch.set_position(queue, f.file_id, None);
                                sequential_replay_machine.replay(item_batch, queue, f.file_id)?;
                            }
                            Ok(None) => break,
                            Err(e)
                                if recovery_mode == RecoveryMode::TolerateCorruptedTailRecords
                                    && is_last =>
                            {
                                warn!("The tail of raft log is corrupted but ignored: {}", e);
                                f.fd.truncate(reader.valid_offset())?;
                                break;
                            }
                            Err(e) => return Err(e),
                        }
                    }
                }
                debug!("Recover queue:{:?} finish.", queue);
                Ok(sequential_replay_machine)
            })
            .try_reduce(
                S::default,
                |mut sequential_replay_machine_left, sequential_replay_machine_right| {
                    sequential_replay_machine_left.merge(sequential_replay_machine_right, queue)?;
                    Ok(sequential_replay_machine_left)
                },
            )?;
        Ok(sequential_replay_machine)
    }

    fn append_bytes(
        &self,
        queue: LogQueue,
        content: &[u8],
        sync: &mut bool,
    ) -> Result<(FileId, u64)> {
        let (file_id, offset, fd) = self.mut_queue(queue).append(content, sync)?;
        for listener in &self.listeners {
            listener.on_append_log_file(queue, file_id, content.len());
        }
        if *sync {
            let start = Instant::now();
            fd.sync()?;
            LOG_SYNC_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        }

        Ok((file_id, offset))
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

impl<B: FileBuilder> PipeLog for FilePipeLog<B> {
    fn file_size(&self, queue: LogQueue, file_id: FileId) -> Result<u64> {
        self.get_queue(queue)
            .get_fd(file_id)
            .map(|fd| fd.file_size().unwrap() as u64)
    }

    fn total_size(&self, queue: LogQueue) -> usize {
        self.get_queue(queue).size()
    }

    fn read_bytes(
        &self,
        queue: LogQueue,
        file_id: FileId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        let fd = self.get_queue(queue).get_fd(file_id)?;
        let mut reader = self.file_builder.build_reader(
            &build_file_path(&self.dir, queue, file_id),
            LogFile::new(fd),
        )?;
        reader.seek(std::io::SeekFrom::Start(offset))?;
        let mut buf = vec![0; len as usize];
        let size = reader.read(&mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }

    fn append(
        &self,
        queue: LogQueue,
        batch: &mut LogBatch,
        mut sync: bool,
    ) -> Result<(FileId, usize)> {
        let bytes = batch.encoded_bytes(self.compression_threshold)?;
        let start = Instant::now();
        let (file_id, offset) = self.append_bytes(queue, bytes, &mut sync)?;
        let len = bytes.len();
        // set fields based on the log file
        batch.set_position(queue, file_id, Some(offset));
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
        Ok((file_id, len))
    }

    fn sync(&self, queue: LogQueue) -> Result<()> {
        if let Some(fd) = self.get_queue(queue).get_active_fd() {
            fd.sync()?;
        }
        Ok(())
    }

    fn active_file_id(&self, queue: LogQueue) -> FileId {
        self.get_queue(queue).active_file_id
    }

    fn first_file_id(&self, queue: LogQueue) -> FileId {
        self.get_queue(queue).first_file_id
    }

    fn file_at(&self, queue: LogQueue, position: f64) -> FileId {
        // TODO: sanitize position
        let cur_size = self.total_size(queue);
        let count = (cur_size as f64 * position) as usize / self.rotate_size;
        let file_num = self.get_queue(queue).first_file_id.forward(count);
        assert!(file_num <= self.active_file_id(queue));
        file_num
    }

    fn new_log_file(&self, queue: LogQueue) -> Result<()> {
        self.mut_queue(queue).new_log_file()
    }

    fn purge_to(&self, queue: LogQueue, file_id: FileId) -> Result<usize> {
        let mut manager = match queue {
            LogQueue::Append => self.appender.write(),
            LogQueue::Rewrite => self.rewriter.write(),
        };
        let purge_count = manager.purge_to(file_id)?;
        drop(manager);

        let mut cur_file_id = file_id.backward(purge_count);
        for i in 0..purge_count {
            let path = build_file_path(&self.dir, queue, cur_file_id);
            if let Err(e) = fs::remove_file(&path) {
                warn!("Remove purged log file {:?} fail: {}", path, e);
                return Ok(i);
            }
            cur_file_id = cur_file_id.forward(1);
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
    struct BlackholeSequentialReplayMachine {}
    impl SequentialReplayMachine for BlackholeSequentialReplayMachine {
        fn replay(&mut self, _: LogItemBatch, _: LogQueue, _: FileId) -> Result<()> {
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
        let mut cfg = Config::default();
        cfg.dir = path.to_owned();
        cfg.bytes_per_sync = ReadableSize(bytes_per_sync as u64);
        cfg.target_file_size = ReadableSize(rotate_size as u64);

        FilePipeLog::open::<BlackholeSequentialReplayMachine>(
            &cfg,
            Arc::new(DefaultFileBuilder {}),
            vec![],
        )
        .unwrap()
        .0
    }

    #[test]
    fn test_file_name() {
        let file_name: &str = "0000000000000123.raftlog";
        assert_eq!(
            parse_file_name(file_name).unwrap(),
            (LogQueue::Append, 123.into())
        );
        assert_eq!(build_file_name(LogQueue::Append, 123.into()), file_name);

        let file_name: &str = "0000000000000123.rewrite";
        assert_eq!(
            parse_file_name(file_name).unwrap(),
            (LogQueue::Rewrite, 123.into())
        );
        assert_eq!(build_file_name(LogQueue::Rewrite, 123.into()), file_name);

        let invalid_file_name: &str = "123.log";
        assert!(parse_file_name(invalid_file_name).is_err());
        assert!(parse_file_name(invalid_file_name).is_err());
    }

    #[test]
    fn test_dir_lock() {
        let dir = Builder::new().prefix("test_lock").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let mut cfg = Config::default();
        cfg.dir = path.to_owned();
        let r1 = FilePipeLog::open::<BlackholeSequentialReplayMachine>(
            &cfg,
            Arc::new(DefaultFileBuilder {}),
            vec![],
        );
        assert_eq!(r1.is_ok(), true);

        // Only one thread can hold file lock
        let r2 = FilePipeLog::open::<BlackholeSequentialReplayMachine>(
            &cfg,
            Arc::new(DefaultFileBuilder {}),
            vec![],
        );
        assert_eq!(r2.is_err(), true);
        assert_eq!(format!("{}", r2.err().unwrap()).contains("maybe another instance is using this directory"), true);
    }

    fn test_pipe_log_impl(queue: LogQueue) {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 1024;
        let bytes_per_sync = 32 * 1024;
        let pipe_log = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.first_file_id(queue), INIT_FILE_ID.into());
        assert_eq!(pipe_log.active_file_id(queue), INIT_FILE_ID.into());

        let header_size = LOG_FILE_HEADER_LEN as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        let (file_num, offset) = pipe_log.append_bytes(queue, &content, &mut false).unwrap();
        assert_eq!(file_num, 1.into());
        assert_eq!(offset, header_size);
        assert_eq!(pipe_log.active_file_id(queue), 1.into());

        let (file_num, offset) = pipe_log.append_bytes(queue, &content, &mut false).unwrap();
        assert_eq!(file_num, 2.into());
        assert_eq!(offset, header_size);
        assert_eq!(pipe_log.active_file_id(queue), 2.into());

        // purge file 1
        assert_eq!(pipe_log.purge_to(queue, 2.into()).unwrap(), 1);
        assert_eq!(pipe_log.first_file_id(queue), 2.into());

        // cannot purge active file
        assert!(pipe_log.purge_to(queue, 3.into()).is_err());

        // append position
        let s_content = b"short content".to_vec();
        let (file_num, offset) = pipe_log
            .append_bytes(queue, &s_content, &mut false)
            .unwrap();
        assert_eq!(file_num, 3.into());
        assert_eq!(offset, header_size);

        let (file_num, offset) = pipe_log
            .append_bytes(queue, &s_content, &mut false)
            .unwrap();
        assert_eq!(file_num, 3.into());
        assert_eq!(offset, header_size as u64 + s_content.len() as u64);

        let content_readed = pipe_log
            .read_bytes(queue, 3.into(), header_size as u64, s_content.len() as u64)
            .unwrap();
        assert_eq!(content_readed, s_content);

        // leave only 1 file to truncate
        assert!(pipe_log.purge_to(queue, 3.into()).is_ok());
        assert_eq!(pipe_log.first_file_id(queue), 3.into());
        assert_eq!(pipe_log.active_file_id(queue), 3.into());
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
