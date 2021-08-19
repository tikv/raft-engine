// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs;
use std::io::{Error as IoError, ErrorKind as IoErrorKind};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use fail::fail_point;
use log::{debug, info, warn};
use nix::errno::Errno;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{close, fsync, ftruncate, lseek, Whence};
use nix::NixPath;
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::codec::{self, NumberEncoder};
use crate::config::{Config, RecoveryMode};
use crate::event_listener::EventListener;
use crate::log_batch::{LogBatch, LogItemBatch, MessageExt};
use crate::metrics::*;
use crate::pipe_log::{FileId, LogQueue, PipeLog};
use crate::reader::LogItemBatchFileReader;
use crate::util::InstantExt;
use crate::{Error, Result};

const LOG_SUFFIX: &str = ".raftlog";
const LOG_NUM_LEN: usize = 16;
const LOG_NAME_LEN: usize = LOG_NUM_LEN + LOG_SUFFIX.len();

const REWRITE_SUFFIX: &str = ".rewrite";
const REWRITE_NUM_LEN: usize = 8;
const REWRITE_NAME_LEN: usize = REWRITE_NUM_LEN + REWRITE_SUFFIX.len();

const INIT_FILE_NUM: u64 = 1;

pub const FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
const DEFAULT_FILES_COUNT: usize = 32;
#[cfg(target_os = "linux")]
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

#[derive(Clone, Copy)]
pub enum Version {
    V1 = 1,
}

impl Version {
    fn current() -> Self {
        Self::V1
    }

    fn len() -> usize {
        8
    }

    fn as_u64(&self) -> u64 {
        *self as u64
    }

    fn from_u64(v: u64) -> Option<Self> {
        match v {
            1 => Some(Self::V1),
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct LogFd(RawFd);

impl LogFd {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let flags = OFlag::O_RDWR;
        let mode = Mode::S_IRWXU;
        fail_point!("fadvise-dontneed", |_| {
            let fd = fcntl::open(path, flags, mode).map_err(|e| parse_nix_error(e, "open"))?;
            let fd = LogFd(fd);
            fd.read_header()?;
            #[cfg(target_os = "linux")]
            unsafe {
                extern crate libc;
                libc::posix_fadvise64(fd.0, 0, fd.file_size()? as i64, libc::POSIX_FADV_DONTNEED);
            }
            Ok(fd)
        });
        let fd = fcntl::open(path, flags, mode).map_err(|e| parse_nix_error(e, "open"))?;
        let fd = LogFd(fd);
        fd.read_header()?;
        Ok(fd)
    }

    pub fn create<P: ?Sized + NixPath>(path: &P) -> Result<Self> {
        let flags = OFlag::O_RDWR | OFlag::O_CREAT;
        let mode = Mode::S_IRWXU;
        let fd = fcntl::open(path, flags, mode).map_err(|e| parse_nix_error(e, "open"))?;
        let fd = LogFd(fd);
        fd.write_header()?;
        Ok(fd)
    }

    pub fn close(&self) -> Result<()> {
        close(self.0).map_err(|e| parse_nix_error(e, "close"))
    }
    pub fn sync(&self) -> Result<()> {
        let start = Instant::now();
        let res = fsync(self.0).map_err(|e| parse_nix_error(e, "fsync"));
        LOG_SYNC_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        res
    }

    pub fn read(&self, mut offset: i64, len: usize) -> Result<Vec<u8>> {
        let mut result = vec![0; len as usize];
        let mut readed = 0;
        while readed < len {
            let bytes = match pread(self.0, &mut result[readed..], offset) {
                Ok(bytes) => bytes,
                Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
                Err(e) => return Err(parse_nix_error(e, "pread")),
            };
            readed += bytes;
            offset += bytes as i64;
        }
        Ok(result)
    }

    /// Read bytes to the given buffer. The given buffer will be resized to `len`.
    /// If the buffer is not empty, the existing range will be skipped.
    /// Returns the number of bytes actually read.
    pub fn read_to(&self, buffer: &mut Vec<u8>, mut offset: i64, len: usize) -> Result<usize> {
        let mut readed = buffer.len();
        offset += readed as i64;
        let mut read_bytes = 0;
        if buffer.len() != len {
            buffer.resize(len, 0);
        }
        while readed < len {
            let bytes = match pread(self.0, &mut buffer[readed..], offset) {
                Ok(bytes) => bytes,
                Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
                Err(e) => return Err(parse_nix_error(e, "pread")),
            };
            readed += bytes;
            offset += bytes as i64;
            read_bytes += bytes;
        }
        Ok(read_bytes)
    }

    pub fn write(&self, mut offset: i64, content: &[u8]) -> Result<()> {
        let mut written = 0;
        while written < content.len() {
            let bytes = match pwrite(self.0, &content[written..], offset) {
                Ok(bytes) => bytes,
                Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
                Err(e) => return Err(parse_nix_error(e, "pwrite")),
            };
            written += bytes;
            offset += bytes as i64;
        }
        Ok(())
    }

    pub fn file_size(&self) -> Result<usize> {
        lseek(self.0, 0, Whence::SeekEnd)
            .map(|n| n as usize)
            .map_err(|e| parse_nix_error(e, "lseek"))
    }

    fn truncate(&self, offset: i64) -> Result<i64> {
        if FILE_MAGIC_HEADER.len() + Version::len() > offset as usize {
            Ok((FILE_MAGIC_HEADER.len() + Version::len()) as i64)
        } else {
            ftruncate(self.0, offset).map_err(|e| parse_nix_error(e, "ftruncate"))?;
            Ok(offset)
        }
    }

    fn read_header(&self) -> Result<()> {
        if self.file_size()? < FILE_MAGIC_HEADER.len() + Version::len() {
            return Err(Error::Corruption("log file too short".to_owned()));
        }
        let buf = self.read(0, FILE_MAGIC_HEADER.len() + Version::len())?;
        if !buf.starts_with(FILE_MAGIC_HEADER) {
            return Err(Error::Corruption(
                "log file magic header mismatch".to_owned(),
            ));
        }
        let v = codec::decode_u64(&mut &buf[FILE_MAGIC_HEADER.len()..])?;
        if Version::from_u64(v).is_none() {
            return Err(Error::Corruption(format!(
                "unrecognized log file version: {}",
                v
            )));
        }
        Ok(())
    }

    fn write_header(&self) -> Result<usize> {
        let len = FILE_MAGIC_HEADER.len() + Version::len();
        let mut header = Vec::with_capacity(len);
        header.extend_from_slice(FILE_MAGIC_HEADER);
        header.encode_u64(Version::current().as_u64()).unwrap();
        self.write(0, &header)?;
        Ok(len)
    }
}

impl Drop for LogFd {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            warn!("Drop LogFd fail: {}", e);
        }
    }
}

fn build_file_name(queue: LogQueue, file_id: FileId) -> String {
    match queue {
        LogQueue::Append => format!("{:0width$}{}", file_id, LOG_SUFFIX, width = LOG_NUM_LEN),
        LogQueue::Rewrite => format!(
            "{:0width$}{}",
            file_id,
            REWRITE_SUFFIX,
            width = REWRITE_NUM_LEN
        ),
    }
}

fn parse_file_name(file_name: &str) -> Result<(LogQueue, FileId)> {
    if file_name.ends_with(LOG_SUFFIX) && file_name.len() == LOG_NAME_LEN {
        if let Ok(num) = file_name[..LOG_NUM_LEN].parse::<u64>() {
            return Ok((LogQueue::Append, num.into()));
        }
    } else if file_name.ends_with(REWRITE_SUFFIX) && file_name.len() == REWRITE_NAME_LEN {
        if let Ok(num) = file_name[..REWRITE_NUM_LEN].parse::<u64>() {
            return Ok((LogQueue::Rewrite, num.into()));
        }
    }
    Err(Error::ParseFileName(file_name.to_owned()))
}

struct LogManager {
    queue: LogQueue,
    dir: String,
    rotate_size: usize,
    bytes_per_sync: usize,
    listeners: Vec<Arc<dyn EventListener>>,

    pub first_file_id: FileId,
    pub active_file_id: FileId,

    pub active_log_size: usize,
    pub active_log_capacity: usize,
    pub last_sync_size: usize,

    pub all_files: VecDeque<Arc<LogFd>>,
}

impl LogManager {
    fn new(cfg: &Config, queue: LogQueue) -> Self {
        Self {
            queue,
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            listeners: vec![],

            first_file_id: INIT_FILE_NUM.into(),
            active_file_id: Default::default(),
            active_log_size: 0,
            active_log_capacity: 0,
            last_sync_size: 0,
            all_files: VecDeque::with_capacity(DEFAULT_FILES_COUNT),
        }
    }

    fn open_files(
        &mut self,
        logs: Vec<String>,
        min_file_id: FileId,
        max_file_id: FileId,
    ) -> Result<()> {
        if !logs.is_empty() {
            self.first_file_id = min_file_id;
            self.active_file_id = max_file_id;

            for (i, file_name) in logs[0..logs.len() - 1].iter().enumerate() {
                let mut path = PathBuf::from(&self.dir);
                path.push(file_name);
                let fd = Arc::new(LogFd::open(&path)?);
                self.all_files.push_back(fd);
                for listener in &self.listeners {
                    listener.post_new_log_file(self.queue, self.first_file_id.forward(i));
                }
            }

            let mut path = PathBuf::from(&self.dir);
            path.push(logs.last().unwrap());
            let fd = Arc::new(LogFd::open(&path)?);
            self.active_log_size = fd.file_size()?;
            self.active_log_capacity = self.active_log_size;
            self.last_sync_size = self.active_log_size;
            self.all_files.push_back(fd);

            for listener in &self.listeners {
                listener.post_new_log_file(self.queue, self.active_file_id);
            }
        }

        self.update_metrics();

        Ok(())
    }

    fn new_log_file(&mut self) -> Result<()> {
        if self.active_file_id.valid() {
            self.truncate_active_log(None)?;
        }
        self.active_file_id = if self.active_file_id.valid() {
            self.active_file_id.forward(1)
        } else {
            self.first_file_id
        };

        let mut path = PathBuf::from(&self.dir);
        path.push(build_file_name(self.queue, self.active_file_id));
        let fd = Arc::new(LogFd::create(&path)?);
        let bytes = fd.file_size()?;

        self.active_log_size = bytes;
        self.active_log_capacity = bytes;
        self.last_sync_size = 0;
        self.all_files.push_back(fd);
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

    fn truncate_active_log(&mut self, offset: Option<usize>) -> Result<()> {
        let mut offset = offset.unwrap_or(self.active_log_size);
        if offset > self.active_log_size {
            let io_error = IoError::new(IoErrorKind::UnexpectedEof, "truncate");
            return Err(Error::Io(io_error));
        }
        let active_fd = self.get_active_fd().unwrap();
        offset = active_fd.truncate(offset as i64)? as usize;
        active_fd.sync()?;
        self.active_log_size = offset;
        self.active_log_capacity = offset;
        self.last_sync_size = self.active_log_size;
        Ok(())
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

    fn reach_sync_limit(&self) -> bool {
        self.active_log_size - self.last_sync_size >= self.bytes_per_sync
    }

    fn on_append(
        &mut self,
        content_len: usize,
        sync: &mut bool,
    ) -> Result<(FileId, u64, Arc<LogFd>)> {
        if self.active_log_size >= self.rotate_size {
            self.new_log_file()?;
        }

        let active_file_id = self.active_file_id;
        let active_log_size = self.active_log_size;
        let fd = self.get_active_fd().unwrap();

        self.active_log_size += content_len;

        #[cfg(target_os = "linux")]
        if self.active_log_size > self.active_log_capacity {
            // Use fallocate to pre-allocate disk space for active file.
            let reserve = self.active_log_size - self.active_log_capacity;
            let alloc_size = std::cmp::max(reserve, FILE_ALLOCATE_SIZE);
            fcntl::fallocate(
                fd.0,
                fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE,
                self.active_log_capacity as _,
                alloc_size as _,
            )
            .map_err(|e| parse_nix_error(e, "fallocate"))?;
            self.active_log_capacity += alloc_size;
        }

        if *sync || self.reach_sync_limit() {
            self.last_sync_size = self.active_log_size;
            *sync = true;
        }
        Ok((active_file_id, active_log_size as u64, fd))
    }

    fn update_metrics(&self) {
        match self.queue {
            LogQueue::Append => LOG_FILE_COUNT.append.set(self.all_files.len() as i64),
            LogQueue::Rewrite => LOG_FILE_COUNT.rewrite.set(self.all_files.len() as i64),
        }
    }
}

#[derive(Clone)]
pub struct FilePipeLog {
    dir: String,
    rotate_size: usize,
    bytes_per_sync: usize,
    compression_threshold: usize,
    recovery_read_block_size: usize,

    appender: Arc<RwLock<LogManager>>,
    rewriter: Arc<RwLock<LogManager>>,
    listeners: Vec<Arc<dyn EventListener>>,
}

impl FilePipeLog {
    fn new(cfg: &Config, listeners: Vec<Arc<dyn EventListener>>) -> FilePipeLog {
        let appender = Arc::new(RwLock::new(LogManager::new(cfg, LogQueue::Append)));
        let rewriter = Arc::new(RwLock::new(LogManager::new(cfg, LogQueue::Rewrite)));
        appender.write().listeners = listeners.clone();
        rewriter.write().listeners = listeners.clone();
        FilePipeLog {
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            compression_threshold: cfg.batch_compression_threshold.0 as usize,
            appender,
            rewriter,
            listeners,
            recovery_read_block_size: cfg.recovery_read_block_size.0 as usize,
        }
    }

    pub fn open(cfg: &Config, listeners: Vec<Arc<dyn EventListener>>) -> Result<FilePipeLog> {
        let path = Path::new(&cfg.dir);
        if !path.exists() {
            info!("Create raft log directory: {}", &cfg.dir);
            fs::create_dir(&cfg.dir)?;
        }
        if !path.is_dir() {
            return Err(box_err!("Not directory: {}", &cfg.dir));
        }

        let pipe_log = FilePipeLog::new(cfg, listeners);

        let (mut min_file_id, mut max_file_id) = (Default::default(), Default::default());
        let (mut min_rewrite_num, mut max_rewrite_num) = (Default::default(), Default::default());
        let (mut log_files, mut rewrite_files) = (vec![], vec![]);
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();
            if !file_path.is_file() {
                continue;
            }

            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            match parse_file_name(file_name)? {
                (LogQueue::Append, file_id) => {
                    min_file_id = FileId::min(min_file_id, file_id);
                    max_file_id = FileId::max(max_file_id, file_id);
                    log_files.push(file_name.to_string());
                }
                (LogQueue::Rewrite, file_id) => {
                    min_rewrite_num = FileId::min(min_rewrite_num, file_id);
                    max_rewrite_num = FileId::max(max_rewrite_num, file_id);
                    rewrite_files.push(file_name.to_string());
                }
            }
        }

        if log_files.is_empty() {
            // New created pipe log, open the first log file.
            pipe_log.mut_queue(LogQueue::Append).new_log_file()?;
            pipe_log.mut_queue(LogQueue::Rewrite).new_log_file()?;
            return Ok(pipe_log);
        }

        log_files.sort();
        rewrite_files.sort();
        if max_file_id.step_after(&min_file_id).is_none() {
            println!(
                "min = {}, max = {}, log files len = {}",
                min_file_id,
                max_file_id,
                log_files.len()
            );
        }
        if log_files.len() != max_file_id.step_after(&min_file_id).unwrap() + 1 {
            return Err(Error::Corruption(
                "Corruption occurs on log files".to_owned(),
            ));
        }
        if !rewrite_files.is_empty()
            && rewrite_files.len() != max_rewrite_num.step_after(&min_rewrite_num).unwrap() + 1
        {
            return Err(Error::Corruption(
                "Corruption occurs on rewrite files".to_owned(),
            ));
        }

        pipe_log
            .mut_queue(LogQueue::Append)
            .open_files(log_files, min_file_id, max_file_id)?;
        pipe_log.mut_queue(LogQueue::Rewrite).open_files(
            rewrite_files,
            min_rewrite_num,
            max_rewrite_num,
        )?;

        Ok(pipe_log)
    }

    fn append_bytes(
        &self,
        queue: LogQueue,
        content: &[u8],
        sync: &mut bool,
    ) -> Result<(FileId, u64, Arc<LogFd>)> {
        // Must hold lock until file is written to avoid corrupted holes.
        let mut log_manager = self.mut_queue(queue);
        let size = content.len();
        let (file_id, offset, fd) = log_manager.on_append(size, sync)?;
        fd.write(offset as i64, content)?;
        for listener in &self.listeners {
            listener.on_append_log_file(queue, file_id, size);
        }

        Ok((file_id, offset, fd))
    }

    fn get_queue(&self, queue: LogQueue) -> RwLockReadGuard<LogManager> {
        match queue {
            LogQueue::Append => self.appender.read(),
            LogQueue::Rewrite => self.rewriter.read(),
        }
    }

    fn mut_queue(&self, queue: LogQueue) -> RwLockWriteGuard<LogManager> {
        match queue {
            LogQueue::Append => self.appender.write(),
            LogQueue::Rewrite => self.rewriter.write(),
        }
    }

    #[cfg(test)]
    fn truncate_active_log(&self, queue: LogQueue, offset: Option<usize>) -> Result<()> {
        self.mut_queue(queue).truncate_active_log(offset)
    }

    #[cfg(test)]
    fn active_log_size(&self, queue: LogQueue) -> usize {
        self.get_queue(queue).active_log_size
    }

    #[cfg(test)]
    fn active_log_capacity(&self, queue: LogQueue) -> usize {
        self.get_queue(queue).active_log_capacity
    }
}

impl PipeLog for FilePipeLog {
    fn close(&self) -> Result<()> {
        self.appender.write().truncate_active_log(None)?;
        self.rewriter.write().truncate_active_log(None)?;
        Ok(())
    }

    fn file_size(&self, queue: LogQueue, file_id: FileId) -> Result<u64> {
        self.get_queue(queue)
            .get_fd(file_id)
            .map(|fd| fd.file_size().unwrap() as u64)
    }

    fn total_size(&self, queue: LogQueue) -> usize {
        let manager = self.get_queue(queue);
        manager
            .active_file_id
            .step_after(&manager.first_file_id)
            .unwrap()
            * self.rotate_size
            + manager.active_log_size
    }

    fn read_bytes(
        &self,
        queue: LogQueue,
        file_id: FileId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>> {
        let fd = self.get_queue(queue).get_fd(file_id)?;
        fd.read(offset as i64, len as usize)
    }

    fn read_file_into_log_item_batch<M: MessageExt>(
        &self,
        queue: LogQueue,
        file_id: FileId,
        mode: RecoveryMode,
        item_batches: &mut Vec<LogItemBatch<M>>,
    ) -> Result<()> {
        debug!("recover from log file {:?}:{:?}", queue, file_id);
        let fd = self.get_queue(queue).get_fd(file_id)?;
        let mut reader = LogItemBatchFileReader::new(
            &fd,
            (FILE_MAGIC_HEADER.len() + Version::len()) as u64,
            self.recovery_read_block_size,
        )?;
        loop {
            match reader.next::<M>() {
                Ok(Some(mut item_batch)) => {
                    item_batch.set_queue_and_file_id(queue, file_id);
                    item_batches.push(item_batch);
                }
                Ok(None) => return Ok(()),
                Err(e) => {
                    return match mode {
                        RecoveryMode::TolerateCorruptedTailRecords => Ok(()),
                        RecoveryMode::AbsoluteConsistency => Err(Error::Corruption(format!(
                            "Raft log content is corrupted: {}",
                            e
                        ))),
                    }
                }
            }
        }
    }

    fn append<M: MessageExt>(
        &self,
        queue: LogQueue,
        batch: &mut LogBatch<M>,
        mut sync: bool,
    ) -> Result<(FileId, usize)> {
        if let Some(content) = batch.encode_to_bytes(self.compression_threshold)? {
            let start = Instant::now();
            let (file_id, offset, fd) = self.append_bytes(queue, &content, &mut sync)?;
            if sync {
                fd.sync()?;
            }

            // set fields based on the log file
            batch.set_position(queue, file_id, offset);

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

            return Ok((file_id, content.len()));
        }
        Ok((Default::default(), 0))
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
            let mut path = PathBuf::from(&self.dir);
            path.push(build_file_name(queue, cur_file_id));
            if let Err(e) = fs::remove_file(&path) {
                warn!("Remove purged log file {:?} fail: {}", path, e);
                return Ok(i);
            }
            cur_file_id = cur_file_id.forward(1);
        }
        Ok(purge_count)
    }
}

fn parse_nix_error(e: nix::Error, custom: &'static str) -> Error {
    match e {
        nix::Error::Sys(no) => {
            let kind = IoError::from(no).kind();
            Error::Io(IoError::new(kind, custom))
        }
        e => box_err!("{}: {:?}", custom, e),
    }
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use tempfile::Builder;

    use super::*;
    use crate::util::ReadableSize;

    fn new_test_pipe_log(path: &str, bytes_per_sync: usize, rotate_size: usize) -> FilePipeLog {
        let mut cfg = Config::default();
        cfg.dir = path.to_owned();
        cfg.bytes_per_sync = ReadableSize(bytes_per_sync as u64);
        cfg.target_file_size = ReadableSize(rotate_size as u64);

        FilePipeLog::open(&cfg, vec![]).unwrap()
    }

    #[test]
    fn test_file_name() {
        let file_name: &str = "0000000000000123.raftlog";
        assert_eq!(
            parse_file_name(file_name).unwrap(),
            (LogQueue::Append, 123.into())
        );
        assert_eq!(build_file_name(LogQueue::Append, 123.into()), file_name);

        let file_name: &str = "00000123.rewrite";
        assert_eq!(
            parse_file_name(file_name).unwrap(),
            (LogQueue::Rewrite, 123.into())
        );
        assert_eq!(build_file_name(LogQueue::Rewrite, 123.into()), file_name);

        let invalid_file_name: &str = "123.log";
        assert!(parse_file_name(invalid_file_name).is_err());
        assert!(parse_file_name(invalid_file_name).is_err());
    }

    fn test_pipe_log_impl(queue: LogQueue) {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 1024;
        let bytes_per_sync = 32 * 1024;
        let pipe_log = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.first_file_id(queue), INIT_FILE_NUM.into());
        assert_eq!(pipe_log.active_file_id(queue), INIT_FILE_NUM.into());

        let header_size = (FILE_MAGIC_HEADER.len() + Version::len()) as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        let (file_num, offset, _) = pipe_log.append_bytes(queue, &content, &mut false).unwrap();
        assert_eq!(file_num, 1.into());
        assert_eq!(offset, header_size);
        assert_eq!(pipe_log.active_file_id(queue), 1.into());

        let (file_num, offset, _) = pipe_log.append_bytes(queue, &content, &mut false).unwrap();
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
        let (file_num, offset, _) = pipe_log
            .append_bytes(queue, &s_content, &mut false)
            .unwrap();
        assert_eq!(file_num, 3.into());
        assert_eq!(offset, header_size);

        let (file_num, offset, _) = pipe_log
            .append_bytes(queue, &s_content, &mut false)
            .unwrap();
        assert_eq!(file_num, 3.into());
        assert_eq!(offset, header_size + s_content.len() as u64);

        assert_eq!(
            pipe_log.active_log_size(queue),
            FILE_MAGIC_HEADER.len() + Version::len() + 2 * s_content.len()
        );

        let content_readed = pipe_log
            .read_bytes(queue, 3.into(), header_size, s_content.len() as u64)
            .unwrap();
        assert_eq!(content_readed, s_content);

        // leave only 1 file to truncate
        assert!(pipe_log.purge_to(queue, 3.into()).is_ok());
        assert_eq!(pipe_log.first_file_id(queue), 3.into());
        assert_eq!(pipe_log.active_file_id(queue), 3.into());

        // truncate file
        pipe_log
            .truncate_active_log(queue, Some(FILE_MAGIC_HEADER.len() + Version::len()))
            .unwrap();
        assert_eq!(
            pipe_log.active_log_size(queue,),
            FILE_MAGIC_HEADER.len() + Version::len()
        );
        let trunc_big_offset = pipe_log.truncate_active_log(
            queue,
            Some(FILE_MAGIC_HEADER.len() + Version::len() + s_content.len()),
        );
        assert!(trunc_big_offset.is_err());

        // reopen
        pipe_log.close().unwrap();
        let pipe_log = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.active_file_id(queue), 3.into());
        assert_eq!(
            pipe_log.active_log_size(queue),
            FILE_MAGIC_HEADER.len() + Version::len()
        );
        assert_eq!(
            pipe_log.active_log_capacity(queue),
            FILE_MAGIC_HEADER.len() + Version::len()
        );
    }

    #[test]
    fn test_pipe_log_append() {
        test_pipe_log_impl(LogQueue::Append)
    }

    #[test]
    fn test_pipe_log_rewrite() {
        test_pipe_log_impl(LogQueue::Rewrite)
    }

    #[test]
    fn test_log_file_validation() {
        // magic header corruption
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        buf.write(b"RAFT-LOG-FILE-HEADER-********************************")
            .unwrap();
        buf.encode_u64(Version::current().as_u64()).unwrap();
        let fd = LogFd::create("test_log_file_validation").unwrap();
        fd.write(0, &buf).unwrap();
        assert!(fd.read_header().is_err());

        // unrecognized version
        buf.clear();
        buf.write(FILE_MAGIC_HEADER).unwrap();
        buf.encode_u64(u64::MAX).unwrap();
        fd.write(0, &buf).unwrap();
        assert!(fd.read_header().is_err());

        fs::remove_file("test_log_file_validation").unwrap();
    }
}
