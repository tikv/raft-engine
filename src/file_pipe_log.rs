// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use log::{info, warn};
use parking_lot::{RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::config::{Config, RecoveryMode};
use crate::event_listener::EventListener;
use crate::file_system::{FileSystem, Readable, Writable};
use crate::log_batch::{LogBatch, LogItemBatch};
use crate::log_file::{LogFd, LogFile, LogFileHeader, LOG_FILE_MIN_HEADER_LEN};
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

const INIT_FILE_ID: u64 = 1;

const DEFAULT_FILES_COUNT: usize = 32;
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

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

fn build_file_path<P: AsRef<Path>>(dir: P, queue: LogQueue, file_id: FileId) -> PathBuf {
    let mut path = PathBuf::from(dir.as_ref());
    path.push(build_file_name(queue, file_id));
    path
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

struct ActiveFile {
    fd: Arc<LogFd>,
    writer: Box<dyn Writable>,
    size: usize,
    capacity: usize,
    last_sync: usize,
}

impl ActiveFile {
    fn open(fd: Arc<LogFd>, writer: Box<dyn Writable>, size: usize) -> Result<Self> {
        let file_size = fd.file_size()?;
        let mut f = Self {
            fd,
            writer,
            size,
            capacity: file_size,
            last_sync: size,
        };
        if size < LOG_FILE_MIN_HEADER_LEN {
            f.write_header()?;
        } else {
            f.writer.seek(std::io::SeekFrom::Start(size as u64))?;
        }
        Ok(f)
    }

    fn reset(&mut self, fd: Arc<LogFd>, writer: Box<dyn Writable>) -> Result<()> {
        self.size = 0;
        self.last_sync = 0;
        self.capacity = fd.file_size()?;
        self.fd = fd;
        self.writer = writer;
        self.write_header()
    }

    fn truncate(&mut self) -> Result<()> {
        self.fd.truncate(self.size as i64)?;
        self.fd.sync()?;
        self.capacity = self.size;
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.seek(std::io::SeekFrom::Start(0))?;
        self.size = 0;
        let mut buf = Vec::with_capacity(LOG_FILE_MIN_HEADER_LEN);
        LogFileHeader::new().encode(&mut buf)?;
        self.write(&buf, true)?;
        Ok(())
    }

    fn write(&mut self, buf: &[u8], sync: bool) -> Result<()> {
        #[cfg(target_os = "linux")]
        if self.size + buf.len() > self.capacity {
            // Use fallocate to pre-allocate disk space for active file.
            let alloc = std::cmp::max(self.size + buf.len() - self.capacity, FILE_ALLOCATE_SIZE);
            self.fd.allocate(self.capacity as i64, alloc as i64)?;
            self.capacity += alloc;
        }
        self.writer.write_all(buf)?;
        self.size += buf.len();
        if sync {
            self.last_sync = self.size;
        }
        Ok(())
    }

    fn since_last_sync(&self) -> usize {
        self.size - self.last_sync
    }
}

struct LogManager {
    queue: LogQueue,
    dir: String,
    rotate_size: usize,
    bytes_per_sync: usize,
    file_system: Option<Arc<dyn FileSystem>>,
    listeners: Vec<Arc<dyn EventListener>>,

    pub first_file_id: FileId,
    pub active_file_id: FileId,

    all_files: VecDeque<Arc<LogFd>>,
    active_file: ActiveFile,
}

impl LogManager {
    fn open<F>(
        cfg: &Config,
        file_system: Option<Arc<dyn FileSystem>>,
        listeners: Vec<Arc<dyn EventListener>>,
        queue: LogQueue,
        mut min_file_id: FileId,
        mut max_file_id: FileId,
        replay: &F,
    ) -> Result<Self>
    where
        F: Fn(LogQueue, FileId, LogItemBatch),
    {
        let mut reader = LogItemBatchFileReader::new(cfg.recovery_read_block_size.0 as usize);
        let mut all_files = VecDeque::with_capacity(DEFAULT_FILES_COUNT);
        if max_file_id.valid() {
            assert!(min_file_id <= max_file_id);
            let mut file_id = min_file_id;
            while file_id <= max_file_id {
                let path = build_file_path(&cfg.dir, queue, file_id);
                let fd = Arc::new(LogFd::open(&path)?);
                all_files.push_back(fd.clone());
                for listener in &listeners {
                    listener.post_new_log_file(queue, file_id);
                }
                // Recover log items.
                let file_size = fd.file_size()?;
                let tolerate_failure = file_id == max_file_id
                    && cfg.recovery_mode == RecoveryMode::TolerateCorruptedTailRecords;
                let raw_file_reader = Box::new(LogFile::new(fd));
                let file_reader = if let Some(ref fs) = file_system {
                    fs.open_file_reader(&path, raw_file_reader as Box<dyn Readable>)?
                } else {
                    raw_file_reader
                };
                if let Err(e) = reader.open(file_reader, file_size) {
                    if !tolerate_failure {
                        return Err(Error::Corruption(format!("Unable to open log file: {}", e)));
                    }
                } else {
                    loop {
                        match reader.next() {
                            Ok(Some(mut item_batch)) => {
                                item_batch.set_position(queue, file_id, None);
                                replay(queue, file_id, item_batch);
                            }
                            Err(e) if tolerate_failure => {
                                return Err(Error::Corruption(format!(
                                    "Raft log content is corrupted: {}",
                                    e
                                )))
                            }
                            _ => break,
                        }
                    }
                }
                file_id = file_id.forward(1);
            }
        } else {
            min_file_id = INIT_FILE_ID.into();
            max_file_id = INIT_FILE_ID.into();
            let fd = Arc::new(LogFd::create(&build_file_path(
                &cfg.dir,
                queue,
                min_file_id,
            ))?);
            all_files.push_back(fd);
            for listener in &listeners {
                listener.post_new_log_file(queue, min_file_id);
            }
        }
        let active_file_size = reader.valid_offset();
        let active_fd = all_files.back().unwrap().clone();
        let raw_writer = Box::new(LogFile::new(active_fd.clone()));
        let active_file = ActiveFile::open(
            active_fd,
            if let Some(ref fs) = file_system {
                fs.open_file_writer(
                    &build_file_path(&cfg.dir, queue, max_file_id),
                    raw_writer as Box<dyn Writable>,
                )?
            } else {
                raw_writer
            },
            active_file_size,
        )?;

        let manager = Self {
            queue,
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            file_system,
            listeners,

            first_file_id: min_file_id,
            active_file_id: max_file_id,

            all_files,
            active_file,
        };
        manager.update_metrics();
        Ok(manager)
    }

    fn new_log_file(&mut self) -> Result<()> {
        if self.active_file_id.valid() {
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

        let raw_writer = Box::new(LogFile::new(fd.clone()));
        self.active_file.reset(
            fd,
            if let Some(ref fs) = self.file_system {
                fs.open_file_writer(&path, raw_writer as Box<dyn Writable>)
                    .unwrap()
            } else {
                raw_writer
            },
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
        if self.active_file.size >= self.rotate_size {
            self.new_log_file()?;
        }
        if self.active_file.since_last_sync() >= self.bytes_per_sync {
            *sync = true;
        }
        let offset = self.active_file.size as u64;
        self.active_file.write(content, *sync)?;
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
            + self.active_file.size
    }
}

#[derive(Clone)]
pub struct FilePipeLog {
    dir: String,
    rotate_size: usize,
    compression_threshold: usize,

    appender: Arc<RwLock<LogManager>>,
    rewriter: Arc<RwLock<LogManager>>,
    file_system: Option<Arc<dyn FileSystem>>,
    listeners: Vec<Arc<dyn EventListener>>,
}

impl FilePipeLog {
    pub fn open<F>(
        cfg: &Config,
        file_system: Option<Arc<dyn FileSystem>>,
        listeners: Vec<Arc<dyn EventListener>>,
        replay: F,
    ) -> Result<FilePipeLog>
    where
        F: Fn(LogQueue, FileId, LogItemBatch),
    {
        let path = Path::new(&cfg.dir);
        if !path.exists() {
            info!("Create raft log directory: {}", &cfg.dir);
            fs::create_dir(&cfg.dir)?;
        }
        if !path.is_dir() {
            return Err(box_err!("Not directory: {}", &cfg.dir));
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

        let appender = Arc::new(RwLock::new(LogManager::open(
            cfg,
            file_system.clone(),
            listeners.clone(),
            LogQueue::Append,
            min_append_id,
            max_append_id,
            &replay,
        )?));
        let rewriter = Arc::new(RwLock::new(LogManager::open(
            cfg,
            file_system.clone(),
            listeners.clone(),
            LogQueue::Rewrite,
            min_rewrite_id,
            max_rewrite_id,
            &replay,
        )?));

        Ok(FilePipeLog {
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            compression_threshold: cfg.batch_compression_threshold.0 as usize,
            appender,
            rewriter,
            file_system,
            listeners,
        })
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
            fd.sync()?;
        }

        Ok((file_id, offset))
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
}

impl PipeLog for FilePipeLog {
    fn close(&self) -> Result<()> {
        self.mut_queue(LogQueue::Rewrite).truncate_active_log()?;
        self.mut_queue(LogQueue::Append).truncate_active_log()
    }

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
        let raw_reader = Box::new(LogFile::new(fd));
        let mut reader = if let Some(ref fs) = self.file_system {
            fs.open_file_reader(&build_file_path(&self.dir, queue, file_id), raw_reader)?
        } else {
            raw_reader
        };
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

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::util::ReadableSize;

    fn new_test_pipe_log(path: &str, bytes_per_sync: usize, rotate_size: usize) -> FilePipeLog {
        let mut cfg = Config::default();
        cfg.dir = path.to_owned();
        cfg.bytes_per_sync = ReadableSize(bytes_per_sync as u64);
        cfg.target_file_size = ReadableSize(rotate_size as u64);

        FilePipeLog::open(&cfg, None, vec![], |_, _, _| {}).unwrap()
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
        assert_eq!(pipe_log.first_file_id(queue), INIT_FILE_ID.into());
        assert_eq!(pipe_log.active_file_id(queue), INIT_FILE_ID.into());

        let header_size = LOG_FILE_MIN_HEADER_LEN as u64;

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
