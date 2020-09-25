use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read};
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::{cmp, u64};

use log::{info, warn};
use nix::errno::Errno;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{close, fsync, ftruncate, lseek, Whence};
use nix::NixPath;
use protobuf::Message;

use crate::config::Config;
use crate::log_batch::{EntryExt, LogBatch, LogItemContent};
use crate::util::HandyRwLock;
use crate::{Error, Result};

const LOG_SUFFIX: &str = ".raftlog";
const LOG_SUFFIX_LEN: usize = 8;
const LOG_NUM_LEN: usize = 16;
const LOG_NAME_LEN: usize = LOG_NUM_LEN + LOG_SUFFIX_LEN;

const REWRITE_SUFFIX: &str = ".rewrite";
const REWRITE_SUFFIX_LEN: usize = 8;
const REWRITE_NUM_LEN: usize = 8;
const REWRITE_NAME_LEN: usize = REWRITE_NUM_LEN + REWRITE_SUFFIX_LEN;

const INIT_FILE_NUM: u64 = 1;

pub const FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
pub const VERSION: &[u8] = b"v1.0.0";
const DEFAULT_FILES_COUNT: usize = 32;
#[cfg(target_os = "linux")]
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

pub trait GenericPipeLog: Sized + Clone + Send {
    /// Read some bytes from the given position.
    fn fread(&self, queue: LogQueue, file_num: u64, offset: u64, len: u64) -> Result<Vec<u8>>;

    /// Check whether the size of current write log has reached the rotate limit.
    fn switch_log_file(&self, queue: LogQueue) -> Result<(u64, Arc<LogFd>)>;

    /// Append some bytes to the given queue.
    fn append(&self, queue: LogQueue, content: &[u8]) -> Result<u64>;

    /// Close the pipe log.
    fn close(&self) -> Result<()>;

    /// Rewrite a batch into the rewrite queue.
    fn rewrite<E: Message, W: EntryExt<E>>(
        &self,
        batch: &LogBatch<E, W>,
        sync: bool,
        file_num: &mut u64,
    ) -> Result<usize>;

    /// Truncate the active log file of `queue`.
    fn truncate_active_log(&self, queue: LogQueue, offset: Option<usize>) -> Result<()>;

    /// Sync the given queue.
    fn sync(&self, queue: LogQueue) -> Result<()>;

    /// Read whole file into buffer, used for recovery.
    fn read_whole_file(&self, queue: LogQueue, file_num: u64) -> Result<Vec<u8>>;

    /// Purge the append queue to the given file number.
    fn purge_to(&self, queue: LogQueue, file_num: u64) -> Result<usize>;

    /// Total size of the append queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    /// Active file number in the given queue.
    fn active_file_num(&self, queue: LogQueue) -> u64;

    /// First file number in the given queue.
    fn first_file_num(&self, queue: LogQueue) -> u64;

    /// Return the last file number before `total - size`. `0` means no such files.
    fn latest_file_before(&self, queue: LogQueue, size: usize) -> u64;

    fn file_len(&self, queue: LogQueue, file_num: u64) -> u64;
}

pub struct LogFd(RawFd);

impl LogFd {
    fn close(&self) -> Result<()> {
        close(self.0).map_err(|e| parse_nix_error(e, "close"))
    }
    pub fn sync(&self) -> Result<()> {
        fsync(self.0).map_err(|e| parse_nix_error(e, "fsync"))
    }
}

impl Drop for LogFd {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            warn!("Drop LogFd fail: {}", e);
        }
    }
}

struct LogManager {
    dir: String,
    name_suffix: &'static str,

    pub first_file_num: u64,
    pub active_file_num: u64,

    pub active_log_size: usize,
    pub active_log_capacity: usize,
    pub last_sync_size: usize,

    pub all_files: VecDeque<Arc<LogFd>>,
}

impl LogManager {
    fn new(cfg: &Config, name_suffix: &'static str) -> Self {
        Self {
            dir: cfg.dir.clone(),
            name_suffix,

            first_file_num: INIT_FILE_NUM,
            active_file_num: 0,
            active_log_size: 0,
            active_log_capacity: 0,
            last_sync_size: 0,
            all_files: VecDeque::with_capacity(DEFAULT_FILES_COUNT),
        }
    }

    fn open_files(
        &mut self,
        logs: Vec<String>,
        min_file_num: u64,
        max_file_num: u64,
    ) -> Result<()> {
        if !logs.is_empty() {
            self.first_file_num = min_file_num;
            self.active_file_num = max_file_num;

            for file_name in &logs[0..logs.len() - 1] {
                let mut path = PathBuf::from(&self.dir);
                path.push(file_name);
                let fd = Arc::new(LogFd(open_frozen_file(&path)?));
                self.all_files.push_back(fd);
            }

            let mut path = PathBuf::from(&self.dir);
            path.push(logs.last().unwrap());
            let fd = Arc::new(LogFd(open_active_file(&path)?));
            fd.sync()?;

            self.active_log_size = file_len(fd.0)?;
            self.active_log_capacity = self.active_log_size;
            self.last_sync_size = self.active_log_size;
            self.all_files.push_back(fd);
        }
        Ok(())
    }

    fn new_log_file(&mut self) -> Result<()> {
        if self.active_file_num > 0 {
            self.truncate_active_log(None)?;
        }
        if let Some(last_file) = self.all_files.back() {
            last_file.sync()?;
        }
        self.active_file_num += 1;

        let mut path = PathBuf::from(&self.dir);
        path.push(generate_file_name(self.active_file_num, self.name_suffix));
        let fd = Arc::new(LogFd(open_active_file(&path)?));
        let bytes = write_file_header(fd.0)?;

        self.active_log_size = bytes;
        self.active_log_capacity = bytes;
        self.last_sync_size = 0;
        self.all_files.push_back(fd);
        Ok(())
    }

    fn truncate_active_log(&mut self, offset: Option<usize>) -> Result<()> {
        let mut offset = offset.unwrap_or(self.active_log_size);
        match offset.cmp(&self.active_log_size) {
            cmp::Ordering::Greater => {
                let io_error = IoError::new(IoErrorKind::UnexpectedEof, "truncate");
                return Err(Error::Io(io_error));
            }
            cmp::Ordering::Equal => return Ok(()),
            _ => {}
        }
        let active_fd = self.get_active_fd().unwrap();
        ftruncate(active_fd.0, offset as _).map_err(|e| parse_nix_error(e, "ftruncate"))?;
        if offset < FILE_MAGIC_HEADER.len() + VERSION.len() {
            // After truncate to 0, write header is necessary.
            offset = write_file_header(active_fd.0)?;
        }
        active_fd.sync()?;
        self.active_log_size = offset;
        self.active_log_capacity = offset;
        self.last_sync_size = self.active_log_size;
        Ok(())
    }

    fn get_fd(&self, file_num: u64) -> Result<Arc<LogFd>> {
        if file_num < self.first_file_num || file_num > self.active_file_num {
            return Err(box_err!("File {} not exist", file_num));
        }
        Ok(self.all_files[(file_num - self.first_file_num) as usize].clone())
    }

    fn get_active_fd(&self) -> Option<Arc<LogFd>> {
        self.all_files.back().cloned()
    }

    fn purge_to(&mut self, file_num: u64) -> Result<usize> {
        if file_num > self.active_file_num {
            return Err(box_err!("Purge active or newer files"));
        }
        let end_offset = (file_num - self.first_file_num) as usize;
        self.all_files.drain(..end_offset);
        self.first_file_num = file_num;
        Ok(end_offset)
    }

    fn on_append(&mut self, content_len: usize) -> Result<(u64, Arc<LogFd>)> {
        let active_log_size = self.active_log_size;
        let fd = self.get_active_fd().unwrap();

        self.active_log_size += content_len;

        #[cfg(target_os = "linux")]
        if self.active_log_size > self.active_log_capacity {
            // Use fallocate to pre-allocate disk space for active file.
            let reserve = self.active_log_size - self.active_log_capacity;
            let alloc_size = cmp::max(reserve, FILE_ALLOCATE_SIZE);
            fcntl::fallocate(
                fd.0,
                fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE,
                self.active_log_capacity as _,
                alloc_size as _,
            )
            .map_err(|e| parse_nix_error(e, "fallocate"))?;
            self.active_log_capacity += alloc_size;
        }

        Ok((active_log_size as u64, fd))
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum LogQueue {
    Append,
    Rewrite,
}

#[derive(Clone)]
pub struct PipeLog {
    dir: String,
    rotate_size: usize,
    bytes_per_sync: usize,

    appender: Arc<RwLock<LogManager>>,
    rewriter: Arc<RwLock<LogManager>>,
}

impl PipeLog {
    fn new(cfg: &Config) -> PipeLog {
        let appender = Arc::new(RwLock::new(LogManager::new(&cfg, LOG_SUFFIX)));
        let rewriter = Arc::new(RwLock::new(LogManager::new(&cfg, REWRITE_SUFFIX)));
        PipeLog {
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            appender,
            rewriter,
        }
    }

    pub fn open(cfg: &Config) -> Result<PipeLog> {
        let path = Path::new(&cfg.dir);
        if !path.exists() {
            info!("Create raft log directory: {}", &cfg.dir);
            fs::create_dir(&cfg.dir)?;
        }
        if !path.is_dir() {
            return Err(box_err!("Not directory: {}", &cfg.dir));
        }

        let pipe_log = PipeLog::new(cfg);

        let (mut min_file_num, mut max_file_num): (u64, u64) = (u64::MAX, 0);
        let (mut min_rewrite_num, mut max_rewrite_num): (u64, u64) = (u64::MAX, 0);
        let (mut log_files, mut rewrite_files) = (vec![], vec![]);
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();
            if !file_path.is_file() {
                continue;
            }

            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if is_log_file(file_name) {
                let file_num = match extract_file_num(file_name, LOG_NUM_LEN) {
                    Ok(num) => num,
                    Err(_) => continue,
                };
                min_file_num = cmp::min(min_file_num, file_num);
                max_file_num = cmp::max(max_file_num, file_num);
                log_files.push(file_name.to_string());
            } else if is_rewrite_file(file_name) {
                let file_num = match extract_file_num(file_name, REWRITE_NUM_LEN) {
                    Ok(num) => num,
                    Err(_) => continue,
                };
                min_rewrite_num = cmp::min(min_rewrite_num, file_num);
                max_rewrite_num = cmp::max(max_rewrite_num, file_num);
                rewrite_files.push(file_name.to_string());
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
        if log_files.len() as u64 != max_file_num - min_file_num + 1 {
            return Err(box_err!("Corruption occurs on log files"));
        }
        if !rewrite_files.is_empty()
            && rewrite_files.len() as u64 != max_rewrite_num - min_rewrite_num + 1
        {
            return Err(box_err!("Corruption occurs on rewrite files"));
        }

        pipe_log
            .mut_queue(LogQueue::Append)
            .open_files(log_files, min_file_num, max_file_num)?;
        pipe_log.mut_queue(LogQueue::Rewrite).open_files(
            rewrite_files,
            min_rewrite_num,
            max_rewrite_num,
        )?;

        Ok(pipe_log)
    }

    fn get_queue(&self, queue: LogQueue) -> RwLockReadGuard<LogManager> {
        match queue {
            LogQueue::Append => self.appender.rl(),
            LogQueue::Rewrite => self.rewriter.rl(),
        }
    }

    fn mut_queue(&self, queue: LogQueue) -> RwLockWriteGuard<LogManager> {
        match queue {
            LogQueue::Append => self.appender.wl(),
            LogQueue::Rewrite => self.rewriter.wl(),
        }
    }

    fn get_name_suffix(&self, queue: LogQueue) -> &'static str {
        match queue {
            LogQueue::Append => LOG_SUFFIX,
            LogQueue::Rewrite => REWRITE_SUFFIX,
        }
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

impl GenericPipeLog for PipeLog {
    fn fread(&self, queue: LogQueue, file_num: u64, offset: u64, len: u64) -> Result<Vec<u8>> {
        let fd = self.get_queue(queue).get_fd(file_num)?;
        pread_exact(fd.0, offset, len as usize)
    }

    fn switch_log_file(&self, queue: LogQueue) -> Result<(u64, Arc<LogFd>)> {
        let mut writer = self.mut_queue(queue);
        if writer.active_log_size >= self.rotate_size {
            writer.new_log_file()?;
        }
        let fd = writer.get_active_fd().unwrap();

        Ok((writer.active_file_num, fd))
    }

    fn append(&self, queue: LogQueue, content: &[u8]) -> Result<u64> {
        let (offset, fd) = self.mut_queue(queue).on_append(content.len())?;
        pwrite_exact(fd.0, offset, content)?;
        Ok(offset)
    }

    fn close(&self) -> Result<()> {
        self.appender.wl().truncate_active_log(None)?;
        self.rewriter.wl().truncate_active_log(None)?;
        Ok(())
    }

    fn rewrite<E: Message, W: EntryExt<E>>(
        &self,
        batch: &LogBatch<E, W>,
        sync: bool,
        file_num: &mut u64,
    ) -> Result<usize> {
        let mut encoded_size = 0;
        if let Some(content) = batch.encode_to_bytes(&mut encoded_size) {
            let (cur_file_num, fd) = self.switch_log_file(LogQueue::Rewrite)?;
            let offset = self.append(LogQueue::Rewrite, &content)?;
            if sync {
                fd.sync()?;
            }
            for item in &batch.items {
                if let LogItemContent::Entries(ref entries) = item.content {
                    entries.update_position(LogQueue::Rewrite, cur_file_num, offset, &None);
                }
            }
            *file_num = cur_file_num;
            return Ok(content.len());
        }
        Ok(0)
    }

    fn truncate_active_log(&self, queue: LogQueue, offset: Option<usize>) -> Result<()> {
        self.mut_queue(queue).truncate_active_log(offset)
    }

    fn sync(&self, queue: LogQueue) -> Result<()> {
        if let Some(fd) = self.get_queue(queue).get_active_fd() {
            fd.sync()?;
        }
        Ok(())
    }

    fn read_whole_file(&self, queue: LogQueue, file_num: u64) -> Result<Vec<u8>> {
        let mut path = PathBuf::from(&self.dir);
        path.push(generate_file_name(file_num, self.get_name_suffix(queue)));
        let meta = fs::metadata(&path)?;
        let mut vec = Vec::with_capacity(meta.len() as usize);

        // Read the whole file.
        let mut file = File::open(&path)?;
        file.read_to_end(&mut vec)?;
        Ok(vec)
    }

    fn purge_to(&self, queue: LogQueue, file_num: u64) -> Result<usize> {
        let (mut manager, name_suffix) = match queue {
            LogQueue::Append => (self.appender.wl(), LOG_SUFFIX),
            LogQueue::Rewrite => (self.rewriter.wl(), REWRITE_SUFFIX),
        };
        let purge_count = manager.purge_to(file_num)?;
        drop(manager);

        for cur_file_num in (file_num - purge_count as u64)..file_num {
            let mut path = PathBuf::from(&self.dir);
            path.push(generate_file_name(cur_file_num, name_suffix));
            if let Err(e) = fs::remove_file(&path) {
                warn!("Remove purged log file {:?} fail: {}", path, e);
                return Ok((cur_file_num + purge_count as u64 - file_num) as usize);
            }
        }
        Ok(purge_count)
    }

    fn total_size(&self, queue: LogQueue) -> usize {
        let manager = self.get_queue(queue);
        (manager.active_file_num - manager.first_file_num) as usize * self.rotate_size
            + manager.active_log_size
    }

    fn active_file_num(&self, queue: LogQueue) -> u64 {
        self.get_queue(queue).active_file_num
    }

    fn first_file_num(&self, queue: LogQueue) -> u64 {
        self.get_queue(queue).first_file_num
    }

    fn latest_file_before(&self, queue: LogQueue, size: usize) -> u64 {
        let cur_size = self.total_size(queue);
        if cur_size <= size {
            return 0;
        }
        let count = (cur_size - size) / self.rotate_size;
        let file_num = self.get_queue(queue).first_file_num + count as u64;
        assert!(file_num <= self.active_file_num(queue));
        file_num
    }

    fn file_len(&self, queue: LogQueue, file_num: u64) -> u64 {
        let fd = self.get_queue(queue).get_fd(file_num).unwrap();
        file_len(fd.0).unwrap() as u64
    }
}

fn generate_file_name(file_num: u64, suffix: &'static str) -> String {
    match suffix {
        LOG_SUFFIX => format!("{:016}{}", file_num, suffix),
        REWRITE_SUFFIX => format!("{:08}{}", file_num, suffix),
        _ => unreachable!(),
    }
}

fn extract_file_num(file_name: &str, file_num_len: usize) -> Result<u64> {
    if file_name.len() < file_num_len {
        return Err(Error::ParseFileName(file_name.to_owned()));
    }
    match file_name[..file_num_len].parse::<u64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(Error::ParseFileName(file_name.to_owned())),
    }
}

fn is_log_file(file_name: &str) -> bool {
    file_name.ends_with(LOG_SUFFIX) && file_name.len() == LOG_NAME_LEN
}

fn is_rewrite_file(file_name: &str) -> bool {
    file_name.ends_with(REWRITE_SUFFIX) && file_name.len() == REWRITE_NAME_LEN
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

fn open_active_file<P: ?Sized + NixPath>(path: &P) -> Result<RawFd> {
    let flags = OFlag::O_RDWR | OFlag::O_CREAT;
    let mode = Mode::S_IRUSR | Mode::S_IWUSR;
    fcntl::open(path, flags, mode).map_err(|e| parse_nix_error(e, "open_active_file"))
}

fn open_frozen_file<P: ?Sized + NixPath>(path: &P) -> Result<RawFd> {
    let flags = OFlag::O_RDONLY;
    let mode = Mode::S_IRWXU;
    fcntl::open(path, flags, mode).map_err(|e| parse_nix_error(e, "open_frozen_file"))
}

fn file_len(fd: RawFd) -> Result<usize> {
    lseek(fd, 0, Whence::SeekEnd)
        .map(|n| n as usize)
        .map_err(|e| parse_nix_error(e, "lseek"))
}

fn pread_exact(fd: RawFd, mut offset: u64, len: usize) -> Result<Vec<u8>> {
    let mut result = vec![0; len as usize];
    let mut readed = 0;
    while readed < len {
        let bytes = match pread(fd, &mut result[readed..], offset as _) {
            Ok(bytes) => bytes,
            Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
            Err(e) => return Err(parse_nix_error(e, "pread")),
        };
        readed += bytes;
        offset += bytes as u64;
    }
    Ok(result)
}

fn pwrite_exact(fd: RawFd, mut offset: u64, content: &[u8]) -> Result<()> {
    let mut written = 0;
    while written < content.len() {
        let bytes = match pwrite(fd, &content[written..], offset as _) {
            Ok(bytes) => bytes,
            Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
            Err(e) => return Err(parse_nix_error(e, "pwrite")),
        };
        written += bytes;
        offset += bytes as u64;
    }
    Ok(())
}

fn write_file_header(fd: RawFd) -> Result<usize> {
    let len = FILE_MAGIC_HEADER.len() + VERSION.len();
    let mut header = Vec::with_capacity(len);
    header.extend_from_slice(FILE_MAGIC_HEADER);
    header.extend_from_slice(VERSION);
    pwrite_exact(fd, 0, &header)?;
    Ok(len)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use raft::eraftpb::Entry;
    use tempfile::Builder;

    use super::*;
    use crate::cache_evict::{CacheSubmitor, CacheTask};
    use crate::util::{ReadableSize, Worker};
    use crate::GlobalStats;

    fn new_test_pipe_log(path: &str, bytes_per_sync: usize, rotate_size: usize) -> PipeLog {
        let mut cfg = Config::default();
        cfg.dir = path.to_owned();
        cfg.bytes_per_sync = ReadableSize(bytes_per_sync as u64);
        cfg.target_file_size = ReadableSize(rotate_size as u64);

        let log = PipeLog::open(&cfg).unwrap();
        log
    }

    #[test]
    fn test_file_name() {
        let file_name: &str = "0000000000000123.raftlog";
        assert_eq!(extract_file_num(file_name, LOG_NUM_LEN).unwrap(), 123);
        assert_eq!(generate_file_name(123, LOG_SUFFIX), file_name);

        let file_name: &str = "00000123.rewrite";
        assert_eq!(extract_file_num(file_name, REWRITE_NUM_LEN).unwrap(), 123);
        assert_eq!(generate_file_name(123, REWRITE_SUFFIX), file_name);

        let invalid_file_name: &str = "123.log";
        assert!(extract_file_num(invalid_file_name, LOG_NUM_LEN).is_err());
        assert!(extract_file_num(invalid_file_name, REWRITE_NUM_LEN).is_err());
    }

    fn test_pipe_log_impl(queue: LogQueue) {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 1024;
        let bytes_per_sync = 32 * 1024;
        let pipe_log = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.first_file_num(queue), INIT_FILE_NUM);
        assert_eq!(pipe_log.active_file_num(queue), INIT_FILE_NUM);

        let header_size = (FILE_MAGIC_HEADER.len() + VERSION.len()) as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        let (file_num, _) = pipe_log.switch_log_file(queue).unwrap();
        let offset = pipe_log.append(queue, &content).unwrap();
        assert_eq!(file_num, 1);
        assert_eq!(offset, header_size);
        assert_eq!(pipe_log.active_file_num(queue), 1);

        let (file_num, _) = pipe_log.switch_log_file(queue).unwrap();
        let offset = pipe_log.append(queue, &content).unwrap();
        assert_eq!(file_num, 2);
        assert_eq!(offset, header_size);
        assert_eq!(pipe_log.active_file_num(queue), 2);

        // purge file 1
        assert_eq!(pipe_log.purge_to(queue, 2).unwrap(), 1);
        assert_eq!(pipe_log.first_file_num(queue), 2);

        // cannot purge active file
        assert!(pipe_log.purge_to(queue, 3).is_err());

        // append position
        let s_content = b"short content".to_vec();
        let (file_num, _) = pipe_log.switch_log_file(queue).unwrap();
        let offset = pipe_log.append(queue, &s_content).unwrap();
        assert_eq!(file_num, 3);
        assert_eq!(offset, header_size);

        let (file_num, _) = pipe_log.switch_log_file(queue).unwrap();
        let offset = pipe_log.append(queue, &s_content).unwrap();
        assert_eq!(file_num, 3);
        assert_eq!(offset, header_size + s_content.len() as u64);

        assert_eq!(
            pipe_log.active_log_size(queue),
            FILE_MAGIC_HEADER.len() + VERSION.len() + 2 * s_content.len()
        );

        // fread
        let content_readed = pipe_log
            .fread(queue, 3, header_size, s_content.len() as u64)
            .unwrap();
        assert_eq!(content_readed, s_content);

        // leave only 1 file to truncate
        assert!(pipe_log.purge_to(queue, 3).is_ok());
        assert_eq!(pipe_log.first_file_num(queue), 3);
        assert_eq!(pipe_log.active_file_num(queue), 3);

        // truncate file
        pipe_log
            .truncate_active_log(queue, Some(FILE_MAGIC_HEADER.len() + VERSION.len()))
            .unwrap();
        assert_eq!(
            pipe_log.active_log_size(queue,),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
        let trunc_big_offset = pipe_log.truncate_active_log(
            queue,
            Some(FILE_MAGIC_HEADER.len() + VERSION.len() + s_content.len()),
        );
        assert!(trunc_big_offset.is_err());

        /**************************
        // read next file
        let mut header: Vec<u8> = vec![];
        header.extend(FILE_MAGIC_HEADER);
        header.extend(VERSION);
        let content = pipe_log.read_next_file().unwrap().unwrap();
        assert_eq!(header, content);
        assert!(pipe_log.read_next_file().unwrap().is_none());
        **************************/

        pipe_log.close().unwrap();

        // reopen
        let pipe_log = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.active_file_num(queue), 3);
        assert_eq!(
            pipe_log.active_log_size(queue),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
        assert_eq!(
            pipe_log.active_log_capacity(queue),
            FILE_MAGIC_HEADER.len() + VERSION.len()
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

    fn write_to_log(
        log: &mut PipeLog,
        submitor: &mut CacheSubmitor,
        batch: &LogBatch<Entry, Entry>,
        file_num: &mut u64,
    ) {
        let mut entries_size = 0;
        if let Some(content) = batch.encode_to_bytes(&mut entries_size) {
            // TODO: `pwrite` is performed in the mutex. Is it possible for concurrence?
            let (cur_file_num, fd) = log.switch_log_file(LogQueue::Append).unwrap();
            let offset = log.append(LogQueue::Append, &content).unwrap();
            let tracker = submitor.get_cache_tracker(cur_file_num, offset);
            submitor.fill_chunk(entries_size);
            for item in &batch.items {
                if let LogItemContent::Entries(ref entries) = item.content {
                    entries.update_position(LogQueue::Append, cur_file_num, offset, &tracker);
                }
            }
            fd.sync().unwrap();
            *file_num = cur_file_num;
        }
    }

    #[test]
    fn test_cache_submitor() {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 6 * 1024; // 6K to rotate.
        let bytes_per_sync = 32 * 1024;
        let mut worker = Worker::new("test".to_owned(), None);
        let mut pipe_log = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        let receiver = worker.take_receiver();
        let stats = Arc::new(GlobalStats::default());
        let mut submitor = CacheSubmitor::new(usize::MAX, 4096, worker.scheduler(), stats);

        let get_1m_batch = || {
            let mut entry = Entry::new();
            entry.set_data(vec![b'a'; 1024]); // 1K data.
            let mut log_batch = LogBatch::<Entry, Entry>::new();
            log_batch.add_entries(1, vec![entry]);
            log_batch
        };

        // Collect `LogBatch`s to avoid `CacheTracker::drop` is called.
        let mut log_batches = Vec::new();
        // Collect received tasks.
        let mut tasks = Vec::new();

        // After 4 batches are written into pipe log, no `CacheTask::NewChunk`
        // task should be triggered. However the last batch will trigger it.
        for i in 0..5 {
            let log_batch = get_1m_batch();
            let mut file_num = 0;
            write_to_log(&mut pipe_log, &mut submitor, &log_batch, &mut file_num);
            log_batches.push(log_batch);
            let x = receiver.recv_timeout(Duration::from_millis(100));
            if i < 4 {
                assert!(x.is_err());
            } else {
                tasks.push(x.unwrap());
            }
        }

        // Write more 2 batches into pipe log. A `CacheTask::NewChunk` will be
        // emit on the second batch because log file is switched.
        for i in 5..7 {
            let log_batch = get_1m_batch();
            let mut file_num = 0;
            write_to_log(&mut pipe_log, &mut submitor, &log_batch, &mut file_num);
            log_batches.push(log_batch);
            let x = receiver.recv_timeout(Duration::from_millis(100));
            if i < 6 {
                assert!(x.is_err());
            } else {
                tasks.push(x.unwrap());
            }
        }

        // Write more batches. No `CacheTask::NewChunk` will be emit because
        // `CacheTracker`s accociated in `EntryIndex`s are droped.
        drop(log_batches);
        for _ in 7..20 {
            let log_batch = get_1m_batch();
            let mut file_num = 0;
            write_to_log(&mut pipe_log, &mut submitor, &log_batch, &mut file_num);
            drop(log_batch);
            assert!(receiver.recv_timeout(Duration::from_millis(100)).is_err());
        }

        // All task's size should be 0 because all cached entries are released.
        for task in &tasks {
            if let Some(CacheTask::NewChunk(ref chunk)) = task {
                chunk.self_check(0);
                continue;
            }
            unreachable!();
        }
    }
}
