use std::collections::VecDeque;
use std::fs::{self, File};
use std::io::Read;
use std::os::unix::io::RawFd;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, RwLock};
use std::{cmp, u64};

use crate::cache_evict::CacheSubmitor;
use crate::config::Config;
use crate::log_batch::{LogBatch, LogItemContent};
use crate::metrics::*;
use crate::util::HandyRwLock;
use crate::{Error, Result};

use nix::fcntl;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::sys::uio;
use nix::unistd;

const LOG_SUFFIX: &str = ".raftlog";
const LOG_SUFFIX_LEN: usize = 8;
const FILE_NUM_LEN: usize = 16;
const FILE_NAME_LEN: usize = FILE_NUM_LEN + LOG_SUFFIX_LEN;
pub const FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
pub const VERSION: &[u8] = b"v1.0.0";
const INIT_FILE_NUM: u64 = 1;
const DEFAULT_FILES_COUNT: usize = 32;
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

struct LogManager {
    pub first_file_num: u64,
    pub active_file_num: u64,

    pub active_log_fd: RawFd,
    pub active_log_size: usize,
    pub active_log_capacity: usize,
    pub last_sync_size: usize,

    pub all_files: VecDeque<RawFd>,
}

impl LogManager {
    pub fn new() -> Self {
        Self {
            first_file_num: INIT_FILE_NUM,
            active_file_num: INIT_FILE_NUM,
            active_log_fd: 0,
            active_log_size: 0,
            active_log_capacity: 0,
            last_sync_size: 0,
            all_files: VecDeque::with_capacity(DEFAULT_FILES_COUNT),
        }
    }
}

#[derive(Clone)]
pub struct PipeLog {
    dir: String,
    rotate_size: usize,
    bytes_per_sync: usize,

    log_manager: Arc<RwLock<LogManager>>,
    cache_submitor: Arc<Mutex<CacheSubmitor>>,

    // Used when recovering from disk.
    current_read_file_num: u64,
}

impl PipeLog {
    fn new(cfg: &Config, cache_submitor: CacheSubmitor) -> PipeLog {
        PipeLog {
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            log_manager: Arc::new(RwLock::new(LogManager::new())),
            cache_submitor: Arc::new(Mutex::new(cache_submitor)),
            current_read_file_num: 0,
        }
    }

    pub fn open(cfg: &Config, cache_submitor: CacheSubmitor) -> Result<PipeLog> {
        let path = Path::new(&cfg.dir);
        if !path.exists() {
            info!("Create raft log directory: {}", &cfg.dir);
            fs::create_dir(&cfg.dir).map_err::<Error, _>(|e| {
                box_err!("Create raft log directory failed, err: {:?}", e)
            })?;
        }

        if !path.is_dir() {
            return Err(box_err!("Not directory: {}", &cfg.dir));
        }

        let mut min_file_num: u64 = u64::MAX;
        let mut max_file_num: u64 = 0;
        let mut log_files = vec![];
        for entry in fs::read_dir(path)? {
            let entry = entry?;
            let file_path = entry.path();

            if !file_path.is_file() {
                continue;
            }

            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            if file_name.ends_with(LOG_SUFFIX) && file_name.len() == FILE_NAME_LEN {
                let file_num = match extract_file_num(file_name) {
                    Ok(num) => num,
                    Err(_) => {
                        continue;
                    }
                };
                min_file_num = cmp::min(min_file_num, file_num);
                max_file_num = cmp::max(max_file_num, file_num);
                log_files.push(file_name.to_string());
            }
        }

        // Initialize.
        let mut pipe_log = PipeLog::new(cfg, cache_submitor);
        if log_files.is_empty() {
            {
                let mut manager = pipe_log.log_manager.wl();
                let new_fd = new_log_file(&pipe_log.dir, manager.active_file_num)?;
                manager.active_log_fd = new_fd;
                manager.all_files.push_back(new_fd);
            }
            pipe_log.write_header()?;
            return Ok(pipe_log);
        }

        log_files.sort();
        log_files.dedup();
        if log_files.len() as u64 != max_file_num - min_file_num + 1 {
            return Err(box_err!("Corruption occurs"));
        }

        {
            let mut manager = pipe_log.log_manager.wl();
            manager.first_file_num = min_file_num;
            manager.active_file_num = max_file_num;
        }
        pipe_log.open_all_files()?;
        Ok(pipe_log)
    }

    fn open_all_files(&mut self) -> Result<()> {
        let mut manager = self.log_manager.wl();
        let mut current_file = manager.first_file_num;
        while current_file <= manager.active_file_num {
            let mut path = PathBuf::from(&self.dir);
            path.push(generate_file_name(current_file));

            let oflag = if current_file < manager.active_file_num {
                // Open inactive files with readonly mode.
                OFlag::O_RDONLY
            } else {
                // Open active file with readwrite mode.
                OFlag::O_RDWR
            };
            let fd = fcntl::open(&path, oflag, Mode::S_IRWXU)
                .map_err(|e| raft::StorageError::Other(e.into()))?;
            manager.all_files.push_back(fd);
            if current_file == manager.active_file_num {
                manager.active_log_fd = fd;
                manager.active_log_size = unistd::lseek(fd, 0, unistd::Whence::SeekEnd)? as usize;
                manager.active_log_capacity = manager.active_log_size;
            }
            current_file += 1;
        }
        Ok(())
    }

    pub fn fread(&self, file_num: u64, offset: u64, len: u64) -> Result<Vec<u8>> {
        let manager = self.log_manager.rl();
        if file_num < manager.first_file_num || file_num > manager.active_file_num {
            return Err(box_err!("File not exist, file number {}", file_num));
        }

        let mut result = vec![0; len as usize];
        let fd = manager.all_files[(file_num - manager.first_file_num) as usize];
        loop {
            match uio::pread(fd, &mut result, offset as _) {
                Ok(ret) => {
                    if ret != len as usize {
                        return Err(box_err!(
                            "Pread failed, expected return size {}, actual return size {}",
                            len,
                            ret
                        ));
                    }
                    return Ok(result);
                }
                Err(e) => {
                    if e.as_errno() == Some(nix::errno::Errno::EAGAIN) {
                        continue;
                    }
                    return Err(box_err!("pread failed on file {}: {:?}", file_num, e));
                }
            }
        }
    }

    pub fn close(&self) -> Result<()> {
        let _write_lock = self.cache_submitor.lock().unwrap();

        let active_log_size = self.log_manager.rl().active_log_size;
        self.truncate_active_log(active_log_size)?;

        for fd in self.log_manager.rl().all_files.iter() {
            nix::unistd::close(*fd)?
        }
        Ok(())
    }

    fn append(&self, content: &[u8], sync: bool) -> Result<(u64, u64)> {
        let (active_log_fd, mut active_log_size, last_sync_size, file_num, offset) = {
            let manager = self.log_manager.rl();
            (
                manager.active_log_fd,
                manager.active_log_size,
                manager.last_sync_size,
                manager.active_file_num,
                manager.active_log_size as u64,
            )
        };
        {
            // Use fallocate to pre-allocate disk space for active file. fallocate is faster than File::set_len,
            // because it will not fill the space with 0s, but File::set_len does.
            let (new_size, mut active_log_capacity) = {
                let manager = self.log_manager.rl();
                (
                    manager.active_log_size + content.len(),
                    manager.active_log_capacity,
                )
            };
            while active_log_capacity < new_size {
                fcntl::fallocate(
                    active_log_fd,
                    fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE,
                    active_log_capacity as _,
                    FILE_ALLOCATE_SIZE as _,
                )
                .map_err::<Error, _>(|e| {
                    box_err!("Allocate disk space for active log failed : {:?}", e)
                })?;
                {
                    let mut manager = self.log_manager.wl();
                    manager.active_log_capacity += FILE_ALLOCATE_SIZE;
                    active_log_capacity = manager.active_log_capacity;
                }
            }
        }

        let mut remaining_content = content;
        // Write to file
        let mut written_bytes: usize = 0;
        let len = content.len();
        while written_bytes < len {
            match uio::pwrite(active_log_fd, remaining_content, active_log_size as _) {
                Ok(write_ret) => {
                    active_log_size += write_ret;
                    written_bytes += write_ret;
                    remaining_content = &remaining_content[write_ret..];
                    continue;
                }
                Err(e) => {
                    if e.as_errno() == Some(nix::errno::Errno::EAGAIN) {
                        continue;
                    }
                    return Err(box_err!("Write to active log failed, err {:?}", e));
                }
            }
        }
        {
            // Update active log size.
            let mut manager = self.log_manager.wl();
            manager.active_log_size = active_log_size;
        }

        // Sync data if needed.
        if sync
            || self.bytes_per_sync > 0 && active_log_size - last_sync_size >= self.bytes_per_sync
        {
            unistd::fsync(active_log_fd).map_err::<Error, _>(|e| {
                box_err!("Allocate disk space for active log failed : {:?}", e)
            })?;
            {
                // Update last sync size.
                let mut manager = self.log_manager.wl();
                manager.last_sync_size = active_log_size;
            }
        }

        // Rotate if needed
        if active_log_size >= self.rotate_size {
            self.rotate_log()?;
        }

        Ok((file_num, offset))
    }

    fn write_header(&self) -> Result<(u64, u64)> {
        // Write HEADER.
        let mut header = Vec::with_capacity(FILE_MAGIC_HEADER.len() + VERSION.len());
        header.extend_from_slice(FILE_MAGIC_HEADER);
        header.extend_from_slice(VERSION);
        self.append(header.as_slice(), true)
    }

    fn rotate_log(&self) -> Result<()> {
        {
            let active_log_size = {
                let manager = self.log_manager.rl();
                manager.active_log_size
            };
            self.truncate_active_log(active_log_size).unwrap();
        }

        // New log file.
        let next_file_num = {
            let manager = self.log_manager.rl();
            manager.active_file_num + 1
        };
        let new_fd = new_log_file(&self.dir, next_file_num)?;
        {
            let mut manager = self.log_manager.wl();
            manager.all_files.push_back(new_fd);
            manager.active_log_fd = new_fd;
            manager.active_log_size = 0;
            manager.active_log_capacity = 0;
            manager.last_sync_size = 0;
            manager.active_file_num = next_file_num;
        }

        // Write Header
        self.write_header()
            .map_err::<Error, _>(|e| box_err!("Write header failed, err {:?}", e))?;
        Ok(())
    }

    pub fn write(&self, batch: &LogBatch, sync: bool, file_num: &mut u64) -> Result<usize> {
        if let Some(content) = batch.encode_to_bytes() {
            let bytes = content.len();

            let mut cache_submitor = self.cache_submitor.lock().unwrap();
            let (cur_file_num, offset) = self.append(&content, sync)?;
            let entries_size = batch.entries_size();
            let tracker = cache_submitor.get_cache_tracker(cur_file_num, offset, entries_size);
            drop(cache_submitor);

            if let Some(tracker) = tracker {
                for item in &batch.items {
                    if let LogItemContent::Entries(ref entries) = item.content {
                        entries.update_offset_when_needed(cur_file_num, offset);
                        entries.attach_cache_tracker(tracker.clone());
                    }
                }
            }

            *file_num = cur_file_num;
            return Ok(bytes);
        }
        Ok(0)
    }

    pub fn purge_to(&self, file_num: u64) -> Result<usize> {
        let (mut first_file_num, active_file_num) = {
            let manager = self.log_manager.rl();
            (manager.first_file_num, manager.active_file_num)
        };
        PIPE_FILES_COUNT_GAUGE.set((active_file_num - first_file_num + 1) as f64);
        if first_file_num >= file_num {
            debug!("Purge nothing.");
            EXPIRED_FILES_PURGED_HISTOGRAM.observe(0.0);
            return Ok(0);
        }

        if file_num > active_file_num {
            return Err(box_err!("Can't purge active log."));
        }

        let old_first_file_num = first_file_num;
        loop {
            if first_file_num >= file_num {
                break;
            }

            // Pop the oldest file.
            let (old_fd, old_file_num) = {
                let mut manager = self.log_manager.wl();
                manager.first_file_num += 1;
                first_file_num = manager.first_file_num;
                (
                    manager.all_files.pop_front().unwrap(),
                    manager.first_file_num - 1,
                )
            };
            // Close the file.
            unistd::close(old_fd)
                .map_err::<Error, _>(|e| box_err!("close file failed, err {:?}", e))?;

            // Remove the file
            let mut path = PathBuf::from(&self.dir);
            path.push(generate_file_name(old_file_num));
            fs::remove_file(path)?;
        }

        let purged = (first_file_num - old_first_file_num) as usize;

        debug!("purge {} expired files", purged);
        EXPIRED_FILES_PURGED_HISTOGRAM.observe(purged as f64);
        Ok(purged)
    }

    // Shrink file size and synchronize.
    pub fn truncate_active_log(&self, offset: usize) -> Result<()> {
        {
            let manager = self.log_manager.rl();
            assert!(
                manager.active_log_size >= offset,
                "attempt to truncate_active_log({}), but active_log_size is {}",
                offset,
                manager.active_log_size
            );
            if manager.active_log_size == offset {
                return Ok(());
            }
            unistd::ftruncate(manager.active_log_fd, offset as _)
                .map_err::<Error, _>(|e| box_err!("Ftruncate file failed, err {:?}", e))?;
            unistd::fsync(manager.active_log_fd)
                .map_err::<Error, _>(|e| box_err!("Fsync file failed, err : {:?}", e))?;
        }
        {
            let mut manager = self.log_manager.wl();
            manager.active_log_size = offset;
            manager.active_log_capacity = offset;
            manager.last_sync_size = manager.active_log_size;
        }

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let manager = self.log_manager.rl();
        unistd::fsync(manager.active_log_fd)
            .map_err::<Error, _>(|e| box_err!("Fsync file failed, err : {:?}", e))?;
        Ok(())
    }

    #[cfg(test)]
    fn active_log_size(&self) -> usize {
        let manager = self.log_manager.rl();
        manager.active_log_size
    }

    #[cfg(test)]
    fn active_log_capacity(&self) -> usize {
        let manager = self.log_manager.rl();
        manager.active_log_capacity
    }

    pub fn active_file_num(&self) -> u64 {
        let manager = self.log_manager.rl();
        manager.active_file_num
    }

    pub fn first_file_num(&self) -> u64 {
        let manager = self.log_manager.rl();
        manager.first_file_num
    }

    pub fn total_size(&self) -> usize {
        let manager = self.log_manager.rl();
        (manager.active_file_num - manager.first_file_num) as usize * self.rotate_size
            + manager.active_log_size
    }

    pub fn read_next_file(&mut self) -> Result<Option<Vec<u8>>> {
        let manager = self.log_manager.rl();
        if self.current_read_file_num == 0 {
            self.current_read_file_num = manager.first_file_num;
        }

        if self.current_read_file_num > manager.active_file_num {
            return Ok(None);
        }

        let mut path = PathBuf::from(&self.dir);
        path.push(generate_file_name(self.current_read_file_num));
        self.current_read_file_num += 1;
        let meta = fs::metadata(&path)?;
        let mut vec = Vec::with_capacity(meta.len() as usize);

        // Read the whole file.
        let mut file = File::open(&path)?;
        file.read_to_end(&mut vec)?;
        Ok(Some(vec))
    }

    /// Return the last file number before `total - size`. `0` means no such files.
    pub fn latest_file_before(&self, size: usize) -> u64 {
        let cur_size = self.total_size();
        if cur_size <= size {
            return 0;
        }
        let count = (cur_size - size) / self.rotate_size;
        let manager = self.log_manager.rl();
        manager.first_file_num + count as u64
    }

    pub fn file_len(&self, file_num: u64) -> u64 {
        let mgr = self.log_manager.rl();
        let fd = mgr.all_files[(file_num - mgr.first_file_num) as usize];
        let offset = unsafe { libc::lseek(fd, 0, libc::SEEK_END) };
        assert!(offset > 0);
        offset as u64
    }

    pub fn cache_submitor(&self) -> MutexGuard<CacheSubmitor> {
        self.cache_submitor.lock().unwrap()
    }
}

fn new_log_file(dir: &str, file_num: u64) -> Result<RawFd> {
    let mut path = PathBuf::from(dir);
    path.push(generate_file_name(file_num));
    fcntl::open(
        &path,
        OFlag::O_RDWR | OFlag::O_CREAT,
        Mode::S_IRUSR | Mode::S_IWUSR,
    )
    .map_err(|e| e.into())
}

fn generate_file_name(file_num: u64) -> String {
    format!("{:016}{}", file_num, LOG_SUFFIX)
}

fn extract_file_num(file_name: &str) -> Result<u64> {
    match file_name[..FILE_NUM_LEN].parse::<u64>() {
        Ok(num) => Ok(num),
        Err(_) => Err(Error::ParseFileName(file_name.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crossbeam::channel::Receiver;
    use raft::eraftpb::Entry;
    use tempfile::Builder;

    use super::*;
    use crate::cache_evict::{CacheSubmitor, CacheTask};
    use crate::engine::SharedCacheStats;
    use crate::util::{ReadableSize, Worker};

    fn new_test_pipe_log(
        path: &str,
        bytes_per_sync: usize,
        rotate_size: usize,
    ) -> (PipeLog, Receiver<Option<CacheTask>>) {
        let mut cfg = Config::default();
        cfg.dir = path.to_owned();
        cfg.bytes_per_sync = ReadableSize(bytes_per_sync as u64);
        cfg.target_file_size = ReadableSize(rotate_size as u64);

        let mut worker = Worker::new("test".to_owned(), None);
        let stats = Arc::new(SharedCacheStats::default());
        let submitor = CacheSubmitor::new(usize::MAX, 4096, worker.scheduler(), stats);
        let log = PipeLog::open(&cfg, submitor).unwrap();
        (log, worker.take_receiver())
    }

    #[test]
    fn test_file_name() {
        let file_name: &str = "0000000000000123.raftlog";
        assert_eq!(extract_file_num(file_name).unwrap(), 123);
        assert_eq!(generate_file_name(123), file_name);

        let invalid_file_name: &str = "000000000000abc123.log";
        assert!(extract_file_num(invalid_file_name).is_err());
    }

    #[test]
    fn test_pipe_log() {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 1024;
        let bytes_per_sync = 32 * 1024;
        let (mut pipe_log, _) = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.first_file_num(), INIT_FILE_NUM);
        assert_eq!(pipe_log.active_file_num(), INIT_FILE_NUM);

        let header_size = (FILE_MAGIC_HEADER.len() + VERSION.len()) as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        assert_eq!(
            pipe_log.append(content.as_slice(), false).unwrap(),
            (1, header_size)
        );
        assert_eq!(pipe_log.active_file_num(), 2);
        assert_eq!(
            pipe_log.append(content.as_slice(), false).unwrap(),
            (2, header_size)
        );
        assert_eq!(pipe_log.active_file_num(), 3);

        // purge file 1
        pipe_log.purge_to(2).unwrap();
        assert_eq!(pipe_log.first_file_num(), 2);

        // purge file 2
        pipe_log.purge_to(3).unwrap();
        assert_eq!(pipe_log.first_file_num(), 3);

        // cannot purge active file
        assert!(pipe_log.purge_to(4).is_err());

        // append position
        let s_content = b"short content";
        assert_eq!(
            pipe_log.append(s_content.as_ref(), false).unwrap(),
            (3, header_size)
        );
        assert_eq!(
            pipe_log.append(s_content.as_ref(), false).unwrap(),
            (3, header_size + s_content.len() as u64)
        );
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len() + 2 * s_content.len()
        );

        // fread
        let content_readed = pipe_log
            .fread(3, header_size, s_content.len() as u64)
            .unwrap();
        assert_eq!(content_readed.as_slice(), s_content.as_ref());

        // truncate file
        pipe_log
            .truncate_active_log(FILE_MAGIC_HEADER.len() + VERSION.len())
            .unwrap();
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
        let trunc_big_offset = std::panic::catch_unwind(|| {
            pipe_log.truncate_active_log(FILE_MAGIC_HEADER.len() + VERSION.len() + s_content.len())
        });
        assert!(trunc_big_offset.is_err());

        // read next file
        let mut header: Vec<u8> = vec![];
        header.extend(FILE_MAGIC_HEADER);
        header.extend(VERSION);
        let content = pipe_log.read_next_file().unwrap().unwrap();
        assert_eq!(header, content);
        assert!(pipe_log.read_next_file().unwrap().is_none());

        pipe_log.close().unwrap();

        // reopen
        let (pipe_log, _) = new_test_pipe_log(path, bytes_per_sync, rotate_size);
        assert_eq!(pipe_log.active_file_num(), 3);
        assert_eq!(
            pipe_log.active_log_size(),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
        assert_eq!(
            pipe_log.active_log_capacity(),
            FILE_MAGIC_HEADER.len() + VERSION.len()
        );
    }

    #[test]
    fn test_cache_submitor() {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();

        let rotate_size = 6 * 1024; // 6K to rotate.
        let bytes_per_sync = 32 * 1024;
        let (pipe_log, receiver) = new_test_pipe_log(path, bytes_per_sync, rotate_size);

        let get_1m_batch = || {
            let mut entry = Entry::new();
            entry.set_data(vec![b'a'; 1024]); // 1K data.
            let mut log_batch = LogBatch::new();
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
            pipe_log.write(&log_batch, true, &mut file_num).unwrap();
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
            pipe_log.write(&log_batch, true, &mut file_num).unwrap();
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
            pipe_log.write(&log_batch, true, &mut file_num).unwrap();
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
