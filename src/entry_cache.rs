use std::cmp::PartialEq;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender};
use std::sync::{Arc, Mutex};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;
use std::{fmt, mem};

use crate::engine::{MemTableAccessor, SharedCacheStats};
use crate::log_batch::{LogBatch, LogItemContent};
use crate::pipe_log::PipeLog;
use crate::util::HandyRwLock;

pub const DEFAULT_CACHE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

const HIGH_WATER_RATIO: f64 = 0.9;
const LOW_WATER_RATIO: f64 = 0.8;

/// Used in `PipLog` to emit `CacheTask::NewChunk` tasks.
pub struct CacheAgent {
    file_num: u64,
    offset: u64,
    // `chunk_size` is different from `size_tracker`. For a given chunk,
    // the former is monotomically increasing, but the latter can decrease.
    chunk_size: usize,
    size_tracker: Arc<AtomicUsize>,

    task_sender: SyncSender<CacheTask>,
    chunk_limit: usize,
}

impl CacheAgent {
    pub fn get_cache_tracker(
        &mut self,
        file_num: u64,
        offset: u64,
        size: usize,
    ) -> Arc<AtomicUsize> {
        if self.file_num == 0 {
            self.file_num = file_num;
            self.offset = offset;
        }

        if self.chunk_size >= self.chunk_limit || self.file_num < file_num {
            // If all entries are released from cache, the chunk can be ignored.
            if self.size_tracker.load(Ordering::Relaxed) > 0 {
                let mut task = CacheChunk {
                    file_num: self.file_num,
                    base_offset: self.offset,
                    end_offset: offset,
                    size_tracker: self.size_tracker.clone(),
                };
                if file_num != self.file_num {
                    // Log file is switched, use `u64::MAX` as the end.
                    task.end_offset = u64::MAX;
                }
                drop(self.task_sender.send(CacheTask::NewChunk(task)));
            }
            self.reset(file_num, offset);
        }
        self.chunk_size += size;
        self.size_tracker.fetch_add(size, Ordering::SeqCst);
        self.size_tracker.clone()
    }

    fn new(task_sender: SyncSender<CacheTask>, chunk_limit: usize) -> CacheAgent {
        CacheAgent {
            file_num: 0,
            offset: 0,
            chunk_size: 0,
            size_tracker: Arc::new(AtomicUsize::new(0)),
            task_sender,
            chunk_limit,
        }
    }

    fn reset(&mut self, file_num: u64, offset: u64) {
        self.file_num = file_num;
        self.offset = offset;
        self.chunk_size = 0;
        self.size_tracker = Arc::new(AtomicUsize::new(0));
    }
}

#[derive(Clone)]
pub struct EntryCache {
    thread_name: String,

    cache_limit: usize,
    cache_stats: Arc<SharedCacheStats>,

    chunk_limit: usize,

    task_sender: SyncSender<CacheTask>,
    progress: Arc<Mutex<EntryCacheProgress>>,
}

enum EntryCacheProgress {
    NotStarted(Receiver<CacheTask>),
    Started(JoinHandle<()>),
    None,
}

impl EntryCache {
    pub fn new(
        thread_name: String,
        cache_limit: usize,
        cache_stats: Arc<SharedCacheStats>,
        chunk_limit: usize,
    ) -> (EntryCache, CacheAgent) {
        let (tx, rx) = mpsc::sync_channel(1024);
        let cache_agent = CacheAgent::new(tx.clone(), chunk_limit);
        let cache = EntryCache {
            thread_name,
            cache_limit,
            cache_stats,
            chunk_limit,
            task_sender: tx,
            progress: Arc::new(Mutex::new(EntryCacheProgress::NotStarted(rx))),
        };
        (cache, cache_agent)
    }

    #[cfg(test)]
    pub fn new_without_background(
        thread_name: String,
        chunk_limit: usize,
    ) -> (Receiver<CacheTask>, CacheAgent) {
        let (mut cache, agent) = Self::new(
            thread_name,
            usize::MAX,
            Arc::new(SharedCacheStats::default()),
            chunk_limit,
        );
        (cache.take_receiver(), agent)
    }

    pub fn start(&mut self, memtables: MemTableAccessor, pipe_log: PipeLog) {
        let mut progress = self.progress.lock().unwrap();
        let receiver = match mem::replace(&mut *progress, EntryCacheProgress::None) {
            EntryCacheProgress::NotStarted(receiver) => receiver,
            EntryCacheProgress::Started(handle) => {
                *progress = EntryCacheProgress::Started(handle);
                return;
            }
            _ => return,
        };

        let mut inner = EntryCacheInner::new(
            self.cache_limit,
            self.cache_stats.clone(),
            self.chunk_limit,
            memtables,
            pipe_log,
        );
        let handle = ThreadBuilder::new()
            .name(self.thread_name.clone())
            .spawn(move || loop {
                match receiver.recv_timeout(Duration::from_secs(1)) {
                    Ok(CacheTask::Stop) | Err(RecvTimeoutError::Disconnected) => return,
                    Ok(CacheTask::NewChunk(chunk)) => inner.handle_new_chunk(chunk),
                    Err(RecvTimeoutError::Timeout) => inner.on_timeout(),
                }
            })
            .unwrap();
        *progress = EntryCacheProgress::Started(handle);
    }

    pub fn stop(&self) {
        let mut progress = self.progress.lock().unwrap();
        let handle = match mem::replace(&mut *progress, EntryCacheProgress::None) {
            EntryCacheProgress::Started(handle) => handle,
            _ => return,
        };
        drop(progress);

        let _ = self.task_sender.send(CacheTask::Stop);
        if let Err(e) = handle.join() {
            warn!("{} exits accidentally with error {:?}", self.thread_name, e);
        }
    }

    #[cfg(test)]
    fn take_receiver(&mut self) -> Receiver<CacheTask> {
        let mut progress = self.progress.lock().unwrap();
        let x = mem::replace(&mut *progress, EntryCacheProgress::None);
        if let EntryCacheProgress::NotStarted(receiver) = x {
            return receiver;
        }
        unreachable!();
    }
}

struct EntryCacheInner {
    cache_limit: usize,
    cache_stats: Arc<SharedCacheStats>,
    chunk_limit: usize,
    valid_cache_chunks: VecDeque<CacheChunk>,
    memtables: MemTableAccessor,
    pipe_log: PipeLog,
}

impl EntryCacheInner {
    fn new(
        cache_limit: usize,
        cache_stats: Arc<SharedCacheStats>,
        chunk_limit: usize,
        memtables: MemTableAccessor,
        pipe_log: PipeLog,
    ) -> EntryCacheInner {
        EntryCacheInner {
            cache_limit,
            cache_stats,
            chunk_limit,
            valid_cache_chunks: Default::default(),
            memtables,
            pipe_log,
        }
    }

    fn handle_new_chunk(&mut self, chunk: CacheChunk) {
        self.valid_cache_chunks.push_back(chunk);
        self.on_timeout();
    }

    fn on_timeout(&mut self) {
        self.retain_valid_cache();
        if self.cache_reach_high_water() {
            self.evict_oldest_cache();
        }
    }

    fn retain_valid_cache(&mut self) {
        let cache_size = self.cache_stats.cache_size();
        if self.valid_cache_chunks.len() * self.chunk_limit >= cache_size * 3 {
            // There could be many empty chunks.
            self.valid_cache_chunks
                .retain(|chunk| chunk.size_tracker.load(Ordering::Relaxed) > 0);
        }
    }

    fn cache_reach_high_water(&self) -> bool {
        let cache_size = self.cache_stats.cache_size();
        cache_size > (self.cache_limit as f64 * HIGH_WATER_RATIO) as usize
    }

    fn cache_reach_low_water(&self) -> bool {
        let cache_size = self.cache_stats.cache_size();
        cache_size <= (self.cache_limit as f64 * LOW_WATER_RATIO) as usize
    }

    fn evict_oldest_cache(&mut self) {
        while !self.cache_reach_low_water() {
            let chunk = match self.valid_cache_chunks.pop_front() {
                Some(chunk) if chunk.size_tracker.load(Ordering::Relaxed) > 0 => chunk,
                Some(_) => continue,
                _ => break,
            };

            let file_num = chunk.file_num;
            let read_len = if chunk.end_offset == u64::MAX {
                self.pipe_log.file_len(file_num) - chunk.base_offset
            } else {
                chunk.end_offset - chunk.base_offset
            };
            let chunk_content = self
                .pipe_log
                .fread(file_num, chunk.base_offset, read_len)
                .unwrap();

            let mut reader: &[u8] = chunk_content.as_ref();
            let mut offset = chunk.base_offset;
            while let Some(b) = LogBatch::from_bytes(&mut reader, file_num, offset).unwrap() {
                offset += read_len - reader.len() as u64;
                for item in b.items {
                    if let LogItemContent::Entries(entries) = item.content {
                        let gc_cache_to = match entries.entries.last() {
                            Some(entry) => entry.index + 1,
                            None => continue,
                        };
                        if let Some(memtable) = self.memtables.get(item.raft_group_id) {
                            memtable.wl().compact_cache_to(gc_cache_to);
                        }
                    }
                }
            }
        }
    }
}

pub enum CacheTask {
    NewChunk(CacheChunk),
    Stop,
}

pub struct CacheChunk {
    file_num: u64,
    base_offset: u64,
    end_offset: u64,
    size_tracker: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct CacheTracker {
    pub chunk_size: Arc<AtomicUsize>,
    pub sub_on_drop: usize,
}

impl Drop for CacheTracker {
    fn drop(&mut self) {
        self.chunk_size
            .fetch_sub(self.sub_on_drop, Ordering::SeqCst);
    }
}

impl fmt::Debug for CacheTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CacheTracker(size={})", self.sub_on_drop)
    }
}

// `CacheTracker` makes no effects when compare.
impl PartialEq for CacheTracker {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl CacheChunk {
    #[cfg(test)]
    pub fn self_check(&self, expected_chunk_size: usize) {
        assert!(self.end_offset > self.base_offset);
        let chunk_size = self.size_tracker.load(Ordering::Relaxed);
        assert_eq!(chunk_size, expected_chunk_size);
    }
}
