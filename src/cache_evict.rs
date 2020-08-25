use std::cmp::PartialEq;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex};

use crate::engine::{MemTableAccessor, SharedCacheStats};
use crate::log_batch::{LogBatch, LogItemContent};
use crate::pipe_log::PipeLog;
use crate::util::{HandyRwLock, Runnable, Scheduler};

pub const DEFAULT_CACHE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

const HIGH_WATER_RATIO: f64 = 0.9;
const LOW_WATER_RATIO: f64 = 0.8;
const CHUNKS_SHRINK_TO: usize = 1024;

#[derive(Clone)]
pub struct CacheFullNotifier {
    cache_limit: usize,
    lock_and_cond: Arc<(Mutex<()>, Condvar)>,
}

impl CacheFullNotifier {
    fn new(cache_limit: usize) -> Self {
        CacheFullNotifier {
            cache_limit,
            lock_and_cond: Arc::new((Mutex::new(()), Condvar::new())),
        }
    }
}

/// Used in `PipLog` to emit `CacheTask::NewChunk` tasks.
pub struct CacheSubmitor {
    file_num: u64,
    offset: u64,
    // `chunk_size` is different from `size_tracker`. For a given chunk,
    // the former is monotomically increasing, but the latter can decrease.
    chunk_size: usize,
    size_tracker: Arc<AtomicUsize>,

    scheduler: Scheduler<CacheTask>,
    chunk_limit: usize,
    cache_stats: Arc<SharedCacheStats>,

    cache_full_notifier: Option<CacheFullNotifier>,
}

impl CacheSubmitor {
    pub fn new(
        chunk_limit: usize,
        scheduler: Scheduler<CacheTask>,
        cache_stats: Arc<SharedCacheStats>,
    ) -> Self {
        CacheSubmitor {
            file_num: 0,
            offset: 0,
            chunk_size: 0,
            size_tracker: Arc::new(AtomicUsize::new(0)),
            scheduler,
            chunk_limit,
            cache_stats,
            cache_full_notifier: None,
        }
    }

    pub fn block_on_full(&mut self, notifier: CacheFullNotifier) {
        self.cache_full_notifier = Some(notifier);
    }

    pub fn nonblock_on_full(&mut self) {
        self.cache_full_notifier = None;
    }

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
                let _ = self.scheduler.schedule(CacheTask::NewChunk(task));
            }
            self.reset(file_num, offset);
        }

        if let Some(ref notifier) = self.cache_full_notifier {
            let cache_size = self.cache_stats.cache_size();
            if cache_size > notifier.cache_limit {
                let _ = self.scheduler.schedule(CacheTask::EvictOldest);
                let lock = notifier.lock_and_cond.0.lock().unwrap();
                let _lock = notifier.lock_and_cond.1.wait(lock).unwrap();
            }
        }
        self.chunk_size += size;
        self.size_tracker.fetch_add(size, Ordering::Release);
        self.cache_stats.add_mem_change(size);
        self.size_tracker.clone()
    }

    fn reset(&mut self, file_num: u64, offset: u64) {
        self.file_num = file_num;
        self.offset = offset;
        self.chunk_size = 0;
        self.size_tracker = Arc::new(AtomicUsize::new(0));
    }
}

pub struct Runner {
    cache_limit: usize,
    cache_stats: Arc<SharedCacheStats>,
    chunk_limit: usize,
    valid_cache_chunks: VecDeque<CacheChunk>,
    memtables: MemTableAccessor,
    pipe_log: PipeLog,
    pub cache_full_notifier: CacheFullNotifier,
}

impl Runner {
    pub fn new(
        cache_limit: usize,
        cache_stats: Arc<SharedCacheStats>,
        chunk_limit: usize,
        memtables: MemTableAccessor,
        pipe_log: PipeLog,
    ) -> Runner {
        let notifier = CacheFullNotifier::new(cache_limit);
        Runner {
            cache_limit,
            cache_stats,
            chunk_limit,
            valid_cache_chunks: Default::default(),
            memtables,
            pipe_log,
            cache_full_notifier: notifier,
        }
    }

    fn retain_valid_cache(&mut self) {
        let cache_size = self.cache_stats.cache_size();
        if self.valid_cache_chunks.len() * self.chunk_limit >= cache_size * 3 {
            // There could be many empty chunks.
            self.valid_cache_chunks
                .retain(|chunk| chunk.size_tracker.load(Ordering::Relaxed) > 0);
            if self.valid_cache_chunks.capacity() > CHUNKS_SHRINK_TO {
                self.valid_cache_chunks.shrink_to(CHUNKS_SHRINK_TO);
            }
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

    fn evict_oldest_cache(&mut self) -> bool {
        while !self.cache_reach_low_water() {
            let chunk = match self.valid_cache_chunks.pop_front() {
                Some(chunk) if chunk.size_tracker.load(Ordering::Relaxed) > 0 => chunk,
                Some(_) => continue,
                _ => return false,
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
        true
    }
}

impl Runnable<CacheTask> for Runner {
    fn run(&mut self, task: CacheTask) -> bool {
        match task {
            CacheTask::NewChunk(chunk) => self.valid_cache_chunks.push_back(chunk),
            CacheTask::EvictOldest => {
                self.evict_oldest_cache();
                return false;
            }
        }
        true
    }
    fn on_tick(&mut self) {
        self.retain_valid_cache();
        if self.cache_reach_high_water() && self.evict_oldest_cache() {
            let lock_and_cond = &self.cache_full_notifier.lock_and_cond;
            let _lock = lock_and_cond.0.lock().unwrap();
            lock_and_cond.1.notify_one();
        }
    }
}

#[derive(Clone)]
pub enum CacheTask {
    NewChunk(CacheChunk),
    EvictOldest,
}

#[derive(Clone)]
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
