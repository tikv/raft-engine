use std::cmp::PartialEq;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use protobuf::Message;

use crate::engine::{MemTableAccessor, SharedCacheStats};
use crate::log_batch::EntryExt;
use crate::util::{HandyRwLock, Runnable, Scheduler};

const HIGH_WATER_RATIO: f64 = 0.9;
const LOW_WATER_RATIO: f64 = 0.8;
const CHUNKS_SHRINK_TO: usize = 1024;

/// Used in `PipLog` to emit `CacheTask::NewChunk` tasks.
pub struct CacheSubmitor {
    file_num: u64,
    size_tracker: Arc<AtomicUsize>,
    group_infos: Vec<(u64, u64)>,

    scheduler: Scheduler<CacheChunk>,
    cache_limit: usize,
    cache_stats: Arc<SharedCacheStats>,
    block_on_full: bool,
}

impl CacheSubmitor {
    pub fn new(
        cache_limit: usize,
        scheduler: Scheduler<CacheChunk>,
        cache_stats: Arc<SharedCacheStats>,
    ) -> Self {
        CacheSubmitor {
            file_num: 0,
            group_infos: vec![],
            size_tracker: Arc::new(AtomicUsize::new(0)),
            scheduler,
            cache_limit,
            cache_stats,
            block_on_full: false,
        }
    }

    pub fn block_on_full(&mut self) {
        self.block_on_full = true;
    }

    pub fn nonblock_on_full(&mut self) {
        self.block_on_full = false;
    }

    pub fn get_cache_tracker(&mut self, file_num: u64) -> Option<Arc<AtomicUsize>> {
        if self.cache_limit == 0 {
            return None;
        }
        if self.file_num != file_num {
            // If all entries are released from cache, the chunk can be ignored.
            if self.size_tracker.load(Ordering::Relaxed) > 0 {
                let group_infos = std::mem::replace(&mut self.group_infos, vec![]);
                let task = CacheChunk {
                    file_num: self.file_num,
                    size_tracker: self.size_tracker.clone(),
                    group_infos,
                };
                let _ = self.scheduler.schedule(task);
            }
            self.reset(file_num);
        }

        Some(self.size_tracker.clone())
    }

    pub fn fill_cache(&mut self, size: usize, group_infos: &mut Vec<(u64, u64)>) {
        if self.cache_limit != 0 {
            self.size_tracker.fetch_add(size, Ordering::Release);
            self.cache_stats.add_mem_change(size);
            self.group_infos.append(group_infos);
        }
    }

    fn reset(&mut self, file_num: u64) {
        self.file_num = file_num;
        self.size_tracker = Arc::new(AtomicUsize::new(0));
    }
}

pub struct Runner<E, W>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
{
    cache_limit: usize,
    cache_stats: Arc<SharedCacheStats>,
    chunk_limit: usize,
    valid_cache_chunks: VecDeque<CacheChunk>,
    memtables: MemTableAccessor<E, W>,
}

impl<E, W> Runner<E, W>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
{
    pub fn new(
        cache_limit: usize,
        cache_stats: Arc<SharedCacheStats>,
        chunk_limit: usize,
        memtables: MemTableAccessor<E, W>,
    ) -> Runner<E, W> {
        Runner {
            cache_limit,
            cache_stats,
            chunk_limit,
            valid_cache_chunks: Default::default(),
            memtables,
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

            for (group_id, index) in chunk.group_infos {
                if let Some(memtable) = self.memtables.get(group_id) {
                    memtable.wl().compact_cache_to(index);
                }
            }
        }
        true
    }
}

impl<E, W> Runnable<CacheChunk> for Runner<E, W>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
{
    fn run(&mut self, chunk: CacheChunk) -> bool {
        self.valid_cache_chunks.push_back(chunk);
        true
    }
    fn on_tick(&mut self) {
        self.retain_valid_cache();
        if self.cache_reach_high_water() {
            self.evict_oldest_cache();
        }
    }
}

#[derive(Clone)]
pub struct CacheChunk {
    file_num: u64,
    size_tracker: Arc<AtomicUsize>,
    group_infos: Vec<(u64, u64)>,
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
