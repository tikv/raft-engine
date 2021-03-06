use std::cmp::PartialEq;
use std::collections::VecDeque;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crossbeam::channel::{bounded, Sender};
use log::{debug, warn};
use protobuf::Message;

use crate::engine::MemTableAccessor;
use crate::log_batch::{EntryExt, LogBatch, LogItemContent};
use crate::pipe_log::{FileId, LogQueue, PipeLog};
use crate::util::{HandyRwLock, Runnable, Scheduler};
use crate::{GlobalStats, Result};

pub const DEFAULT_CACHE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

const HIGH_WATER_RATIO: f64 = 0.9;
const LOW_WATER_RATIO: f64 = 0.8;
const CHUNKS_SHRINK_TO: usize = 1024;

/// Used in `PipLog` to emit `CacheTask::NewChunk` tasks.
pub struct CacheSubmitor {
    cache_limit: usize,
    chunk_limit: usize,

    file_id: FileId,
    offset: u64,

    chunk_size: usize,
    // Cached size of current chunk.
    size_tracker: Arc<AtomicUsize>,

    scheduler: Scheduler<CacheTask>,

    global_stats: Arc<GlobalStats>,
    block_on_full: bool,
}

impl CacheSubmitor {
    pub fn new(
        cache_limit: usize,
        chunk_limit: usize,
        scheduler: Scheduler<CacheTask>,
        global_stats: Arc<GlobalStats>,
    ) -> Self {
        CacheSubmitor {
            cache_limit,
            chunk_limit,
            file_id: Default::default(),
            offset: 0,
            chunk_size: 0,
            size_tracker: Arc::new(AtomicUsize::new(0)),
            scheduler,
            global_stats,
            block_on_full: false,
        }
    }

    pub fn set_block_on_full(&mut self, block_on_full: bool) {
        self.block_on_full = block_on_full;
    }

    /// Returns the cache chunk info for newly appended block.
    pub fn submit_new_block(
        &mut self,
        file_id: FileId,
        offset: u64,
        size: usize,
    ) -> Option<ChunkCacheInfo> {
        if self.cache_limit == 0 || size == 0 {
            return None;
        }

        if !self.file_id.valid() {
            self.file_id = file_id;
            self.offset = offset;
        }

        if self.chunk_size >= self.chunk_limit || self.file_id != file_id {
            debug_assert!(self.file_id <= file_id);
            // If all entries are released from cache, the chunk can be ignored.
            if self.size_tracker.load(Ordering::Relaxed) > 0 {
                let mut task = CacheChunk {
                    file_id: self.file_id,
                    base_offset: self.offset,
                    end_offset: offset,
                    size_tracker: self.size_tracker.clone(),
                };
                if file_id != self.file_id {
                    // Log file is switched, use `u64::MAX` as the end.
                    task.end_offset = u64::MAX;
                }
                if let Err(e) = self.scheduler.schedule(CacheTask::NewChunk(task)) {
                    warn!("Failed to schedule cache chunk to evictor: {}", e);
                }
            }
            self.reset(file_id, offset);
        }

        if self.block_on_full {
            let cache_size = self.global_stats.cache_size();
            if cache_size > self.cache_limit {
                let (tx, rx) = bounded(1);
                if self.scheduler.schedule(CacheTask::EvictOldest(tx)).is_ok() {
                    let _ = rx.recv();
                }
            }
        }

        self.chunk_size += size;
        self.size_tracker.fetch_add(size, Ordering::Release);
        Some(ChunkCacheInfo::new(
            self.global_stats.clone(),
            self.size_tracker.clone(),
        ))
    }

    fn reset(&mut self, file_id: FileId, offset: u64) {
        self.file_id = file_id;
        self.offset = offset;
        self.chunk_size = 0;
        self.size_tracker = Arc::new(AtomicUsize::new(0));
    }
}

pub struct Runner<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
    P: PipeLog,
{
    cache_limit: usize,
    chunk_limit: usize,
    global_stats: Arc<GlobalStats>,
    valid_cache_chunks: VecDeque<CacheChunk>,
    memtables: MemTableAccessor<E, W>,
    pipe_log: P,
}

impl<E, W, P> Runner<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
    P: PipeLog,
{
    pub fn new(
        cache_limit: usize,
        chunk_limit: usize,
        global_stats: Arc<GlobalStats>,
        memtables: MemTableAccessor<E, W>,
        pipe_log: P,
    ) -> Runner<E, W, P> {
        Runner {
            cache_limit,
            chunk_limit,
            global_stats,
            valid_cache_chunks: Default::default(),
            memtables,
            pipe_log,
        }
    }

    fn retain_valid_cache(&mut self) {
        let cache_size = self.global_stats.cache_size();
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
        let cache_size = self.global_stats.cache_size();
        cache_size > (self.cache_limit as f64 * HIGH_WATER_RATIO) as usize
    }

    fn cache_reach_low_water(&self) -> bool {
        let cache_size = self.global_stats.cache_size();
        cache_size <= (self.cache_limit as f64 * LOW_WATER_RATIO) as usize
    }

    fn evict_oldest_cache(&mut self) -> bool {
        while !self.cache_reach_low_water() {
            let chunk = match self.valid_cache_chunks.pop_front() {
                Some(chunk) if chunk.size_tracker.load(Ordering::Relaxed) > 0 => chunk,
                Some(_) => continue,
                _ => return false,
            };

            let (read_len, chunk_content) = match self.read_chunk(&chunk) {
                Ok((len, content)) => (len, content),
                Err(e) => {
                    debug!("Evictor read chunk {:?} fail: {}", chunk, e);
                    continue;
                }
            };

            let mut reader: &[u8] = chunk_content.as_ref();
            let (file_id, mut offset) = (chunk.file_id, chunk.base_offset);
            while let Some(b) = LogBatch::<E, W>::from_bytes(&mut reader, file_id, offset).unwrap()
            {
                offset += read_len - reader.len() as u64;
                for item in b.items {
                    if let LogItemContent::Entries(entries) = item.content {
                        let gc_cache_to = match entries.entries.last() {
                            Some(entry) => W::index(entry) + 1,
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

    fn read_chunk(&self, chunk: &CacheChunk) -> Result<(u64, Vec<u8>)> {
        let read_len = if chunk.end_offset == u64::MAX {
            let file_size = self.pipe_log.file_size(LogQueue::Append, chunk.file_id)?;
            file_size - chunk.base_offset
        } else {
            chunk.end_offset - chunk.base_offset
        };

        let content = self.pipe_log.read_bytes(
            LogQueue::Append,
            chunk.file_id,
            chunk.base_offset,
            read_len,
        )?;
        Ok((read_len, content))
    }
}

impl<E, W, P> Runnable<CacheTask> for Runner<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
    P: PipeLog,
{
    fn run(&mut self, task: CacheTask) -> bool {
        match task {
            CacheTask::NewChunk(chunk) => self.valid_cache_chunks.push_back(chunk),
            CacheTask::EvictOldest(tx) => {
                assert!(self.evict_oldest_cache());
                let _ = tx.send(());
                return false;
            }
        }
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
pub enum CacheTask {
    NewChunk(CacheChunk),
    EvictOldest(Sender<()>),
}

#[derive(Clone, Debug)]
pub struct CacheChunk {
    file_id: FileId,
    base_offset: u64,
    end_offset: u64,
    size_tracker: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct ChunkCacheInfo {
    global_stats: Arc<GlobalStats>,
    chunk_size: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct CacheTracker {
    chunk_info: ChunkCacheInfo,
    block_size: usize,
}

impl ChunkCacheInfo {
    pub fn new(global_stats: Arc<GlobalStats>, chunk_size: Arc<AtomicUsize>) -> Self {
        ChunkCacheInfo {
            global_stats,
            chunk_size,
        }
    }

    pub fn spawn_tracker(&self, block_size: usize) -> CacheTracker {
        let chunk_info = self.clone();
        chunk_info.global_stats.add_mem_change(block_size);
        CacheTracker {
            chunk_info: self.clone(),
            block_size,
        }
    }

    pub fn on_block_dropped(&self, block_size: usize) {
        self.global_stats.sub_mem_change(block_size);
        self.chunk_size.fetch_sub(block_size, Ordering::Release);
    }
}

impl Drop for CacheTracker {
    fn drop(&mut self) {
        if self.block_size > 0 {
            self.chunk_info.on_block_dropped(self.block_size);
        }
    }
}

impl fmt::Debug for CacheTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "CacheTracker(size={})", self.block_size)
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
        let chunk_size = self.size_tracker.load(Ordering::Acquire);
        assert_eq!(chunk_size, expected_chunk_size);
    }
}
