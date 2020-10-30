#![feature(shrink_to)]
#![feature(cell_update)]

use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::RwLock;

use crate::pipe_log::{LogQueue, PipeLogHook};
use crate::util::HandyRwLock;

macro_rules! box_err {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<dyn Error + Sync + Send> = format!("[{}:{}]: {}", file!(), line!(),  $e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        box_err!(format!($f, $($arg),+))
    });
}

pub mod codec;

mod cache_evict;
mod config;
mod engine;
mod errors;
mod log_batch;
mod memtable;
mod pipe_log;
mod purge;
mod util;

use crate::pipe_log::PipeLog;

pub use self::config::{Config, RecoveryMode};
pub use self::errors::{Error, Result};
pub use self::log_batch::{EntryExt, LogBatch};
pub use self::util::ReadableSize;
pub type RaftLogEngine<X, Y> = self::engine::Engine<X, Y, PipeLog>;

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}

#[derive(Default)]
pub struct GlobalStats {
    cache_hit: AtomicUsize,
    cache_miss: AtomicUsize,
    cache_size: AtomicUsize,

    // How many operations in the rewrite queue.
    rewrite_operations: AtomicUsize,
    // How many compacted operations in the rewrite queue.
    compacted_rewrite_operations: AtomicUsize,

    // There could be a gap between a write batch is written into a file and applied to memtables.
    // If a log-rewrite happens at the time, entries and kvs in the batch can be omited. So after
    // the log file gets purged, those entries and kvs will be lost.
    //
    // This field is used to forbid the bad case. Log files `active_log_files` can't be purged.
    // It's only used for the append queue. The rewrite queue doesn't need this.
    active_log_files: RwLock<VecDeque<(u64, AtomicUsize)>>,
}

impl GlobalStats {
    pub fn sub_mem_change(&self, bytes: usize) {
        self.cache_size.fetch_sub(bytes, Ordering::Release);
    }
    pub fn add_mem_change(&self, bytes: usize) {
        self.cache_size.fetch_add(bytes, Ordering::Release);
    }
    pub fn add_cache_hit(&self, count: usize) {
        self.cache_hit.fetch_add(count, Ordering::Relaxed);
    }
    pub fn add_cache_miss(&self, count: usize) {
        self.cache_miss.fetch_add(count, Ordering::Relaxed);
    }

    pub fn cache_hit(&self) -> usize {
        self.cache_hit.load(Ordering::Relaxed)
    }
    pub fn cache_miss(&self) -> usize {
        self.cache_miss.load(Ordering::Relaxed)
    }
    pub fn cache_size(&self) -> usize {
        self.cache_size.load(Ordering::Acquire)
    }

    pub fn flush_cache_stats(&self) -> CacheStats {
        CacheStats {
            hit: self.cache_hit.swap(0, Ordering::SeqCst),
            miss: self.cache_miss.swap(0, Ordering::SeqCst),
            cache_size: self.cache_size.load(Ordering::SeqCst),
        }
    }

    pub fn add_rewrite(&self, count: usize) {
        self.rewrite_operations.fetch_add(count, Ordering::Release);
    }
    pub fn add_compacted_rewrite(&self, count: usize) {
        self.compacted_rewrite_operations
            .fetch_add(count, Ordering::Release);
    }
    pub fn rewrite_operations(&self) -> usize {
        self.rewrite_operations.load(Ordering::Acquire)
    }
    pub fn compacted_rewrite_operations(&self) -> usize {
        self.compacted_rewrite_operations.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn reset_cache(&self) {
        self.cache_hit.store(0, Ordering::Relaxed);
        self.cache_miss.store(0, Ordering::Relaxed);
        self.cache_size.store(0, Ordering::Relaxed);
    }
}

impl PipeLogHook for GlobalStats {
    fn post_new_log_file(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.wl();
            if let Some(num) = active_log_files.back().map(|x| x.0) {
                assert_eq!(num + 1, file_num, "active log files should be contiguous");
            }
            let counter = AtomicUsize::new(0);
            active_log_files.push_back((file_num, counter));
        }
    }

    fn on_append_log_file(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(file_num - front) as usize].1;
            counter.fetch_add(1, Ordering::Release);
        }
    }

    fn post_apply_memtables(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(file_num - front) as usize].1;
            counter.fetch_sub(1, Ordering::Release);
        }
    }

    fn ready_for_purge(&self, queue: LogQueue, file_num: u64) -> bool {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            for (i, n) in (front..=file_num).enumerate() {
                assert_eq!(active_log_files[i].0, n);
                let counter = &active_log_files[i].1;
                if counter.load(Ordering::Acquire) > 0 {
                    return false;
                }
            }
        }
        true
    }

    fn post_purge(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.wl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            for x in front..=file_num {
                let (y, counter) = active_log_files.pop_front().unwrap();
                assert_eq!(x, y);
                assert_eq!(counter.load(Ordering::Acquire), 0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::log_batch::EntryExt;
    use raft::eraftpb::Entry;

    #[ctor::ctor]
    fn init() {
        env_logger::init();
    }

    impl EntryExt<Entry> for Entry {
        fn index(e: &Entry) -> u64 {
            e.get_index()
        }
    }
}
