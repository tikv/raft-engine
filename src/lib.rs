#![feature(shrink_to)]
#![feature(cell_update)]

use std::sync::atomic::{AtomicUsize, Ordering};

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

pub use self::config::Config;
pub use self::errors::{Error, Result};
pub use self::log_batch::{EntryExt, LogBatch};
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
