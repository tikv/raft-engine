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

mod config;
mod engine;
mod errors;
mod event_listener;
mod file_pipe_log;
mod log_batch;
mod memtable;
mod pipe_log;
mod purge;
mod reader;
mod util;

use crate::file_pipe_log::FilePipeLog;

pub use self::config::{Config, RecoveryMode};
pub use self::errors::{Error, Result};
pub use self::log_batch::{EntryExt, LogBatch};
pub use self::util::ReadableSize;
pub type RaftLogEngine<X, Y> = self::engine::Engine<X, Y, FilePipeLog>;

#[derive(Default)]
pub struct GlobalStats {
    // How many operations in the rewrite queue.
    rewrite_operations: AtomicUsize,
    // How many compacted operations in the rewrite queue.
    compacted_rewrite_operations: AtomicUsize,
}

impl GlobalStats {
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
