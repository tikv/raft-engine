// Copyright (c) 2017-present, PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

#![feature(shrink_to)]
#![feature(cell_update)]
#![feature(generic_associated_types)]
#![feature(test)]

#[macro_use]
extern crate lazy_static;
extern crate test;

use std::sync::atomic::{AtomicUsize, Ordering};

macro_rules! box_err {
    ($e:expr) => ({
        use std::error::Error;
        let e: Box<dyn Error + Send + Sync> = format!("[{}:{}]: {}", file!(), line!(),  $e).into();
        e.into()
    });
    ($f:tt, $($arg:expr),+) => ({
        box_err!(format!($f, $($arg),+))
    });
}

pub mod codec;

mod config;
mod consistency;
mod engine;
mod errors;
mod event_listener;
mod file_builder;
mod file_pipe_log;
mod log_batch;
mod memtable;
mod metrics;
mod pipe_log;
mod purge;
#[cfg(test)]
mod test_util;
mod util;
mod write_barrier;

pub use config::{Config, RecoveryMode};
pub use errors::{Error, Result};
pub use event_listener::EventListener;
pub use file_builder::FileBuilder;
pub use log_batch::{Command, LogBatch, MessageExt};
pub use pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue};
pub use util::ReadableSize;
pub type Engine<FileBuilder = file_builder::DefaultFileBuilder> = engine::Engine<FileBuilder>;

#[derive(Default)]
pub struct GlobalStats {
    entries: [AtomicUsize; 2],
    deleted_entries: [AtomicUsize; 2],
}

impl GlobalStats {
    #[inline]
    pub fn add(&self, queue: LogQueue, count: usize) {
        self.entries[queue as usize].fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn delete(&self, queue: LogQueue, count: usize) {
        self.deleted_entries[queue as usize].fetch_add(count, Ordering::Relaxed);
    }

    #[inline]
    pub fn entries(&self, queue: LogQueue) -> usize {
        self.entries[queue as usize].load(Ordering::Relaxed)
    }

    #[inline]
    pub fn deleted_entries(&self, queue: LogQueue) -> usize {
        self.deleted_entries[queue as usize].load(Ordering::Relaxed)
    }

    #[inline]
    pub fn live_entries(&self, queue: LogQueue) -> usize {
        let op = self.entries[queue as usize].load(Ordering::Relaxed);
        let cop = self.entries[queue as usize].load(Ordering::Relaxed);
        debug_assert!(op >= cop);
        op.saturating_sub(cop)
    }

    #[inline]
    pub fn merge(&self, rhs: &Self) {
        for queue in [LogQueue::Append, LogQueue::Rewrite] {
            self.add(queue, rhs.entries(queue));
            self.delete(queue, rhs.deleted_entries(queue));
        }
    }

    #[inline]
    pub fn flush_metrics(&self) {
        let mut total_entries = 0;
        let mut tombstones = 0;
        for queue in [LogQueue::Append, LogQueue::Rewrite] {
            total_entries += self.entries(queue);
            tombstones += self.deleted_entries(queue);
        }
        metrics::LOG_ENTRY_COUNT.with_label_values(&["total"]).set(total_entries as i64);
        metrics::LOG_ENTRY_COUNT.with_label_values(&["tombstone"]).set(tombstones as i64);
    }
}

#[cfg(test)]
mod tests {
    use crate::log_batch::MessageExt;
    use raft::eraftpb::Entry;

    #[ctor::ctor]
    fn init() {
        env_logger::init();
    }

    impl MessageExt for Entry {
        type Entry = Entry;

        fn index(e: &Self::Entry) -> u64 {
            e.index
        }
    }
}
