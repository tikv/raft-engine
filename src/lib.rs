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
mod log_file;
mod memtable;
mod metrics;
mod pipe_log;
mod purge;
mod reader;
#[cfg(test)]
mod test_util;
mod util;
mod write_barrier;

pub use config::{Config, RecoveryMode};
pub use errors::{Error, Result};
pub use event_listener::EventListener;
pub use file_builder::FileBuilder;
pub use log_batch::{Command, LogBatch, MessageExt};
pub use pipe_log::{FileId, LogQueue};
pub use util::ReadableSize;
pub type Engine<FileBuilder = file_builder::DefaultFileBuilder> = engine::Engine<FileBuilder>;

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

    pub fn merge(&self, rhs: &Self) {
        self.add_rewrite(rhs.rewrite_operations());
        self.add_compacted_rewrite(rhs.compacted_rewrite_operations())
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
