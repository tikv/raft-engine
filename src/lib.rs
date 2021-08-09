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

#[macro_use]
extern crate lazy_static;

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
mod metrics;
mod pipe_log;
mod purge;
mod reader;
mod util;

use crate::file_pipe_log::FilePipeLog;

pub use self::config::{Config, RecoveryMode};
pub use self::errors::{Error, Result};
pub use self::log_batch::{Command, LogBatch, MessageExt};
pub use self::util::ReadableSize;
pub type RaftLogEngine<X> = self::engine::Engine<X, FilePipeLog>;

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
