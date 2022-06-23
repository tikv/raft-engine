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

//! # Raft Engine

#![cfg_attr(feature = "nightly", feature(test))]
#![cfg_attr(feature = "swap", feature(allocator_api))]
#![cfg_attr(feature = "swap", feature(slice_ptr_get))]
#![cfg_attr(feature = "swap", feature(nonnull_slice_from_raw_parts))]
#![cfg_attr(feature = "swap", feature(slice_ptr_len))]
#![cfg_attr(feature = "swap", feature(alloc_layout_extra))]
#![cfg_attr(all(test, feature = "nightly"), feature(map_first_last))]
#![cfg_attr(all(test, feature = "swap"), feature(alloc_error_hook))]
#![cfg_attr(all(test, feature = "swap"), feature(cfg_sanitize))]

#[macro_use]
extern crate lazy_static;
extern crate scopeguard;
#[cfg(feature = "nightly")]
extern crate test;

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

mod codec;
mod config;
mod consistency;
mod engine;
mod errors;
mod event_listener;
mod file_pipe_log;
#[cfg(feature = "scripting")]
mod filter;
mod log_batch;
mod memtable;
mod metrics;
mod pipe_log;
mod purge;
#[cfg(feature = "swap")]
mod swappy_allocator;
#[cfg(test)]
mod test_util;
mod util;
mod write_barrier;

pub mod env;

pub use config::{Config, RecoveryMode};
pub use engine::Engine;
pub use errors::{Error, Result};
pub use log_batch::{Command, LogBatch, MessageExt};
pub use util::ReadableSize;

#[cfg(feature = "internals")]
pub mod internals {
    //! A selective view of key components in Raft Engine. Exported under the
    //! `internals` feature only.
    pub use crate::event_listener::*;
    pub use crate::file_pipe_log::*;
    pub use crate::memtable::*;
    pub use crate::pipe_log::*;
    #[cfg(feature = "swap")]
    pub use crate::swappy_allocator::*;
    pub use crate::write_barrier::*;
}

use std::sync::atomic::{AtomicUsize, Ordering};

#[derive(Default)]
pub struct GlobalStats {
    live_append_entries: AtomicUsize,
    rewrite_entries: AtomicUsize,
    deleted_rewrite_entries: AtomicUsize,
}

impl GlobalStats {
    #[inline]
    pub fn add(&self, queue: pipe_log::LogQueue, count: usize) {
        match queue {
            pipe_log::LogQueue::Append => {
                self.live_append_entries.fetch_add(count, Ordering::Relaxed);
            }
            pipe_log::LogQueue::Rewrite => {
                self.rewrite_entries.fetch_add(count, Ordering::Relaxed);
            }
        }
    }

    #[inline]
    pub fn delete(&self, queue: pipe_log::LogQueue, count: usize) {
        match queue {
            pipe_log::LogQueue::Append => {
                self.live_append_entries.fetch_sub(count, Ordering::Relaxed);
            }
            pipe_log::LogQueue::Rewrite => {
                self.deleted_rewrite_entries
                    .fetch_add(count, Ordering::Relaxed);
            }
        }
    }

    #[inline]
    pub fn rewrite_entries(&self) -> usize {
        self.rewrite_entries.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn deleted_rewrite_entries(&self) -> usize {
        self.deleted_rewrite_entries.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn reset_rewrite_counters(&self) {
        let dop = self.deleted_rewrite_entries.load(Ordering::Relaxed);
        self.deleted_rewrite_entries
            .fetch_sub(dop, Ordering::Relaxed);
        self.rewrite_entries.fetch_sub(dop, Ordering::Relaxed);
    }

    #[inline]
    pub fn live_entries(&self, queue: pipe_log::LogQueue) -> usize {
        match queue {
            pipe_log::LogQueue::Append => self.live_append_entries.load(Ordering::Relaxed),
            pipe_log::LogQueue::Rewrite => {
                let op = self.rewrite_entries.load(Ordering::Relaxed);
                let dop = self.deleted_rewrite_entries.load(Ordering::Relaxed);
                debug_assert!(op >= dop);
                op.saturating_sub(dop)
            }
        }
    }

    #[inline]
    pub fn flush_metrics(&self) {
        metrics::LOG_ENTRY_COUNT
            .rewrite
            .set(self.live_entries(pipe_log::LogQueue::Rewrite) as i64);
        metrics::LOG_ENTRY_COUNT
            .append
            .set(self.live_entries(pipe_log::LogQueue::Append) as i64);
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
