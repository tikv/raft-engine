#![feature(ptr_offset_from)]
#![allow(clippy::missing_safety_doc)]

#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
#[macro_use]
extern crate quick_error;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate prometheus;

#[macro_export]
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
pub mod config;
pub mod engine;
mod errors;
pub mod log_batch;
pub mod memtable;
pub mod metrics;
pub mod pipe_log;
pub mod util;

pub use self::config::Config;
pub use self::engine::FileEngine;
pub use self::errors::{Error, Result};
pub use self::log_batch::LogBatch;

use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;

pub trait RaftEngine: Clone + Sync + Send + 'static {
    type RecoveryMode;
    type LogBatch: RaftLogBatch;

    fn log_batch(&self, capacity: usize) -> Self::LogBatch;

    /// Recover the Raft engine.
    fn recover(&mut self, recovery_mode: Self::RecoveryMode) -> Result<()>;

    /// Synchronize the Raft engine.
    fn sync(&self) -> Result<()>;

    // FIXME: compact only memtable or not?
    /// Compact Raft logs for `raft_group_id` to `index`.
    fn compact_to(&self, raft_group_id: u64, index: u64) -> Result<()>;

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>>;

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>>;

    /// Return total size of fetched entries.
    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize>;

    /// Consume the write batch by moving the content into the engine itself.
    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<()>;
    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        max_capacity: usize,
        shrink_to: usize,
    ) -> Result<()>;

    fn clean(
        &self,
        raft_group_id: u64,
        state: &RaftLocalState,
        batch: &mut Self::LogBatch,
    ) -> Result<()>;

    /// Append some raft entries and return appended bytes.
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&self, raft_group_id: u64, entries: &[Entry]) -> Result<usize>;

    /// Remove Raft logs in [`from`, `to`) which will be overwritten later.
    fn remove(&self, raft_group_id: u64, from: u64, to: u64) -> Result<()>;

    /// Like `remove` but the range could be very large. Return the deleted count.
    fn gc(&self, raft_group_id: u64, from: u64, to: u64) -> Result<usize>;

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;
}

pub trait RaftLogBatch: Send {
    /// Append some raft entries and return appended bytes.
    /// Note: `RaftLocalState` won't be updated in this call.
    fn append(&mut self, raft_group_id: u64, entries: &[Entry]) -> Result<usize>;

    /// Remove Raft logs in [`from`, `to`) which will be overwritten later.
    fn remove(&mut self, raft_group_id: u64, from: u64, to: u64) -> Result<()>;

    fn put_raft_state(&mut self, raft_group_id: u64, state: &RaftLocalState) -> Result<()>;
    fn is_empty(&self) -> bool;
}
