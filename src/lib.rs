#![feature(shrink_to)]
#![feature(cell_update)]

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
