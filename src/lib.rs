#![feature(shrink_to)]

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

mod cache_evict;
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
pub use self::engine::FileEngine as RaftLogEngine;
pub use self::errors::{Error, Result};
pub use self::log_batch::{EntryExt, LogBatch};

#[derive(Clone, Copy, Default)]
pub struct CacheStats {
    pub hit: usize,
    pub miss: usize,
    pub cache_size: usize,
}
