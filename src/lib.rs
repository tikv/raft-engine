#![feature(ptr_offset_from)]

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

use std::io::Error as IoError;
use std::{error, num};

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

quick_error! {
    #[derive(Debug)]
    pub enum Error {
        Other(err: Box<dyn error::Error + Send + Sync>) {
            from()
            cause(err.as_ref())
            description(err.description())
        }
        Io(err: IoError) {
            from()
            cause(err)
            description(err.description())
        }
        Codec(err: codec::Error) {
            from()
            cause(err)
            description(err.description())
            display("Codec {}", err)
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("protobuf error {:?}", err)
        }
        ParseError(err: num::ParseIntError) {
            from()
            cause(err)
            description(err.description())
            display("Parse int error {:?}", err)
        }
        CheckSumError {
            description("checksum is not correct")
        }
        TooShort {
            description("content too short")
        }
    }
}

pub mod codec;
pub mod config;
pub mod engine;
pub mod log_batch;
pub mod memtable;
pub mod metrics;
pub mod pipe_log;
pub mod util;

pub type Result<T> = ::std::result::Result<T, Error>;

pub use self::config::Config;
pub use self::engine::{RaftEngine, RecoveryMode};
pub use self::log_batch::LogBatch;
