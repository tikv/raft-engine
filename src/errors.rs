use std::error;
use std::io::Error as IoError;

use thiserror::Error;

use crate::codec::Error as CodecError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Other(#[from] Box<dyn error::Error + Send + Sync>),
    #[error("{0}")]
    Io(#[from] IoError),
    #[error("Codec {0}")]
    Codec(#[from] CodecError),
    #[error("Protobuf error {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("Parse file name {0} error")]
    ParseFileName(String),
    #[error("Checksum expected {0}, but got {1}")]
    IncorrectChecksum(u32, u32),
    #[error("content too short")]
    TooShort,
    #[error("Raft group not found: {0}")]
    RaftNotFound(u64),
    #[error("Entries index is empty and unavailable to read")]
    StorageUnavailable,
    #[error("The entry acquired has been compacted")]
    StorageCompacted,
    #[error("Corruption: {0}")]
    Corruption(String),
}

pub type Result<T> = ::std::result::Result<T, Error>;
