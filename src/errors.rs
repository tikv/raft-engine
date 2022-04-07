// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::error;
use std::io::Error as IoError;

use thiserror::Error;

use crate::codec::Error as CodecError;

#[derive(Debug, Error)]
pub enum Error {
    #[error("Invalid Argument: {0}")]
    InvalidArgument(String),
    #[error("Corruption: {0}")]
    Corruption(String),
    #[error("IO Error: {0}")]
    Io(#[from] IoError),
    #[error("Codec Error: {0}")]
    Codec(#[from] CodecError),
    #[error("Protobuf Error: {0}")]
    Protobuf(#[from] protobuf::ProtobufError),
    #[error("Entry Compacted")]
    EntryCompacted,
    #[error("Entry Not Found")]
    EntryNotFound,
    #[error("Full")]
    Full,
    #[error("Other Error: {0}")]
    Other(#[from] Box<dyn error::Error + Send + Sync>),
}

pub type Result<T> = ::std::result::Result<T, Error>;
