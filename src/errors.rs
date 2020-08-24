use std::error;
use std::io::Error as IoError;

use crate::codec::Error as CodecError;

use raft::StorageError;

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
        Codec(err: CodecError) {
            from()
            cause(err)
            description(err.description())
            display("Codec {}", err)
        }
        Protobuf(err: protobuf::ProtobufError) {
            from()
            cause(err)
            description(err.description())
            display("Protobuf error {:?}", err)
        }
        ParseFileName(file_name: String) {
            description("Parse file name fail")
            display("Parse file name {} error", file_name)
        }
        IncorrectChecksum(expected: u32, got: u32) {
            description("Checksum is not correct")
            display("Checksum expected {}, but got {}", expected, got)
        }
        TooShort {
            description("content too short")
        }
        RaftNotFound(raft_group_id: u64) {
            description("Raft group not found")
            display("Raft group not found: {}", raft_group_id)
        }
        Storage(err: StorageError) {
            from()
            cause(err)
            description(err.description())
        }
    }
}

impl From<Error> for raft::Error {
    fn from(err: Error) -> raft::Error {
        match err {
            Error::Storage(e) => raft::Error::Store(e),
            e => {
                let boxed = Box::new(e) as Box<dyn std::error::Error + Sync + Send>;
                raft::Error::Store(StorageError::Other(boxed))
            }
        }
    }
}

impl From<nix::Error> for Error {
    fn from(err: nix::Error) -> Error {
        raft::StorageError::Other(err.into()).into()
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
