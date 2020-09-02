use std::error;
use std::io::Error as IoError;

use crate::codec::Error as CodecError;

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
        StorageUnavailable {
            description("Entries index is empty and unavailable to read")
        }
        StorageCompacted {
            description("The entry acquired has been compacted")
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;
