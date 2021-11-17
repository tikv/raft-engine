// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::BufRead;
use std::path::{Path, PathBuf};

use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};

use crate::codec::{self, NumberEncoder};
use crate::pipe_log::{FileId, LogQueue};
use crate::{Error, Result};

// File name.
const LOG_NUM_LEN: usize = 16;
const LOG_APPEND_SUFFIX: &str = ".raftlog";
const LOG_REWRITE_SUFFIX: &str = ".rewrite";
// File header.
const LOG_FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
const LOG_FILE_HEADER_LEN: usize = LOG_FILE_MAGIC_HEADER.len() + std::mem::size_of::<Version>();

pub trait FileNameExt: Sized {
    fn parse_file_name(file_name: &str) -> Option<Self>;

    fn build_file_name(&self) -> String;

    fn build_file_path<P: AsRef<Path>>(&self, dir: P) -> PathBuf {
        let mut path = PathBuf::from(dir.as_ref());
        path.push(self.build_file_name());
        path
    }
}

impl FileNameExt for FileId {
    fn parse_file_name(file_name: &str) -> Option<FileId> {
        if file_name.len() > LOG_NUM_LEN {
            if let Ok(seq) = file_name[..LOG_NUM_LEN].parse::<u64>() {
                if file_name.ends_with(LOG_APPEND_SUFFIX) {
                    return Some(FileId {
                        queue: LogQueue::Append,
                        seq,
                    });
                } else if file_name.ends_with(LOG_REWRITE_SUFFIX) {
                    return Some(FileId {
                        queue: LogQueue::Rewrite,
                        seq,
                    });
                }
            }
        }
        None
    }

    fn build_file_name(&self) -> String {
        match self.queue {
            LogQueue::Append => format!(
                "{:0width$}{}",
                self.seq,
                LOG_APPEND_SUFFIX,
                width = LOG_NUM_LEN
            ),
            LogQueue::Rewrite => format!(
                "{:0width$}{}",
                self.seq,
                LOG_REWRITE_SUFFIX,
                width = LOG_NUM_LEN
            ),
        }
    }
}

pub fn lock_file_path<P: AsRef<Path>>(dir: P) -> PathBuf {
    let mut path = PathBuf::from(dir.as_ref());
    path.push("LOCK");
    path
}

#[derive(Clone, Copy, FromPrimitive, ToPrimitive)]
#[repr(u64)]
enum Version {
    V1 = 1,
}

pub struct LogFileHeader {
    version: Version,
}

impl Default for LogFileHeader {
    fn default() -> Self {
        Self {
            version: Version::V1,
        }
    }
}

impl LogFileHeader {
    #[inline]
    pub fn len() -> usize {
        LOG_FILE_HEADER_LEN
    }

    pub fn decode(buf: &mut &[u8]) -> Result<LogFileHeader> {
        if buf.len() < LOG_FILE_HEADER_LEN {
            return Err(Error::Corruption("log file header too short".to_owned()));
        }
        if !buf.starts_with(LOG_FILE_MAGIC_HEADER) {
            return Err(Error::Corruption(
                "log file magic header mismatch".to_owned(),
            ));
        }
        buf.consume(LOG_FILE_MAGIC_HEADER.len());
        let v = codec::decode_u64(buf)?;
        if let Some(version) = Version::from_u64(v) {
            Ok(Self { version })
        } else {
            Err(Error::Corruption(format!(
                "unrecognized log file version: {}",
                v
            )))
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
        buf.encode_u64(self.version.to_u64().unwrap())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_name() {
        let file_name: &str = "0000000000000123.raftlog";
        let file_id = FileId {
            queue: LogQueue::Append,
            seq: 123,
        };
        assert_eq!(FileId::parse_file_name(file_name).unwrap(), file_id,);
        assert_eq!(file_id.build_file_name(), file_name);

        let file_name: &str = "0000000000000123.rewrite";
        let file_id = FileId {
            queue: LogQueue::Rewrite,
            seq: 123,
        };
        assert_eq!(FileId::parse_file_name(file_name).unwrap(), file_id,);
        assert_eq!(file_id.build_file_name(), file_name);

        let invalid_cases = vec!["0000000000000123.log", "123.rewrite"];
        for case in invalid_cases {
            assert!(FileId::parse_file_name(case).is_none());
        }
    }
}
