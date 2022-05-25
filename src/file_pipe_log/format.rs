// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Representations of objects in filesystem.

use std::io::BufRead;
use std::path::{Path, PathBuf};

use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};

use crate::codec::{self, NumberEncoder};
use crate::pipe_log::{FileId, LogQueue};
use crate::{Error, Result};

/// Width to format log sequence number.
const LOG_SEQ_WIDTH: usize = 16;
/// Name suffix for Append queue files.
const LOG_APPEND_SUFFIX: &str = ".raftlog";
/// Name suffix for Rewrite queue files.
const LOG_REWRITE_SUFFIX: &str = ".rewrite";
/// File header.
const LOG_FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";

/// `FileNameExt` offers file name formatting extensions to [`FileId`].
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
        if file_name.len() > LOG_SEQ_WIDTH {
            if let Ok(seq) = file_name[..LOG_SEQ_WIDTH].parse::<u64>() {
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
                width = LOG_SEQ_WIDTH
            ),
            LogQueue::Rewrite => format!(
                "{:0width$}{}",
                self.seq,
                LOG_REWRITE_SUFFIX,
                width = LOG_SEQ_WIDTH
            ),
        }
    }
}

/// Path to the lock file under `dir`.
pub(super) fn lock_file_path<P: AsRef<Path>>(dir: P) -> PathBuf {
    let mut path = PathBuf::from(dir.as_ref());
    path.push("LOCK");
    path
}

/// Version of log file format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, FromPrimitive, ToPrimitive)]
#[repr(u64)]
pub enum Version {
    V1 = 1,
}

impl Default for Version {
    fn default() -> Self {
        Version::V1
    }
}

/// In-memory representation of the log file header.
#[derive(Clone, Default)]
pub struct LogFileHeader {
    version: Version,
}

impl LogFileHeader {
    #[allow(dead_code)]
    pub fn new(version: u64) -> Self {
        if let Some(v) = Version::from_u64(version) {
            Self { version: v }
        } else {
            Self {
                version: Version::default(),
            }
        }
    }

    pub fn from_version(version: Version) -> Self {
        Self { version }
    }

    #[allow(dead_code)]
    pub fn version(&self) -> Version {
        self.version
    }
}

impl LogFileHeader {
    /// Length of header written on storage.
    pub const fn len() -> usize {
        LOG_FILE_MAGIC_HEADER.len() + std::mem::size_of::<Version>()
    }

    /// Decodes a slice of bytes into a `LogFileHeader`.
    pub fn decode(buf: &mut &[u8]) -> Result<LogFileHeader> {
        if buf.len() < Self::len() {
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

    /// Encodes this header and appends the bytes to the provided buffer.
    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
        buf.encode_u64(self.version.to_u64().unwrap())?;
        let corrupted = || {
            fail::fail_point!("log_file_header::corrupted", |_| true);
            false
        };
        if corrupted() {
            buf[0] += 1;
        }
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

    #[test]
    fn test_version() {
        let version = Version::default();
        assert_eq!(Version::V1.to_u64().unwrap(), version.to_u64().unwrap());
        let version2 = Version::from_u64(1).unwrap();
        assert_eq!(version, version2);
    }

    #[test]
    fn test_file_header() {
        let header1 = LogFileHeader::default();
        assert_eq!(header1.version().to_u64().unwrap(), 1);

        let header2 = LogFileHeader::new(2); // forced to be "V1"
        assert_eq!(header2.version().to_u64(), Some(1));
        let header3 = LogFileHeader::from_version(Version::default());
        assert_eq!(header3.version().to_u64(), header1.version().to_u64());

        let header4 = LogFileHeader::from_version(Version::default());
        assert_eq!(header4.version(), Version::default());
    }
}
