// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Representations of objects in filesystem.

use std::io::BufRead;
use std::mem;
use std::path::{Path, PathBuf};

use num_traits::{FromPrimitive, ToPrimitive};
use strum::EnumIter;

use crate::codec::{self, NumberEncoder};
use crate::pipe_log::{DataLayout, FileId, LogQueue, Version};
use crate::{Error, Result};

/// Width to format log sequence number.
const LOG_SEQ_WIDTH: usize = 16;
/// Name suffix for Append queue files.
const LOG_APPEND_SUFFIX: &str = ".raftlog";
/// Name suffix for Rewrite queue files.
const LOG_REWRITE_SUFFIX: &str = ".rewrite";
/// File header.
const LOG_FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
/// Mask of Version.
const LOG_FILE_HEADER_VERSION_MASK: u64 = 0x00FFFFFFFFFFFFFF;

/// Default aligned block size for reading header from a log file.
pub(crate) const LOG_FILE_HEADER_ALIGNMENT_SIZE: usize = 4096; // 4 kb as default

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

/// In-memory representation of `Format` in log files.
#[derive(Copy, Clone, Debug, Default, Eq, PartialEq)]
pub struct LogFileFormat {
    version: Version,
    data_layout: DataLayout,
}

impl LogFileFormat {
    pub fn new(version: Version, data_layout: DataLayout) -> Self {
        Self {
            version,
            data_layout,
        }
    }

    /// Length of header written on storage.
    pub const fn len() -> usize {
        LOG_FILE_MAGIC_HEADER.len() + std::mem::size_of::<Version>()
    }

    pub fn from_version(version: Version) -> Self {
        Self {
            version,
            data_layout: DataLayout::default(),
        }
    }

    pub fn version(&self) -> Version {
        self.version
    }

    pub fn data_layout(&self) -> DataLayout {
        self.data_layout
    }

    /// Decodes a slice of bytes into a `LogFileFormat`.
    pub fn decode(buf: &mut &[u8]) -> Result<LogFileFormat> {
        if buf.len() < Self::len() {
            return Err(Error::Corruption("log file header too short".to_owned()));
        }
        if !buf.starts_with(LOG_FILE_MAGIC_HEADER) {
            return Err(Error::Corruption(
                "log file magic header mismatch".to_owned(),
            ));
        }
        buf.consume(LOG_FILE_MAGIC_HEADER.len());
        // Raw content of LogFileFormat
        let format_content = codec::decode_u64(buf)?;
        let version = Version::from_u64(format_content & LOG_FILE_HEADER_VERSION_MASK);
        let data_layout =
            DataLayout::from_u8(((format_content & !LOG_FILE_HEADER_VERSION_MASK) >> 56) as u8);
        if version.is_none() || data_layout.is_none() {
            return Err(Error::Corruption(format!(
                "unrecognized log file header, version: {}, data_layout: {}",
                format_content & LOG_FILE_HEADER_VERSION_MASK,
                ((format_content & !LOG_FILE_HEADER_VERSION_MASK) >> 56) as u8
            )));
        }
        Ok(Self {
            version: version.unwrap(),
            data_layout: data_layout.unwrap(),
        })
    }

    /// Encodes this header and appends the bytes to the provided buffer.
    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
        let format_content: u64 = {
            (self.version.to_u64().unwrap() & LOG_FILE_HEADER_VERSION_MASK)
                | ((self.data_layout.to_u8() as u64) << 56)
        };
        buf.encode_u64(format_content)?;
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

/// Types of records in the log file.
#[repr(u8)]
#[derive(Clone, Copy, Debug, EnumIter, Eq, PartialEq)]
#[allow(dead_code)]
pub enum LogRecordType {
    Full = 0,
    First = 1,
    Middle = 2,
    Last = 3,
}

impl LogRecordType {
    #[allow(dead_code)]
    pub fn from_u8(t: u8) -> Option<Self> {
        if t <= LogRecordType::Last as u8 {
            Some(unsafe { mem::transmute(t) })
        } else {
            None
        }
    }

    #[allow(dead_code)]
    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_log::LogFileContext;
    use strum::IntoEnumIterator;

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
        let header1 = LogFileFormat::default();
        assert_eq!(header1.version().to_u64().unwrap(), 1);
        assert_eq!(header1.data_layout().to_u8(), 0);
        let header2 = LogFileFormat::from_version(Version::default());
        assert_eq!(header2.version().to_u64(), header1.version().to_u64());
        assert_eq!(header1.data_layout().to_u8(), 0);
        let header3 = LogFileFormat::from_version(header1.version());
        assert_eq!(header3.version(), header1.version());
        assert_eq!(header1.data_layout().to_u8(), 0);
    }

    #[test]
    fn test_encoding_decoding_file_format() {
        fn enc_dec_file_format(file_format: LogFileFormat) -> Result<LogFileFormat> {
            let mut buf = Vec::with_capacity(LogFileFormat::len());
            assert!(file_format.encode(&mut buf).is_ok());
            LogFileFormat::decode(&mut &buf[..])
        }
        // normal header
        for version in Version::iter() {
            for layout in DataLayout::iter() {
                let file_format = LogFileFormat::new(version, layout);
                assert_eq!(file_format, enc_dec_file_format(file_format).unwrap());
            }
        }
        // header with abnormal version
        {
            let mut buf = Vec::with_capacity(LogFileFormat::len());
            let format_content: u64 = {
                (100 & LOG_FILE_HEADER_VERSION_MASK) /* abnormal version */
                    | ((DataLayout::AlignWithFragments as u64) << 56)
            };
            assert!(buf.encode_u64(format_content).is_ok());
            assert!(LogFileFormat::decode(&mut &buf[..]).is_err());
        }
        // header with abnormal data_layout
        {
            let mut buf = Vec::with_capacity(LogFileFormat::len());
            let format_content: u64 = {
                (Version::V2.to_u64().unwrap() & LOG_FILE_HEADER_VERSION_MASK) | ((100_u64) << 56)
                /* abnormal data_layout */
            };
            assert!(buf.encode_u64(format_content).is_ok());
            assert!(LogFileFormat::decode(&mut &buf[..]).is_err());
        }
    }

    #[test]
    fn test_file_context() {
        let mut file_context =
            LogFileContext::new(FileId::dummy(LogQueue::Append), LogFileFormat::default());
        assert_eq!(file_context.get_signature(), None);
        file_context.id.seq = 10;
        file_context.format.version = Version::V2;
        assert_eq!(file_context.get_signature().unwrap(), 10);
        let abnormal_seq = (file_context.id.seq << 32) as u64 + 100_u64;
        file_context.id.seq = abnormal_seq;
        assert_ne!(file_context.get_signature().unwrap() as u64, abnormal_seq);
        assert_eq!(file_context.get_signature().unwrap(), 100);
    }

    #[test]
    fn test_log_record_type() {
        assert!(LogRecordType::from_u8(10).is_none());
        for t in LogRecordType::iter() {
            let t_u8: u8 = t.to_u8();
            assert_eq!(LogRecordType::from_u8(t_u8).unwrap(), t);
        }
    }
}
