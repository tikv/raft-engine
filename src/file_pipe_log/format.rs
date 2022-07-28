// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Representations of objects in filesystem.

use std::io::BufRead;
use std::path::{Path, PathBuf};

use num_traits::{FromPrimitive, ToPrimitive};

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

/// Check whether the given `buf` is a valid padding or not.
///
/// To simplify the checking strategy, we just check the first
/// and last byte in the `buf`.
///
/// In most common cases, the paddings will be filled with `0`,
/// and several corner cases, where there exists corrupted blocks
/// in the disk, might pass through this rule, but will failed in
/// followed processing. So, we can just keep it simplistic.
#[inline]
pub(crate) fn is_valid_paddings(buf: &[u8]) -> bool {
    buf.is_empty() || (buf[0] == 0 && buf[buf.len() - 1] == 0)
}

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

    /// Length of whole `LogFileFormat` written on storage.
    pub fn enc_len(&self) -> usize {
        Self::header_len() + Self::payload_len(self.version)
    }

    /// Length of header written on storage.
    pub const fn header_len() -> usize {
        LOG_FILE_MAGIC_HEADER.len() + std::mem::size_of::<Version>()
    }

    /// Length of serialized `DataLayout` written on storage.
    pub const fn payload_len(version: Version) -> usize {
        match version {
            Version::V1 => 0,
            Version::V2 => DataLayout::len(),
        }
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
        let buf_len = buf.len();
        if buf_len < Self::header_len() {
            return Err(Error::Corruption("log file header too short".to_owned()));
        }
        if !buf.starts_with(LOG_FILE_MAGIC_HEADER) {
            return Err(Error::Corruption(
                "log file magic header mismatch".to_owned(),
            ));
        }
        buf.consume(LOG_FILE_MAGIC_HEADER.len());
        // Parse `Version` of LogFileFormat from header of the file.
        let version = {
            let dec_version = codec::decode_u64(buf)?;
            if let Some(v) = Version::from_u64(dec_version) {
                v
            } else {
                return Err(Error::Corruption(format!(
                    "unrecognized log file version: {}",
                    dec_version
                )));
            }
        };
        // Parse `DataLayout` of LogFileFormat from header of the file.
        let payload_len = Self::payload_len(version);
        if payload_len == 0 {
            // No alignment.
            return Ok(Self {
                version,
                data_layout: DataLayout::default(),
            });
        }
        if_chain::if_chain! {
            if payload_len > 0;
            if buf_len >= Self::header_len() + payload_len;
            if let Ok(layout_block_size) = codec::decode_u64(buf);
            then {
                // If the decoded `payload_len > 0`, serialized data_layout
                // should be extracted from the file.
                Ok(Self {
                    version,
                    data_layout: if layout_block_size == 0 {
                        DataLayout::default()
                    } else {
                        DataLayout::Alignment(layout_block_size)
                    },
                })
            } else {
                // Here, we mark this special err, that is, corrupted `payload`,
                // with InvalidArgument.
                Err(Error::InvalidArgument(format!(
                    "invalid data_layout in the header, len: {}",
                    buf_len - Self::header_len()
                )))
            }
        }
    }

    /// Encodes this header and appends the bytes to the provided buffer.
    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
        buf.encode_u64(self.version.to_u64().unwrap())?; // encode version
        if Self::payload_len(self.version) > 0 {
            buf.encode_u64(self.data_layout.to_u64())?; // encode datay_layout
            let corrupted_data_layout = || {
                fail::fail_point!("log_file_header::corrupted_data_layout", |_| true);
                false
            };
            if corrupted_data_layout() {
                buf.pop();
            }
        }
        let corrupted = || {
            fail::fail_point!("log_file_header::corrupted", |_| true);
            false
        };
        if corrupted() {
            buf[0] += 1;
        }
        Ok(())
    }

    /// Return the aligned block size.
    #[inline]
    pub fn get_aligned_block_size(&self) -> usize {
        self.data_layout.to_u64() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_log::LogFileContext;

    #[test]
    fn test_check_paddings_is_valid() {
        // normal buffer
        let mut buf = vec![0; 128];
        // len < 8
        assert!(is_valid_paddings(&buf[0..6]));
        // len == 8
        assert!(is_valid_paddings(&buf[120..]));
        // len > 8
        assert!(is_valid_paddings(&buf[..]));

        // abnormal buffer
        buf[127] = 3_u8;
        assert!(is_valid_paddings(&buf[0..110]));
        assert!(is_valid_paddings(&buf[120..125]));
        assert!(!is_valid_paddings(&buf[124..128]));
        assert!(!is_valid_paddings(&buf[120..]));
        assert!(!is_valid_paddings(&buf[..]));
    }

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
    fn test_data_layout() {
        let data_layout = DataLayout::default();
        assert_eq!(data_layout.to_u64(), DataLayout::NoAlignment.to_u64());
        assert_eq!(DataLayout::Alignment(16).to_u64(), 16);
        assert_eq!(DataLayout::from_u64(0), DataLayout::default());
        assert_eq!(DataLayout::from_u64(4096), DataLayout::Alignment(4096));
        assert_eq!(DataLayout::len(), 8);
    }

    #[test]
    fn test_file_header() {
        let header1 = LogFileFormat::default();
        assert_eq!(header1.version().to_u64().unwrap(), 1);
        assert_eq!(header1.data_layout().to_u64(), 0);
        let header2 = LogFileFormat::from_version(Version::default());
        assert_eq!(header2.version().to_u64(), header1.version().to_u64());
        assert_eq!(header1.data_layout().to_u64(), 0);
        let header3 = LogFileFormat::from_version(header1.version());
        assert_eq!(header3.version(), header1.version());
        assert_eq!(header1.data_layout().to_u64(), 0);
        assert_eq!(header1.enc_len(), LogFileFormat::header_len());
        assert_eq!(header2.enc_len(), LogFileFormat::header_len());
        assert_eq!(header3.enc_len(), LogFileFormat::header_len());
        let header4 = LogFileFormat {
            version: Version::V2,
            data_layout: DataLayout::Alignment(16),
        };
        assert_eq!(
            header4.enc_len(),
            LogFileFormat::header_len() + LogFileFormat::payload_len(header4.version)
        );
    }

    #[test]
    fn test_encoding_decoding_file_format() {
        fn enc_dec_file_format(file_format: LogFileFormat) -> Result<LogFileFormat> {
            let mut buf = Vec::with_capacity(
                LogFileFormat::header_len() + LogFileFormat::payload_len(file_format.version),
            );
            assert!(file_format.encode(&mut buf).is_ok());
            LogFileFormat::decode(&mut &buf[..])
        }
        // header with aligned-sized data_layout
        {
            let mut buf = Vec::with_capacity(LogFileFormat::header_len());
            let version = Version::V2;
            let data_layout = DataLayout::Alignment(4096);
            buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
            assert!(buf.encode_u64(version.to_u64().unwrap()).is_ok());
            assert!(buf.encode_u64(data_layout.to_u64()).is_ok());
            assert_eq!(
                LogFileFormat::decode(&mut &buf[..]).unwrap(),
                LogFileFormat::new(version, data_layout)
            );
        }
        // header with abnormal version
        {
            let mut buf = Vec::with_capacity(LogFileFormat::header_len());
            let abnormal_version = 4_u64; /* abnormal version */
            buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
            assert!(buf.encode_u64(abnormal_version).is_ok());
            assert!(buf.encode_u64(16).is_ok());
            assert!(LogFileFormat::decode(&mut &buf[..]).is_err());
        }
        // header with Version::default and DataLayout::Alignment(_)
        {
            let file_format = LogFileFormat::new(Version::default(), DataLayout::Alignment(0));
            assert_eq!(
                LogFileFormat::new(Version::default(), DataLayout::NoAlignment),
                enc_dec_file_format(file_format).unwrap()
            );
            let file_format = LogFileFormat::new(Version::default(), DataLayout::Alignment(4096));
            assert_eq!(
                LogFileFormat::new(Version::default(), DataLayout::NoAlignment),
                enc_dec_file_format(file_format).unwrap()
            );
        }
        // header with Version::V2 and DataLayout::Alignment(0)
        {
            let file_format = LogFileFormat::new(Version::V2, DataLayout::Alignment(0));
            assert_eq!(
                LogFileFormat::new(Version::V2, DataLayout::NoAlignment),
                enc_dec_file_format(file_format).unwrap()
            );
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
}
