// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Log file types.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use fail::fail_point;
use log::warn;

use crate::env::{FileSystem, Handle, WriteExt};
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, LogFileContext, Version};
use crate::{Error, Result};

use super::format::LogFileFormat;

/// Maximum number of bytes to allocate ahead.
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

/// Combination of `[Handle]` and `[Version]`, specifying a handler of a file.
#[derive(Debug)]
pub struct FileHandler<F: FileSystem> {
    pub handle: Arc<F::Handle>,
    pub context: LogFileContext,
}

/// Build a file writer.
///
/// * `handle`: standard handle of a log file.
/// * `format`: format infos of the log file.
/// * `force_reset`: if true => rewrite the header of this file.
pub(super) fn build_file_writer<F: FileSystem>(
    system: &F,
    handle: Arc<F::Handle>,
    format: LogFileFormat,
    force_reset: bool,
) -> Result<LogFileWriter<F>> {
    let writer = system.new_writer(handle.clone())?;
    LogFileWriter::open(handle, writer, format, force_reset)
}

/// Append-only writer for log file.
pub struct LogFileWriter<F: FileSystem> {
    /// header of file
    pub header: LogFileFormat,
    writer: F::Writer,
    written: usize,
    capacity: usize,
    last_sync: usize,
}

impl<F: FileSystem> LogFileWriter<F> {
    fn open(
        handle: Arc<F::Handle>,
        writer: F::Writer,
        format: LogFileFormat,
        force_reset: bool,
    ) -> Result<Self> {
        let file_size = handle.file_size()?;
        let mut f = Self {
            header: format,
            writer,
            written: file_size,
            capacity: file_size,
            last_sync: file_size,
        };
        if file_size < LogFileFormat::header_len() || force_reset {
            f.write_header()?;
        } else {
            f.writer.seek(SeekFrom::Start(file_size as u64))?;
        }
        Ok(f)
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.seek(SeekFrom::Start(0))?;
        self.last_sync = 0;
        self.written = 0;
        let mut buf = Vec::with_capacity(LogFileFormat::header_len());
        self.header.encode(&mut buf)?;
        self.write(&buf, 0)
    }

    pub fn close(&mut self) -> Result<()> {
        // Necessary to truncate extra zeros from fallocate().
        self.truncate()?;
        self.sync()
    }

    pub fn truncate(&mut self) -> Result<()> {
        if self.written < self.capacity {
            fail_point!("file_pipe_log::log_file_writer::skip_truncate", |_| {
                Ok(())
            });
            self.writer.truncate(self.written)?;
            self.capacity = self.written;
        }
        Ok(())
    }

    pub fn write(&mut self, buf: &[u8], target_size_hint: usize) -> Result<()> {
        let new_written = self.written + buf.len();
        if self.capacity < new_written {
            let _t = StopWatch::new(&*LOG_ALLOCATE_DURATION_HISTOGRAM);
            let alloc = std::cmp::max(
                new_written - self.capacity,
                std::cmp::min(
                    FILE_ALLOCATE_SIZE,
                    target_size_hint.saturating_sub(self.capacity),
                ),
            );
            if let Err(e) = self.writer.allocate(self.capacity, alloc) {
                warn!("log file allocation failed: {}", e);
            }
            self.capacity += alloc;
        }
        self.writer.write_all(buf).map_err(|e| {
            self.writer
                .seek(SeekFrom::Start(self.written as u64))
                .unwrap_or_else(|e| {
                    panic!("failed to reseek after write failure: {}", e);
                });
            e
        })?;
        self.written = new_written;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.last_sync < self.written {
            let _t = StopWatch::new(&*LOG_SYNC_DURATION_HISTOGRAM);
            self.writer.sync()?;
            self.last_sync = self.written;
        }
        Ok(())
    }

    #[inline]
    pub fn since_last_sync(&self) -> usize {
        self.written - self.last_sync
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.written
    }
}

/// Build a file reader.
///
/// Attention please, the reader do not need a specified `[LogFileFormat]` from
/// users.
///
/// * `handle`: standard handle of a log file.
/// * `format`: if `[None]`, reloads the log file header and parse
/// the relevant `LogFileFormat` before building the `reader`.
pub(super) fn build_file_reader<F: FileSystem>(
    system: &F,
    handle: Arc<F::Handle>,
    format: Option<LogFileFormat>,
) -> Result<LogFileReader<F>> {
    let reader = system.new_reader(handle.clone())?;
    LogFileReader::open(handle, reader, format)
}

/// Random-access reader for log file.
pub struct LogFileReader<F: FileSystem> {
    format: LogFileFormat,
    handle: Arc<F::Handle>,
    reader: F::Reader,

    offset: u64,
}

impl<F: FileSystem> LogFileReader<F> {
    fn open(
        handle: Arc<F::Handle>,
        reader: F::Reader,
        format: Option<LogFileFormat>,
    ) -> Result<Self> {
        match format {
            Some(fmt) => Ok(Self {
                format: fmt,
                handle,
                reader,
                // Set to an invalid offset to force a reseek at first read.
                offset: u64::MAX,
            }),
            None => {
                let mut reader = Self {
                    format: LogFileFormat::default(),
                    handle,
                    reader,
                    // Set to an invalid offset to force a reseek at first read.
                    offset: u64::MAX,
                };
                reader.parse_format()?;
                Ok(reader)
            }
        }
    }

    pub fn read(&mut self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let mut buf = vec![0; handle.len as usize];
        let size = self.read_to(handle.offset, &mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }

    /// Polls bytes from the file. Stops only when the buffer is filled or
    /// reaching the "end of file".
    pub fn read_to(&mut self, offset: u64, mut buf: &mut [u8]) -> Result<usize> {
        if offset != self.offset {
            self.reader.seek(SeekFrom::Start(offset))?;
            self.offset = offset;
        }
        loop {
            match self.reader.read(buf) {
                Ok(0) => break,
                Ok(n) => {
                    self.offset += n as u64;
                    buf = &mut buf[n..];
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => return Err(Error::Io(e)),
            }
        }
        Ok((self.offset - offset) as usize)
    }

    /// Function for reading the header of the log file, and return a
    /// `[LogFileFormat]`.
    ///
    /// Attention please, this function would move the `reader.offset`
    /// to `0`, that is, the beginning of the file, to parse the
    /// related `[LogFileFormat]`.
    pub fn parse_format(&mut self) -> Result<LogFileFormat> {
        // Here, the caller expected that the given `handle` has pointed to
        // a log file with valid format. Otherwise, it should return with
        // `Err`.
        let file_size = self.handle.file_size()?;
        // [1] If the length lessed than the standard `LogFileFormat::header_len()`.
        let header_len = LogFileFormat::header_len();
        if file_size < header_len {
            return Err(Error::InvalidArgument(format!(
                "invalid header len of log file, expected len: {}, actual len: {}",
                header_len, file_size,
            )));
        }
        // [2] Parse the format of the file.
        let expected_container_len =
            LogFileFormat::header_len() + LogFileFormat::payload_len(Version::V2);
        let mut container = vec![0; expected_container_len];
        let size = self.read_to(0, &mut container[..])?;
        container.truncate(size);
        match LogFileFormat::decode(&mut container.as_slice()) {
            Err(Error::InvalidArgument(err_msg)) => {
                if file_size <= expected_container_len {
                    // Here, it means that we parsed an corrupted V2 header.
                    Err(Error::InvalidArgument(err_msg))
                } else {
                    // Here, the `file_size` is not expected, representing the
                    // whole file is corrupted.
                    Err(Error::Corruption(err_msg))
                }
            }
            Err(e) => {
                if file_size == LogFileFormat::header_len() {
                    // Here, it means that we parsed an corrupted V1 header. We
                    // mark this special err with InvalidArgument, to represent
                    // this log is corrupted on its header.
                    Err(Error::InvalidArgument(e.to_string()))
                } else {
                    Err(e)
                }
            }
            Ok(format) => {
                self.format = format;
                Ok(format)
            }
        }
    }

    #[inline]
    pub fn file_size(&self) -> Result<usize> {
        Ok(self.handle.file_size()?)
    }

    #[inline]
    pub fn file_format(&self) -> &LogFileFormat {
        &self.format
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_traits::ToPrimitive;
    use strum::IntoEnumIterator;
    use tempfile::Builder;

    use crate::env::DefaultFileSystem;
    use crate::file_pipe_log::format::{FileNameExt, LogFileFormat};
    use crate::pipe_log::{DataLayout, FileId, LogQueue, Version};
    use crate::util::ReadableSize;

    fn prepare_mocked_log_file<F: FileSystem>(
        file_system: &F,
        path: &str,
        file_id: FileId,
        format: LogFileFormat,
        file_size: usize,
    ) -> Result<LogFileWriter<F>> {
        let data = vec![b'm'; 1024];
        let fd = Arc::new(file_system.create(&file_id.build_file_path(path))?);
        let mut writer = build_file_writer(file_system, fd, format, true)?;
        let mut offset: usize = 0;
        while offset < file_size {
            writer.write(&data[..], file_size)?;
            offset += data.len();
        }
        Ok(writer)
    }

    fn read_data_from_mocked_log_file<F: FileSystem>(
        file_system: &F,
        path: &str,
        file_id: FileId,
        format: Option<LogFileFormat>,
        file_block_handle: FileBlockHandle,
    ) -> Result<Vec<u8>> {
        let fd = Arc::new(file_system.open(&file_id.build_file_path(path))?);
        let mut reader = build_file_reader(file_system, fd, format)?;
        reader.read(file_block_handle)
    }

    #[test]
    fn test_log_file_write_read() {
        let dir = Builder::new()
            .prefix("test_log_file_write_read")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let target_file_size = ReadableSize::mb(4);
        let file_system = Arc::new(DefaultFileSystem);
        let fs_block_size = 32768;

        for version in Version::iter() {
            for data_layout in [DataLayout::NoAlignment, DataLayout::Alignment(64)] {
                let file_format = LogFileFormat::new(version, data_layout);
                let file_id = FileId {
                    seq: version.to_u64().unwrap(),
                    queue: LogQueue::Append,
                };
                assert!(prepare_mocked_log_file(
                    file_system.as_ref(),
                    path,
                    file_id,
                    file_format,
                    target_file_size.0 as usize,
                )
                .is_ok());
                // mocked file_block_handle
                let file_block_handle = FileBlockHandle {
                    id: file_id,
                    offset: (fs_block_size + 77) as u64,
                    len: fs_block_size + 77,
                };
                let buf = read_data_from_mocked_log_file(
                    file_system.as_ref(),
                    path,
                    file_id,
                    None,
                    file_block_handle,
                )
                .unwrap();
                assert_eq!(buf.len(), file_block_handle.len);
            }
        }
    }
}
