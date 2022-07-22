// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Log file types.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use fail::fail_point;
use log::warn;

use crate::env::{FileSystem, Handle, WriteExt};
use crate::metrics::*;
use crate::pipe_log::{DataLayout, FileBlockHandle, LogFileContext};
use crate::util::{round_down, round_up};
use crate::{Error, Result};

use super::format::{LogFileFormat, LOG_FILE_HEADER_ALIGNMENT_SIZE};

/// Maximum number of bytes to allocate ahead.
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

/// Default alignment size.
pub(crate) const FILE_ALIGNMENT_SIZE: usize = 32768; // 32kb as default

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
        if file_size < LogFileFormat::len() || force_reset {
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
        let mut buf = Vec::with_capacity(LogFileFormat::len());
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
        let (start_offset, length) = {
            match self.format.data_layout() {
                DataLayout::NoAlignment => (handle.offset as usize, handle.len),
                DataLayout::AlignWithIntegration => {
                    let start_offset = round_down(handle.offset as usize, FILE_ALIGNMENT_SIZE);
                    let end_offset =
                        round_up(handle.offset as usize + handle.len, FILE_ALIGNMENT_SIZE);
                    (start_offset, end_offset - start_offset)
                }
                DataLayout::AlignWithFragments => {
                    // @TODO: lucasliang,
                    // Currently, it's an implementation with integrated `read`.
                    // This part should be compatible to fragmented records in LogRecordType.
                    unimplemented!()
                }
            }
        };
        let mut buf = vec![0; length];
        let size = self.read_to(start_offset as u64, &mut buf)?;
        buf.truncate(size);
        if buf.len() > handle.len {
            // Drain redundant parts of data in the buffer.
            buf.drain(..handle.offset as usize - start_offset); // head
            buf.drain(handle.len..); // tail
        }
        Ok(buf)
    }

    /// Polls bytes from the file. Stops only when the buffer is filled or
    /// reaching the "end of file".
    pub fn read_to(&mut self, offset: u64, mut buf: &mut [u8]) -> Result<usize> {
        match self.format.data_layout() {
            DataLayout::AlignWithFragments | DataLayout::AlignWithIntegration => {
                // Meet the prerequisite when `data_layout == AlignWithXXX`.
                debug_assert!(offset % FILE_ALIGNMENT_SIZE as u64 == 0);
                debug_assert!(buf.len() % FILE_ALIGNMENT_SIZE == 0);
            }
            _ => {}
        }
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
        // [1] If the length lessed than the standard `LogFileFormat::len()`.
        let header_len = LogFileFormat::len();
        if file_size < header_len {
            return Err(Error::Corruption("Invalid header of LogFile!".to_owned()));
        }
        // [2] Parse the header of the file.
        let mut container = vec![0; round_up(header_len, LOG_FILE_HEADER_ALIGNMENT_SIZE)];
        self.read_to(0, &mut container[..])?;
        self.format = LogFileFormat::decode(&mut container.as_slice())?;
        Ok(self.format)
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

        for version in Version::iter() {
            for data_layout in DataLayout::iter() {
                if data_layout == DataLayout::AlignWithFragments {
                    // Not implemented yet.
                    continue;
                }
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
                    offset: (FILE_ALIGNMENT_SIZE + 77) as u64,
                    len: FILE_ALIGNMENT_SIZE + 77,
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
