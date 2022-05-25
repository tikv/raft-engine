// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Log file types.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileSeq};
use crate::{Error, Result};

use super::format::{LogFileHeader, Version};
use crate::env::{FileSystem, Handle, WriteExt};

/// Maximum number of bytes to allocate ahead.
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

/// Combination of `[Handle]` and `[Version]`, specifying a handler of a file.
#[derive(Clone, Debug)]
pub struct FileHandler<F: FileSystem> {
    pub seq: FileSeq,
    pub handle: Arc<F::Handle>,
    pub version: Option<Version>,
}

/// Function for reading the header of the log file, and return a
/// `[LogFileHeader]`.
///
/// Attention please, to avoid to move the offset of the given `[handle]`, we
/// use a copy of the `[handle]` to parse the header of the given file.
pub(super) fn build_file_header<F: FileSystem>(
    system: &F,
    handle: Arc<F::Handle>,
    expected_reader: Option<&mut F::Reader>,
) -> Result<LogFileHeader> {
    let file_size: usize = match handle.file_size() {
        Ok(size) => size,
        Err(_) => {
            return Err(Error::Corruption("Corrupted file!".to_owned())); // invalid file
        }
    };
    // [1] If the file was a new file, we just return the default `LogFileHeader`.
    if file_size == 0 {
        return Ok(LogFileHeader::default());
    }
    // [2] If the length lessed than the standard `LogFileHeader::len()`.
    let header_len = LogFileHeader::len();
    if file_size < header_len {
        return Err(Error::Corruption("Invalid header of LogFile!".to_owned()));
    }
    // [3] Parse the header of the file.
    let parse_file_header = |handle: Arc<F::Handle>, header_len: usize| -> Result<LogFileHeader> {
        let mut local_reader;
        let reader = if let Some(rd) = expected_reader {
            rd
        } else {
            local_reader = system.new_reader(handle)?;
            &mut local_reader
        };
        reader.seek(SeekFrom::Start(0))?; // move to head of the file.

        // Read and parse the header.
        let mut container = vec![0; header_len as usize];
        let mut buf = &mut container[..];
        loop {
            match reader.read(buf) {
                Ok(0) => {
                    break;
                }
                Ok(n) => {
                    buf = &mut buf[n..];
                }
                Err(e) => return Err(Error::Io(e)),
            }
        }
        LogFileHeader::decode(&mut container.as_slice())
    };
    parse_file_header(handle, header_len)
}

/// Build a file writer.
///
/// * `[handle]`: standard handle of a log file.
/// * `[rewrite_flag]`:
///     - `true` => rewrite the header with `[expected_hdr]`;
///     - `false`=> use the original header from the log file.
pub(super) fn build_file_writer<F: FileSystem>(
    system: &F,
    handle: Arc<F::Handle>,
    expected_hdr: Option<LogFileHeader>,
    rewrite_flag: bool,
) -> Result<LogFileWriter<F>> {
    let writer = system.new_writer(handle.clone())?;
    let header = if expected_hdr.is_none() || !rewrite_flag {
        Some(build_file_header(system, handle.clone(), None)?)
    } else {
        expected_hdr
    };
    LogFileWriter::open(handle, writer, header.unwrap().version())
}

/// Append-only writer for log file.
pub struct LogFileWriter<F: FileSystem> {
    /// header of file
    pub header: LogFileHeader,
    writer: F::Writer,
    written: usize,
    capacity: usize,
    last_sync: usize,
}

impl<F: FileSystem> LogFileWriter<F> {
    fn open(handle: Arc<F::Handle>, writer: F::Writer, version: Version) -> Result<Self> {
        let file_size = handle.file_size()?;
        let mut f = Self {
            header: LogFileHeader::from_version(version),
            writer,
            written: file_size,
            capacity: file_size,
            last_sync: file_size,
        };
        if file_size < LogFileHeader::len() {
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
        let mut buf = Vec::with_capacity(LogFileHeader::len());
        // LogFileHeader::default().encode(&mut buf)?;
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
            self.writer.truncate(self.written)?;
            self.capacity = self.written;
        }
        Ok(())
    }

    pub fn write(&mut self, buf: &[u8], target_size_hint: usize) -> Result<()> {
        let new_written = self.written + buf.len();
        if self.capacity < new_written {
            let _t = StopWatch::new(&LOG_ALLOCATE_DURATION_HISTOGRAM);
            let alloc = std::cmp::max(
                new_written - self.capacity,
                std::cmp::min(
                    FILE_ALLOCATE_SIZE,
                    target_size_hint.saturating_sub(self.capacity),
                ),
            );
            self.writer.allocate(self.capacity, alloc)?;
            self.capacity += alloc;
        }
        self.writer.write_all(buf)?;
        self.written = new_written;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.last_sync < self.written {
            let _t = StopWatch::new(&LOG_SYNC_DURATION_HISTOGRAM);
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
/// Attention please, the reader do not need a specified `[LogFileHeader]` from
/// users.
///
/// * `[handle]`: standard handle of a log file.
/// * `[expected_version]`: if `[None]`, reloads the log file header and parse
/// the relevant `Version` before building the `reader`.
pub(super) fn build_file_reader<F: FileSystem>(
    system: &F,
    handle: Arc<F::Handle>,
    expected_version: Option<Version>,
) -> Result<LogFileReader<F>> {
    let mut reader = system.new_reader(handle.clone())?;
    if let Some(version) = expected_version {
        LogFileReader::open(handle, reader, version)
    } else {
        let file_version =
            (build_file_header(system, handle.clone(), Some(&mut reader))?).version();
        LogFileReader::open(handle, reader, file_version)
    }
}

/// Random-access reader for log file.
pub struct LogFileReader<F: FileSystem> {
    pub header: LogFileHeader,
    handle: Arc<F::Handle>,
    reader: F::Reader,

    offset: u64,
}

impl<F: FileSystem> LogFileReader<F> {
    fn open(handle: Arc<F::Handle>, reader: F::Reader, version: Version) -> Result<Self> {
        Ok(Self {
            header: LogFileHeader::from_version(version),
            handle,
            reader,
            // Set to an invalid offset to force a reseek at first read.
            offset: u64::MAX,
        })
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

    #[inline]
    pub fn file_size(&self) -> Result<usize> {
        Ok(self.handle.file_size()?)
    }
}
