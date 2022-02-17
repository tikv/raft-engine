// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Log file types.

use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;

use crate::metrics::*;
use crate::pipe_log::FileBlockHandle;
use crate::Result;

use super::format::LogFileHeader;
use crate::env::{FileSystem, Handle, WriteExt};

/// Maximum number of bytes to allocate ahead.
const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

pub(super) fn build_file_writer<F: FileSystem>(
    system: &F,
    handle: Arc<F::Handle>,
) -> Result<LogFileWriter<F>> {
    let writer = system.new_writer(handle.clone())?;
    LogFileWriter::open(handle, writer)
}

/// Append-only writer for log file.
pub struct LogFileWriter<F: FileSystem> {
    writer: F::Writer,
    written: usize,
    capacity: usize,
    last_sync: usize,
}

impl<F: FileSystem> LogFileWriter<F> {
    fn open(handle: Arc<F::Handle>, writer: F::Writer) -> Result<Self> {
        let file_size = handle.file_size()?;
        let mut f = Self {
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
        self.written = 0;
        let mut buf = Vec::with_capacity(LogFileHeader::len());
        LogFileHeader::default().encode(&mut buf)?;
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

pub(super) fn build_file_reader<F: FileSystem>(
    system: &F,
    handle: Arc<F::Handle>,
) -> Result<LogFileReader<F>> {
    let reader = system.new_reader(handle.clone())?;
    LogFileReader::open(handle, reader)
}

/// Random-access reader for log file.
pub struct LogFileReader<F: FileSystem> {
    handle: Arc<F::Handle>,
    reader: F::Reader,

    offset: usize,
}

impl<F: FileSystem> LogFileReader<F> {
    fn open(handle: Arc<F::Handle>, reader: F::Reader) -> Result<Self> {
        Ok(Self {
            handle,
            reader,
            // Set to an invalid offset to force a reseek at first read.
            offset: usize::MAX,
        })
    }

    pub fn read(&mut self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let mut buf = vec![0; handle.len];
        let size = self.read_to(handle.offset, &mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }

    pub fn read_to(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        if offset != self.offset as u64 {
            self.reader.seek(SeekFrom::Start(offset))?;
            self.offset = offset as usize;
        }
        let size = self.reader.read(buffer)?;
        self.offset += size;
        Ok(size)
    }

    #[inline]
    pub fn file_size(&self) -> Result<usize> {
        Ok(self.handle.file_size()?)
    }
}
