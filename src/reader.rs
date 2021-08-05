// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::os::unix::prelude::RawFd;

use log::trace;
use protobuf::Message;

use crate::util::{file_size, pread_exact};
use crate::{EntryExt, Error, LogBatch, Result};

#[derive(Debug)]
pub struct LogBatchFileReader {
    fd: RawFd,
    fsize: usize,
    buffer: Vec<u8>,
    offset: u64, // buffer offset
    len: usize,  // buffer len
    cursor: u64, // monotonic read cursor

    read_block_size: usize,
}

impl LogBatchFileReader {
    pub fn new(fd: RawFd, offset: u64, read_block_size: usize) -> Result<Self> {
        Ok(Self {
            fd,
            fsize: file_size(fd)?,
            offset,
            cursor: offset,
            buffer: vec![],
            len: 0,
            read_block_size,
        })
    }

    pub fn next<E: Message, W: EntryExt<E>>(&mut self) -> Result<Option<LogBatch<E, W>>> {
        if self.cursor > self.fsize as u64 {
            return Err(Error::Corruption(format!(
                "unexpected eof at {}",
                &self.cursor
            )));
        }
        if self.cursor == self.fsize as u64 {
            return Ok(None);
        }

        // invariance: make sure buffer can cover the range before reads.
        let mut needed_buffer_size = LogBatch::<E, W>::header_size();
        if self.buffer_remain_size() < needed_buffer_size {
            self.extend_buffer(needed_buffer_size)?;
        }
        needed_buffer_size = LogBatch::<E, W>::recovery_size(
            &mut self.buffer_slice_from_cursor(needed_buffer_size),
        )?;
        trace!("LogBatch::recovery_size = {}", needed_buffer_size);
        if self.buffer_remain_size() < needed_buffer_size {
            self.extend_buffer(needed_buffer_size)?;
        }
        trace!(
            "recover LogBatch offset:{} len:{}",
            self.cursor,
            needed_buffer_size
        );
        match LogBatch::<E, W>::from_bytes(
            &mut self.buffer_slice_from_cursor(needed_buffer_size),
            self.cursor,
        )? {
            Some((batch, skip)) => {
                self.cursor += (needed_buffer_size + skip) as u64;
                trace!("cursor jump to: {}", self.cursor);
                Ok(Some(batch))
            }
            None => unreachable!(),
        }
    }

    pub fn buffer_remain_size(&self) -> usize {
        if self.cursor as usize > self.offset as usize + self.len {
            0
        } else {
            self.offset as usize + self.len - self.cursor as usize
        }
    }

    fn buffer_slice_from_cursor(&self, len: usize) -> &[u8] {
        trace!(
            "::slice buffer offset:{} cursor:{}",
            self.offset,
            self.cursor
        );
        assert!(self.offset <= self.cursor);
        assert!(self.offset as usize + self.len >= self.cursor as usize + len);
        let start = (self.cursor - self.offset) as usize;
        let end = start + len;
        &self.buffer[start..end]
    }

    fn extend_buffer(&mut self, needed_buffer_size: usize) -> Result<()> {
        self.fread(self.cursor, needed_buffer_size)?;
        if (self.offset as usize + self.len - self.cursor as usize) < needed_buffer_size {
            return Err(Error::Corruption(format!(
                "unexpected eof at {}",
                self.offset + self.len as u64
            )));
        }
        Ok(())
    }

    fn fread(&mut self, offset: u64, len: usize) -> Result<()> {
        trace!("called fread at {}:{}", offset, len);
        let offset = std::cmp::max(self.offset + self.len as u64, offset);
        let mut len = std::cmp::max(len, self.read_block_size);
        if offset as usize + len > self.fsize {
            len -= offset as usize + len - self.fsize;
        }

        if offset == self.offset + self.len as u64 {
            trace!("::extend buffer {}:{}", offset, len);
            self.buffer.extend(pread_exact(self.fd, offset, len)?);
        } else {
            trace!("::replace buffer {}:{}", offset, len);
            self.buffer = pread_exact(self.fd, offset, len)?;
            self.offset = offset;
        }
        self.len = self.buffer.len();
        trace!("new buffer {}:{}", self.offset, self.len);
        Ok(())
    }
}
