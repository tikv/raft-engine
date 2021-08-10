// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::file_pipe_log::LogFd;
use crate::log_batch::HEADER_LEN;
use crate::{Error, LogBatch, MessageExt, Result};

use log::trace;

#[derive(Debug)]
pub struct LogBatchFileReader<'a> {
    fd: &'a LogFd,
    fsize: usize,
    buffer: Vec<u8>,
    offset: u64, // buffer offset
    len: usize,  // buffer len
    cursor: u64, // monotonic read cursor

    read_block_size: usize,
}

impl<'a> LogBatchFileReader<'a> {
    pub fn new(fd: &'a LogFd, offset: u64, read_block_size: usize) -> Result<Self> {
        Ok(Self {
            fd,
            fsize: fd.file_size()?,
            offset,
            cursor: offset,
            buffer: vec![],
            len: 0,
            read_block_size,
        })
    }

    pub fn next<M: MessageExt>(&mut self) -> Result<Option<LogBatch<M>>> {
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

        // read & parse header
        let mut needed_buffer_size = HEADER_LEN;
        if self.buffer_remain_size() < needed_buffer_size {
            self.extend_buffer(needed_buffer_size, 0)?;
        }
        let (len, offset, compression_type) =
            LogBatch::<M>::parse_header(&mut self.buffer_slice_from_cursor(needed_buffer_size))?;
        let entries_offset = self.cursor + HEADER_LEN as u64;
        let entries_len = offset as usize - HEADER_LEN;

        // skip entries
        self.cursor += offset;

        // read footer & recover
        needed_buffer_size = len - offset as usize;
        trace!("LogBatch::recovery_size = {}", needed_buffer_size);
        if self.buffer_remain_size() < needed_buffer_size {
            self.extend_buffer(needed_buffer_size, HEADER_LEN)?;
        }
        trace!(
            "recover LogBatch offset:{} len:{}",
            self.cursor,
            needed_buffer_size
        );
        let batch = LogBatch::<M>::from_footer(
            &mut self.buffer_slice_from_cursor(needed_buffer_size),
            entries_offset,
            entries_len,
            compression_type,
        )?;
        self.cursor += needed_buffer_size as u64;
        Ok(Some(batch))
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

    fn extend_buffer(&mut self, needed_buffer_size: usize, tail_hint: usize) -> Result<()> {
        self.fread(self.cursor, needed_buffer_size + tail_hint)?;
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
            self.buffer.extend(self.fd.read(offset as i64, len)?);
        } else {
            trace!("::replace buffer {}:{}", offset, len);
            self.buffer = self.fd.read(offset as i64, len)?;
            self.offset = offset;
        }
        self.len = self.buffer.len();
        trace!("new buffer {}:{}", self.offset, self.len);
        Ok(())
    }
}
