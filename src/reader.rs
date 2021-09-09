// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::io::{Read, Seek};

use crate::file_pipe_log::{LogFileHeader, LOG_FILE_HEADER_LEN};
use crate::log_batch::{LogBatch, LogItemBatch, LOG_BATCH_HEADER_LEN};
use crate::{Error, Result};

pub struct LogItemBatchFileReader<R: Read + Seek> {
    file: Option<R>,
    size: usize,

    buffer: Vec<u8>,
    buffer_offset: usize,
    valid_offset: usize,

    read_block_size: usize,
}

impl<R: Read + Seek> LogItemBatchFileReader<R> {
    pub fn new(read_block_size: usize) -> Self {
        Self {
            file: None,
            size: 0,

            buffer: Vec::new(),
            buffer_offset: 0,
            valid_offset: 0,

            read_block_size,
        }
    }

    pub fn open(&mut self, file: R, size: usize) -> Result<()> {
        self.file = Some(file);
        self.size = size;
        self.buffer.clear();
        self.buffer_offset = 0;
        self.valid_offset = 0;
        let mut header = self.peek(0, LOG_FILE_HEADER_LEN, LOG_BATCH_HEADER_LEN)?;
        LogFileHeader::decode(&mut header)?;
        self.valid_offset = LOG_FILE_HEADER_LEN;
        Ok(())
    }

    pub fn valid_offset(&self) -> usize {
        self.valid_offset
    }

    pub fn next(&mut self) -> Result<Option<LogItemBatch>> {
        if self.valid_offset < LOG_BATCH_HEADER_LEN {
            return Err(Error::Corruption(
                "attempt to read file with broken header".to_owned(),
            ));
        }
        if self.valid_offset < self.size {
            let (footer_offset, compression_type, len) = LogBatch::decode_header(&mut self.peek(
                self.valid_offset,
                LOG_BATCH_HEADER_LEN,
                0,
            )?)?;
            let entries_offset = self.valid_offset + LOG_BATCH_HEADER_LEN;
            let entries_len = footer_offset - LOG_BATCH_HEADER_LEN;

            let item_batch = LogItemBatch::decode(
                &mut self.peek(
                    self.valid_offset + footer_offset,
                    len - footer_offset,
                    LOG_BATCH_HEADER_LEN,
                )?,
                entries_offset,
                entries_len,
                compression_type,
            )?;
            self.valid_offset += len;
            return Ok(Some(item_batch));
        }
        Ok(None)
    }

    fn peek(&mut self, offset: usize, size: usize, prefetch: usize) -> Result<&[u8]> {
        debug_assert!(offset >= self.buffer_offset);
        let f = self.file.as_mut().unwrap();
        let end = self.buffer_offset + self.buffer.len();
        if offset > end {
            self.buffer_offset = offset;
            self.buffer
                .resize(std::cmp::max(size + prefetch, self.read_block_size), 0);
            f.seek(std::io::SeekFrom::Start(self.buffer_offset as u64))?;
            let read = f.read(&mut self.buffer)?;
            if read < size {
                return Err(Error::Corruption(format!(
                    "unexpected eof at {}",
                    self.buffer_offset + read
                )));
            }
            self.buffer.truncate(read);
            Ok(&self.buffer[..size])
        } else {
            let should_read = (offset + size + prefetch).saturating_sub(end);
            if should_read > 0 {
                let read_offset = self.buffer_offset + self.buffer.len();
                let prev_len = self.buffer.len();
                self.buffer.resize(
                    prev_len + std::cmp::max(should_read, self.read_block_size),
                    0,
                );
                let read = f.read(&mut self.buffer[prev_len..])?;
                if read + prefetch < should_read {
                    return Err(Error::Corruption(format!(
                        "unexpected eof at {}",
                        read_offset + read,
                    )));
                }
                self.buffer.truncate(prev_len + read);
            }
            Ok(&self.buffer[offset - self.buffer_offset..offset - self.buffer_offset + size])
        }
    }
}
