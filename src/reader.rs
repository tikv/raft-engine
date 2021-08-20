// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::file_system::Readable;
use crate::log_batch::{LogBatch, LogItemBatch, LOG_BATCH_HEADER_LEN};
use crate::log_file::{LogFileHeader, LOG_FILE_MAX_HEADER_LEN};
use crate::{Error, Result};

pub struct LogItemBatchFileReader {
    file: Option<Box<dyn Readable>>,
    size: usize,

    buffer: Vec<u8>,
    buffer_offset: usize,
    valid_offset: usize,

    read_block_size: usize,
}

impl LogItemBatchFileReader {
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

    pub fn open(&mut self, file: Box<dyn Readable>, size: usize) -> Result<()> {
        self.file = Some(file);
        self.size = size;
        self.buffer.clear();
        self.buffer_offset = 0;
        self.valid_offset = 0;
        let peek_size = std::cmp::min(LOG_FILE_MAX_HEADER_LEN, size);
        let mut header = self.peek(0, peek_size, LOG_BATCH_HEADER_LEN)?;
        LogFileHeader::decode(&mut header)?;
        self.valid_offset = peek_size - header.len();
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
            self.buffer.resize(read, 0);
            Ok(&self.buffer[..size])
        } else {
            let to_read = (offset + size + prefetch).saturating_sub(end);
            if to_read > 0 {
                let read_offset = self.buffer_offset + self.buffer.len();
                let buffer_offset = self.buffer.len();
                self.buffer.resize(buffer_offset + to_read, 0);
                let read = f.read(&mut self.buffer[buffer_offset..])?;
                if read + prefetch < to_read {
                    return Err(Error::Corruption(format!(
                        "unexpected eof at {}",
                        read_offset + read,
                    )));
                }
                self.buffer.resize(buffer_offset + read, 0);
            }
            Ok(&self.buffer[offset - self.buffer_offset..offset - self.buffer_offset + size])
        }
    }
}
