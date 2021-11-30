// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::file_builder::FileBuilder;
use crate::log_batch::{LogBatch, LogItemBatch, LOG_BATCH_HEADER_LEN};
use crate::pipe_log::{FileBlockHandle, FileId};
use crate::{Error, Result};

use super::format::LogFileHeader;
use super::log_file::LogFileReader;

pub struct LogItemBatchFileReader<B: FileBuilder> {
    file_id: Option<FileId>,
    reader: Option<LogFileReader<B>>,
    size: usize,

    buffer: Vec<u8>,
    // File offset of the data contained in `buffer`.
    buffer_offset: usize,
    // File offset of the end of last decoded log batch.
    valid_offset: usize,

    read_block_size: usize,
}

impl<B: FileBuilder> LogItemBatchFileReader<B> {
    pub fn new(read_block_size: usize) -> Self {
        Self {
            file_id: None,
            reader: None,
            size: 0,

            buffer: Vec::new(),
            buffer_offset: 0,
            valid_offset: 0,

            read_block_size,
        }
    }

    pub fn open(&mut self, file_id: FileId, reader: LogFileReader<B>) -> Result<()> {
        self.file_id = Some(file_id);
        self.size = reader.file_size()?;
        self.reader = Some(reader);
        self.buffer.clear();
        self.buffer_offset = 0;
        self.valid_offset = 0;
        let mut header = self.peek(0, LogFileHeader::len(), LOG_BATCH_HEADER_LEN)?;
        LogFileHeader::decode(&mut header)?;
        self.valid_offset = LogFileHeader::len();
        Ok(())
    }

    #[allow(dead_code)]
    pub fn reset(&mut self) {
        self.file_id = None;
        self.reader = None;
        self.size = 0;
        self.buffer.clear();
        self.buffer_offset = 0;
        self.valid_offset = 0;
    }

    pub fn next(&mut self) -> Result<Option<LogItemBatch>> {
        if self.valid_offset < self.size {
            if self.valid_offset < LOG_BATCH_HEADER_LEN {
                return Err(Error::Corruption(
                    "attempt to read file with broken header".to_owned(),
                ));
            }
            let (footer_offset, compression_type, len) = LogBatch::decode_header(&mut self.peek(
                self.valid_offset,
                LOG_BATCH_HEADER_LEN,
                0,
            )?)?;
            if self.valid_offset + len > self.size {
                return Err(Error::Corruption("log batch header broken".to_owned()));
            }

            let handle = FileBlockHandle {
                id: self.file_id.unwrap(),
                offset: (self.valid_offset + LOG_BATCH_HEADER_LEN) as u64,
                len: footer_offset - LOG_BATCH_HEADER_LEN,
            };
            let item_batch = LogItemBatch::decode(
                &mut self.peek(
                    self.valid_offset + footer_offset,
                    len - footer_offset,
                    LOG_BATCH_HEADER_LEN,
                )?,
                handle,
                compression_type,
            )?;
            self.valid_offset += len;
            return Ok(Some(item_batch));
        }
        Ok(None)
    }

    fn peek(&mut self, offset: usize, size: usize, prefetch: usize) -> Result<&[u8]> {
        debug_assert!(offset >= self.buffer_offset);
        let reader = self.reader.as_mut().unwrap();
        let end = self.buffer_offset + self.buffer.len();
        if offset > end {
            self.buffer_offset = offset;
            self.buffer
                .resize(std::cmp::max(size + prefetch, self.read_block_size), 0);
            let read = reader.read_to(self.buffer_offset as u64, &mut self.buffer)?;
            if read < size {
                return Err(Error::Corruption(format!(
                    "Unexpected eof at {}",
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
                let read = reader.read_to(read_offset as u64, &mut self.buffer[prev_len..])?;
                if read + prefetch < should_read {
                    return Err(Error::Corruption(format!(
                        "Unexpected eof at {}",
                        read_offset + read,
                    )));
                }
                self.buffer.truncate(prev_len + read);
            }
            Ok(&self.buffer[offset - self.buffer_offset..offset - self.buffer_offset + size])
        }
    }

    pub fn valid_offset(&self) -> usize {
        self.valid_offset
    }
}
