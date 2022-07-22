// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use crate::env::FileSystem;
use crate::log_batch::{LogBatch, LogItemBatch, LOG_BATCH_HEADER_LEN};
use crate::pipe_log::{DataLayout, FileBlockHandle, FileId, LogFileContext};
use crate::util::{round_down, round_up};
use crate::{Error, Result};

use super::format::LogFileFormat;
use super::log_file::{LogFileReader, FILE_ALIGNMENT_SIZE};

/// A reusable reader over [`LogItemBatch`]s in a log file.
pub(super) struct LogItemBatchFileReader<F: FileSystem> {
    file_context: Option<LogFileContext>,
    reader: Option<LogFileReader<F>>,
    size: usize,

    buffer: Vec<u8>,
    /// File offset of the data contained in `buffer`.
    buffer_offset: usize,
    /// File offset of the end of last decoded log batch.
    valid_offset: usize,

    /// The maximum number of bytes to prefetch.
    read_block_size: usize,
}

impl<F: FileSystem> LogItemBatchFileReader<F> {
    /// Creates a new reader.
    pub fn new(read_block_size: usize) -> Self {
        Self {
            file_context: None,
            reader: None,
            size: 0,

            buffer: Vec::new(),
            buffer_offset: 0,
            valid_offset: 0,

            read_block_size,
        }
    }

    /// Opens a file that can be accessed through the given reader.
    pub fn open(&mut self, file_id: FileId, reader: LogFileReader<F>) -> Result<()> {
        self.file_context = Some(LogFileContext::new(file_id, *reader.file_format()));
        self.size = reader.file_size()?;
        self.reader = Some(reader);
        self.buffer.clear();
        self.buffer_offset = 0;
        self.valid_offset = LogFileFormat::len();
        Ok(())
    }

    /// Closes any ongoing file access.
    pub fn reset(&mut self) {
        self.reader = None;
        self.size = 0;
        self.buffer.clear();
        self.buffer_offset = 0;
        self.valid_offset = 0;
        self.file_context = None;
    }

    /// Returns the next [`LogItemBatch`] in current opened file. Returns
    /// `None` if there is no more data or no opened file.
    pub fn next(&mut self) -> Result<Option<LogItemBatch>> {
        if self.valid_offset < self.size {
            debug_assert!(self.file_context.is_some());
            match self.file_context.as_ref().unwrap().format.data_layout() {
                DataLayout::NoAlignment | DataLayout::AlignWithIntegration => {
                    if self.valid_offset < LOG_BATCH_HEADER_LEN {
                        return Err(Error::Corruption(
                            "attempt to read file with broken header".to_owned(),
                        ));
                    }
                    let (footer_offset, compression_type, len) = LogBatch::decode_header(
                        &mut self.peek(self.valid_offset, LOG_BATCH_HEADER_LEN, 0)?,
                    )?;
                    if self.valid_offset + len > self.size {
                        return Err(Error::Corruption("log batch header broken".to_owned()));
                    }
                    let file_context = self.file_context.as_ref().unwrap().clone();
                    let handle = FileBlockHandle {
                        id: file_context.id,
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
                        &file_context,
                    )?;
                    self.valid_offset += len;
                    return Ok(Some(item_batch));
                }
                DataLayout::AlignWithFragments => {
                    // @TODO: lucasliang
                    // Implement the reading strategy for DataLayout::AlignWithFragments,
                    // considering that records in log files might be fragmented.
                    // Referring to the design of WAL in rocksdb:
                    // [https://github.com/facebook/rocksdb/wiki/Write-Ahead-Log-File-Format],
                    // this strategy will introduce LogRecordType as the candidate
                    // mark to remark each fragment of serialized LogBatchs in the log.
                    //
                    // Expected fragemented records should be aligned and typed with
                    // `LogRecordType`, default with LogRecordType::Full. And we need
                    // supply extra necessary structure to annotate each
                    // part of one fragmented record, i.e.
                    //
                    // | <------------------ 32kb --------------------> | 32kb | ...
                    // |len(32bit)|type(8bit)| serialized LogBatch(...) |  ... | ...
                    //
                    panic!("attempt to read file with an unimplemented DataLayout");
                }
            }
        }
        Ok(None)
    }

    /// Reads some bytes starting at `offset`. Pulls bytes from the file into
    /// its internal buffer if necessary, and attempts to prefetch in that
    /// process.
    ///
    /// Returns a slice of internal buffer with specified size.
    fn peek(&mut self, offset: usize, size: usize, prefetch: usize) -> Result<&[u8]> {
        debug_assert!(offset >= self.buffer_offset);
        let reader = self.reader.as_mut().unwrap();
        let end = self.buffer_offset + self.buffer.len();
        if offset > end {
            self.buffer_offset = offset;
            let (calibrate_offset, calibrate_len) = {
                match self.file_context.as_ref().unwrap().format.data_layout() {
                    DataLayout::NoAlignment => (
                        self.buffer_offset,
                        std::cmp::max(size + prefetch, self.read_block_size),
                    ),
                    _ => (
                        round_down(self.buffer_offset, FILE_ALIGNMENT_SIZE),
                        round_up(
                            std::cmp::max(size + prefetch, self.read_block_size),
                            FILE_ALIGNMENT_SIZE,
                        ),
                    ),
                }
            };
            self.buffer.resize(calibrate_len, 0);
            let read = reader.read_to(calibrate_offset as u64, &mut self.buffer)?;
            if read < size {
                return Err(Error::Corruption(format!(
                    "Unexpected eof at {}",
                    self.buffer_offset + read
                )));
            }
            // Calibrate the first redundant part read from the log file by
            // removing it when `cfg.format_data_layout = DataLayout::Alignment`.
            if calibrate_offset != self.buffer_offset {
                self.buffer.drain(..self.buffer_offset - calibrate_offset);
            }
            self.buffer.truncate(read);
            Ok(&self.buffer[..size])
        } else {
            let should_read = (offset + size + prefetch).saturating_sub(end);
            if should_read > 0 {
                let read_offset = self.buffer_offset + self.buffer.len();
                let prev_len = self.buffer.len();
                let (calibrate_offset, calibrate_len) = {
                    match self.file_context.as_ref().unwrap().format.data_layout() {
                        DataLayout::NoAlignment => (
                            read_offset,
                            std::cmp::max(should_read, self.read_block_size),
                        ),
                        _ => (
                            round_down(read_offset, FILE_ALIGNMENT_SIZE),
                            round_up(
                                std::cmp::max(should_read, self.read_block_size),
                                FILE_ALIGNMENT_SIZE,
                            ),
                        ),
                    }
                };
                self.buffer.resize(prev_len + calibrate_len, 0);
                let read = reader.read_to(calibrate_offset as u64, &mut self.buffer[prev_len..])?;
                if read + prefetch < should_read {
                    return Err(Error::Corruption(format!(
                        "Unexpected eof at {}",
                        calibrate_offset + read,
                    )));
                }
                // Calibrate the first redundant part read from the log file by
                // removing it when `cfg.format_data_layout = DataLayout::Alignment`.
                if read_offset != calibrate_offset {
                    self.buffer
                        .drain(prev_len..prev_len + read_offset - calibrate_offset);
                }
                self.buffer.truncate(prev_len + read);
            }
            Ok(&self.buffer[offset - self.buffer_offset..offset - self.buffer_offset + size])
        }
    }

    /// Returns the offset to the end of verified and decoded data in current
    /// file. Returns zero if there is no file opened.
    pub fn valid_offset(&self) -> usize {
        self.valid_offset
    }

    pub fn file_format(&self) -> Option<LogFileFormat> {
        self.reader.as_ref().map(|reader| *reader.file_format())
    }
}
