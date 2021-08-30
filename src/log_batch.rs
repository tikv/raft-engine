// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::io::BufRead;
use std::{mem, u64};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::error;
use protobuf::parse_from_bytes;
use protobuf::Message;

use crate::codec::{self, Error as CodecError, NumberEncoder};
use crate::engine::ParallelRecoverContext;
use crate::memtable::EntryIndex;
use crate::pipe_log::{FileId, LogQueue};
use crate::util::{crc32, lz4};
use crate::{Error, Result};

pub const LOG_BATCH_HEADER_LEN: usize = 16;
pub const LOG_BATCH_CHECKSUM_LEN: usize = 4;

const TYPE_ENTRIES: u8 = 0x01;
const TYPE_COMMAND: u8 = 0x02;
const TYPE_KV: u8 = 0x3;

const CMD_CLEAN: u8 = 0x01;
const CMD_COMPACT: u8 = 0x02;

const DEFAULT_BATCH_CAP: usize = 64;

pub trait MessageExt: Send + Sync {
    type Entry: Message + Clone + PartialEq;

    fn index(e: &Self::Entry) -> u64;
}

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
}

impl CompressionType {
    pub fn from_u8(t: u8) -> CompressionType {
        unsafe { mem::transmute(t) }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

type SliceReader<'a> = &'a [u8];

// Format:
// { count | first index | [ tail offsets ] }
#[derive(Debug, PartialEq)]
pub struct EntriesIndex(pub Vec<EntryIndex>);

impl EntriesIndex {
    pub fn decode(buf: &mut SliceReader, entries_size: &mut usize) -> Result<Self> {
        let mut count = codec::decode_var_u64(buf)?;
        let mut entries_index = Vec::with_capacity(count as usize);
        let mut index = 0;
        if count > 0 {
            index = codec::decode_var_u64(buf)?;
        }
        while count > 0 {
            let entry_len = (codec::decode_var_u64(buf)? as usize) - *entries_size;
            let entry_index = EntryIndex {
                index,
                entry_offset: *entries_size as u64,
                entry_len,
                ..Default::default()
            };
            *entries_size += entry_len;
            entries_index.push(entry_index);
            index += 1;
            count -= 1;
        }
        Ok(Self(entries_index))
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let count = self.0.len() as u64;
        buf.encode_var_u64(count)?;
        if count > 0 {
            buf.encode_var_u64(self.0[0].index)?;
        }
        for ei in self.0.iter() {
            buf.encode_var_u64(ei.entry_offset + ei.entry_len as u64)?;
        }
        Ok(())
    }

    fn size(&self) -> usize {
        8 /*count*/ + 8 /*first index*/ + 8 /*tail offset*/ * self.0.len()
    }
}

// Format:
// { type | (index) }
#[derive(Debug, PartialEq)]
pub enum Command {
    Clean,
    Compact { index: u64 },
}

impl Command {
    pub fn encode(&self, vec: &mut Vec<u8>) {
        match *self {
            Command::Clean => {
                vec.push(CMD_CLEAN);
            }
            Command::Compact { index } => {
                vec.push(CMD_COMPACT);
                vec.encode_var_u64(index).unwrap();
            }
        }
    }

    pub fn decode(buf: &mut SliceReader) -> Result<Command> {
        let command_type = codec::read_u8(buf)?;
        match command_type {
            CMD_CLEAN => Ok(Command::Clean),
            CMD_COMPACT => {
                let index = codec::decode_var_u64(buf)?;
                Ok(Command::Compact { index })
            }
            _ => unreachable!(),
        }
    }

    fn size(&self) -> usize {
        match &self {
            Command::Clean => 1,              /*type*/
            Command::Compact { .. } => 1 + 8, /*type + index*/
        }
    }
}

#[repr(u8)]
#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OpType {
    Put = 1,
    Del = 2,
}

impl OpType {
    pub fn from_u8(t: u8) -> OpType {
        unsafe { mem::transmute(t) }
    }

    pub fn to_u8(self) -> u8 {
        self as u8
    }
}

// Format:
// { op_type | key len | key | ( value len | value ) }
#[derive(Debug, PartialEq)]
pub struct KeyValue {
    pub op_type: OpType,
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
}

impl KeyValue {
    pub fn new(op_type: OpType, key: Vec<u8>, value: Option<Vec<u8>>) -> KeyValue {
        KeyValue {
            op_type,
            key,
            value,
        }
    }

    pub fn decode(buf: &mut SliceReader) -> Result<KeyValue> {
        let op_type = OpType::from_u8(codec::read_u8(buf)?);
        let k_len = codec::decode_var_u64(buf)? as usize;
        let key = &buf[..k_len];
        buf.consume(k_len);
        match op_type {
            OpType::Put => {
                let v_len = codec::decode_var_u64(buf)? as usize;
                let value = &buf[..v_len];
                buf.consume(v_len);
                Ok(KeyValue::new(
                    OpType::Put,
                    key.to_vec(),
                    Some(value.to_vec()),
                ))
            }
            OpType::Del => Ok(KeyValue::new(OpType::Del, key.to_vec(), None)),
        }
    }

    pub fn encode(&self, vec: &mut Vec<u8>) -> Result<()> {
        vec.push(self.op_type.to_u8());
        vec.encode_var_u64(self.key.len() as u64)?;
        vec.extend_from_slice(self.key.as_slice());
        match self.op_type {
            OpType::Put => {
                vec.encode_var_u64(self.value.as_ref().unwrap().len() as u64)?;
                vec.extend_from_slice(self.value.as_ref().unwrap().as_slice());
            }
            OpType::Del => {}
        }
        Ok(())
    }

    fn size(&self) -> usize {
        1 /*op*/ + 8 /*k_len*/ + self.key.len() + 8 /*v_len*/ + self.value.as_ref().map_or_else(|| 0, |v| v.len())
    }
}

// Format:
// { 8 byte region id | 1 byte type | item }
#[derive(Debug, PartialEq)]
pub struct LogItem {
    pub raft_group_id: u64,
    pub content: LogItemContent,
}

#[derive(Debug, PartialEq)]
pub enum LogItemContent {
    EntriesIndex(EntriesIndex),
    Command(Command),
    Kv(KeyValue),
}

impl LogItem {
    pub fn new_entry_indexes(raft_group_id: u64, entry_indexes: Vec<EntryIndex>) -> LogItem {
        LogItem {
            raft_group_id,
            content: LogItemContent::EntriesIndex(EntriesIndex(entry_indexes)),
        }
    }

    pub fn new_command(raft_group_id: u64, command: Command) -> LogItem {
        LogItem {
            raft_group_id,
            content: LogItemContent::Command(command),
        }
    }

    pub fn new_kv(
        raft_group_id: u64,
        op_type: OpType,
        key: Vec<u8>,
        value: Option<Vec<u8>>,
    ) -> LogItem {
        LogItem {
            raft_group_id,
            content: LogItemContent::Kv(KeyValue::new(op_type, key, value)),
        }
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.encode_var_u64(self.raft_group_id)?;
        match &self.content {
            LogItemContent::EntriesIndex(entries_index) => {
                buf.push(TYPE_ENTRIES);
                entries_index.encode(buf)?;
            }
            LogItemContent::Command(command) => {
                buf.push(TYPE_COMMAND);
                command.encode(buf);
            }
            LogItemContent::Kv(kv) => {
                buf.push(TYPE_KV);
                kv.encode(buf)?;
            }
        }
        Ok(())
    }

    pub fn decode(buf: &mut SliceReader, entries_size: &mut usize) -> Result<LogItem> {
        let raft_group_id = codec::decode_var_u64(buf)?;
        let item_type = buf.read_u8()?;
        let content = match item_type {
            TYPE_ENTRIES => {
                let entries_index = EntriesIndex::decode(buf, entries_size)?;
                LogItemContent::EntriesIndex(entries_index)
            }
            TYPE_COMMAND => {
                let cmd = Command::decode(buf)?;
                LogItemContent::Command(cmd)
            }
            TYPE_KV => {
                let kv = KeyValue::decode(buf)?;
                LogItemContent::Kv(kv)
            }
            _ => return Err(Error::Codec(CodecError::KeyNotFound)),
        };
        Ok(LogItem {
            raft_group_id,
            content,
        })
    }

    fn size(&self) -> usize {
        match &self.content {
            LogItemContent::EntriesIndex(entries_index) => {
                8 /*r_id*/ + 1 /*type*/ + entries_index.size()
            }
            LogItemContent::Command(cmd) => 8 + 1 + cmd.size(),
            LogItemContent::Kv(kv) => 8 + 1 + kv.size(),
        }
    }
}

pub type LogItemDrain<'a> = std::vec::Drain<'a, LogItem>;

// Format:
// { item count | [items] | crc32 }
#[derive(Debug, PartialEq)]
pub struct LogItemBatch {
    items: Vec<LogItem>,
    item_size: usize,
}

impl Default for LogItemBatch {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BATCH_CAP)
    }
}

impl LogItemBatch {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            item_size: 0,
        }
    }

    pub fn pick_parallel_recover_context(&self) -> ParallelRecoverContext {
        let mut context = ParallelRecoverContext::default();
        for item in self.items.iter() {
            match &item.content {
                LogItemContent::Command(Command::Clean) => {
                    // Removed raft_group_id will never be used again.
                    context.removed_memtables.insert(item.raft_group_id);
                }
                LogItemContent::Command(Command::Compact { index }) => {
                    context
                        .compacted_memtables
                        .insert(item.raft_group_id, *index);
                }
                LogItemContent::Kv(KeyValue { op_type, key, .. }) => {
                    if *op_type == OpType::Del {
                        context
                            .removed_keys
                            .insert((item.raft_group_id, key.clone()));
                    }
                }
                _ => {}
            }
        }
        context
    }

    pub fn drain(&mut self) -> LogItemDrain {
        self.item_size = 0;
        self.items.drain(..)
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: Option<u64>) {
        for item in self.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                for ei in entries_index.0.iter_mut() {
                    ei.queue = queue;
                    ei.file_id = file_id;
                    if let Some(offset) = offset {
                        ei.entries_offset += offset;
                    }
                }
            }
        }
    }

    pub fn add_entry_indexes(&mut self, region_id: u64, entry_indexes: Vec<EntryIndex>) {
        let item = LogItem::new_entry_indexes(region_id, entry_indexes);
        self.item_size += item.size();
        self.items.push(item);
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) {
        let item = LogItem::new_command(region_id, cmd);
        self.item_size += item.size();
        self.items.push(item);
    }

    pub fn delete_message(&mut self, region_id: u64, key: Vec<u8>) {
        let item = LogItem::new_kv(region_id, OpType::Del, key, None);
        self.item_size += item.size();
        self.items.push(item);
    }

    pub fn put_message<S: Message>(&mut self, region_id: u64, key: Vec<u8>, s: &S) -> Result<()> {
        self.put(region_id, key, s.write_to_bytes()?);
        Ok(())
    }

    pub fn put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) {
        let item = LogItem::new_kv(region_id, OpType::Put, key, Some(value));
        self.item_size += item.size();
        self.items.push(item);
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let offset = buf.len();
        let count = self.items.len() as u64;
        buf.encode_var_u64(count)?;
        for item in self.items.iter() {
            item.encode(buf)?;
        }
        let checksum = crc32(&buf[offset..]);
        buf.encode_u32_le(checksum)?;
        Ok(())
    }

    pub fn decode(
        buf: &mut SliceReader,
        entries_offset: usize,
        entries_len: usize,
        compression_type: CompressionType,
    ) -> Result<LogItemBatch> {
        verify_checksum(buf)?;
        let count = codec::decode_var_u64(buf)?;
        assert!(count > 0 && !buf.is_empty());
        let mut items = LogItemBatch::with_capacity(count as usize);
        let mut entries_size = 0;
        for _ in 0..count {
            let item = LogItem::decode(buf, &mut entries_size)?;
            items.item_size += item.size();
            items.items.push(item);
        }

        for item in items.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                for ei in entries_index.0.iter_mut() {
                    ei.compression_type = compression_type;
                    ei.entries_offset = entries_offset as u64;
                    ei.entries_len = entries_len;
                }
            }
        }
        Ok(items)
    }

    pub fn size(&self) -> usize {
        8 /*count*/ + self.item_size + LOG_BATCH_CHECKSUM_LEN
    }
}

#[derive(Debug, PartialEq)]
enum BufState {
    Uninitialized,
    EntriesFilled,
    Sealed(usize),
    Incomplete,
}

// Format:
// { u56 len | u8 compression type | u64 item offset | (compressed) entries | crc32 | item batch }
#[derive(Debug, PartialEq)]
pub struct LogBatch {
    item_batch: LogItemBatch,
    buf: Vec<u8>,
    buf_state: BufState,
}

impl Default for LogBatch {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BATCH_CAP)
    }
}

impl LogBatch {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            item_batch: LogItemBatch::with_capacity(cap),
            buf: Vec::with_capacity(4096),
            buf_state: BufState::Uninitialized,
        }
    }

    pub fn drain(&mut self) -> LogItemDrain {
        self.buf.clear();
        self.buf_state = BufState::Uninitialized;
        self.item_batch.drain()
    }

    pub fn merge(&mut self, rhs: &mut Self) {
        self.item_batch.items.append(&mut rhs.item_batch.items);
        if rhs.buf_state != BufState::Uninitialized {
            // truncate both footers
            if let BufState::Sealed(footer) = self.buf_state {
                self.buf.truncate(footer);
            }
            if let BufState::Sealed(footer) = rhs.buf_state {
                rhs.buf.truncate(footer);
            }
            match self.buf_state {
                BufState::Uninitialized => {
                    self.buf.append(&mut rhs.buf);
                }
                _ => {
                    self.buf.extend_from_slice(&rhs.buf[LOG_BATCH_HEADER_LEN..]);
                    rhs.buf.clear();
                }
            }
            self.buf_state = BufState::EntriesFilled;
            rhs.buf_state = BufState::Uninitialized;
        }
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: Option<u64>) {
        self.item_batch.set_position(queue, file_id, offset);
    }

    fn prepare_buffer_for_write(&mut self) -> Result<()> {
        if self.buf_state == BufState::Uninitialized {
            self.buf_state = BufState::Incomplete;
            self.buf.encode_u64(0)?;
            self.buf.encode_u64(0)?;
        } else if self.buf_state != BufState::EntriesFilled {
            // return error
        } else {
            self.buf_state = BufState::Incomplete;
        }
        Ok(())
    }

    pub fn add_entries<M: MessageExt>(
        &mut self,
        region_id: u64,
        mut entries: Vec<M::Entry>,
    ) -> Result<()> {
        let mut entry_indexes = Vec::with_capacity(entries.len());
        self.prepare_buffer_for_write()?;
        for e in entries.drain(..) {
            let buf_offset = self.buf.len();
            e.write_to_vec(&mut self.buf)?;
            entry_indexes.push(EntryIndex {
                index: M::index(&e),
                entry_offset: (buf_offset - LOG_BATCH_HEADER_LEN) as u64,
                entry_len: self.buf.len() - buf_offset,
                ..Default::default()
            });
        }
        self.buf_state = BufState::EntriesFilled;
        self.item_batch.add_entry_indexes(region_id, entry_indexes);
        Ok(())
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) {
        self.item_batch.add_command(region_id, cmd);
    }

    pub fn delete_message(&mut self, region_id: u64, key: Vec<u8>) {
        self.item_batch.delete_message(region_id, key);
    }

    pub fn put_message<S: Message>(&mut self, region_id: u64, key: Vec<u8>, s: &S) -> Result<()> {
        self.item_batch.put_message(region_id, key, s)
    }

    pub fn put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) {
        self.item_batch.put(region_id, key, value)
    }

    pub fn is_empty(&self) -> bool {
        self.item_batch.items.is_empty()
    }

    pub fn encoded_bytes(&mut self, compression_threshold: usize) -> Result<&[u8]> {
        // TODO: avoid to write a large batch into one compressed chunk.
        if self.is_empty() || matches!(self.buf_state, BufState::Sealed(_)) {
            return Ok(&self.buf);
        }
        self.prepare_buffer_for_write()?;

        // encode entries.
        let compression_type = if compression_threshold > 0
            && (self.buf.len() - LOG_BATCH_HEADER_LEN) > compression_threshold
        {
            self.buf =
                lz4::encode_block(&self.buf[LOG_BATCH_HEADER_LEN..], LOG_BATCH_HEADER_LEN, 4);
            CompressionType::Lz4
        } else {
            CompressionType::None
        };
        if self.buf.len() > LOG_BATCH_HEADER_LEN {
            let checksum = crc32(&self.buf[LOG_BATCH_HEADER_LEN..]);
            self.buf.encode_u32_le(checksum)?;
        }
        let offset = self.buf.len() as u64;

        self.item_batch.encode(&mut self.buf)?;

        // encode header.
        let len = ((self.buf.len() as u64) << 8) | u64::from(compression_type.to_u8());
        (&mut self.buf[..LOG_BATCH_HEADER_LEN]).write_u64::<BigEndian>(len)?;
        (&mut self.buf[8..LOG_BATCH_HEADER_LEN]).write_u64::<BigEndian>(offset)?;

        self.buf_state = BufState::Sealed(offset as usize);

        // update entry indexes.
        for item in self.item_batch.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                for ei in entries_index.0.iter_mut() {
                    ei.compression_type = compression_type;
                    ei.entries_offset = LOG_BATCH_HEADER_LEN as u64; // need to add batch offset of the file after written.
                    ei.entries_len = offset as usize - LOG_BATCH_HEADER_LEN;
                }
            }
        }
        Ok(&self.buf)
    }

    // Not meaningful after recovery.
    pub fn size(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            match self.buf_state {
                BufState::Uninitialized => LOG_BATCH_HEADER_LEN + self.item_batch.size(),
                BufState::EntriesFilled => {
                    LOG_BATCH_HEADER_LEN
                        + self.buf.len()
                        + LOG_BATCH_CHECKSUM_LEN
                        + self.item_batch.size()
                }
                BufState::Sealed(_) => self.buf.len(),
                BufState::Incomplete => {
                    error!("querying incomplete log batch");
                    0
                }
            }
        }
    }

    pub fn decode_header(buf: &mut SliceReader) -> Result<(usize, CompressionType, usize)> {
        assert!(buf.len() >= LOG_BATCH_HEADER_LEN);
        let len = codec::decode_u64(buf)? as usize;
        let offset = codec::decode_u64(buf)? as usize;
        let compression_type = CompressionType::from_u8(len as u8);
        Ok((offset, compression_type, len >> 8))
    }

    pub fn parse_entry<M: MessageExt>(buf: &[u8], idx: &EntryIndex) -> Result<M::Entry> {
        let len = idx.entries_len;
        verify_checksum(&buf[0..len])?;
        let bytes = match idx.compression_type {
            CompressionType::None => {
                buf[idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len].to_owned()
            }
            CompressionType::Lz4 => {
                let decompressed = lz4::decode_block(&buf[..len - LOG_BATCH_CHECKSUM_LEN]);
                decompressed[idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len]
                    .to_vec()
            }
        };
        Ok(parse_from_bytes(&bytes)?)
    }
}

fn verify_checksum(buf: &[u8]) -> Result<()> {
    if buf.len() <= LOG_BATCH_CHECKSUM_LEN {
        return Err(Error::TooShort);
    }
    let expected = codec::decode_u32_le(&mut &buf[buf.len() - LOG_BATCH_CHECKSUM_LEN..])?;
    let actual = crc32(&buf[..buf.len() - LOG_BATCH_CHECKSUM_LEN]);
    if actual != expected {
        return Err(Error::IncorrectChecksum(expected, actual));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::test_util::{generate_entries, generate_entry_indexes};
    use protobuf::parse_from_bytes;
    use raft::eraftpb::Entry;

    fn decode_entries_from_bytes<E: Message>(
        buf: &[u8],
        entries_index: &[EntryIndex],
        encoded: bool,
    ) -> Vec<E> {
        let mut data = buf.to_owned();
        if encoded {
            data = lz4::decode_block(&data[..data.len() - LOG_BATCH_CHECKSUM_LEN]);
        }
        let mut entries = vec![];
        for entry_index in entries_index {
            entries.push(
                parse_from_bytes(
                    &data[entry_index.entry_offset as usize
                        ..entry_index.entry_offset as usize + entry_index.entry_len],
                )
                .unwrap(),
            );
        }
        entries
    }

    #[test]
    fn test_command_enc_dec() {
        let cmd = Command::Clean;
        let mut encoded = vec![];
        cmd.encode(&mut encoded);
        let mut bytes_slice = encoded.as_slice();
        let decoded_cmd = Command::decode(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(cmd, decoded_cmd);
    }

    #[test]
    fn test_kv_enc_dec() {
        let kv = KeyValue::new(OpType::Put, b"key".to_vec(), Some(b"value".to_vec()));
        let mut encoded = vec![];
        kv.encode(&mut encoded).unwrap();
        let mut bytes_slice = encoded.as_slice();
        let decoded_kv = KeyValue::decode(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(kv, decoded_kv);
    }

    #[test]
    fn test_log_item_enc_dec() {
        let region_id = 8;
        let entries_len = 10;
        let entry_indexes =
            generate_entry_indexes(1, 1 + entries_len, LogQueue::Append, FileId::from(0));
        let items = vec![
            LogItem::new_entry_indexes(region_id, entry_indexes),
            LogItem::new_command(region_id, Command::Clean),
            LogItem::new_kv(
                region_id,
                OpType::Put,
                b"key".to_vec(),
                Some(b"value".to_vec()),
            ),
        ];

        for item in items.into_iter() {
            let mut encoded_item = vec![];
            item.encode(&mut encoded_item).unwrap();

            let mut s = encoded_item.as_slice();
            let decoded_item = LogItem::decode(&mut s, &mut 0).unwrap();

            assert_eq!(s.len(), 0);
            assert_eq!(item, decoded_item);
        }
    }

    #[test]
    fn test_log_item_batch_enc_dec() {
        let region_id = 8;
        let entries_size = 0;
        let mut entry_indexes = vec![];
        entry_indexes.append(&mut generate_entry_indexes(
            1,
            1 + entries_size as u64,
            LogQueue::Append,
            FileId::from(0),
        ));
        entry_indexes.append(&mut generate_entry_indexes(
            100,
            100 + entries_size as u64,
            LogQueue::Append,
            FileId::from(0),
        ));

        let mut batch = LogItemBatch::with_capacity(3);
        batch.add_entry_indexes(region_id + 100, entry_indexes);
        batch.add_command(region_id, Command::Clean);
        batch.put(region_id, b"key".to_vec(), b"value".to_vec());
        batch.delete_message(region_id, b"key2".to_vec());

        let mut encoded_batch = vec![];
        batch.encode(&mut encoded_batch).unwrap();
        let decoded_batch =
            LogItemBatch::decode(&mut encoded_batch.as_slice(), 0, 0, CompressionType::None)
                .unwrap();
        assert_eq!(batch, decoded_batch);
    }

    #[test]
    fn test_log_batch_enc_dec() {
        let region_id = 8;
        let mut batch = LogBatch::default();
        batch
            .add_entries::<Entry>(
                region_id,
                generate_entries(1, 10, Some(vec![b'x'; 1024].into())),
            )
            .unwrap();
        batch.add_command(region_id, Command::Clean);
        batch.put(region_id, b"key".to_vec(), b"value".to_vec());
        batch.delete_message(region_id, b"key2".to_vec());
        batch
            .add_entries::<Entry>(
                region_id,
                generate_entries(1, 10, Some(vec![b'x'; 1024].into())),
            )
            .unwrap();

        let encoded = batch.encoded_bytes(0).unwrap();

        // decode item batch
        let (offset, compression_type, len) = LogBatch::decode_header(&mut &encoded[..]).unwrap();
        assert_eq!(encoded.len(), len);
        let decoded_item_batch = LogItemBatch::decode(
            &mut &encoded[offset..],
            LOG_BATCH_HEADER_LEN,
            offset - LOG_BATCH_HEADER_LEN,
            compression_type,
        )
        .unwrap();

        // decode and assert entries
        let mut entries = &encoded[LOG_BATCH_HEADER_LEN..offset as usize];
        for item in decoded_item_batch.items.iter() {
            if let LogItemContent::EntriesIndex(entries_index) = &item.content {
                let origin_entries = generate_entries(1, 10, Some(vec![b'x'; 1024].into()));
                let decoded_entries =
                    decode_entries_from_bytes(&mut entries, &entries_index.0, false);
                assert_eq!(origin_entries, decoded_entries);
            }
        }
    }
}
