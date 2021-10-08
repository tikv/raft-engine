// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::io::BufRead;
use std::{mem, u64};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::error;
use protobuf::parse_from_bytes;
use protobuf::Message;

use crate::codec::{self, Error as CodecError, NumberEncoder};
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

const DEFAULT_LOG_ITEM_BATCH_CAP: usize = 64;
const MAX_LOG_BATCH_BUFFER_CAP: usize = 4 * 1024 * 1024; // 4MB

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
    pub fn from_u8(t: u8) -> Result<Self> {
        if t <= CompressionType::Lz4 as u8 {
            Ok(unsafe { mem::transmute(t) })
        } else {
            Err(Error::Corruption(format!(
                "Unrecognized compression type: {}",
                t
            )))
        }
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
    pub fn from_u8(t: u8) -> Result<Self> {
        if t <= OpType::Del as u8 {
            Ok(unsafe { mem::transmute(t) })
        } else {
            Err(Error::Corruption(format!("Unrecognized op type: {}", t)))
        }
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
        let op_type = OpType::from_u8(codec::read_u8(buf)?)?;
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

impl LogItemBatch {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            item_size: 0,
        }
    }

    pub fn iter(&self) -> std::slice::Iter<LogItem> {
        self.items.iter()
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

#[derive(Copy, Clone, Debug, PartialEq)]
enum BufState {
    /// Buffer contains header and optionally entries.
    /// # Invariants
    /// LOG_BATCH_HEADER_LEN <= buf.len()
    Open,
    /// Buffer contains header, entries and footer; ready to be written. This
    /// state only briefly exists between encoding and writing, user operation
    /// will panic under this state.
    /// # Content
    /// counter: Buffer offset of header.
    /// # Invariants
    /// LOG_BATCH_HEADER_LEN <= buf.len()
    Sealed(usize),
    /// Buffer is undergoing writes. User operation will panic under this state.
    Incomplete,
}

// Format:
// { u56 len | u8 compression type | u64 item offset | (compressed) entries | crc32 | item batch }
pub struct LogBatch {
    item_batch: LogItemBatch,
    buf_state: BufState,
    buf: Vec<u8>,
}

impl Default for LogBatch {
    fn default() -> Self {
        Self::with_capacity(DEFAULT_LOG_ITEM_BATCH_CAP)
    }
}

impl LogBatch {
    pub fn with_capacity(cap: usize) -> Self {
        let mut buf = Vec::with_capacity(4096);
        buf.resize(LOG_BATCH_HEADER_LEN, 0);
        Self {
            item_batch: LogItemBatch::with_capacity(cap),
            buf_state: BufState::Open,
            buf,
        }
    }

    pub(crate) fn drain(&mut self) -> LogItemDrain {
        debug_assert!(matches!(self.buf_state, BufState::Sealed(_)));

        self.buf.shrink_to(MAX_LOG_BATCH_BUFFER_CAP);
        self.buf.truncate(LOG_BATCH_HEADER_LEN);
        self.buf_state = BufState::Open;
        self.item_batch.drain()
    }

    pub fn merge(&mut self, rhs: &mut Self) -> Result<()> {
        debug_assert!(self.buf_state == BufState::Open && rhs.buf_state == BufState::Open);

        if !rhs.buf.is_empty() {
            self.buf_state = BufState::Incomplete;
            let buf_offset = self.buf.len() - LOG_BATCH_HEADER_LEN;
            self.buf.extend_from_slice(&rhs.buf[LOG_BATCH_HEADER_LEN..]);
            rhs.buf.shrink_to(MAX_LOG_BATCH_BUFFER_CAP);
            rhs.buf.truncate(LOG_BATCH_HEADER_LEN);
            for item in &mut rhs.item_batch.items {
                if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                    for ei in entries_index.0.iter_mut() {
                        ei.entries_offset += buf_offset as u64;
                    }
                }
            }
            self.buf_state = BufState::Open;
        }
        self.item_batch.items.append(&mut rhs.item_batch.items);
        Ok(())
    }

    pub(crate) fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: Option<u64>) {
        self.item_batch.set_position(queue, file_id, offset);
    }

    pub fn add_entries<M: MessageExt>(
        &mut self,
        region_id: u64,
        entries: &[M::Entry],
    ) -> Result<()> {
        debug_assert!(self.buf_state == BufState::Open);

        let mut entry_indexes = Vec::with_capacity(entries.len());
        self.buf_state = BufState::Incomplete;
        for e in entries {
            let buf_offset = self.buf.len();
            e.write_to_vec(&mut self.buf)?;
            entry_indexes.push(EntryIndex {
                index: M::index(e),
                entry_offset: (buf_offset - LOG_BATCH_HEADER_LEN) as u64,
                entry_len: self.buf.len() - buf_offset,
                ..Default::default()
            });
        }
        self.buf_state = BufState::Open;
        self.item_batch.add_entry_indexes(region_id, entry_indexes);
        Ok(())
    }

    pub(crate) fn add_raw_entries(
        &mut self,
        region_id: u64,
        mut entry_indexes: Vec<EntryIndex>,
        entries: Vec<Vec<u8>>,
    ) -> Result<()> {
        debug_assert!(entry_indexes.len() == entries.len());
        debug_assert!(self.buf_state == BufState::Open);

        self.buf_state = BufState::Incomplete;
        for (ei, e) in entry_indexes.iter_mut().zip(entries.iter()) {
            let buf_offset = self.buf.len();
            self.buf.extend(e);
            ei.entry_offset = (buf_offset - LOG_BATCH_HEADER_LEN) as u64;
            ei.entry_len = self.buf.len() - buf_offset;
        }
        self.buf_state = BufState::Open;
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

    pub(crate) fn encoded_bytes(&mut self, compression_threshold: usize) -> Result<&[u8]> {
        // TODO: avoid to write a large batch into one compressed chunk.
        if let BufState::Sealed(header_offset) = self.buf_state {
            return Ok(&self.buf[header_offset..]);
        }
        debug_assert!(self.buf_state == BufState::Open);
        self.buf_state = BufState::Incomplete;

        // entries
        let (header_offset, compression_type) = if compression_threshold > 0
            && self.buf.len() > LOG_BATCH_HEADER_LEN + compression_threshold
        {
            let buf_len = self.buf.len();
            lz4::append_encode_block(&mut self.buf, LOG_BATCH_HEADER_LEN);
            (buf_len - LOG_BATCH_HEADER_LEN, CompressionType::Lz4)
        } else {
            (0, CompressionType::None)
        };

        // checksum
        if self.buf.len() > header_offset + LOG_BATCH_HEADER_LEN {
            let checksum = crc32(&self.buf[header_offset + LOG_BATCH_HEADER_LEN..]);
            self.buf.encode_u32_le(checksum)?;
        }
        let footer_roffset = self.buf.len() - header_offset;

        // footer
        self.item_batch.encode(&mut self.buf)?;

        // header
        let len =
            (((self.buf.len() - header_offset) as u64) << 8) | u64::from(compression_type.to_u8());
        (&mut self.buf[header_offset..header_offset + 8]).write_u64::<BigEndian>(len)?;
        (&mut self.buf[header_offset + 8..header_offset + 16])
            .write_u64::<BigEndian>(footer_roffset as u64)?;

        self.buf_state = BufState::Sealed(header_offset);

        // update entry indexes.
        for item in self.item_batch.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                for ei in entries_index.0.iter_mut() {
                    ei.compression_type = compression_type;
                    ei.entries_offset = LOG_BATCH_HEADER_LEN as u64; // need to add batch offset of the file after written.
                    ei.entries_len = footer_roffset - LOG_BATCH_HEADER_LEN;
                }
            }
        }
        Ok(&self.buf[header_offset..])
    }

    /// Report encoded size of current log batch.
    pub fn size(&self) -> usize {
        if self.is_empty() {
            0
        } else {
            match self.buf_state {
                BufState::Open => self.buf.len() + LOG_BATCH_CHECKSUM_LEN + self.item_batch.size(),
                BufState::Sealed(header_offset) => self.buf.len() - header_offset,
                s => {
                    error!("querying incomplete log batch with state {:?}", s);
                    0
                }
            }
        }
    }

    pub(crate) fn decode_header(buf: &mut SliceReader) -> Result<(usize, CompressionType, usize)> {
        assert!(buf.len() >= LOG_BATCH_HEADER_LEN);

        let len = codec::decode_u64(buf)? as usize;
        let offset = codec::decode_u64(buf)? as usize;
        let compression_type = CompressionType::from_u8(len as u8)?;
        if offset > len {
            return Err(Error::Corruption(
                "Log item offset exceeds log batch length".to_owned(),
            ));
        } else if offset < LOG_BATCH_HEADER_LEN {
            return Err(Error::Corruption(
                "Log item offset is smaller than log batch header length".to_owned(),
            ));
        }
        Ok((offset, compression_type, len >> 8))
    }

    pub(crate) fn parse_entry<M: MessageExt>(buf: &[u8], idx: &EntryIndex) -> Result<M::Entry> {
        let len = idx.entries_len;
        if len > 0 {
            // protobuf message can be serialized to empty string.
            verify_checksum(&buf[0..len])?;
        }
        match idx.compression_type {
            CompressionType::None => Ok(parse_from_bytes(
                &buf[idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len],
            )?),
            CompressionType::Lz4 => {
                let decompressed = lz4::decode_block(&buf[..len - LOG_BATCH_CHECKSUM_LEN]);
                Ok(parse_from_bytes(
                    &decompressed
                        [idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len],
                )?)
            }
        }
    }

    pub(crate) fn parse_entry_bytes(buf: &[u8], idx: &EntryIndex) -> Result<Vec<u8>> {
        let len = idx.entries_len;
        if len > 0 {
            verify_checksum(&buf[0..len])?;
        }
        match idx.compression_type {
            CompressionType::None => Ok(buf
                [idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len]
                .to_owned()),
            CompressionType::Lz4 => {
                let decompressed = lz4::decode_block(&buf[..len - LOG_BATCH_CHECKSUM_LEN]);
                Ok(decompressed
                    [idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len]
                    .to_owned())
            }
        }
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
    use raft::eraftpb::Entry;
    use rand::{thread_rng, Rng};

    fn decode_entries_from_bytes<M: MessageExt>(
        buf: &[u8],
        entry_indexes: &[EntryIndex],
        _encoded: bool,
    ) -> Vec<M::Entry> {
        let mut entries = Vec::with_capacity(entry_indexes.len());
        for ei in entry_indexes {
            entries.push(LogBatch::parse_entry::<M>(buf, ei).unwrap());
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
                &generate_entries(1, 10, Some(vec![b'x'; 1024].into())),
            )
            .unwrap();
        batch.add_command(region_id, Command::Clean);
        batch.put(region_id, b"key".to_vec(), b"value".to_vec());
        batch.delete_message(region_id, b"key2".to_vec());
        batch
            .add_entries::<Entry>(
                region_id,
                &generate_entries(1, 10, Some(vec![b'x'; 1024].into())),
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
                    decode_entries_from_bytes::<Entry>(&mut entries, &entries_index.0, false);
                assert_eq!(origin_entries, decoded_entries);
            }
        }
    }

    #[bench]
    fn bench_log_batch_add_entry_and_encode(b: &mut test::Bencher) {
        fn details(log_batch: &mut LogBatch, entries: &Vec<Entry>, regions: usize) {
            for _ in 0..regions {
                log_batch
                    .add_entries::<Entry>(thread_rng().gen(), entries)
                    .unwrap();
            }
            log_batch.encoded_bytes(1).unwrap();
            let _ = log_batch.drain();
        }
        let data: Vec<u8> = (0..128).map(|_| thread_rng().gen()).collect();
        let entries = generate_entries(1, 10, Some(data));
        let mut log_batch = LogBatch::default();
        // warm up
        details(&mut log_batch, &entries, 100);
        b.iter(move || {
            details(&mut log_batch, &entries, 100);
        });
    }
}
