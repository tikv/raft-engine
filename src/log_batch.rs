// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::io::BufRead;
use std::marker::PhantomData;
use std::{mem, u64};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use log::trace;
use protobuf::Message;

use crate::codec::{self, Error as CodecError, NumberEncoder};
use crate::memtable::EntryIndex;
use crate::pipe_log::{FileId, LogQueue};
use crate::util::{crc32, lz4};
use crate::{Error, Result};

pub const HEADER_LEN: usize = 16;
pub const CHECKSUM_LEN: usize = 4;

const TYPE_ENTRIES: u8 = 0x01;
const TYPE_COMMAND: u8 = 0x02;
const TYPE_KV: u8 = 0x3;

const CMD_CLEAN: u8 = 0x01;
const CMD_COMPACT: u8 = 0x02;

const DEFAULT_BATCH_CAP: usize = 64;
const DEFAULT_ENTRIES_PER_ITEM: usize = 10;

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
    pub fn from_byte(t: u8) -> CompressionType {
        unsafe { mem::transmute(t) }
    }

    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

type SliceReader<'a> = &'a [u8];

#[derive(Debug, PartialEq)]
pub struct EntriesIndex(pub Vec<EntryIndex>);

impl EntriesIndex {
    pub fn with_capacity(cap: usize) -> Self {
        Self(Vec::with_capacity(cap))
    }

    pub fn from_bytes(buf: &mut SliceReader<'_>, entries_size: &mut usize) -> Result<Self> {
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

    pub fn encode_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // layout = { count | first_index | [ tail_offset ] }
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

    pub fn set_queue_and_file_id(&mut self, queue: LogQueue, file_id: FileId) {
        for idx in self.0.iter_mut() {
            idx.queue = queue;
            idx.file_id = file_id;
        }
    }

    fn compute_approximate_size(&self) -> usize {
        8 /*count*/ + (8/*index*/+8/*tail_offset*/) * self.0.len()
    }
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Clean,
    Compact { index: u64 },
}

impl Command {
    pub fn encode_to(&self, vec: &mut Vec<u8>) {
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

    pub fn from_bytes(buf: &mut SliceReader<'_>) -> Result<Command> {
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

    fn compute_approximate_size(&self) -> usize {
        match &self {
            Command::Clean => 1,              /*type*/
            Command::Compact { .. } => 1 + 8, /*type + index*/
        }
    }
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OpType {
    Put = 0x01,
    Del = 0x02,
}

impl OpType {
    pub fn encode_to(self, vec: &mut Vec<u8>) {
        vec.push(self as u8);
    }

    pub fn from_bytes(buf: &mut SliceReader<'_>) -> Result<OpType> {
        let op_type = buf.read_u8()?;
        unsafe { Ok(mem::transmute(op_type)) }
    }
}

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

    pub fn from_bytes(buf: &mut SliceReader<'_>) -> Result<KeyValue> {
        let op_type = OpType::from_bytes(buf)?;
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

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        // layout = { op_type | k_len | key | v_len | value }
        self.op_type.encode_to(vec);
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

    fn compute_approximate_size(&self) -> usize {
        1 /*op*/ + 8 /*k_len*/ + self.key.len() + 8 /*v_len*/ + self.value.as_ref().map_or_else(|| 0, |v| v.len())
    }
}

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
    /// `from_entries` only reserve an EntriesIndex slot for given entries.
    pub fn from_entries<M: MessageExt>(raft_group_id: u64, entries: &Vec<M::Entry>) -> LogItem {
        LogItem {
            raft_group_id,
            content: LogItemContent::EntriesIndex(EntriesIndex::with_capacity(entries.len())),
        }
    }

    pub fn from_command(raft_group_id: u64, command: Command) -> LogItem {
        LogItem {
            raft_group_id,
            content: LogItemContent::Command(command),
        }
    }

    pub fn from_kv(
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

    pub fn encode_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // layout = { 8 byte id | 1 byte type | item layout }
        buf.encode_var_u64(self.raft_group_id)?;
        match &self.content {
            LogItemContent::EntriesIndex(entries_index) => {
                buf.push(TYPE_ENTRIES);
                entries_index.encode_to(buf)?;
            }
            LogItemContent::Command(command) => {
                buf.push(TYPE_COMMAND);
                command.encode_to(buf);
            }
            LogItemContent::Kv(kv) => {
                buf.push(TYPE_KV);
                kv.encode_to(buf)?;
            }
        }
        Ok(())
    }

    pub fn from_bytes(buf: &mut SliceReader<'_>, entries_size: &mut usize) -> Result<LogItem> {
        let raft_group_id = codec::decode_var_u64(buf)?;
        trace!("decoding log item for {}", raft_group_id);
        let item_type = buf.read_u8()?;
        let content = match item_type {
            TYPE_ENTRIES => {
                let entries_index = EntriesIndex::from_bytes(buf, entries_size)?;
                LogItemContent::EntriesIndex(entries_index)
            }
            TYPE_COMMAND => {
                let cmd = Command::from_bytes(buf)?;
                trace!("decoding command: {:?}", cmd);
                LogItemContent::Command(cmd)
            }
            TYPE_KV => {
                let kv = KeyValue::from_bytes(buf)?;
                trace!("decoding kv: {:?}", kv);
                LogItemContent::Kv(kv)
            }
            _ => return Err(Error::Codec(CodecError::KeyNotFound)),
        };
        Ok(LogItem {
            raft_group_id,
            content,
        })
    }

    fn compute_approximate_size(&self) -> usize {
        match &self.content {
            LogItemContent::EntriesIndex(entries_index) => {
                8 /*r_id*/ + 1 /*type*/ + entries_index.compute_approximate_size()
            }
            LogItemContent::Command(cmd) => 8 + 1 + cmd.compute_approximate_size(),
            LogItemContent::Kv(kv) => 8 + 1 + kv.compute_approximate_size(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct LogItemBatch<M: MessageExt> {
    items: Vec<LogItem>,
    _phantom: PhantomData<M>,
}

impl<M> Default for LogItemBatch<M>
where
    M: MessageExt,
{
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BATCH_CAP)
    }
}

impl<M> LogItemBatch<M>
where
    M: MessageExt,
{
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            _phantom: PhantomData,
        }
    }

    pub fn merge(&mut self, rhs: &mut Self) {
        self.items.append(&mut rhs.items);
    }

    pub fn iter_mut(&mut self) -> core::slice::IterMut<LogItem> {
        self.items.iter_mut()
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, LogItem> {
        self.items.drain(..)
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn set_entries_indexes(&mut self, entry_indexes: &mut Vec<EntryIndex>) {
        for item in self.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                *entries_index =
                    EntriesIndex(entry_indexes.drain(..entries_index.0.capacity()).collect());
            }
        }
        assert!(entry_indexes.is_empty());
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: u64) {
        for item in self.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                for ei in entries_index.0.iter_mut() {
                    ei.queue = queue;
                    ei.file_id = file_id;
                    ei.entries_offset += offset;
                }
            }
        }
    }

    pub fn set_queue_and_file_id(&mut self, queue: LogQueue, file_id: FileId) {
        for item in self.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                entries_index.set_queue_and_file_id(queue, file_id);
            }
        }
    }

    pub fn add_entries(&mut self, region_id: u64, entries: &Vec<M::Entry>) -> usize {
        let item = LogItem::from_entries::<M>(region_id, entries);
        let size = item.compute_approximate_size();
        self.items.push(item);
        size
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) -> usize {
        let item = LogItem::from_command(region_id, cmd);
        let size = item.compute_approximate_size();
        self.items.push(item);
        size
    }

    pub fn delete_message(&mut self, region_id: u64, key: Vec<u8>) -> usize {
        let item = LogItem::from_kv(region_id, OpType::Del, key, None);
        let size = item.compute_approximate_size();
        self.items.push(item);
        size
    }

    pub fn put_message<S: Message>(
        &mut self,
        region_id: u64,
        key: Vec<u8>,
        s: &S,
    ) -> Result<usize> {
        self.put(region_id, key, s.write_to_bytes()?)
    }

    pub fn put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<usize> {
        let item = LogItem::from_kv(region_id, OpType::Put, key, Some(value));
        let size = item.compute_approximate_size();
        self.items.push(item);
        Ok(size)
    }

    pub fn encode_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        let offset = buf.len();
        let count = self.items.len() as u64;
        buf.encode_var_u64(count)?;
        for item in self.items.iter() {
            item.encode_to(buf)?;
        }
        let checksum = crc32(&buf[offset..]);
        buf.encode_u32_le(checksum)?;
        Ok(())
    }

    pub fn from_bytes(
        buf: &mut SliceReader<'_>,
        entries_offset: u64,
        entries_len: usize,
        compression_type: CompressionType,
    ) -> Result<LogItemBatch<M>> {
        test_checksum(buf)?;
        let mut count = codec::decode_var_u64(buf)?;
        assert!(count > 0 && !buf.is_empty());
        let mut items = LogItemBatch::with_capacity(count as usize);
        let mut entries_size = 0;
        while count > 0 {
            let item = LogItem::from_bytes(buf, &mut entries_size)?;
            items.items.push(item);
            count -= 1;
        }

        for item in items.items.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                for ei in entries_index.0.iter_mut() {
                    ei.compression_type = compression_type;
                    ei.entries_offset = entries_offset;
                    ei.entries_len = entries_len;
                }
            }
        }
        buf.consume(CHECKSUM_LEN);

        Ok(items)
    }
}

#[derive(Debug, PartialEq)]
pub struct LogBatch<M: MessageExt> {
    item_batch: LogItemBatch<M>,
    entries: Vec<M::Entry>,
    content_approximate_size: usize,
}

impl<M> Default for LogBatch<M>
where
    M: MessageExt,
{
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BATCH_CAP)
    }
}

impl<M> LogBatch<M>
where
    M: MessageExt,
{
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            item_batch: LogItemBatch::with_capacity(cap),
            entries: Vec::with_capacity(cap * DEFAULT_ENTRIES_PER_ITEM),
            content_approximate_size: 0,
        }
    }

    pub fn merge(&mut self, rhs: &mut Self) {
        self.item_batch.merge(&mut rhs.item_batch);
        self.content_approximate_size += rhs.content_approximate_size;
        self.entries.append(&mut rhs.entries);
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: u64) {
        self.item_batch.set_position(queue, file_id, offset);
    }

    pub fn set_queue_and_file_id(&mut self, queue: LogQueue, file_id: FileId) {
        self.item_batch.set_queue_and_file_id(queue, file_id);
    }

    pub fn add_entries(&mut self, region_id: u64, mut entries: Vec<M::Entry>) {
        self.content_approximate_size += self.item_batch.add_entries(region_id, &entries);
        for i in 0..entries.len() {
            self.content_approximate_size += entries[i].compute_size() as usize;
            if i != 0 {
                assert_eq!(M::index(&entries[i]) - 1, M::index(&entries[i - 1]));
            }
        }
        self.entries.append(&mut entries);
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) {
        self.content_approximate_size += self.item_batch.add_command(region_id, cmd);
    }

    pub fn delete_message(&mut self, region_id: u64, key: Vec<u8>) {
        self.content_approximate_size += self.item_batch.delete_message(region_id, key);
    }

    pub fn put_message<S: Message>(&mut self, region_id: u64, key: Vec<u8>, s: &S) -> Result<()> {
        self.content_approximate_size += self.item_batch.put_message(region_id, key, s)?;
        Ok(())
    }

    pub fn put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        self.content_approximate_size += self.item_batch.put(region_id, key, value)?;
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.item_batch.is_empty()
    }

    pub fn items_batch(&mut self) -> &mut LogItemBatch<M> {
        &mut self.item_batch
    }

    pub fn parse_header(buf: &mut SliceReader<'_>) -> Result<(usize, u64, CompressionType)> {
        assert_eq!(buf.len(), HEADER_LEN);
        let mut len = codec::decode_u64(buf)? as usize;
        let offset = codec::decode_u64(buf)?;
        let compression_type = CompressionType::from_byte(len as u8);
        len >>= 8;
        Ok((len, offset, compression_type))
    }

    /// `encode_to_bytes` will drain entries in the batch, leaves items only.
    pub fn encode_to_bytes(&mut self, compression_threshold: usize) -> Result<Option<Vec<u8>>> {
        // TODO: avoid to write a large batch into one compressed chunk.
        if self.is_empty() {
            return Ok(None);
        }

        // layout:
        // <---     header     ---><---        entries        ---><---                    footer                    --->
        // { u64 len | u64 offset | (compressed) entries | crc32 | item count | entry indexes / commands / kvs | crc32 }
        //                                                      ^ offset = <offset>

        let mut buf = Vec::with_capacity(4096);

        // reserve for header
        buf.encode_u64(0)?;
        buf.encode_u64(0)?;

        // encode entries first, set entries_index
        let mut entries_size = 0;
        let mut entry_indexes = vec![EntryIndex::default(); self.entries.len()];
        for (i, e) in self.entries.drain(..).enumerate() {
            e.write_to_vec(&mut buf)?;
            entry_indexes[i].index = M::index(&e);
            entry_indexes[i].entry_offset = entries_size as u64;
            entry_indexes[i].entry_len = buf.len() - entries_size - HEADER_LEN;
            entries_size += entry_indexes[i].entry_len;
        }
        self.item_batch.set_entries_indexes(&mut entry_indexes);

        let compression_type =
            if compression_threshold > 0 && (buf.len() - HEADER_LEN) > compression_threshold {
                buf = lz4::encode_block(&buf[HEADER_LEN..], HEADER_LEN, 4);
                CompressionType::Lz4
            } else {
                CompressionType::None
            };
        let checksum = crc32(&buf[HEADER_LEN..]);
        buf.encode_u32_le(checksum)?;

        let offset = buf.len() as u64;

        // encode footer (entries_index / commands / kvs) with checksum
        self.item_batch.encode_to(&mut buf)?;

        // write header
        let len = ((buf.len() as u64) << 8) | u64::from(compression_type.to_byte());

        (&mut buf[..HEADER_LEN]).write_u64::<BigEndian>(len)?;
        (&mut buf[8..HEADER_LEN]).write_u64::<BigEndian>(offset)?;

        // update entries_index
        for item in self.item_batch.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &mut item.content {
                for ei in entries_index.0.iter_mut() {
                    ei.compression_type = compression_type;
                    ei.entries_offset = HEADER_LEN as u64; // need to add batch offset of the file after written.
                    ei.entries_len = offset as usize - HEADER_LEN;
                }
            }
        }
        Ok(Some(buf))
    }

    // Don't account for compression and varint encoding.
    // Not meaningful after recovery.
    pub fn approximate_size(&self) -> usize {
        if self.item_batch.is_empty() {
            0
        } else {
            8 /*len*/ + 8 /*offset*/ + 8 /*items count*/
            + self.content_approximate_size + CHECKSUM_LEN * 2
        }
    }
}

pub fn test_checksum(buf: &[u8]) -> Result<()> {
    if buf.len() <= CHECKSUM_LEN {
        return Err(Error::TooShort);
    }

    let batch_len = buf.len();
    let mut s = &buf[(batch_len - CHECKSUM_LEN)..batch_len];
    let expected = codec::decode_u32_le(&mut s)?;
    let got = crc32(&buf[..(batch_len - CHECKSUM_LEN)]);
    if got != expected {
        return Err(Error::IncorrectChecksum(expected, got));
    }
    Ok(())
}

// NOTE: lz4::decode_block will truncate the output buffer first.
pub fn decompress(buf: &[u8]) -> Vec<u8> {
    self::lz4::decode_block(buf)
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
            data = decompress(&data[..data.len() - CHECKSUM_LEN]);
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
        cmd.encode_to(&mut encoded);
        let mut bytes_slice = encoded.as_slice();
        let decoded_cmd = Command::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(cmd, decoded_cmd);
    }

    #[test]
    fn test_kv_enc_dec() {
        let kv = KeyValue::new(OpType::Put, b"key".to_vec(), Some(b"value".to_vec()));
        let mut encoded = vec![];
        kv.encode_to(&mut encoded).unwrap();
        let mut bytes_slice = encoded.as_slice();
        let decoded_kv = KeyValue::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(kv, decoded_kv);
    }

    #[test]
    fn test_log_item_enc_dec() {
        let region_id = 8;
        let entries_len = 10;
        let entry_indexes =
            generate_entry_indexes(1, 1 + entries_len, LogQueue::Append, FileId::from(0));
        let mut items = vec![
            LogItem::from_entries::<Entry>(
                region_id,
                &vec![Entry::default(); entries_len as usize],
            ),
            LogItem::from_command(region_id, Command::Clean),
            LogItem::from_kv(
                region_id,
                OpType::Put,
                b"key".to_vec(),
                Some(b"value".to_vec()),
            ),
        ];
        if let LogItemContent::EntriesIndex(eis) = &mut items[0].content {
            *eis = EntriesIndex(entry_indexes);
        }

        for item in items.into_iter() {
            let mut encoded_item = vec![];
            item.encode_to(&mut encoded_item).unwrap();

            let mut s = encoded_item.as_slice();
            let decoded_item = LogItem::from_bytes(&mut s, &mut 0).unwrap();

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

        let mut batch = LogItemBatch::<Entry>::with_capacity(3);
        batch.add_entries(region_id, &vec![Entry::default(); entries_size]);
        batch.add_command(region_id, Command::Clean);
        batch
            .put(region_id, b"key".to_vec(), b"value".to_vec())
            .unwrap();
        batch.delete_message(region_id, b"key2".to_vec());
        batch.add_entries(region_id + 100, &vec![Entry::default(); entries_size]);
        batch.set_entries_indexes(&mut entry_indexes);

        let mut encoded_batch = vec![];
        batch.encode_to(&mut encoded_batch).unwrap();
        let decoded_batch =
            LogItemBatch::<Entry>::from_bytes(&mut &encoded_batch[..], 0, 0, CompressionType::None)
                .unwrap();
        assert_eq!(batch, decoded_batch);
    }

    #[test]
    fn test_log_batch_enc_dec() {
        let region_id = 8;
        let mut batch = LogBatch::<Entry>::default();
        batch.add_entries(
            region_id,
            generate_entries(1, 10, Some(vec![b'x'; 1024].into())),
        );
        batch.add_command(region_id, Command::Clean);
        batch
            .put(region_id, b"key".to_vec(), b"value".to_vec())
            .unwrap();
        batch.delete_message(region_id, b"key2".to_vec());
        batch.add_entries(
            region_id,
            generate_entries(1, 10, Some(vec![b'x'; 1024].into())),
        );

        let encoded = batch.encode_to_bytes(0).unwrap().unwrap();

        let mut s = encoded.as_slice();

        // decode item batch
        let (len, offset, compression_type) =
            LogBatch::<Entry>::parse_header(&mut &s[..HEADER_LEN]).unwrap();
        assert_eq!(s.len(), len);
        s.consume(offset as usize);
        let mut decoded_item_batch = LogItemBatch::from_bytes(
            &mut s,
            HEADER_LEN as u64,
            offset as usize - HEADER_LEN,
            compression_type,
        )
        .unwrap();
        assert_eq!(s.len(), 0);

        // decode and assert entries
        let s = &encoded[HEADER_LEN..offset as usize];
        for item in decoded_item_batch.iter_mut() {
            if let LogItemContent::EntriesIndex(entries_index) = &item.content {
                let origin_entries = generate_entries(1, 10, Some(vec![b'x'; 1024].into()));
                let decoded_entries = decode_entries_from_bytes(s, &entries_index.0, false);
                assert_eq!(origin_entries, decoded_entries);
            }
        }

        let decoded_batch = LogBatch {
            item_batch: decoded_item_batch,
            // entries should be drained
            entries: vec![],
            // no need to care `content_approximate_size`
            content_approximate_size: batch.content_approximate_size,
        };
        assert_eq!(batch, decoded_batch);
    }
}
