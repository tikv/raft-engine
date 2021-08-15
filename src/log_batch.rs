// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::fmt::Debug;
use std::io::BufRead;
use std::marker::PhantomData;
use std::{mem, u64, vec};

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

#[derive(Debug)]
pub struct Entries<M>
where
    M: MessageExt,
{
    pub entries: Vec<M::Entry>,
    // EntryIndex may be update after write to file.
    pub entries_index: Vec<EntryIndex>,
}

impl<M: MessageExt> PartialEq for Entries<M> {
    fn eq(&self, other: &Entries<M>) -> bool {
        self.entries == other.entries && self.entries_index == other.entries_index
    }
}

impl<M: MessageExt> Entries<M> {
    pub fn new(entries: Vec<M::Entry>, entries_index: Option<Vec<EntryIndex>>) -> Entries<M> {
        let entries_index =
            entries_index.unwrap_or_else(|| vec![EntryIndex::default(); entries.len()]);
        Entries {
            entries,
            entries_index,
        }
    }

    pub fn from_bytes(buf: &mut SliceReader<'_>, entries_size: &mut usize) -> Result<Entries<M>> {
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
        Ok(Entries::new(vec![], Some(entries_index)))
    }

    pub fn encode_entries_to(&mut self, buf: &mut Vec<u8>, entries_size: &mut usize) -> Result<()> {
        // only encode entries and set entries_index in memory, entries_index will be encoded later.
        for (i, e) in self.entries.iter().enumerate() {
            let content = e.write_to_bytes()?;
            if !self.entries_index[i].file_id.valid() {
                self.entries_index[i].index = M::index(e);
                self.entries_index[i].entry_offset = *entries_size as u64;
                self.entries_index[i].entry_len = content.len();
                *entries_size += self.entries_index[i].entry_len;
            }
            if i != 0 {
                assert_eq!(
                    self.entries_index[i].index - 1,
                    self.entries_index[i - 1].index
                )
            }
            buf.extend_from_slice(&content);
        }
        Ok(())
    }

    pub fn encode_entries_index_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // layout = { count | first_index | [ tail_offset ] }
        let count = self.entries_index.len() as u64;
        buf.encode_var_u64(count)?;
        if count > 0 {
            buf.encode_var_u64(self.entries_index[0].index)?;
        }
        for ei in self.entries_index.iter() {
            buf.encode_var_u64(ei.entry_offset + ei.entry_len as u64)?;
        }
        Ok(())
    }

    pub fn set_queue_and_file_id(&mut self, queue: LogQueue, file_id: FileId) {
        for idx in self.entries_index.iter_mut() {
            idx.queue = queue;
            idx.file_id = file_id;
        }
    }

    fn compute_approximate_size(&self) -> usize {
        let mut size = 8 /*count*/;
        if !self.entries.is_empty() {
            for e in self.entries.iter() {
                size += (8/*index*/+8/*tail_offset*/) + e.compute_size() as usize;
            }
        } else {
            for ei in self.entries_index.iter() {
                size += (8/*index*/+8/*tail_offset*/) + ei.entry_len;
            }
        }
        size
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
pub struct LogItem<M>
where
    M: MessageExt,
{
    pub raft_group_id: u64,
    pub content: LogItemContent<M>,
}

#[derive(Debug, PartialEq)]
pub enum LogItemContent<M>
where
    M: MessageExt,
{
    Entries(Entries<M>),
    Command(Command),
    Kv(KeyValue),
}

impl<M: MessageExt> LogItem<M> {
    pub fn from_entries(raft_group_id: u64, entries: Vec<M::Entry>) -> LogItem<M> {
        LogItem {
            raft_group_id,
            content: LogItemContent::Entries(Entries::new(entries, None)),
        }
    }

    pub fn from_command(raft_group_id: u64, command: Command) -> LogItem<M> {
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
    ) -> LogItem<M> {
        LogItem {
            raft_group_id,
            content: LogItemContent::Kv(KeyValue::new(op_type, key, value)),
        }
    }

    pub fn encode_entries_to(&mut self, buf: &mut Vec<u8>, entries_size: &mut usize) -> Result<()> {
        if let LogItemContent::Entries(entries) = &mut self.content {
            entries.encode_entries_to(buf, entries_size)?;
        }
        Ok(())
    }

    /// Must be called after `encode_entries_to`.
    pub fn encode_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // layout = { 8 byte id | 1 byte type | item layout }
        buf.encode_var_u64(self.raft_group_id)?;
        match &self.content {
            LogItemContent::Entries(entries) => {
                buf.push(TYPE_ENTRIES);
                entries.encode_entries_index_to(buf)?;
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

    pub fn from_bytes(buf: &mut SliceReader<'_>, entries_size: &mut usize) -> Result<LogItem<M>> {
        let raft_group_id = codec::decode_var_u64(buf)?;
        trace!("decoding log item for {}", raft_group_id);
        let item_type = buf.read_u8()?;
        let content = match item_type {
            TYPE_ENTRIES => {
                let entries = Entries::from_bytes(buf, entries_size)?;
                LogItemContent::Entries(entries)
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
            LogItemContent::Entries(entries) => {
                8 /*r_id*/ + 1 /*type*/ + entries.compute_approximate_size()
            }
            LogItemContent::Command(cmd) => 8 + 1 + cmd.compute_approximate_size(),
            LogItemContent::Kv(kv) => 8 + 1 + kv.compute_approximate_size(),
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct LogBatch<M: MessageExt> {
    items: Vec<LogItem<M>>,
    items_approximate_size: usize,
    _phantom: PhantomData<M>,
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
            items: Vec::with_capacity(cap),
            items_approximate_size: 0,
            _phantom: PhantomData,
        }
    }

    pub fn merge(&mut self, rhs: &mut Self) {
        self.items_approximate_size += rhs.items_approximate_size;
        self.items.append(&mut rhs.items);
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, LogItem<M>> {
        self.items_approximate_size = 0;
        self.items.drain(..)
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: u64) {
        for item in self.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                for ei in entries.entries_index.iter_mut() {
                    ei.queue = queue;
                    ei.file_id = file_id;
                    ei.entries_offset += offset;
                }
            }
        }
    }

    pub fn set_queue_and_file_id(&mut self, queue: LogQueue, file_id: FileId) {
        for item in self.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                entries.set_queue_and_file_id(queue, file_id);
            }
        }
    }

    pub fn add_entries(&mut self, region_id: u64, entries: Vec<M::Entry>) {
        let item = LogItem::from_entries(region_id, entries);
        self.items_approximate_size += item.compute_approximate_size();
        self.items.push(item);
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) {
        let item = LogItem::from_command(region_id, cmd);
        self.items_approximate_size += item.compute_approximate_size();
        self.items.push(item);
    }

    pub fn delete_message(&mut self, region_id: u64, key: Vec<u8>) {
        let item = LogItem::from_kv(region_id, OpType::Del, key, None);
        self.items_approximate_size += item.compute_approximate_size();
        self.items.push(item);
    }

    pub fn put_message<S: Message>(&mut self, region_id: u64, key: Vec<u8>, s: &S) -> Result<()> {
        self.put(region_id, key, s.write_to_bytes()?)
    }

    pub fn put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) -> Result<()> {
        let item = LogItem::from_kv(region_id, OpType::Put, key, Some(value));
        self.items_approximate_size += item.compute_approximate_size();
        self.items.push(item);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn parse_header(buf: &mut SliceReader<'_>) -> Result<(usize, u64, CompressionType)> {
        assert_eq!(buf.len(), HEADER_LEN);
        let mut len = codec::decode_u64(buf)? as usize;
        let offset = codec::decode_u64(buf)?;
        let compression_type = CompressionType::from_byte(len as u8);
        len >>= 8;
        Ok((len, offset, compression_type))
    }

    pub fn from_footer(
        buf: &mut SliceReader<'_>,
        entries_offset: u64,
        entries_len: usize,
        compression_type: CompressionType,
    ) -> Result<LogBatch<M>> {
        test_checksum(buf)?;
        let mut count = codec::decode_var_u64(buf)?;
        assert!(count > 0 && !buf.is_empty());
        let mut log_batch = LogBatch::with_capacity(count as usize);
        while count > 0 {
            let item = LogItem::from_bytes(buf, &mut 0)?;
            log_batch.items_approximate_size += item.compute_approximate_size();
            log_batch.items.push(item);
            count -= 1;
        }

        for item in log_batch.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                for ei in entries.entries_index.iter_mut() {
                    ei.compression_type = compression_type;
                    ei.entries_offset = entries_offset;
                    ei.entries_len = entries_len;
                }
            }
        }
        buf.consume(CHECKSUM_LEN);

        Ok(log_batch)
    }

    // TODO: avoid to write a large batch into one compressed chunk.
    pub fn encode_to_bytes(&mut self, compression_threshold: usize) -> Result<Option<Vec<u8>>> {
        if self.items.is_empty() {
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
        for item in self.items.iter_mut() {
            item.encode_entries_to(&mut buf, &mut 0)?;
        }
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

        // encode footer (entries_index / commands / kvs)
        let count = self.items.len() as u64;
        buf.encode_var_u64(count)?;
        for item in self.items.iter() {
            item.encode_to(&mut buf)?;
        }
        let checksum = crc32(&buf[offset as usize..]);
        buf.encode_u32_le(checksum)?;

        // write header
        let len = ((buf.len() as u64) << 8) | u64::from(compression_type.to_byte());

        (&mut buf[..HEADER_LEN]).write_u64::<BigEndian>(len)?;
        (&mut buf[8..HEADER_LEN]).write_u64::<BigEndian>(offset)?;

        // update entries_index
        for item in self.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                for ei in entries.entries_index.iter_mut() {
                    ei.compression_type = compression_type;
                    ei.entries_offset = HEADER_LEN as u64; // need to add batch offset of the file after written.
                    ei.entries_len = offset as usize - HEADER_LEN;
                }
            }
        }
        Ok(Some(buf))
    }

    // Don't account for compression and varint encoding.
    pub fn approximate_size(&self) -> usize {
        if self.items.is_empty() {
            0
        } else {
            8 /*len*/ + 8 /*section offset*/ + 8/*items count*/
            +self.items_approximate_size + CHECKSUM_LEN * 2 /*checksum*/
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

    use crate::test_util::generate_entries;
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
    fn test_entries_enc_dec() {
        for pb_entries in vec![
            generate_entries(1, 10, None),
            vec![], // Empty entries.
        ] {
            let mut entries = Entries::<Entry>::new(pb_entries, None);
            let mut encoded_entries = vec![];
            let mut encoded_entries_index = vec![];
            entries
                .encode_entries_to(&mut encoded_entries, &mut 0)
                .unwrap();
            entries
                .encode_entries_index_to(&mut encoded_entries_index)
                .unwrap();

            let mut s = encoded_entries_index.as_slice();
            let mut decoded_entries = Entries::<Entry>::from_bytes(&mut s, &mut 0).unwrap();
            assert_eq!(s.len(), 0);
            decoded_entries.entries =
                decode_entries_from_bytes(&encoded_entries, &decoded_entries.entries_index, false);
            assert_eq!(entries.entries, decoded_entries.entries);
            assert_eq!(entries.entries_index, decoded_entries.entries_index);
        }
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
        let items = vec![
            LogItem::<Entry>::from_entries(region_id, generate_entries(1, 10, None)),
            LogItem::from_command(region_id, Command::Clean),
            LogItem::from_kv(
                region_id,
                OpType::Put,
                b"key".to_vec(),
                Some(b"value".to_vec()),
            ),
        ];

        for mut item in items.into_iter() {
            let (mut encoded_item, mut encoded_entries) = (vec![], vec![]);
            item.encode_entries_to(&mut encoded_entries, &mut 0)
                .unwrap();
            item.encode_to(&mut encoded_item).unwrap();

            let mut s = encoded_item.as_slice();
            let mut decoded_item = LogItem::from_bytes(&mut s, &mut 0).unwrap();
            if let LogItemContent::Entries(entries) = decoded_item.content {
                decoded_item.content = LogItemContent::Entries(Entries::new(
                    decode_entries_from_bytes(&encoded_entries, &entries.entries_index, false),
                    Some(entries.entries_index),
                ));
            }
            assert_eq!(s.len(), 0);
            assert_eq!(item, decoded_item);
        }
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

        let encoded = batch.encode_to_bytes(0).unwrap().unwrap();

        let mut s = encoded.as_slice();

        // decide batch without entries
        let (len, offset, compression_type) =
            LogBatch::<Entry>::parse_header(&mut &s[..HEADER_LEN]).unwrap();
        assert_eq!(s.len(), len);
        s.consume(offset as usize);
        let mut decoded_batch = LogBatch::from_footer(
            &mut s,
            HEADER_LEN as u64,
            offset as usize - HEADER_LEN,
            compression_type,
        )
        .unwrap();
        assert_eq!(s.len(), 0);

        let s = &encoded[HEADER_LEN..offset as usize];
        for item in decoded_batch.items.as_mut_slice() {
            if let LogItemContent::Entries(entries) = &item.content {
                item.content = LogItemContent::Entries(Entries::new(
                    decode_entries_from_bytes(s, &entries.entries_index, false),
                    Some(entries.entries_index.to_owned()),
                ));
            }
        }

        assert_eq!(batch, decoded_batch);
    }
}
