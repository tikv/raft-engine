// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::borrow::{Borrow, Cow};
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

pub const BATCH_MIN_SIZE: usize = HEADER_LEN + CHECKSUM_LEN;
pub const HEADER_LEN: usize = 8;
pub const CHECKSUM_LEN: usize = 4;

const TYPE_ENTRIES: u8 = 0x01;
const TYPE_COMMAND: u8 = 0x02;
const TYPE_STATE: u8 = 0x3;

const CMD_CLEAN: u8 = 0x01;
const CMD_COMPACT: u8 = 0x02;

const DEFAULT_BATCH_CAP: usize = 64;

pub trait MessageExt: Send + Sync + std::fmt::Debug {
    type Entry: Message + Clone + PartialEq;
    type State: Message + Clone + PartialEq;

    fn entry_index(e: &Self::Entry) -> u64;
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
pub struct Entries<M: MessageExt> {
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

    pub fn from_bytes(
        buf: &mut SliceReader<'_>,
        file_id: FileId,
        base_offset: u64,  // Offset of the batch from its log file.
        batch_offset: u64, // Offset of the item from in its batch.
        entries_size: &mut usize,
    ) -> Result<Entries<M>> {
        let content_len = buf.len() as u64;
        let mut count = codec::decode_var_u64(buf)? as usize;
        trace!("decoding entries, count: {}", count);
        let mut entries = Vec::with_capacity(count);
        let mut entries_index = Vec::with_capacity(count);
        while count > 0 {
            let len = codec::decode_var_u64(buf)? as usize;
            trace!("decoding an entry, len: {}", len);
            let mut e = M::Entry::new();
            e.merge_from_bytes(&buf[..len])?;

            let entry_index = EntryIndex {
                index: M::entry_index(&e),
                file_id,
                base_offset,
                offset: batch_offset + content_len - buf.len() as u64,
                len: len as u64,
                ..Default::default()
            };
            *entries_size += entry_index.len as usize;

            buf.consume(len);
            entries.push(e);
            entries_index.push(entry_index);

            count -= 1;
        }
        Ok(Entries::new(entries, Some(entries_index)))
    }

    pub fn encode_to(&mut self, vec: &mut Vec<u8>, entries_size: &mut usize) -> Result<()> {
        // layout = { entries count | multiple entries }
        // entries layout = { entry layout | ... | entry layout }
        // entry layout = { len | entry content }
        vec.encode_var_u64(self.entries.len() as u64)?;
        for (i, e) in self.entries.iter().enumerate() {
            let content = e.write_to_bytes()?;
            vec.encode_var_u64(content.len() as u64)?;

            if !self.entries_index[i].file_id.valid() {
                self.entries_index[i].index = M::entry_index(e);
                // This offset doesn't count the header.
                self.entries_index[i].offset = vec.len() as u64;
                self.entries_index[i].len = content.len() as u64;
                *entries_size += self.entries_index[i].len as usize;
            }

            vec.extend_from_slice(&content);
        }
        Ok(())
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: u64) {
        for idx in self.entries_index.iter_mut() {
            debug_assert!(!idx.file_id.valid() && idx.base_offset == 0);
            idx.queue = queue;
            idx.file_id = file_id;
            idx.base_offset = offset;
        }
    }

    pub fn set_queue(&mut self, queue: LogQueue) {
        for idx in self.entries_index.iter_mut() {
            debug_assert!(idx.file_id.valid() && idx.base_offset > 0);
            idx.queue = queue;
        }
    }

    fn update_compression_type(&mut self, compression_type: CompressionType, batch_len: u64) {
        for idx in self.entries_index.iter_mut() {
            idx.compression_type = compression_type;
            idx.batch_len = batch_len;
        }
    }

    fn compute_size(&self) -> usize {
        let mut size = 8 /*count*/;
        for e in self.entries.iter() {
            size += 8 /*len*/ + e.compute_size() as usize;
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

    fn compute_size(&self) -> usize {
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
pub struct State<M: MessageExt> {
    pub op_type: OpType,
    pub key: Vec<u8>,
    pub state: Option<M::State>,
}

impl<M: MessageExt> State<M> {
    pub fn new(op_type: OpType, key: Vec<u8>, state: Option<M::State>) -> State<M> {
        State {
            op_type,
            key,
            state,
        }
    }

    pub fn from_bytes(buf: &mut SliceReader<'_>) -> Result<State<M>> {
        let op_type = OpType::from_bytes(buf)?;
        let k_len = codec::decode_var_u64(buf)? as usize;
        let key = &buf[..k_len];
        buf.consume(k_len);
        match op_type {
            OpType::Put => {
                let v_len = codec::decode_var_u64(buf)? as usize;
                let value = &buf[..v_len];
                buf.consume(v_len);
                let mut s = M::State::new();
                s.merge_from_bytes(value)?;
                Ok(State::new(OpType::Put, key.to_vec(), Some(s)))
            }
            OpType::Del => Ok(State::new(OpType::Del, key.to_vec(), None)),
        }
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        // layout = { op_type | k_len | key | v_len | state }
        self.op_type.encode_to(vec);
        vec.encode_var_u64(self.key.len() as u64)?;
        vec.extend_from_slice(self.key.as_slice());
        match self.op_type {
            OpType::Put => {
                let content = self.state.as_ref().unwrap().write_to_bytes()?;
                vec.encode_var_u64(content.len() as u64)?;
                vec.extend_from_slice(&content);
            }
            OpType::Del => {}
        }
        Ok(())
    }

    fn compute_size(&self) -> usize {
        1 /*op*/ + 8 /*k_len*/ + self.key.len() + 8 /*v_len*/ + self.state.as_ref().map_or_else(|| 0, |v| v.compute_size() as usize)
    }
}

#[derive(Debug, PartialEq)]
pub struct LogItem<M: MessageExt> {
    pub raft_group_id: u64,
    pub content: LogItemContent<M>,
}

#[derive(Debug, PartialEq)]
pub enum LogItemContent<M: MessageExt> {
    Entries(Entries<M>),
    Command(Command),
    State(State<M>),
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

    pub fn from_state(
        raft_group_id: u64,
        op_type: OpType,
        key: Vec<u8>,
        state: Option<M::State>,
    ) -> LogItem<M> {
        LogItem {
            raft_group_id,
            content: LogItemContent::State(State::new(op_type, key, state)),
        }
    }

    fn compute_size(&self) -> usize {
        match &self.content {
            LogItemContent::Entries(entries) => 8 /*r_id*/ + 1 /*type*/ + entries.compute_size(),
            LogItemContent::Command(cmd) => 8 + 1 + cmd.compute_size(),
            LogItemContent::State(state) => 8 + 1 + state.compute_size(),
        }
    }

    pub fn encode_to(&mut self, vec: &mut Vec<u8>, entries_size: &mut usize) -> Result<()> {
        // layout = { 8 byte id | 1 byte type | item layout }
        vec.encode_var_u64(self.raft_group_id)?;
        match &mut self.content {
            LogItemContent::Entries(entries) => {
                vec.push(TYPE_ENTRIES);
                entries.encode_to(vec, entries_size)?;
            }
            LogItemContent::Command(command) => {
                vec.push(TYPE_COMMAND);
                command.encode_to(vec);
            }
            LogItemContent::State(state) => {
                vec.push(TYPE_STATE);
                state.encode_to(vec)?;
            }
        }
        Ok(())
    }

    pub fn from_bytes(
        buf: &mut SliceReader<'_>,
        file_id: FileId,
        base_offset: u64,      // Offset of the batch from its log file.
        mut batch_offset: u64, // Offset of the item from in its batch.
        entries_size: &mut usize,
    ) -> Result<LogItem<M>> {
        let buf_len = buf.len();
        let raft_group_id = codec::decode_var_u64(buf)?;
        batch_offset += (buf_len - buf.len()) as u64;
        trace!("decoding log item for {}", raft_group_id);
        let item_type = buf.read_u8()?;
        batch_offset += 1;
        let content = match item_type {
            TYPE_ENTRIES => {
                let entries =
                    Entries::from_bytes(buf, file_id, base_offset, batch_offset, entries_size)?;
                LogItemContent::Entries(entries)
            }
            TYPE_COMMAND => {
                let cmd = Command::from_bytes(buf)?;
                trace!("decoding command: {:?}", cmd);
                LogItemContent::Command(cmd)
            }
            TYPE_STATE => {
                let state = State::from_bytes(buf)?;
                trace!("decoding state: {:?}", state);
                LogItemContent::State(state)
            }
            _ => return Err(Error::Codec(CodecError::KeyNotFound)),
        };
        Ok(LogItem {
            raft_group_id,
            content,
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct LogBatch<M: MessageExt> {
    items: Vec<LogItem<M>>,
    items_size: usize,
    _phantom: PhantomData<M>,
}

impl<M: MessageExt> Default for LogBatch<M> {
    fn default() -> Self {
        Self {
            items: Vec::with_capacity(DEFAULT_BATCH_CAP),
            items_size: 0,
            _phantom: PhantomData,
        }
    }
}

impl<M: MessageExt> LogBatch<M> {
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            items_size: 0,
            _phantom: PhantomData,
        }
    }

    pub fn merge(&mut self, rhs: Self) {
        self.items_size += rhs.items_size;
        self.items.extend(rhs.items);
    }

    pub fn drain(&mut self) -> std::vec::Drain<'_, LogItem<M>> {
        self.items_size = 0;
        self.items.drain(..)
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, offset: u64) {
        for item in self.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                entries.set_position(queue, file_id, offset);
            }
        }
    }

    pub fn set_queue(&mut self, queue: LogQueue) {
        for item in self.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                entries.set_queue(queue);
            }
        }
    }

    pub fn add_entries(&mut self, region_id: u64, entries: Vec<M::Entry>) {
        let item = LogItem::from_entries(region_id, entries);
        self.items_size += item.compute_size();
        self.items.push(item);
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) {
        let item = LogItem::from_command(region_id, cmd);
        self.items_size += item.compute_size();
        self.items.push(item);
    }

    pub fn delete_state(&mut self, region_id: u64, key: Vec<u8>) {
        let item = LogItem::from_state(region_id, OpType::Del, key, None);
        self.items_size += item.compute_size();
        self.items.push(item);
    }

    pub fn put_state(&mut self, region_id: u64, key: Vec<u8>, s: M::State) -> Result<()> {
        let item = LogItem::from_state(region_id, OpType::Put, key, Some(s));
        self.items_size += item.compute_size();
        self.items.push(item);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn from_bytes(
        buf: &mut SliceReader<'_>,
        file_id: FileId,
        // The offset of the batch from its log file.
        base_offset: u64,
    ) -> Result<Option<LogBatch<M>>> {
        if buf.is_empty() {
            return Ok(None);
        }
        if buf.len() < BATCH_MIN_SIZE {
            return Err(Error::TooShort);
        }

        let header = codec::decode_u64(buf)? as usize;
        let batch_len = header >> 8;
        let batch_type = CompressionType::from_byte(header as u8);
        test_batch_checksum(&buf[..batch_len])?;
        trace!("decoding log batch({}, {:?})", batch_len, batch_type);

        let decompressed = match batch_type {
            CompressionType::None => Cow::Borrowed(&buf[..(batch_len - CHECKSUM_LEN)]),
            CompressionType::Lz4 => Cow::Owned(decompress(&buf[..batch_len - CHECKSUM_LEN])),
        };

        let mut reader: SliceReader = decompressed.borrow();
        let content_len = reader.len() + HEADER_LEN; // For its header.

        let mut items_count = codec::decode_var_u64(&mut reader)? as usize;
        assert!(items_count > 0 && !reader.is_empty());
        let mut log_batch = LogBatch::with_capacity(items_count);
        let mut entries_size = 0;
        while items_count > 0 {
            let content_offset = (content_len - reader.len()) as u64;
            let item = LogItem::from_bytes(
                &mut reader,
                file_id,
                base_offset,
                content_offset,
                &mut entries_size,
            )?;
            log_batch.items_size += item.compute_size();
            log_batch.items.push(item);
            items_count -= 1;
        }
        assert!(reader.is_empty());
        buf.consume(batch_len);

        for item in log_batch.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                entries.update_compression_type(batch_type, batch_len as u64);
            }
        }

        Ok(Some(log_batch))
    }

    // TODO: avoid to write a large batch into one compressed chunk.
    pub fn encode_to_bytes(&mut self, compression_threshold: usize) -> Option<Vec<u8>> {
        if self.items.is_empty() {
            return None;
        }

        // layout = { 8 bytes len | item count | multiple items | 4 bytes checksum }
        let mut vec = Vec::with_capacity(4096);
        let mut entries_size = 0;
        vec.encode_u64(0).unwrap();
        vec.encode_var_u64(self.items.len() as u64).unwrap();
        for item in self.items.iter_mut() {
            item.encode_to(&mut vec, &mut entries_size).unwrap();
        }

        let compression_type = if compression_threshold > 0 && vec.len() > compression_threshold {
            vec = lz4::encode_block(&vec[HEADER_LEN..], HEADER_LEN, 4);
            CompressionType::Lz4
        } else {
            CompressionType::None
        };

        let checksum = crc32(&vec[8..]);
        vec.encode_u32_le(checksum).unwrap();
        let len = vec.len() as u64 - 8;
        let mut header = len << 8;
        header |= u64::from(compression_type.to_byte());
        vec.as_mut_slice().write_u64::<BigEndian>(header).unwrap();

        let batch_len = (vec.len() - 8) as u64;
        for item in self.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                entries.update_compression_type(compression_type, batch_len as u64);
            }
        }

        Some(vec)
    }

    // Don't account for compression and varint encoding.
    pub fn approximate_size(&self) -> usize {
        if self.items.is_empty() {
            0
        } else {
            8 /*len*/ + 8 /*count*/ + self.items_size + 4 /*checksum*/
        }
    }
}

pub fn test_batch_checksum(buf: &[u8]) -> Result<()> {
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
    use kvproto::raft_serverpb::RaftLocalState;
    use raft::eraftpb::Entry;

    #[test]
    fn test_entries_enc_dec() {
        for pb_entries in vec![
            vec![Entry::new(); 10],
            vec![], // Empty entries.
        ] {
            let file_id = 1.into();
            let mut entries = Entries::<Entry>::new(pb_entries, None);

            let (mut encoded, mut entries_size1) = (vec![], 0);
            entries.encode_to(&mut encoded, &mut entries_size1).unwrap();
            for idx in entries.entries_index.iter_mut() {
                idx.file_id = file_id;
            }
            let (mut s, mut entries_size2) = (encoded.as_slice(), 0);
            let decode_entries =
                Entries::<Entry>::from_bytes(&mut s, file_id, 0, 0, &mut entries_size2).unwrap();
            assert_eq!(s.len(), 0);
            assert_eq!(entries.entries, decode_entries.entries);
            assert_eq!(entries.entries_index, decode_entries.entries_index);
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

    // TODO(tabokie)
    #[test]
    fn test_state_enc_dec() {
        let state = State::<RaftLocalState>::new(
            OpType::Put,
            b"key".to_vec(),
            Some(RaftLocalState::default()),
        );
        let mut encoded = vec![];
        state.encode_to(&mut encoded).unwrap();
        let mut bytes_slice = encoded.as_slice();
        let decoded_state = State::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(state, decoded_state);
    }

    #[test]
    fn test_log_item_enc_dec() {
        let region_id = 8;
        let items = vec![
            LogItem::<Entry>::from_entries(region_id, vec![Entry::new(); 10]),
            LogItem::from_command(region_id, Command::Clean),
            LogItem::<Entry>::from_state(
                region_id,
                OpType::Put,
                b"key".to_vec(),
                Some(RaftLocalState::default()),
            ),
        ];

        for mut item in items.into_iter() {
            let (mut encoded, mut entries_size1) = (vec![], 0);
            item.encode_to(&mut encoded, &mut entries_size1).unwrap();
            let (mut s, mut entries_size2) = (encoded.as_slice(), 0);
            let decoded_item =
                LogItem::<Entry>::from_bytes(&mut s, 0.into(), 0, 0, &mut entries_size2).unwrap();
            assert_eq!(s.len(), 0);
            assert_eq!(item, decoded_item);
        }
    }

    #[test]
    fn test_log_batch_enc_dec() {
        let region_id = 8;
        let mut batch = LogBatch::<Entry>::default();
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());
        batch.add_entries(region_id, vec![entry; 10]);
        batch.add_command(region_id, Command::Clean);
        batch
            .put_state(region_id, b"key".to_vec(), RaftLocalState::default())
            .unwrap();
        batch.delete_state(region_id, b"key2".to_vec());

        let encoded = batch.encode_to_bytes(0).unwrap();
        let mut s = encoded.as_slice();
        let decoded_batch = LogBatch::<Entry>::from_bytes(&mut s, 0.into(), 0)
            .unwrap()
            .unwrap();
        assert_eq!(s.len(), 0);
        assert_eq!(batch, decoded_batch);
    }
}
