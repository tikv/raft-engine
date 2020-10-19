use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::io::{BufRead, SeekFrom};
use std::marker::PhantomData;
use std::{mem, u64};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use protobuf::Message;

use crate::cache_evict::CacheTracker;
use crate::codec::{self, Error as CodecError, NumberEncoder};
use crate::compression::{decode_block, decode_blocks};
use crate::memtable::EntryIndex;
use crate::pipe_log::LogQueue;
use crate::util::crc32;
use crate::{Error, IoVecs, Result};

pub const HEADER_LEN: usize = 8;
pub const CHECKSUM_LEN: usize = 4;

const TYPE_ENTRIES: u8 = 0x01;
const TYPE_COMMAND: u8 = 0x02;
const TYPE_KV: u8 = 0x3;

const CMD_CLEAN: u8 = 0x01;
const CMD_COMPACT: u8 = 0x02;

const BATCH_MIN_SIZE: usize = HEADER_LEN + CHECKSUM_LEN;
const COMPRESSION_THRESHOLD: usize = 4096;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CompressionType {
    None = 0,
    Lz4 = 1,
    Lz4Blocks = 2,
}

impl CompressionType {
    pub fn from_byte(t: u8) -> CompressionType {
        debug_assert!(t <= 2);
        unsafe { mem::transmute(t) }
    }

    pub fn to_byte(&self) -> u8 {
        *self as u8
    }
}

type SliceReader<'a> = &'a [u8];

pub trait EntryExt<M: Message>: Send + Sync {
    fn index(m: &M) -> u64;
}

#[derive(Debug)]
pub struct Entries<E>
where
    E: Message,
{
    pub entries: Vec<E>,
    // EntryIndex may be update after write to file.
    pub entries_index: RefCell<Vec<EntryIndex>>,
}

impl<E: Message + PartialEq> PartialEq for Entries<E> {
    fn eq(&self, other: &Entries<E>) -> bool {
        self.entries == other.entries && self.entries_index == other.entries_index
    }
}

impl<E: Message> Entries<E> {
    pub fn new(entries: Vec<E>, entries_index: Option<Vec<EntryIndex>>) -> Entries<E> {
        let entries_index = RefCell::new(
            entries_index.unwrap_or_else(|| vec![EntryIndex::default(); entries.len()]),
        );
        Entries {
            entries,
            entries_index,
        }
    }

    pub fn from_bytes<W: EntryExt<E>>(
        buf: &mut SliceReader<'_>,
        file_num: u64,
        base_offset: u64,  // Offset of the batch from its log file.
        batch_offset: u64, // Offset of the item from in its batch.
        entries_size: &mut usize,
    ) -> Result<Entries<E>> {
        let content_len = buf.len() as u64;
        let mut count = codec::decode_var_u64(buf)? as usize;
        let mut entries = Vec::with_capacity(count);
        let mut entries_index = Vec::with_capacity(count);
        while count > 0 {
            let len = codec::decode_u32(buf)? as usize;
            let mut e = E::new();
            e.merge_from_bytes(&buf[..len])?;

            let mut entry_index = EntryIndex::default();
            entry_index.index = W::index(&e);
            entry_index.file_num = file_num;
            entry_index.base_offset = base_offset;
            entry_index.offset = batch_offset + content_len - buf.len() as u64;
            entry_index.len = len as u64;
            *entries_size += entry_index.len as usize;

            buf.consume(len);
            entries.push(e);
            entries_index.push(entry_index);

            count -= 1;
        }
        Ok(Entries::new(entries, Some(entries_index)))
    }

    pub fn encode_to<W, T>(&self, vec: &mut T, entries_size: &mut usize) -> Result<()>
    where
        W: EntryExt<E>,
        T: IoVecs,
    {
        if self.entries.is_empty() {
            return Ok(());
        }

        // layout = { entries count | multiple entries }
        // entries layout = { entry layout | ... | entry layout }
        // entry layout = { len(u32) | entry content }
        vec.encode_var_u64(self.entries.len() as u64)?;
        for (i, e) in self.entries.iter().enumerate() {
            let prefixed_len_offset = vec.content_len();
            vec.encode_u32(0)?;
            e.write_to_writer_without_buffer(vec)?;
            let content_len = vec.content_len() - prefixed_len_offset - 4;
            vec.seek(SeekFrom::Start(prefixed_len_offset as u64))?;
            vec.encode_u32(content_len as u32)?;
            vec.seek(SeekFrom::End(0))?;

            // file_num = 0 means entry index is not initialized.
            let mut entries_index = self.entries_index.borrow_mut();
            if entries_index[i].file_num == 0 {
                entries_index[i].index = W::index(&e);
                // This offset doesn't count the header.
                entries_index[i].offset = prefixed_len_offset as u64 + 4;
                entries_index[i].len = content_len as u64;
                *entries_size += entries_index[i].len as usize;
            }
        }
        Ok(())
    }

    pub fn update_position(
        &self,
        queue: LogQueue,
        file_num: u64,
        base: u64,
        tracker: &Option<CacheTracker>,
    ) {
        for idx in self.entries_index.borrow_mut().iter_mut() {
            debug_assert!(idx.file_num == 0 && idx.base_offset == 0);
            debug_assert!(idx.cache_tracker.is_none());
            idx.queue = queue;
            idx.file_num = file_num;
            idx.base_offset = base;
            if let Some(ref tkr) = tracker {
                let mut tkr = tkr.clone();
                tkr.global_stats.add_mem_change(idx.len as usize);
                tkr.sub_on_drop = idx.len as usize;
                idx.cache_tracker = Some(tkr);
            }
        }
    }

    pub fn attach_cache_tracker(&self, tracker: CacheTracker) {
        for idx in self.entries_index.borrow_mut().iter_mut() {
            let mut tkr = tracker.clone();
            tkr.global_stats.add_mem_change(idx.len as usize);
            tkr.sub_on_drop = idx.len as usize;
            idx.cache_tracker = Some(tkr);
        }
    }

    fn update_compression_type(&self, compression_type: CompressionType, batch_len: u64) {
        for idx in self.entries_index.borrow_mut().iter_mut() {
            idx.compression_type = compression_type;
            idx.batch_len = batch_len;
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum Command {
    Clean,
    Compact { index: u64 },
}

impl Command {
    pub fn encode_to<T: IoVecs>(&self, vec: &mut T) {
        match *self {
            Command::Clean => {
                vec.write_u8(CMD_CLEAN).unwrap();
            }
            Command::Compact { index } => {
                vec.write_u8(CMD_COMPACT).unwrap();
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
}

#[derive(Debug, PartialEq, Copy, Clone)]
pub enum OpType {
    Put = 0x01,
    Del = 0x02,
}

impl OpType {
    pub fn encode_to<T: IoVecs>(self, vec: &mut T) {
        vec.write_u8(self as u8).unwrap();
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

    pub fn encode_to<T: IoVecs>(&self, vec: &mut T) -> Result<()> {
        // layout = { op_type | k_len | key | v_len | value }
        self.op_type.encode_to(vec);
        vec.encode_var_u64(self.key.len() as u64)?;
        vec.write_all(self.key.as_slice())?;
        match self.op_type {
            OpType::Put => {
                vec.encode_var_u64(self.value.as_ref().unwrap().len() as u64)?;
                vec.write_all(self.value.as_ref().unwrap().as_slice())?;
            }
            OpType::Del => {}
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq)]
pub struct LogItem<E>
where
    E: Message,
{
    pub raft_group_id: u64,
    pub content: LogItemContent<E>,
}

#[derive(Debug, PartialEq)]
pub enum LogItemContent<E>
where
    E: Message,
{
    Entries(Entries<E>),
    Command(Command),
    Kv(KeyValue),
}

impl<E: Message> LogItem<E> {
    pub fn from_entries(raft_group_id: u64, entries: Vec<E>) -> LogItem<E> {
        LogItem {
            raft_group_id,
            content: LogItemContent::Entries(Entries::new(entries, None)),
        }
    }

    pub fn from_command(raft_group_id: u64, command: Command) -> LogItem<E> {
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
    ) -> LogItem<E> {
        LogItem {
            raft_group_id,
            content: LogItemContent::Kv(KeyValue::new(op_type, key, value)),
        }
    }

    pub fn encode_to<W, T>(&self, vec: &mut T, entries_size: &mut usize) -> Result<()>
    where
        W: EntryExt<E>,
        T: IoVecs,
    {
        // layout = { 8 byte id | 1 byte type | item layout }
        vec.encode_var_u64(self.raft_group_id)?;
        match self.content {
            LogItemContent::Entries(ref entries) => {
                vec.write_u8(TYPE_ENTRIES)?;
                entries.encode_to::<W, T>(vec, entries_size)?;
            }
            LogItemContent::Command(ref command) => {
                vec.write_u8(TYPE_COMMAND)?;
                command.encode_to(vec);
            }
            LogItemContent::Kv(ref kv) => {
                vec.write_u8(TYPE_KV)?;
                kv.encode_to(vec)?;
            }
        }
        Ok(())
    }

    pub fn from_bytes<W: EntryExt<E>>(
        buf: &mut SliceReader<'_>,
        file_num: u64,
        base_offset: u64,      // Offset of the batch from its log file.
        mut batch_offset: u64, // Offset of the item from in its batch.
        entries_size: &mut usize,
    ) -> Result<LogItem<E>> {
        let buf_len = buf.len();
        let raft_group_id = codec::decode_var_u64(buf)?;
        batch_offset += (buf_len - buf.len()) as u64;
        let item_type = buf.read_u8()?;
        batch_offset += 1;
        let content = match item_type {
            TYPE_ENTRIES => {
                let entries = Entries::from_bytes::<W>(
                    buf,
                    file_num,
                    base_offset,
                    batch_offset,
                    entries_size,
                )?;
                LogItemContent::Entries(entries)
            }
            TYPE_COMMAND => {
                let cmd = Command::from_bytes(buf)?;
                LogItemContent::Command(cmd)
            }
            TYPE_KV => {
                let kv = KeyValue::from_bytes(buf)?;
                LogItemContent::Kv(kv)
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
pub struct LogBatch<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    pub items: Vec<LogItem<E>>,
    entries_size: RefCell<usize>,
    _phantom: PhantomData<W>,
}

impl<E, W> Default for LogBatch<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    fn default() -> Self {
        Self {
            items: Vec::with_capacity(16),
            entries_size: RefCell::new(0),
            _phantom: PhantomData,
        }
    }
}

impl<E, W> LogBatch<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            entries_size: RefCell::new(0),
            _phantom: PhantomData,
        }
    }

    pub fn add_entries(&mut self, region_id: u64, entries: Vec<E>) {
        let item = LogItem::from_entries(region_id, entries);
        self.items.push(item);
    }

    pub fn clean_region(&mut self, region_id: u64) {
        self.add_command(region_id, Command::Clean);
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) {
        let item = LogItem::from_command(region_id, cmd);
        self.items.push(item);
    }

    pub fn delete(&mut self, region_id: u64, key: Vec<u8>) {
        let item = LogItem::from_kv(region_id, OpType::Del, key, None);
        self.items.push(item);
    }

    pub fn put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) {
        let item = LogItem::from_kv(region_id, OpType::Put, key, Some(value));
        self.items.push(item);
    }

    pub fn put_msg<M: protobuf::Message>(
        &mut self,
        region_id: u64,
        key: Vec<u8>,
        m: &M,
    ) -> Result<()> {
        let value = m.write_to_bytes()?;
        self.put(region_id, key, value);
        Ok(())
    }

    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn from_bytes(
        buf: &mut SliceReader<'_>,
        file_num: u64,
        // The offset of the batch from its log file.
        base_offset: u64,
    ) -> Result<Option<LogBatch<E, W>>> {
        if buf.is_empty() {
            return Ok(None);
        }
        if buf.len() < BATCH_MIN_SIZE {
            return Err(Error::TooShort);
        }

        let mut header_buf = *buf;
        let header = codec::decode_u64(&mut header_buf)? as usize;
        let batch_len = header >> 8;
        let batch_type = CompressionType::from_byte(header as u8);
        test_batch_checksum(&buf[..batch_len + HEADER_LEN + CHECKSUM_LEN])?;
        *buf = &buf[HEADER_LEN..];

        let decompressed = match batch_type {
            CompressionType::None => Cow::Borrowed(&buf[..batch_len]),
            CompressionType::Lz4 => Cow::Owned(decode_block(&buf[..batch_len])),
            CompressionType::Lz4Blocks => Cow::Owned(decode_blocks(&buf[..batch_len])),
        };

        let mut reader: SliceReader = decompressed.borrow();
        let content_len = reader.len() + HEADER_LEN; // For its header.

        let mut items_count = codec::decode_var_u64(&mut reader)? as usize;
        assert!(items_count > 0 && !reader.is_empty());
        let mut log_batch = LogBatch::with_capacity(items_count);
        while items_count > 0 {
            let content_offset = (content_len - reader.len()) as u64;
            let item = LogItem::from_bytes::<W>(
                &mut reader,
                file_num,
                base_offset,
                content_offset,
                &mut log_batch.entries_size.borrow_mut(),
            )?;
            log_batch.items.push(item);
            items_count -= 1;
        }
        assert!(reader.is_empty());
        buf.consume(batch_len + CHECKSUM_LEN);

        for item in &log_batch.items {
            if let LogItemContent::Entries(ref entries) = item.content {
                entries.update_compression_type(batch_type, batch_len as u64);
            }
        }

        Ok(Some(log_batch))
    }

    // TODO: avoid to write a large batch into one compressed chunk.
    pub fn encode_to_bytes<T: IoVecs>(&self, vec: &mut T) -> bool {
        assert_eq!(vec.content_len(), 0);
        if self.items.is_empty() {
            return false;
        }

        // layout = { 8 bytes len | item count | multiple items | 4 bytes checksum }
        vec.encode_u64(0).unwrap();
        vec.encode_var_u64(self.items.len() as u64).unwrap();
        for item in &self.items {
            item.encode_to::<W, _>(vec, &mut *self.entries_size.borrow_mut())
                .unwrap();
        }

        let compression_type = if vec.content_len() > COMPRESSION_THRESHOLD {
            vec.compress(HEADER_LEN, HEADER_LEN, CHECKSUM_LEN)
        } else {
            CompressionType::None
        };

        let len = (vec.content_len() - HEADER_LEN) as u64;
        let mut header = len << 8;
        header |= u64::from(compression_type.to_byte());
        vec.seek(SeekFrom::Start(0)).unwrap();
        vec.write_u64::<BigEndian>(header).unwrap();
        vec.seek(SeekFrom::End(0)).unwrap();

        let checksum = vec.crc32();
        vec.encode_u32_le(checksum).unwrap();

        let batch_len = (vec.content_len() - HEADER_LEN - CHECKSUM_LEN) as u64;
        for item in &self.items {
            if let LogItemContent::Entries(ref entries) = item.content {
                entries.update_compression_type(compression_type, batch_len as u64);
            }
        }

        true
    }

    pub fn entries_size(&self) -> usize {
        *self.entries_size.borrow()
    }
}

/// The passed in buf's layout: { header | content | checksum }.
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

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::Entry;
    use std::io::Cursor;

    #[test]
    fn test_entries_enc_dec() {
        let pb_entries = vec![Entry::new(); 10];
        let file_num = 1;
        let entries = Entries::new(pb_entries, None);

        let (mut encoded, mut entries_size1) = (Cursor::new(vec![]), 0);
        entries
            .encode_to::<Entry, Cursor<Vec<u8>>>(&mut encoded, &mut entries_size1)
            .unwrap();
        for idx in entries.entries_index.borrow_mut().iter_mut() {
            idx.file_num = file_num;
        }
        let (mut s, mut entries_size2) = (encoded.get_ref().as_slice(), 0);
        let decode_entries =
            Entries::from_bytes::<Entry>(&mut s, file_num, 0, 0, &mut entries_size2).unwrap();
        assert_eq!(s.len(), 0);
        assert_eq!(entries.entries, decode_entries.entries);
        assert_eq!(entries.entries_index, decode_entries.entries_index);
    }

    #[test]
    fn test_command_enc_dec() {
        let cmd = Command::Clean;
        let mut encoded = Cursor::new(vec![]);
        cmd.encode_to(&mut encoded);
        let mut bytes_slice = encoded.get_ref().as_slice();
        let decoded_cmd = Command::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(cmd, decoded_cmd);
    }

    #[test]
    fn test_kv_enc_dec() {
        let kv = KeyValue::new(OpType::Put, b"key".to_vec(), Some(b"value".to_vec()));
        let mut encoded = Cursor::new(vec![]);
        kv.encode_to(&mut encoded).unwrap();
        let mut bytes_slice = encoded.get_ref().as_slice();
        let decoded_kv = KeyValue::from_bytes(&mut bytes_slice).unwrap();
        assert_eq!(bytes_slice.len(), 0);
        assert_eq!(kv, decoded_kv);
    }

    #[test]
    fn test_log_item_enc_dec() {
        let region_id = 8;
        let items = vec![
            LogItem::from_entries(region_id, vec![Entry::new(); 10]),
            LogItem::from_command(region_id, Command::Clean),
            LogItem::from_kv(
                region_id,
                OpType::Put,
                b"key".to_vec(),
                Some(b"value".to_vec()),
            ),
        ];

        for item in items {
            let (mut encoded, mut entries_size1) = (Cursor::new(vec![]), 0);
            item.encode_to::<Entry, Cursor<Vec<u8>>>(&mut encoded, &mut entries_size1)
                .unwrap();
            let (mut s, mut entries_size2) = (encoded.get_ref().as_slice(), 0);
            let decoded_item =
                LogItem::from_bytes::<Entry>(&mut s, 0, 0, 0, &mut entries_size2).unwrap();
            assert_eq!(s.len(), 0);
            assert_eq!(item, decoded_item);
        }
    }

    #[test]
    fn test_log_batch_enc_dec() {
        let region_id = 8;
        let mut batch = LogBatch::<Entry, Entry>::new();
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024]);
        batch.add_entries(region_id, vec![entry; 10]);
        batch.add_command(region_id, Command::Clean);
        batch.put(region_id, b"key".to_vec(), b"value".to_vec());
        batch.delete(region_id, b"key2".to_vec());

        let mut encoded = Cursor::new(vec![]);
        batch.encode_to_bytes(&mut encoded);
        let mut s = encoded.get_ref().as_slice();
        let decoded_batch = LogBatch::from_bytes(&mut s, 0, 0).unwrap().unwrap();
        assert_eq!(s.len(), 0);
        assert_eq!(batch, decoded_batch);
    }
}
