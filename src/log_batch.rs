use std::borrow::{Borrow, Cow};
use std::cell::RefCell;
use std::io::BufRead;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::{mem, u64};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use protobuf::Message;

use crate::cache_evict::CacheTracker;
use crate::codec::{self, Error as CodecError, NumberEncoder};
use crate::memtable::EntryIndex;
use crate::{Error, Result};

pub const BATCH_MIN_SIZE: usize = HEADER_LEN + CHECKSUM_LEN;
pub const HEADER_LEN: usize = 8;
pub const CHECKSUM_LEN: usize = 4;

const TYPE_ENTRIES: u8 = 0x01;
const TYPE_COMMAND: u8 = 0x02;
const TYPE_KV: u8 = 0x3;

const CMD_CLEAN: u8 = 0x01;
const CMD_COMPACT: u8 = 0x02;

const COMPRESSION_SIZE: usize = 4096;

#[inline]
fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

mod lz4 {
    use std::{i32, ptr};

    // TODO: use in place compression instead.
    #[inline]
    pub fn encode_block(src: &[u8]) -> Vec<u8> {
        unsafe {
            let bound = lz4_sys::LZ4_compressBound(src.len() as i32);
            if bound > 0 && src.len() <= i32::MAX as usize {
                let mut output = Vec::<u8>::with_capacity(bound as usize + 4);
                let le_len = src.len().to_le_bytes();
                ptr::copy_nonoverlapping(le_len.as_ptr(), output.as_mut_ptr(), 4);
                let size = lz4_sys::LZ4_compress_default(
                    src.as_ptr() as _,
                    output.as_mut_ptr().add(4) as _,
                    src.len() as i32,
                    bound,
                );
                if size > 0 {
                    output.set_len(size as usize + 4);
                    return output;
                }
                panic!("compression fail: {}", size);
            }
            panic!("input size is too large: {}", src.len());
        }
    }

    #[inline]
    pub fn decode_block(src: &[u8]) -> Vec<u8> {
        unsafe {
            if src.len() > 4 {
                let len = u32::from_le(ptr::read_unaligned(src.as_ptr() as *const u32));
                let mut dst = Vec::with_capacity(len as usize);
                let l = lz4_sys::LZ4_decompress_safe(
                    src.as_ptr().add(4) as _,
                    dst.as_mut_ptr() as _,
                    src.len() as i32 - 4,
                    dst.capacity() as i32,
                );
                if l == len as i32 {
                    dst.set_len(l as usize);
                    return dst;
                }
                if l < 0 {
                    panic!("decompress failed: {}", l);
                } else {
                    panic!("length of decompress result not match {} != {}", len, l);
                }
            }
            panic!("data is too short: {} <= 4", src.len());
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_basic() {
            let data: Vec<&'static [u8]> = vec![b"", b"123", b"12345678910"];
            for d in data {
                let compressed = super::encode_block(d);
                assert!(compressed.len() > 4);
                let res = super::decode_block(&compressed);
                assert_eq!(res, d);
            }
        }
    }
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

    pub fn to_byte(&self) -> u8 {
        *self as u8
    }
}

type SliceReader<'a> = &'a [u8];
pub trait Entry: Message {
    fn index(&self) -> u64;
}

#[derive(Debug, PartialEq)]
pub struct Entries<T: Entry> {
    pub entries: Vec<T>,
    // EntryIndex may be update after write to file.
    pub entries_index: RefCell<Vec<EntryIndex>>,
}

impl<T: Entry> Entries<T> {
    pub fn new(entries: Vec<T>, entries_index: Option<Vec<EntryIndex>>) -> Entries<T> {
        let len = entries.len();
        Entries {
            entries,
            entries_index: match entries_index {
                Some(index) => RefCell::new(index),
                None => RefCell::new(vec![EntryIndex::default(); len]),
            },
        }
    }

    pub fn from_bytes(
        buf: &mut SliceReader<'_>,
        file_num: u64,
        base_offset: u64,  // Offset of the batch from its log file.
        batch_offset: u64, // Offset of the item from in its batch.
    ) -> Result<Entries<T>> {
        let content_len = buf.len() as u64;
        let mut count = codec::decode_var_u64(buf)? as usize;
        let mut entries = Vec::with_capacity(count);
        let mut entries_index = Vec::with_capacity(count);
        while count > 0 {
            let len = codec::decode_var_u64(buf)? as usize;
            let mut e = T::new();
            e.merge_from_bytes(&buf[..len])?;

            let mut entry_index = EntryIndex::default();
            entry_index.index = e.index();
            entry_index.file_num = file_num;
            entry_index.base_offset = base_offset;
            entry_index.offset = batch_offset + content_len - buf.len() as u64;
            entry_index.len = len as u64;

            buf.consume(len);
            entries.push(e);
            entries_index.push(entry_index);

            count -= 1;
        }
        Ok(Entries::new(entries, Some(entries_index)))
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        if self.entries.is_empty() {
            return Ok(());
        }

        // layout = { entries count | multiple entries }
        // entries layout = { entry layout | ... | entry layout }
        // entry layout = { len | entry content }
        vec.encode_var_u64(self.entries.len() as u64)?;
        for (i, e) in self.entries.iter().enumerate() {
            let content = e.write_to_bytes()?;
            vec.encode_var_u64(content.len() as u64)?;

            // file_num = 0 means entry index is not initialized.
            let mut entries_index = self.entries_index.borrow_mut();
            if entries_index[i].file_num == 0 {
                entries_index[i].index = e.index();
                // This offset doesn't count the header.
                entries_index[i].offset = vec.len() as u64;
                entries_index[i].len = content.len() as u64;
            }

            vec.extend_from_slice(&content);
        }
        Ok(())
    }

    pub fn update_offset_when_needed(&self, file_num: u64, base: u64) {
        for idx in self.entries_index.borrow_mut().iter_mut() {
            if idx.file_num == 0 {
                debug_assert_eq!(idx.base_offset, 0);
                idx.file_num = file_num;
                idx.base_offset = base;
            }
        }
    }

    pub fn attach_cache_tracker(&self, size: Arc<AtomicUsize>) {
        for idx in self.entries_index.borrow_mut().iter_mut() {
            let sub_on_drop = idx.len as usize;
            let chunk_size = size.clone();
            let tracker = CacheTracker {
                chunk_size,
                sub_on_drop,
            };
            idx.cache_tracker = Some(tracker);
        }
    }

    pub fn update_compression_type(&self, compression_type: CompressionType, batch_len: u64) {
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
}

#[derive(Debug, PartialEq)]
pub struct LogItem<T: Entry> {
    pub raft_group_id: u64,
    pub content: LogItemContent<T>,
}

#[derive(Debug, PartialEq)]
pub enum LogItemContent<T: Entry> {
    Entries(Entries<T>),
    Command(Command),
    Kv(KeyValue),
}

impl<T: Entry> LogItem<T> {
    pub fn from_entries(raft_group_id: u64, entries: Vec<T>) -> LogItem<T> {
        LogItem {
            raft_group_id,
            content: LogItemContent::Entries(Entries::new(entries, None)),
        }
    }

    pub fn from_command(raft_group_id: u64, command: Command) -> LogItem<T> {
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
    ) -> LogItem<T> {
        LogItem {
            raft_group_id,
            content: LogItemContent::Kv(KeyValue::new(op_type, key, value)),
        }
    }

    pub fn encode_to(&self, vec: &mut Vec<u8>) -> Result<()> {
        // layout = { 8 byte id | 1 byte type | item layout }
        vec.encode_var_u64(self.raft_group_id)?;
        match self.content {
            LogItemContent::Entries(ref entries) => {
                vec.push(TYPE_ENTRIES);
                entries.encode_to(vec)?;
            }
            LogItemContent::Command(ref command) => {
                vec.push(TYPE_COMMAND);
                command.encode_to(vec);
            }
            LogItemContent::Kv(ref kv) => {
                vec.push(TYPE_KV);
                kv.encode_to(vec)?;
            }
        }
        Ok(())
    }

    pub fn from_bytes(
        buf: &mut SliceReader<'_>,
        file_num: u64,
        base_offset: u64,      // Offset of the batch from its log file.
        mut batch_offset: u64, // Offset of the item from in its batch.
    ) -> Result<LogItem<T>> {
        let buf_len = buf.len();
        let raft_group_id = codec::decode_var_u64(buf)?;
        batch_offset += (buf_len - buf.len()) as u64;
        let item_type = buf.read_u8()?;
        batch_offset += 1;
        let content = match item_type {
            TYPE_ENTRIES => {
                let entries = Entries::from_bytes(buf, file_num, base_offset, batch_offset)?;
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

#[derive(Debug)]
pub struct LogBatch<T: Entry> {
    pub items: Vec<LogItem<T>>,
}

impl<T: Entry> Default for LogBatch<T> {
    fn default() -> Self {
        Self {
            items: Vec::with_capacity(16),
        }
    }
}

impl<T: Entry + PartialEq> PartialEq for LogBatch<T> {
    fn eq(&self, other: &LogBatch<T>) -> bool {
        self.items == other.items
    }
}

impl<T: Entry> LogBatch<T> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
        }
    }

    pub fn add_entries(&mut self, region_id: u64, entries: Vec<T>) {
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
    ) -> Result<Option<LogBatch<T>>> {
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

        let decompressed = match batch_type {
            CompressionType::None => Cow::Borrowed(&buf[..(batch_len - CHECKSUM_LEN)]),
            CompressionType::Lz4 => Cow::Owned(decompress(&buf[..batch_len - CHECKSUM_LEN])),
        };

        let mut reader: SliceReader = decompressed.borrow();
        let content_len = reader.len() + HEADER_LEN; // For its header.

        let mut items_count = codec::decode_var_u64(&mut reader)? as usize;
        assert!(items_count > 0 && !reader.is_empty());
        let mut log_batch = LogBatch::with_capacity(items_count);
        while items_count > 0 {
            let content_offset = (content_len - reader.len()) as u64;
            let item = LogItem::from_bytes(&mut reader, file_num, base_offset, content_offset)?;
            log_batch.items.push(item);
            items_count -= 1;
        }
        assert!(reader.is_empty());
        buf.consume(batch_len);

        for item in &log_batch.items {
            if let LogItemContent::Entries(ref entries) = item.content {
                entries.update_compression_type(batch_type, batch_len as u64);
            }
        }

        Ok(Some(log_batch))
    }

    // TODO: avoid to write a large batch into one compressed chunk.
    pub fn encode_to_bytes(&self) -> Option<Vec<u8>> {
        if self.items.is_empty() {
            return None;
        }

        // layout = { 8 bytes len | item count | multiple items | 4 bytes checksum }
        let mut vec = Vec::with_capacity(4096);
        vec.encode_u64(0).unwrap();
        vec.encode_var_u64(self.items.len() as u64).unwrap();
        for item in &self.items {
            item.encode_to(&mut vec).unwrap();
        }

        let compression_type = if vec.len() > COMPRESSION_SIZE {
            let dst = lz4::encode_block(&vec[8..]);
            vec.truncate(8);
            vec.extend_from_slice(&dst);
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
        for item in &self.items {
            if let LogItemContent::Entries(ref entries) = item.content {
                entries.update_compression_type(compression_type, batch_len as u64);
            }
        }

        Some(vec)
    }

    pub fn entries_size(&self) -> usize {
        let mut size = 0;
        for item in &self.items {
            if let LogItemContent::Entries(ref entries) = item.content {
                for entry in entries.entries_index.borrow().iter() {
                    size += entry.len as usize;
                }
            }
        }
        size
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

    use raft::eraftpb::Entry;

    #[test]
    fn test_entries_enc_dec() {
        let pb_entries = vec![Entry::new(); 10];
        let file_num = 1;
        let entries = Entries::new(pb_entries, None);

        let mut encoded = vec![];
        entries.encode_to(&mut encoded).unwrap();
        for idx in entries.entries_index.borrow_mut().iter_mut() {
            idx.file_num = file_num;
        }
        let mut s = encoded.as_slice();
        let decode_entries = Entries::from_bytes(&mut s, file_num, 0, 0).unwrap();
        assert_eq!(s.len(), 0);
        assert_eq!(entries.entries, decode_entries.entries);
        assert_eq!(entries.entries_index, decode_entries.entries_index);
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
        let file_num = 1;
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
            let mut encoded = vec![];
            item.encode_to(&mut encoded).unwrap();
            let mut s = encoded.as_slice();
            let decoded_item = LogItem::from_bytes(&mut s, file_num, 0, 0).unwrap();
            assert_eq!(s.len(), 0);

            if let LogItemContent::Entries(ref entries) = item.content {
                entries.update_offset_when_needed(file_num, 0);
            }
            assert_eq!(item, decoded_item);
        }
    }

    #[test]
    fn test_log_batch_enc_dec() {
        let region_id = 8;
        let file_num = 1;
        let mut batch = LogBatch::new();
        batch.add_entries(region_id, vec![Entry::new(); 10]);
        batch.add_command(region_id, Command::Clean);
        batch.put(region_id, b"key".to_vec(), b"value".to_vec());
        batch.delete(region_id, b"key2".to_vec());

        let encoded = batch.encode_to_bytes().unwrap();
        let mut s = encoded.as_slice();
        let decoded_batch = LogBatch::from_bytes(&mut s, file_num, 0).unwrap().unwrap();
        assert_eq!(s.len(), 0);

        for item in batch.items.iter_mut() {
            if let LogItemContent::Entries(ref entries) = item.content {
                entries.update_offset_when_needed(file_num, 0);
            }
        }
        assert_eq!(batch, decoded_batch);
    }
}
