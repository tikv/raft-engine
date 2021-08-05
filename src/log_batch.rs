use std::fmt::Debug;
use std::io::BufRead;
use std::marker::PhantomData;
use std::{mem, u64, vec};

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use crc32fast::Hasher;
use log::trace;
use protobuf::Message;

use crate::codec::{self, Error as CodecError, NumberEncoder};
use crate::memtable::EntryIndex;
use crate::pipe_log::{FileId, LogQueue};
use crate::{Error, Result};

pub const BATCH_MIN_SIZE: usize = HEADER_LEN + SECTION_OFFSET_LEN + CHECKSUM_LEN;
pub const HEADER_LEN: usize = 8;
pub const CHECKSUM_LEN: usize = 4;
pub const SECTION_OFFSET_LEN: usize = 8;

const TYPE_ENTRIES: u8 = 0x01;
const TYPE_COMMAND: u8 = 0x02;
const TYPE_KV: u8 = 0x3;

const CMD_CLEAN: u8 = 0x01;
const CMD_COMPACT: u8 = 0x02;

const DEFAULT_BATCH_CAP: usize = 64;

#[inline]
fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

mod lz4 {
    use std::{i32, ptr};

    pub fn encode_block(src: &[u8], head_reserve: usize, tail_alloc: usize) -> Vec<u8> {
        unsafe {
            let bound = lz4_sys::LZ4_compressBound(src.len() as i32);
            assert!(bound > 0 && src.len() <= i32::MAX as usize);

            // Layout: { header | decoded_len | content | checksum }.
            let capacity = head_reserve + 4 + bound as usize + tail_alloc;
            let mut output: Vec<u8> = Vec::with_capacity(capacity);

            let le_len = src.len().to_le_bytes();
            ptr::copy_nonoverlapping(le_len.as_ptr(), output.as_mut_ptr().add(head_reserve), 4);

            let size = lz4_sys::LZ4_compress_default(
                src.as_ptr() as _,
                output.as_mut_ptr().add(head_reserve + 4) as _,
                src.len() as i32,
                bound,
            );
            assert!(size > 0);
            output.set_len(head_reserve + 4 + size as usize);
            output
        }
    }

    pub fn decode_block(src: &[u8]) -> Vec<u8> {
        assert!(src.len() > 4, "data is too short: {} <= 4", src.len());
        unsafe {
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
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_basic() {
            let data: Vec<&'static [u8]> = vec![b"", b"123", b"12345678910"];
            for d in data {
                let compressed = super::encode_block(d, 0, 0);
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

    pub fn to_byte(self) -> u8 {
        self as u8
    }
}

type SliceReader<'a> = &'a [u8];

pub trait EntryExt<M: Message>: Send + Sync + Debug {
    fn index(m: &M) -> u64;
}

#[derive(Debug)]
pub struct Entries<E>
where
    E: Message,
{
    pub entries: Vec<E>,
    // EntryIndex may be update after write to file.
    pub entries_index: Vec<EntryIndex>,
}

impl<E: Message + PartialEq> PartialEq for Entries<E> {
    fn eq(&self, other: &Entries<E>) -> bool {
        self.entries == other.entries && self.entries_index == other.entries_index
    }
}

impl<E: Message> Entries<E> {
    pub fn new(entries: Vec<E>, entries_index: Option<Vec<EntryIndex>>) -> Entries<E> {
        let entries_index =
            entries_index.unwrap_or_else(|| vec![EntryIndex::default(); entries.len()]);
        Entries {
            entries,
            entries_index,
        }
    }

    pub fn from_bytes<W: EntryExt<E>>(
        buf: &mut SliceReader<'_>,
        entries_size: &mut usize,
    ) -> Result<Entries<E>> {
        let mut count = codec::decode_var_u64(buf)?;
        let mut entries_index = Vec::with_capacity(count as usize);
        while count > 0 {
            let index = codec::decode_var_u64(buf)?;
            let len = codec::decode_var_u64(buf)? - *entries_size as u64;
            let entry_index = EntryIndex {
                index,
                offset: *entries_size as u64,
                len,
                ..Default::default()
            };
            *entries_size += len as usize;
            entries_index.push(entry_index);
            count -= 1;
        }
        Ok(Entries::new(vec![], Some(entries_index)))
    }

    pub fn encode_to<W>(
        &mut self,
        buf: &mut Vec<u8>,
        entries_buf: &mut Vec<u8>,
        entries_size: &mut usize,
    ) -> Result<()>
    where
        W: EntryExt<E>,
    {
        // buf layout = { entries count | [ entry_index | entry_tail_offset ] }
        // entries_buf layout = { [ content ] }
        buf.encode_var_u64(self.entries.len() as u64)?;
        for (i, e) in self.entries.iter().enumerate() {
            let content = e.write_to_bytes()?;
            if !self.entries_index[i].file_id.valid() {
                self.entries_index[i].index = W::index(e);
                self.entries_index[i].offset = *entries_size as u64;
                self.entries_index[i].len = content.len() as u64;
                *entries_size += self.entries_index[i].len as usize;
            }
            buf.encode_var_u64(W::index(e))?;
            buf.encode_var_u64(*entries_size as u64)?;
            entries_buf.extend_from_slice(&content);
        }
        Ok(())
    }

    pub fn set_position(&mut self, queue: LogQueue, file_id: FileId, base_offset: u64) {
        for idx in self.entries_index.iter_mut() {
            debug_assert!(!idx.file_id.valid() && idx.base_offset == 0);
            idx.queue = queue;
            idx.file_id = file_id;
            idx.base_offset = base_offset;
        }
    }

    pub fn set_queue_and_file_id(&mut self, queue: LogQueue, file_id: FileId) {
        for idx in self.entries_index.iter_mut() {
            idx.queue = queue;
            idx.file_id = file_id;
        }
    }

    fn update_compression_info(
        &mut self,
        compression_type: CompressionType,
        base_offset: Option<u64>,
        section_offset: u64,
        section_len: u64,
    ) {
        for idx in self.entries_index.iter_mut() {
            idx.compression_type = compression_type;
            idx.section_offset = section_offset;
            idx.section_len = section_len;
            if let Some(batch_offset) = base_offset {
                idx.base_offset = batch_offset;
            }
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
                size += (8/*index*/+8/*tail_offset*/) + ei.len as usize;
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

    pub fn encode_to<W>(
        &mut self,
        buf: &mut Vec<u8>,
        entries_buf: &mut Vec<u8>,
        entries_size: &mut usize,
    ) -> Result<()>
    where
        W: EntryExt<E>,
    {
        // layout = { 8 byte id | 1 byte type | item layout }
        buf.encode_var_u64(self.raft_group_id)?;
        match &mut self.content {
            LogItemContent::Entries(entries) => {
                buf.push(TYPE_ENTRIES);
                entries.encode_to::<W>(buf, entries_buf, entries_size)?;
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

    pub fn from_bytes<W: EntryExt<E>>(
        buf: &mut SliceReader<'_>,
        entries_size: &mut usize,
    ) -> Result<LogItem<E>> {
        let raft_group_id = codec::decode_var_u64(buf)?;
        trace!("decoding log item for {}", raft_group_id);
        let item_type = buf.read_u8()?;
        let content = match item_type {
            TYPE_ENTRIES => {
                let entries = Entries::from_bytes::<W>(buf, entries_size)?;
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
pub struct LogBatch<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    pub items: Vec<LogItem<E>>,
    approximate_size: usize,
    _phantom: PhantomData<W>,
}

impl<E, W> Default for LogBatch<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    fn default() -> Self {
        Self::with_capacity(DEFAULT_BATCH_CAP)
    }
}

impl<E, W> LogBatch<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            items: Vec::with_capacity(cap),
            approximate_size: 0,
            _phantom: PhantomData,
        }
    }

    pub fn merge(&mut self, rhs: &mut Self) {
        self.approximate_size += rhs.approximate_size;
        self.items.append(&mut rhs.items);
    }

    pub fn add_entries(&mut self, region_id: u64, entries: Vec<E>) {
        let item = LogItem::from_entries(region_id, entries);
        self.approximate_size += item.compute_approximate_size();
        self.items.push(item);
    }

    pub fn clean_region(&mut self, region_id: u64) {
        self.add_command(region_id, Command::Clean);
    }

    pub fn add_command(&mut self, region_id: u64, cmd: Command) {
        let item = LogItem::from_command(region_id, cmd);
        self.approximate_size += item.compute_approximate_size();
        self.items.push(item);
    }

    pub fn delete(&mut self, region_id: u64, key: Vec<u8>) {
        let item = LogItem::from_kv(region_id, OpType::Del, key, None);
        self.approximate_size += item.compute_approximate_size();
        self.items.push(item);
    }

    pub fn put(&mut self, region_id: u64, key: Vec<u8>, value: Vec<u8>) {
        let item = LogItem::from_kv(region_id, OpType::Put, key, Some(value));
        self.approximate_size += item.compute_approximate_size();
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
        // The offset of the batch from its log file.
        base_offset: u64,
    ) -> Result<Option<(LogBatch<E, W>, usize)>> {
        if buf.is_empty() {
            return Ok(None);
        }
        if buf.len() < BATCH_MIN_SIZE {
            return Err(Error::TooShort);
        }

        let header = codec::decode_u64(buf)? as usize;
        let compression_type = CompressionType::from_byte(header as u8);
        let batch_len = header >> 8;
        let section_offset = codec::decode_u64(buf)?;
        let section_len = batch_len as u64 - section_offset;
        test_checksum(&buf[..section_offset as usize])?;

        let mut items_count = codec::decode_var_u64(buf)? as usize;
        assert!(items_count > 0 && !buf.is_empty());
        let mut log_batch = LogBatch::with_capacity(items_count);
        while items_count > 0 {
            let item = LogItem::from_bytes::<W>(buf, &mut 0)?;
            log_batch.approximate_size += item.compute_approximate_size();
            log_batch.items.push(item);
            items_count -= 1;
        }

        for item in log_batch.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                entries.update_compression_info(
                    compression_type,
                    Some(base_offset),
                    section_offset,
                    section_len,
                );
            }
        }
        buf.consume(CHECKSUM_LEN);

        Ok(Some((log_batch, section_len as usize)))
    }

    // TODO: avoid to write a large batch into one compressed chunk.
    pub fn encode_to_bytes(&mut self, compression_threshold: usize) -> Option<Vec<u8>> {
        if self.items.is_empty() {
            return None;
        }

        // layout = { 8 bytes batch_len | 8 bytes section offset | item count | multiple items (without entries) | crc32 | section (entries) | crc32 }
        //          ^base_offset in file                         ^offset = 0
        //                                                       |<------------------------------------ batch_len ---------------------------------->|
        //                                                                                                              ^offset = section_offset
        //                                                                                                              |<------- section_len ------>|
        // log file layout: { magic | version | LogBatch | LogBatch | LogBatch | ... | LogBatch }
        //                                    ^base_offset
        let mut buf = Vec::with_capacity(1024);
        let mut entries_buf = Vec::with_capacity(4096);

        buf.encode_u64(0).unwrap();
        buf.encode_u64(0).unwrap();
        buf.encode_var_u64(self.items.len() as u64).unwrap();

        for item in self.items.iter_mut() {
            item.encode_to::<W>(&mut buf, &mut entries_buf, &mut 0)
                .unwrap();
        }
        let buf_checksum = crc32(&buf[16..]);
        buf.encode_u32_le(buf_checksum).unwrap();

        let compression_type =
            if compression_threshold > 0 && entries_buf.len() > compression_threshold {
                entries_buf = lz4::encode_block(&entries_buf, 0, 4);
                CompressionType::Lz4
            } else {
                CompressionType::None
            };
        let entries_buf_checksum = crc32(&entries_buf);
        entries_buf.encode_u32_le(entries_buf_checksum).unwrap();

        let section_offset = buf.len() as u64 - 16;
        let section_len = entries_buf.len() as u64;
        (&mut buf[8..])
            .write_u64::<BigEndian>(section_offset)
            .unwrap();

        // merge two buffers
        buf.append(&mut entries_buf);
        let batch_len = buf.len() as u64 - 16;
        let mut header = batch_len << 8;
        header |= u64::from(compression_type.to_byte());
        buf.as_mut_slice().write_u64::<BigEndian>(header).unwrap();

        for item in self.items.iter_mut() {
            if let LogItemContent::Entries(entries) = &mut item.content {
                entries.update_compression_info(
                    compression_type,
                    None,
                    section_offset,
                    section_len,
                );
            }
        }

        Some(buf)
    }

    // Don't account for compression and varint encoding.
    pub fn approximate_size(&self) -> usize {
        if self.items.is_empty() {
            0
        } else {
            8 /*len*/ + 8 /*section offset*/ + 8/*items count*/
            +self.approximate_size + CHECKSUM_LEN * 2 /*checksum*/
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
                    &data[entry_index.offset as usize
                        ..(entry_index.offset + entry_index.len) as usize],
                )
                .unwrap(),
            );
        }
        entries
    }

    #[test]
    fn test_entries_enc_dec() {
        for pb_entries in vec![
            vec![Entry::new(); 10],
            vec![], // Empty entries.
        ] {
            let mut entries = Entries::new(pb_entries, None);

            let (mut encoded_entries_index, mut encoded_entries, mut entries_size1) =
                (vec![], vec![], 0);
            entries
                .encode_to::<Entry>(
                    &mut encoded_entries_index,
                    &mut encoded_entries,
                    &mut entries_size1,
                )
                .unwrap();

            let (mut s, mut entries_size2) = (encoded_entries_index.as_slice(), 0);
            let mut decoded_entries =
                Entries::from_bytes::<Entry>(&mut s, &mut entries_size2).unwrap();
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
            LogItem::from_entries(region_id, vec![Entry::new(); 10]),
            LogItem::from_command(region_id, Command::Clean),
            LogItem::from_kv(
                region_id,
                OpType::Put,
                b"key".to_vec(),
                Some(b"value".to_vec()),
            ),
        ];

        for mut item in items.into_iter() {
            let (mut encoded_entries_index, mut encoded_entries, mut entries_size1) =
                (vec![], vec![], 0);
            item.encode_to::<Entry>(
                &mut encoded_entries_index,
                &mut encoded_entries,
                &mut entries_size1,
            )
            .unwrap();

            let (mut s, mut entries_size2) = (encoded_entries_index.as_slice(), 0);
            let mut decoded_item =
                LogItem::from_bytes::<Entry>(&mut s, &mut entries_size2).unwrap();
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
        let mut batch = LogBatch::<Entry, Entry>::default();
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());
        batch.add_entries(region_id, vec![entry; 10]);
        batch.add_command(region_id, Command::Clean);
        batch.put(region_id, b"key".to_vec(), b"value".to_vec());
        batch.delete(region_id, b"key2".to_vec());

        let encoded = batch.encode_to_bytes(0).unwrap();
        let mut s = encoded.as_slice();
        let (mut decoded_batch, skip) = LogBatch::from_bytes(&mut s, 0).unwrap().unwrap();
        assert_eq!(s.len(), skip);
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
