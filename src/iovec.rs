use std::cmp::Ordering;
use std::io::{Cursor, Error, ErrorKind, Result, Seek, SeekFrom, Write};

use crc32fast::Hasher;
use nix::sys::uio::IoVec as NixIoVec;

use crate::compression::{encode_block, encode_blocks};
use crate::log_batch::CompressionType;
use crate::util::crc32;

pub trait IoVecs: Write + Seek + Default {
    fn content_len(&self) -> usize;
    fn crc32(&self) -> u32;
    fn compress(
        &mut self,
        skip_header: usize,
        head_reserve: usize,
        tail_alloc: usize,
    ) -> CompressionType;
}

#[derive(Default)]
pub struct LengthFixedIoVecs {
    row_capacity: usize,
    buffers: Vec<Vec<u8>>,
    position: Option<(usize, usize)>,
}

impl LengthFixedIoVecs {
    pub fn new(row_capacity: usize) -> LengthFixedIoVecs {
        LengthFixedIoVecs {
            row_capacity,
            buffers: vec![],
            position: None,
        }
    }

    /// If it contains only one single iovec, call `into_vec` instead of `to_nix_iovecs`.
    pub fn has_single_iovec(&self) -> bool {
        self.buffers.len() == 1
    }

    pub fn to_nix_iovecs(&self) -> Vec<NixIoVec<&[u8]>> {
        let mut vecs = Vec::with_capacity(self.buffers.len());
        for buffer in &self.buffers {
            let vec = NixIoVec::from_slice(buffer.as_slice());
            vecs.push(vec);
        }
        vecs
    }

    pub fn into_vec(mut self) -> Vec<u8> {
        self.take_vec()
    }

    fn take_vec(&mut self) -> Vec<u8> {
        debug_assert!(self.has_single_iovec());
        let buffers = std::mem::take(&mut self.buffers);
        buffers.into_iter().next().unwrap()
    }

    fn reset_vec(&mut self, vec: Vec<u8>) {
        self.row_capacity = vec.capacity();
        self.buffers = vec![vec];
        self.position = None;
    }

    fn update_position(&mut self, row: usize, offset: usize) {
        if self.row_capacity * row + offset == self.content_len() {
            self.position = None;
            return;
        }
        self.position = Some((row, offset));
    }

    fn append(&mut self, buf: &[u8]) -> Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.buffers.last().unwrap().len() == self.row_capacity {
            self.buffers.push(Vec::with_capacity(self.row_capacity));
        }

        let buffer = self.buffers.last_mut().unwrap();
        if buffer.len() + buf.len() <= self.row_capacity {
            buffer.extend_from_slice(buf);
            return Ok(buf.len());
        }
        let size = self.row_capacity - buffer.len();
        buffer.extend_from_slice(&buf[..size]);
        Ok(size)
    }

    fn pwrite(&mut self, mut buf: &[u8]) -> Result<usize> {
        let return_value = buf.len();
        if return_value > 0 {
            let (mut row, mut offset) = self.position.unwrap();
            debug_assert!(self.row_capacity * row + offset < self.content_len());
            loop {
                let src_len = buf.len();
                let dst_len = self.buffers[row].len() - offset;
                if dst_len >= src_len {
                    copy_slice(buf, &mut self.buffers[row][offset..], src_len);
                    offset += src_len;
                    break;
                }
                copy_slice(buf, &mut self.buffers[row][offset..], dst_len);
                row += 1;
                offset = 0;
                buf = &buf[dst_len..];
            }
            self.update_position(row, offset);
        }
        Ok(return_value)
    }
}

impl Write for LengthFixedIoVecs {
    fn write(&mut self, buf: &[u8]) -> Result<usize> {
        if self.buffers.is_empty() {
            debug_assert_eq!(self.position.unwrap_or_default(), (0, 0));
            self.buffers.push(Vec::with_capacity(self.row_capacity));
            self.position = None;
        }
        if let Some((row, offset)) = self.position {
            let offset = self.row_capacity * row + offset;
            let content_len = self.content_len();
            if offset + buf.len() <= content_len {
                return self.pwrite(buf);
            }
            let len = self.pwrite(&buf[..content_len - offset])?;
            debug_assert_eq!(len, content_len - offset);
            return Ok(len + self.append(&buf[len..])?);
        }
        self.append(buf)
    }

    fn flush(&mut self) -> Result<()> {
        Ok(())
    }
}

impl Seek for LengthFixedIoVecs {
    fn seek(&mut self, pos: SeekFrom) -> Result<u64> {
        #[inline]
        fn seek_from_start(vec: &mut LengthFixedIoVecs, offset: u64) -> Result<u64> {
            match (offset as usize).cmp(&vec.content_len()) {
                Ordering::Less => {
                    let full = offset as usize / vec.row_capacity;
                    let partial = offset as usize % vec.row_capacity;
                    vec.position = Some((full, partial));
                    Ok(offset)
                }
                Ordering::Equal => {
                    vec.position = None;
                    Ok(offset)
                }
                Ordering::Greater => Err(Error::from(ErrorKind::InvalidInput)),
            }
        }

        let offset = match pos {
            SeekFrom::Start(offset) => return seek_from_start(self, offset),
            SeekFrom::End(offset) => self.content_len() as i64 + offset,
            SeekFrom::Current(offset) => match self.position {
                Some((r, off)) => (self.row_capacity * r + off) as i64 + offset,
                None => self.content_len() as i64 + offset,
            },
        };
        if offset < 0 {
            return Err(Error::from(ErrorKind::InvalidInput));
        }
        seek_from_start(self, offset as u64)
    }
}

fn copy_slice(src: &[u8], dst: &mut [u8], len: usize) {
    debug_assert!(src.len() >= len);
    debug_assert!(dst.len() >= len);
    unsafe { std::ptr::copy(src.as_ptr(), dst.as_mut_ptr(), len) }
}

impl IoVecs for LengthFixedIoVecs {
    fn content_len(&self) -> usize {
        if let Some(last) = self.buffers.last() {
            return (self.buffers.len() - 1) * self.row_capacity + last.len();
        }
        0
    }

    fn crc32(&self) -> u32 {
        let mut hasher = Hasher::new();
        for slice in &self.buffers {
            hasher.update(slice.as_slice());
        }
        hasher.finalize()
    }

    fn compress(
        &mut self,
        mut skip_header: usize,
        head_reserve: usize,
        tail_alloc: usize,
    ) -> CompressionType {
        debug_assert!(self.content_len() > 0);
        if self.has_single_iovec() {
            let mut cursor = Cursor::new(self.take_vec());
            let compress_type = cursor.compress(skip_header, head_reserve, tail_alloc);
            self.reset_vec(cursor.into_inner());
            return compress_type;
        }

        let (mut start_row, mut start_offset) = (0, 0);
        for buffer in &self.buffers {
            if buffer.len() > skip_header {
                start_offset = skip_header;
                break;
            }
            start_row += 1;
            skip_header -= buffer.len();
        }

        let iter_on_slices = || {
            let buffers = self.buffers[start_row..].iter();
            buffers.enumerate().map(|(i, buffer)| {
                let begin = if i == 0 { start_offset } else { 0 };
                &buffer.as_slice()[begin..buffer.len()]
            })
        };
        let output = encode_blocks(iter_on_slices, head_reserve, tail_alloc);
        self.reset_vec(output);
        CompressionType::Lz4Blocks
    }
}

impl IoVecs for Cursor<Vec<u8>> {
    fn content_len(&self) -> usize {
        self.get_ref().len()
    }

    fn crc32(&self) -> u32 {
        crc32(self.get_ref().as_slice())
    }

    fn compress(
        &mut self,
        skip_header: usize,
        head_reserve: usize,
        tail_alloc: usize,
    ) -> CompressionType {
        debug_assert!(self.content_len() > 0);
        let buf = self.get_ref().as_slice();
        let vec = encode_block(&buf[skip_header..], head_reserve, tail_alloc);
        *self = Cursor::new(vec);
        CompressionType::Lz4
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compression::{decode_block, decode_blocks};

    #[test]
    fn test_length_fixed_io_vecs() {
        let mut iovecs = LengthFixedIoVecs::new(1024);
        assert_eq!(iovecs.write(vec![b'x'; 128].as_slice()).unwrap(), 128);
        assert_eq!(iovecs.write(vec![b'y'; 1024].as_slice()).unwrap(), 896);
        iovecs.write_all(vec![b'z'; 2048 + 128].as_slice()).unwrap();
        assert_eq!(iovecs.buffers.len(), 4);

        // Test seek + full pwrite.
        iovecs.seek(SeekFrom::Start(1023)).unwrap();
        assert_eq!(iovecs.write(&[b'a', b'a']).unwrap(), 2);
        assert_eq!(iovecs.buffers[0][1023], b'a');
        assert_eq!(iovecs.buffers[1][0], b'a');
        assert_eq!(iovecs.position.unwrap(), (1, 1));

        // Test seek + partial pwrite + append.
        iovecs.seek(SeekFrom::End(-1)).unwrap();
        assert_eq!(iovecs.write(&[b'a', b'a']).unwrap(), 2);
        assert_eq!(iovecs.buffers[3][127], b'a');
        assert_eq!(iovecs.buffers[3][128], b'a');
        assert!(iovecs.position.is_none());

        // Test seek + partial pwrite.
        assert_eq!(iovecs.write(vec![b'z'; 895].as_slice()).unwrap(), 895);
        iovecs.seek(SeekFrom::End(-1)).unwrap();
        assert_eq!(iovecs.write(&[b'a', b'a']).unwrap(), 2);
        assert_eq!(iovecs.buffers[3][1023], b'a');
        assert_eq!(iovecs.buffers.len(), 5);
        assert!(iovecs.position.is_none());

        // Test write zero.
        assert_eq!(iovecs.write(vec![b'z'; 1024].as_slice()).unwrap(), 1023);
        assert_eq!(iovecs.buffers.len(), 5);
        assert!(iovecs.write_all(&[b'x']).is_ok());

        // Test compression.
        assert_eq!(iovecs.compress(0, 0, 0), CompressionType::Lz4Blocks);
        assert!(iovecs.has_single_iovec());
        let decoded_1 = decode_blocks(&iovecs.into_vec());

        let mut iovecs1 = LengthFixedIoVecs::default();
        iovecs1.reset_vec(decoded_1.clone());
        assert!(iovecs1.has_single_iovec());
        assert_eq!(iovecs1.compress(0, 0, 0), CompressionType::Lz4);
        let decoded_2 = decode_block(&iovecs1.into_vec());
        assert_eq!(decoded_1, decoded_2);
    }
}
