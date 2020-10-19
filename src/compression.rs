use std::i32;
use std::ptr::{copy_nonoverlapping, read_unaligned};

use libc::c_int;
use lz4_sys::{
    LZ4StreamEncode, LZ4_compressBound, LZ4_compress_default, LZ4_createStreamDecode,
    LZ4_decompress_safe, LZ4_decompress_safe_continue, LZ4_freeStreamDecode,
};

// Layout of single block compression:
// header + decoded_size + content + cap(tail).
pub fn encode_block(src: &[u8], head_reserve: usize, tail_alloc: usize) -> Vec<u8> {
    unsafe {
        let bound = LZ4_compressBound(src.len() as i32);
        assert!(bound > 0 && src.len() <= i32::MAX as usize);

        let capacity = head_reserve + 4 + bound as usize + tail_alloc;
        let mut output: Vec<u8> = Vec::with_capacity(capacity);

        let le_len = src.len().to_le_bytes();
        copy_nonoverlapping(le_len.as_ptr(), output.as_mut_ptr().add(head_reserve), 4);

        let size = LZ4_compress_default(
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
        let len = u32::from_le(read_unaligned(src.as_ptr() as *const u32));
        let mut dst = Vec::with_capacity(len as usize);
        let l = LZ4_decompress_safe(
            src.as_ptr().add(4) as _,
            dst.as_mut_ptr() as _,
            src.len() as i32 - 4,
            dst.capacity() as i32,
        );
        assert_eq!(l, len as i32);
        dst.set_len(l as usize);
        dst
    }
}

// Layout of multi blocks compression:
// header + decoded_size + vec[encoded_len_and_content] + cap(tail).
pub fn encode_blocks<'a, F, I>(inputs: F, head_reserve: usize, tail_alloc: usize) -> Vec<u8>
where
    F: Fn() -> I,
    I: Iterator<Item = &'a [u8]>,
{
    let (mut encoded_len, mut decoded_len) = (0, 0u64);
    for buffer in inputs() {
        let len = buffer.len();
        decoded_len += len as u64;
        let size = unsafe { lz4_sys::LZ4_compressBound(len as i32) };
        assert!(size > 0);
        encoded_len += (4 + size) as usize; // Length and content.
    }

    let capacity = head_reserve + 8 + encoded_len + tail_alloc;
    let mut output: Vec<u8> = Vec::with_capacity(capacity);
    unsafe {
        copy_nonoverlapping(
            decoded_len.to_le_bytes().as_ptr(),
            output.as_mut_ptr().add(head_reserve),
            8,
        );

        let (stream, mut offset) = (lz4_sys::LZ4_createStream(), head_reserve + 8);
        for buffer in inputs() {
            let bytes = LZ4_compress_fast_continue(
                stream,
                buffer.as_ptr() as _,
                output.as_mut_ptr().add(offset + 4),
                buffer.len() as i32,
                (capacity - offset) as i32,
                1, /* acceleration */
            );
            assert!(bytes > 0);
            copy_nonoverlapping(
                (bytes as u32).to_le_bytes().as_ptr(),
                output.as_mut_ptr().add(offset),
                4,
            );
            offset += (bytes + 4) as usize;
        }

        lz4_sys::LZ4_freeStream(stream);
        output.set_len(offset);
    }
    output
}

pub fn decode_blocks(mut src: &[u8]) -> Vec<u8> {
    assert!(src.len() > 8, "data is too short: {} <= 8", src.len());
    unsafe {
        let decoded_len = u64::from_le(read_unaligned(src.as_ptr() as *const u64));
        let mut dst: Vec<u8> = Vec::with_capacity(decoded_len as usize);
        src = &src[8..];

        let (decoder, mut offset) = (LZ4_createStreamDecode(), 0);
        while !src.is_empty() {
            let len = u32::from_le(read_unaligned(src.as_ptr() as *const u32));
            let bytes = LZ4_decompress_safe_continue(
                decoder,
                src.as_ptr().add(4) as _,
                dst.as_mut_ptr().add(offset) as _,
                len as i32,
                (dst.capacity() - offset) as i32,
            );
            assert!(bytes >= 0);
            offset += bytes as usize;
            src = &src[(4 + len as usize)..];
        }
        LZ4_freeStreamDecode(decoder);
        assert_eq!(offset, decoded_len as usize);
        dst.set_len(offset);
        dst
    }
}

extern "C" {
    // It's not in lz4_sys.
    fn LZ4_compress_fast_continue(
        LZ4_stream: *mut LZ4StreamEncode,
        source: *const u8,
        dest: *mut u8,
        input_size: c_int,
        dest_capacity: c_int,
        acceleration: c_int,
    ) -> c_int;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        let data: Vec<&'static [u8]> = vec![b"", b"123", b"12345678910"];
        for d in data {
            let compressed = encode_block(d, 0, 0);
            assert!(compressed.len() > 4);
            let res = decode_block(&compressed);
            assert_eq!(res, d);
        }
    }

    #[test]
    fn test_blocks() {
        let raw_inputs = vec![
            b"".to_vec(),
            b"123".to_vec(),
            b"12345678910".to_vec(),
            vec![b'x'; 99999],
            vec![0; 33333],
        ];

        let mut input = Vec::with_capacity(raw_inputs.iter().map(|x| x.len()).sum());
        for x in &raw_inputs {
            input.extend_from_slice(x);
        }

        let encoded = encode_blocks(|| raw_inputs.iter().map(|x| x.as_slice()), 0, 0);
        let decoded = decode_blocks(&encoded);
        assert_eq!(input, decoded);
    }
}
