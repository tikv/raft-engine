// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fmt::{self, Display, Write};
use std::ops::{Div, Mul};
use std::str::FromStr;
use std::time::{Duration, Instant};

use crc32fast::Hasher;
use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

const UNIT: u64 = 1;

const BINARY_DATA_MAGNITUDE: u64 = 1024;
pub const B: u64 = UNIT;
pub const KIB: u64 = B * BINARY_DATA_MAGNITUDE;
pub const MIB: u64 = KIB * BINARY_DATA_MAGNITUDE;
pub const GIB: u64 = MIB * BINARY_DATA_MAGNITUDE;
pub const TIB: u64 = GIB * BINARY_DATA_MAGNITUDE;
pub const PIB: u64 = TIB * BINARY_DATA_MAGNITUDE;

#[derive(Clone, Debug, Copy, PartialEq, PartialOrd)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub const fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KIB)
    }

    pub const fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MIB)
    }

    pub const fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GIB)
    }

    pub const fn as_mb(self) -> u64 {
        self.0 / MIB
    }
}

impl Div<u64> for ReadableSize {
    type Output = ReadableSize;

    fn div(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 / rhs)
    }
}

impl Div<ReadableSize> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: ReadableSize) -> u64 {
        self.0 / rhs.0
    }
}

impl Mul<u64> for ReadableSize {
    type Output = ReadableSize;

    fn mul(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 * rhs)
    }
}

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{}KiB", size).unwrap();
        } else if size % PIB == 0 {
            write!(buffer, "{}PiB", size / PIB).unwrap();
        } else if size % TIB == 0 {
            write!(buffer, "{}TiB", size / TIB).unwrap();
        } else if size % GIB as u64 == 0 {
            write!(buffer, "{}GiB", size / GIB).unwrap();
        } else if size % MIB as u64 == 0 {
            write!(buffer, "{}MiB", size / MIB).unwrap();
        } else if size % KIB as u64 == 0 {
            write!(buffer, "{}KiB", size / KIB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    // This method parses value in binary unit.
    fn from_str(s: &str) -> Result<ReadableSize, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{:?} is not a valid size.", s));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {:?}", s));
        }

        // size: digits and '.' as decimal separator
        let size_len = size_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || ['.', 'e', 'E', '-', '+'].contains(c))
            .count();

        // unit: alphabetic characters
        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" | "KiB" => KIB,
            "M" | "MB" | "MiB" => MIB,
            "G" | "GB" | "GiB" => GIB,
            "T" | "TB" | "TiB" => TIB,
            "P" | "PB" | "PiB" => PIB,
            "B" | "" => B,
            _ => {
                return Err(format!(
                    "only B, KB, KiB, MB, MiB, GB, GiB, TB, TiB, PB, and PiB are supported: {:?}",
                    s
                ));
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {:?}", s)),
        }
    }
}

impl Display for ReadableSize {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0 >= PIB {
            write!(f, "{:.1}PiB", self.0 as f64 / PIB as f64)
        } else if self.0 >= TIB {
            write!(f, "{:.1}TiB", self.0 as f64 / TIB as f64)
        } else if self.0 >= GIB {
            write!(f, "{:.1}GiB", self.0 as f64 / GIB as f64)
        } else if self.0 >= MIB {
            write!(f, "{:.1}MiB", self.0 as f64 / MIB as f64)
        } else if self.0 >= KIB {
            write!(f, "{:.1}KiB", self.0 as f64 / KIB as f64)
        } else {
            write!(f, "{}B", self.0)
        }
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

pub trait InstantExt {
    fn saturating_elapsed(&self) -> Duration;
}

impl InstantExt for Instant {
    #[inline]
    fn saturating_elapsed(&self) -> Duration {
        Instant::now().saturating_duration_since(*self)
    }
}

/// Take slices in the range.
///
/// ### Panics
///
/// if [low, high) is out of bound.
pub fn slices_in_range<T>(entry: &VecDeque<T>, low: usize, high: usize) -> (&[T], &[T]) {
    let (first, second) = entry.as_slices();
    if low >= first.len() {
        (&second[low - first.len()..high - first.len()], &[])
    } else if high <= first.len() {
        (&first[low..high], &[])
    } else {
        (&first[low..], &second[..high - first.len()])
    }
}

#[inline]
pub fn crc32(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

pub mod lz4 {
    use crate::{Error, Result};
    use std::{i32, ptr};

    /// Compress content in `buf[skip..]`, and append output to `buf`.
    pub fn append_compress_block(buf: &mut Vec<u8>, skip: usize) -> Result<()> {
        let buf_len = buf.len();
        let content_len = buf_len - skip;
        if content_len > 0 {
            if content_len > i32::MAX as usize {
                return Err(Error::InvalidArgument(format!(
                    "Content too long {}",
                    content_len
                )));
            }
            unsafe {
                let bound = lz4_sys::LZ4_compressBound(content_len as i32);
                debug_assert!(bound > 0);

                // Layout: { decoded_len | content }
                buf.reserve(buf_len + 4 + bound as usize);
                let buf_ptr = buf.as_mut_ptr();

                let le_len = content_len.to_le_bytes();
                ptr::copy_nonoverlapping(le_len.as_ptr(), buf_ptr.add(buf_len), 4);

                let compressed = lz4_sys::LZ4_compress_default(
                    buf_ptr.add(skip) as _,
                    buf_ptr.add(buf_len + 4) as _,
                    content_len as i32,
                    bound,
                );
                if compressed == 0 {
                    return Err(Error::Other(box_err!("Compression failed")));
                }
                buf.set_len(buf_len + 4 + compressed as usize);
            }
        }
        Ok(())
    }

    pub fn decompress_block(src: &[u8]) -> Result<Vec<u8>> {
        if src.len() > 4 {
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
                    Ok(dst)
                } else if l < 0 {
                    Err(Error::Other(box_err!("Decompression failed {}", l)))
                } else {
                    Err(Error::Corruption(format!(
                        "Decompressed content length mismatch {} != {}",
                        l, len
                    )))
                }
            }
        } else if !src.is_empty() {
            Err(Error::Corruption(format!(
                "Content to compress to short {}",
                src.len()
            )))
        } else {
            Ok(Vec::new())
        }
    }

    #[cfg(test)]
    mod tests {
        #[test]
        fn test_basic() {
            let vecs: Vec<Vec<u8>> = vec![b"".to_vec(), b"123".to_vec(), b"12345678910".to_vec()];
            for mut vec in vecs.into_iter() {
                let uncompressed_len = vec.len();
                super::append_compress_block(&mut vec, 0).unwrap();
                let res = super::decompress_block(&vec[uncompressed_len..]).unwrap();
                assert_eq!(res, vec[..uncompressed_len].to_owned());
            }
        }
    }
}
