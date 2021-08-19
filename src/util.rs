// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::{HashMap as StdHashMap, VecDeque};
use std::fmt::{self, Display, Write};
use std::hash::BuildHasherDefault;
use std::ops::{Div, Mul};
use std::str::FromStr;
use std::time::{Duration, Instant};

use crc32fast::Hasher;
use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub use crossbeam::channel::SendError as ScheduleError;
pub type HashMap<K, V> = StdHashMap<K, V, BuildHasherDefault<fxhash::FxHasher>>;

const UNIT: u64 = 1;
const DATA_MAGNITUDE: u64 = 1024;
pub const KB: u64 = UNIT * DATA_MAGNITUDE;
pub const MB: u64 = KB * DATA_MAGNITUDE;
pub const GB: u64 = MB * DATA_MAGNITUDE;
pub const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
pub const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub const fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KB)
    }

    pub const fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MB)
    }

    pub const fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GB)
    }

    pub const fn as_mb(self) -> u64 {
        self.0 / MB
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
        } else if size % PB == 0 {
            write!(buffer, "{}PiB", size / PB).unwrap();
        } else if size % TB == 0 {
            write!(buffer, "{}TiB", size / TB).unwrap();
        } else if size % GB as u64 == 0 {
            write!(buffer, "{}GiB", size / GB).unwrap();
        } else if size % MB as u64 == 0 {
            write!(buffer, "{}MiB", size / MB).unwrap();
        } else if size % KB as u64 == 0 {
            write!(buffer, "{}KiB", size / KB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

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
            .take_while(|c| char::is_ascii_digit(c) || *c == '.')
            .count();

        // unit: alphabetic characters
        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" | "KiB" => KB,
            "M" | "MB" | "MiB" => MB,
            "G" | "GB" | "GiB" => GB,
            "T" | "TB" | "TiB" => TB,
            "P" | "PB" | "PiB" => PB,
            "B" | "" => UNIT,
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
        if self.0 >= PB {
            write!(f, "{:.1}PiB", self.0 as f64 / PB as f64)
        } else if self.0 >= TB {
            write!(f, "{:.1}TiB", self.0 as f64 / TB as f64)
        } else if self.0 >= GB {
            write!(f, "{:.1}GiB", self.0 as f64 / GB as f64)
        } else if self.0 >= MB {
            write!(f, "{:.1}MiB", self.0 as f64 / MB as f64)
        } else if self.0 >= KB {
            write!(f, "{:.1}KiB", self.0 as f64 / KB as f64)
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
