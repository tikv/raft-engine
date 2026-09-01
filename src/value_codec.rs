// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Pluggable value codecs.
//!
//! The built-in [`ProtobufCodec`] reproduces the byte layout used by Raft
//! Engine 0.4 and earlier, so adopting this abstraction is on-disk compatible.
//! [`BincodeCodec`] and [`JsonCodec`] let entries be plain `serde` types
//! instead of protobuf messages.

use crate::Result;

/// Describes how values of type `T` are encoded into and decoded from the log.
///
/// # Example
///
/// ```
/// use raft_engine::{Result, ValueCodec};
///
/// struct RawBytesCodec;
///
/// impl ValueCodec<Vec<u8>> for RawBytesCodec {
///     fn encode_to(v: &Vec<u8>, buf: &mut Vec<u8>) -> Result<()> {
///         buf.extend_from_slice(v);
///         Ok(())
///     }
///
///     fn decode(bytes: &[u8]) -> Result<Vec<u8>> {
///         Ok(bytes.to_owned())
///     }
/// }
/// ```
pub trait ValueCodec<T> {
    /// Appends the encoded form of `v` to `buf`.
    ///
    /// Implementations must only append; bytes already in `buf` belong to
    /// other records in the same [`LogBatch`](crate::LogBatch).
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> Result<()>;

    /// Decodes a value from a complete encoded byte slice.
    fn decode(bytes: &[u8]) -> Result<T>;

    /// Encodes `v` into a freshly allocated buffer.
    ///
    /// The default implementation starts from an empty `Vec`. Codecs that can
    /// cheaply compute the encoded size should override this to pre-allocate.
    #[inline]
    fn encode_to_vec(v: &T) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        Self::encode_to(v, &mut buf)?;
        Ok(buf)
    }
}

/// The legacy codec, backed by `rust-protobuf` 2.x.
///
/// Byte-for-byte identical to the encoding used by Raft Engine before value
/// codecs were introduced. This is what you want unless you are creating a
/// brand new data directory.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProtobufCodec;

impl<T: protobuf::Message> ValueCodec<T> for ProtobufCodec {
    #[inline]
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> Result<()> {
        v.write_to_vec(buf)?;
        Ok(())
    }

    #[inline]
    fn decode(bytes: &[u8]) -> Result<T> {
        Ok(protobuf::parse_from_bytes(bytes)?)
    }

    /// PERF: overrides the provided default, which would start from an empty
    /// `Vec` and grow it. `Message::write_to_bytes` pre-sizes the buffer using
    /// `compute_size()`, exactly reproducing the pre-codec `put_message` path.
    #[inline]
    fn encode_to_vec(v: &T) -> Result<Vec<u8>> {
        Ok(v.write_to_bytes()?)
    }
}

/// A `serde` codec backed by [`bincode`], available under the `serde-bincode` feature.
#[cfg(feature = "serde-bincode")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BincodeCodec;

#[cfg(feature = "serde-bincode")]
impl<T> ValueCodec<T> for BincodeCodec
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    #[inline]
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> Result<()> {
        // NOTE: `serialize_into` appends through `io::Write`, it does not
        // clobber the existing contents of `buf`.
        bincode::serialize_into(&mut *buf, v).map_err(|e| box_err!("bincode encode: {}", e))
    }

    #[inline]
    fn decode(bytes: &[u8]) -> Result<T> {
        bincode::deserialize(bytes).map_err(|e| box_err!("bincode decode: {}", e))
    }
}

/// A `serde` codec backed by [`serde_json`], available under the `serde-json` feature.
#[cfg(feature = "serde-json")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct JsonCodec;

#[cfg(feature = "serde-json")]
impl<T> ValueCodec<T> for JsonCodec
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    #[inline]
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> Result<()> {
        serde_json::to_writer(&mut *buf, v).map_err(|e| box_err!("json encode: {}", e))
    }

    #[inline]
    fn decode(bytes: &[u8]) -> Result<T> {
        serde_json::from_slice(bytes).map_err(|e| box_err!("json decode: {}", e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use protobuf::Message;
    use raft::eraftpb::Entry;

    fn entry(index: u64, data_len: usize) -> Entry {
        let mut e = Entry::new();
        e.set_index(index);
        e.set_term(7);
        e.set_data(vec![b'x'; data_len].into());
        e
    }

    #[test]
    fn test_protobuf_codec_matches_legacy_encoding() {
        for len in [0, 1, 7, 128, 4096] {
            let e = entry(len as u64 + 1, len);
            let mut legacy_appended = Vec::new();
            e.write_to_vec(&mut legacy_appended).unwrap();
            let mut via_codec = Vec::new();
            ProtobufCodec::encode_to(&e, &mut via_codec).unwrap();
            assert_eq!(legacy_appended, via_codec, "len={len}");
            assert_eq!(
                e.write_to_bytes().unwrap(),
                ProtobufCodec::encode_to_vec(&e).unwrap(),
                "len={len}"
            );
        }
    }

    /// `encode_to` must append, never clobber: `LogBatch` packs every entry of
    /// a batch into one shared buffer.
    #[test]
    fn test_encode_to_appends() {
        let prefix = b"already here".to_vec();

        let mut buf = prefix.clone();
        let e = entry(1, 32);
        ProtobufCodec::encode_to(&e, &mut buf).unwrap();
        assert_eq!(&buf[..prefix.len()], &prefix[..]);
        let decoded: Entry = ProtobufCodec::decode(&buf[prefix.len()..]).unwrap();
        assert_eq!(decoded, e);

        #[cfg(feature = "serde-bincode")]
        {
            let mut buf = prefix.clone();
            let v = vec![1u32, 2, 3];
            BincodeCodec::encode_to(&v, &mut buf).unwrap();
            assert_eq!(&buf[..prefix.len()], &prefix[..]);
            let decoded: Vec<u32> = BincodeCodec::decode(&buf[prefix.len()..]).unwrap();
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_protobuf_codec_roundtrip() {
        let e = entry(42, 256);
        let bytes = ProtobufCodec::encode_to_vec(&e).unwrap();
        let decoded: Entry = ProtobufCodec::decode(&bytes).unwrap();
        assert_eq!(decoded, e);
    }

    #[cfg(feature = "serde-bincode")]
    #[test]
    fn test_bincode_codec_roundtrip() {
        use serde::{Deserialize, Serialize};

        #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
        struct Payload {
            index: u64,
            name: String,
            blob: Vec<u8>,
        }
        let v = Payload {
            index: 9,
            name: "hello".to_owned(),
            blob: vec![3; 100],
        };
        let bytes = BincodeCodec::encode_to_vec(&v).unwrap();
        let decoded: Payload = BincodeCodec::decode(&bytes).unwrap();
        assert_eq!(decoded, v);
        assert!(<BincodeCodec as ValueCodec<Payload>>::decode(&[0xff, 0x01]).is_err());
    }

    #[cfg(feature = "serde-json")]
    #[test]
    fn test_json_codec_roundtrip() {
        let v = vec!["a".to_owned(), "b".to_owned()];
        let bytes = JsonCodec::encode_to_vec(&v).unwrap();
        assert_eq!(bytes, br#"["a","b"]"#.to_vec());
        let decoded: Vec<String> = JsonCodec::decode(&bytes).unwrap();
        assert_eq!(decoded, v);
    }
}
