// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Pluggable value codecs.
//!
//! The built-in [`ProtobufCodec`] reproduces the byte layout used by Raft
//! Engine 0.4 and earlier, so adopting this abstraction is on-disk compatible.
//! The optional `prost-codec` feature adds support for Prost messages.
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
/// codecs were introduced. Other codecs must use a compatible message schema
/// to read existing records.
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

/// A Protobuf codec for [`prost::Message`] types.
///
/// Enable the `prost-codec` feature to use this codec. Pass it to
/// [`LogBatch::add_entries_with`](crate::LogBatch::add_entries_with) or
/// [`LogBatch::put_value`](crate::LogBatch::put_value).
///
/// The message schema must match existing records when you change codecs.
/// Prost discards unknown fields when it decodes a message.
#[cfg(feature = "prost-codec")]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProstCodec;

#[cfg(feature = "prost-codec")]
impl<T: prost::Message + Default> ValueCodec<T> for ProstCodec {
    #[inline]
    fn encode_to(v: &T, buf: &mut Vec<u8>) -> Result<()> {
        v.encode(buf).map_err(|e| crate::Error::Other(Box::new(e)))
    }

    #[inline]
    fn decode(bytes: &[u8]) -> Result<T> {
        T::decode(bytes).map_err(|e| crate::Error::Other(Box::new(e)))
    }

    #[inline]
    fn encode_to_vec(v: &T) -> Result<Vec<u8>> {
        Ok(v.encode_to_vec())
    }
}

/// A `serde` codec backed by [`bincode`], available under the `serde-bincode`
/// feature.
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

/// A `serde` codec backed by [`serde_json`], available under the `serde-json`
/// feature.
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
pub(crate) mod test_codecs {
    use super::*;
    use crate::log_batch::MessageExt;
    #[derive(Clone, Debug, Default, PartialEq)]
    pub(crate) struct RawEntry {
        pub index: u64,
        pub data: Vec<u8>,
    }
    impl RawEntry {
        pub(crate) fn new(index: u64, data_len: usize) -> Self {
            RawEntry {
                index,
                data: vec![(index % 251) as u8; data_len],
            }
        }
    }
    pub(crate) struct RawCodec;
    impl ValueCodec<RawEntry> for RawCodec {
        fn encode_to(v: &RawEntry, buf: &mut Vec<u8>) -> Result<()> {
            buf.extend_from_slice(&v.index.to_le_bytes());
            buf.extend_from_slice(&v.data);
            Ok(())
        }
        fn decode(bytes: &[u8]) -> Result<RawEntry> {
            if bytes.len() < 8 {
                return Err(box_err!("raw codec: input too short"));
            }
            Ok(RawEntry {
                index: u64::from_le_bytes(bytes[..8].try_into().unwrap()),
                data: bytes[8..].to_owned(),
            })
        }
    }

    pub(crate) struct FailingCodec;

    impl ValueCodec<RawEntry> for FailingCodec {
        fn encode_to(v: &RawEntry, buf: &mut Vec<u8>) -> Result<()> {
            if v.index == FAILING_INDEX {
                buf.extend_from_slice(b"PARTIAL-GARBAGE");
                return Err(box_err!("codec blew up"));
            }
            RawCodec::encode_to(v, buf)
        }

        fn decode(bytes: &[u8]) -> Result<RawEntry> {
            RawCodec::decode(bytes)
        }
    }
    pub(crate) const FAILING_INDEX: u64 = u64::MAX;

    pub(crate) struct RawExt;

    impl MessageExt<RawCodec> for RawExt {
        type Entry = RawEntry;

        fn index(e: &Self::Entry) -> u64 {
            e.index
        }
    }

    pub(crate) struct RawExtWrongIndex;

    impl MessageExt<RawCodec> for RawExtWrongIndex {
        type Entry = RawEntry;
        fn index(e: &Self::Entry) -> u64 {
            e.index + 1
        }
    }

    pub(crate) struct FailExt;

    impl MessageExt<FailingCodec> for FailExt {
        type Entry = RawEntry;
        fn index(e: &Self::Entry) -> u64 {
            e.index
        }
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
