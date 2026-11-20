// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

#![cfg(feature = "prost-codec")]

use raft_engine::{Config, Engine, LogBatch, MessageExt, ProstCodec, ProtobufCodec, ValueCodec};

// These fields use the same tags as raft::eraftpb::Entry.
#[derive(Clone, PartialEq, prost::Message)]
struct Entry {
    #[prost(uint64, tag = "2")]
    term: u64,
    #[prost(uint64, tag = "3")]
    index: u64,
    #[prost(bytes = "vec", tag = "4")]
    data: Vec<u8>,
}

struct ProstEntryExt;

impl MessageExt<ProstCodec> for ProstEntryExt {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

struct ProtobufEntryExt;

impl MessageExt for ProtobufEntryExt {
    type Entry = raft::eraftpb::Entry;

    fn index(entry: &Self::Entry) -> u64 {
        entry.index
    }
}

#[test]
fn codec_appends_and_reports_invalid_messages() {
    let entry = Entry {
        term: 3,
        index: 7,
        data: b"payload".to_vec(),
    };
    let mut bytes = b"prefix".to_vec();
    ProstCodec::encode_to(&entry, &mut bytes).unwrap();
    assert_eq!(&bytes[..6], b"prefix");
    let decoded: Entry = ProstCodec::decode(&bytes[6..]).unwrap();
    assert_eq!(decoded, entry);
    assert_eq!(ProstCodec::encode_to_vec(&entry).unwrap(), bytes[6..]);
    assert!(<ProstCodec as ValueCodec<Entry>>::decode(&[0x80]).is_err());
}

#[test]
fn codecs_share_records_after_recovery() {
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let entries: Vec<Entry> = (1..=8)
        .map(|index| Entry {
            term: 2,
            index,
            data: vec![index as u8; 1024],
        })
        .collect();
    {
        let engine = Engine::open(config.clone()).unwrap();
        let mut batch = LogBatch::default();
        batch
            .add_entries_with::<ProstEntryExt, ProstCodec>(1, &entries)
            .unwrap();
        let protobuf_entries: Vec<raft::eraftpb::Entry> = entries
            .iter()
            .map(|entry| ProtobufCodec::decode(&ProstCodec::encode_to_vec(entry).unwrap()).unwrap())
            .collect();
        batch
            .add_entries::<ProtobufEntryExt>(2, &protobuf_entries)
            .unwrap();
        batch
            .put_value::<ProstCodec, _>(1, b"state".to_vec(), &entries[7])
            .unwrap();
        batch
            .put_message(2, b"state".to_vec(), &protobuf_entries[7])
            .unwrap();
        engine.write(&mut batch, true).unwrap();
    }

    let engine = Engine::open(config).unwrap();
    for group in [1, 2] {
        let mut fetched = Vec::new();
        let count = engine
            .fetch_entries_to_with::<ProstEntryExt, ProstCodec>(group, 1, 9, None, &mut fetched)
            .unwrap();
        assert_eq!(count, entries.len());
        assert_eq!(fetched, entries);
        for entry in &entries {
            assert_eq!(
                engine
                    .get_entry_with::<ProstEntryExt, ProstCodec>(group, entry.index)
                    .unwrap(),
                Some(entry.clone())
            );
            let legacy = engine
                .get_entry::<ProtobufEntryExt>(group, entry.index)
                .unwrap()
                .unwrap();
            assert_eq!(legacy.term, entry.term);
            assert_eq!(legacy.index, entry.index);
            assert_eq!(legacy.data.as_ref(), entry.data.as_slice());
        }
        assert_eq!(
            engine
                .get_value::<Entry, ProstCodec>(group, b"state")
                .unwrap(),
            Some(entries[7].clone())
        );
        let legacy: raft::eraftpb::Entry = engine.get_message(group, b"state").unwrap().unwrap();
        assert_eq!(legacy.index, entries[7].index);
        assert_eq!(legacy.data.as_ref(), entries[7].data.as_slice());
    }
}
