// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use raft::eraftpb::Entry;

use crate::{
    memtable::EntryIndex,
    pipe_log::{FileId, LogQueue},
};

pub fn generate_entries(begin_index: u64, end_index: u64, data: Option<Vec<u8>>) -> Vec<Entry> {
    let mut v = vec![Entry::new(); (end_index - begin_index) as usize];
    let mut index = begin_index;
    for e in v.iter_mut() {
        e.set_index(index);
        if let Some(ref data) = data {
            e.set_data(data.clone().into())
        }
        index += 1;
    }
    v
}

pub fn generate_entry_indexes(
    begin_idx: u64,
    end_idx: u64,
    queue: LogQueue,
    file_id: FileId,
) -> Vec<EntryIndex> {
    assert!(end_idx >= begin_idx);
    let mut ents_idx = vec![];
    let mut offset = 0;
    for idx in begin_idx..end_idx {
        let ent_idx = EntryIndex {
            index: idx,
            queue,
            file_id,
            entry_offset: offset, // fake offset
            entry_len: 1,         // fake size
            ..Default::default()
        };
        offset += ent_idx.entry_len as u64;

        ents_idx.push(ent_idx);
    }
    ents_idx
}
