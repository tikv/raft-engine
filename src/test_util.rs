// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::{
    memtable::EntryIndex,
    pipe_log::{FileId, LogQueue},
};

use raft::eraftpb::Entry;

pub fn generate_entries(first_index: u64, len: usize, data: Option<Vec<u8>>) -> Vec<Entry> {
    let mut v = vec![Entry::new(); len];
    let mut index = first_index;
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
        let mut ent_idx = EntryIndex::default();
        ent_idx.index = idx;
        ent_idx.queue = queue;
        ent_idx.file_id = file_id;
        ent_idx.entry_offset = offset; // fake offset
        ent_idx.entry_len = 1; // fake size
        offset += ent_idx.entry_len as u64;

        ents_idx.push(ent_idx);
    }
    ents_idx
}
