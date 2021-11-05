// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use hashbrown::HashMap;

use crate::file_pipe_log::ReplayMachine;
use crate::log_batch::{LogItemBatch, LogItemContent};
use crate::pipe_log::{FileId, LogQueue};
use crate::Result;

#[derive(Default)]
pub struct ConsistencyChecker {
    // Raft group id -> first index, last index
    raft_groups: HashMap<u64, (u64, u64)>,
    // Raft group id -> last unaffected index
    corrupted: HashMap<u64, u64>,
}

impl ConsistencyChecker {
    pub fn finish(self) -> HashMap<u64, u64> {
        self.corrupted
    }
}

impl ReplayMachine for ConsistencyChecker {
    fn replay(&mut self, item_batch: LogItemBatch, _file_id: FileId) -> Result<()> {
        for item in item_batch.iter() {
            if let LogItemContent::EntryIndexes(ents) = &item.content {
                if !ents.0.is_empty() {
                    let incoming_first_index = ents.0.first().unwrap().index;
                    let incoming_last_index = ents.0.last().unwrap().index;
                    let last_index = self
                        .raft_groups
                        .entry(item.raft_group_id)
                        .or_insert((incoming_first_index, incoming_last_index));
                    if last_index.1 + 1 < incoming_first_index {
                        self.corrupted
                            .entry(item.raft_group_id)
                            .or_insert(last_index.1);
                    }
                    last_index.1 = incoming_last_index;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, mut rhs: Self, _queue: LogQueue) -> Result<()> {
        let mut new_corrupted: HashMap<u64, u64> = HashMap::default();
        // Find holes between self and rhs.
        for (id, (first, last)) in rhs.raft_groups.drain() {
            self.raft_groups
                .entry(id)
                .and_modify(|(_, l)| {
                    if *l + 1 < first {
                        new_corrupted.insert(id, *l);
                    }
                    *l = last;
                })
                .or_insert((first, last));
        }
        for (id, last_index) in new_corrupted.drain() {
            self.corrupted.entry(id).or_insert(last_index);
        }
        for (id, last_index) in rhs.corrupted.drain() {
            self.corrupted.entry(id).or_insert(last_index);
        }
        Ok(())
    }
}
