// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use hashbrown::HashMap;

use crate::file_pipe_log::ReplayMachine;
use crate::log_batch::{LogItemBatch, LogItemContent};
use crate::pipe_log::{FileId, LogQueue};
use crate::Result;

#[derive(Default)]
pub struct ConsistencyChecker {
    // Raft group id -> last index
    pending: HashMap<u64, u64>,
    // Raft gropu id -> last uncorrupted index
    corrupted: HashMap<u64, u64>,
}

impl ConsistencyChecker {
    pub fn finish(self) -> Vec<(u64, u64)> {
        self.corrupted.into_iter().collect()
    }
}

impl ReplayMachine for ConsistencyChecker {
    fn replay(
        &mut self,
        item_batch: LogItemBatch,
        _queue: LogQueue,
        _file_id: FileId,
    ) -> Result<()> {
        for item in item_batch.iter() {
            if let LogItemContent::EntryIndexes(ents) = &item.content {
                if !ents.0.is_empty() {
                    let incoming_first_index = ents.0.first().unwrap().index;
                    let incoming_last_index = ents.0.last().unwrap().index;
                    let last_index = self
                        .pending
                        .entry(item.raft_group_id)
                        .or_insert(incoming_last_index);
                    if *last_index + 1 < incoming_first_index {
                        self.corrupted
                            .insert(item.raft_group_id, incoming_first_index);
                    }
                    *last_index = incoming_last_index;
                }
            }
        }
        Ok(())
    }

    fn merge(&mut self, mut rhs: Self, _queue: LogQueue) -> Result<()> {
        for (id, last_index) in rhs.pending.drain() {
            self.pending.insert(id, last_index);
        }
        for (id, last_index) in rhs.corrupted.drain() {
            self.corrupted.insert(id, last_index);
        }
        Ok(())
    }
}
