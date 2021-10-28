// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::borrow::BorrowMut;
use std::cmp;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::sync::Arc;

use fail::fail_point;
use hashbrown::HashMap;
use log::debug;
use parking_lot::{Mutex, RwLock};

use crate::file_pipe_log::ReplayMachine;
use crate::log_batch::{
    Command, CompressionType, KeyValue, LogBatch, LogItemBatch, LogItemContent, LogItemDrain,
    OpType,
};
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue};
use crate::util::slices_in_range;
use crate::{Error, GlobalStats, Result};

const SHRINK_CACHE_CAPACITY: usize = 64;
const SHRINK_CACHE_LIMIT: usize = 512;

const MEMTABLE_SLOT_COUNT: usize = 128;

#[derive(Debug, Clone, PartialEq)]
pub struct EntryIndex {
    pub index: u64,

    // Compressed section physical position in file.
    pub compression_type: CompressionType,
    // offset and length of current entry within `entries`.
    pub entry_offset: u64,
    pub entry_len: usize,

    pub entries: Option<FileBlockHandle>,
}

impl Default for EntryIndex {
    fn default() -> EntryIndex {
        EntryIndex {
            index: 0,
            compression_type: CompressionType::None,
            entry_offset: 0,
            entry_len: 0,
            entries: None,
        }
    }
}

/*
 * Each region has an individual `MemTable` to store all entries indices.
 * `MemTable` also have a map to store all key value pairs for this region.
 *
 * All entries indices [******************************************]
 *                      ^                                        ^
 *                      |                                        |
 *                 first entry                               last entry
 */
pub struct MemTable {
    region_id: u64,

    // Entries are pushed back with continuously ascending indexes.
    entry_indexes: VecDeque<EntryIndex>,
    // The amount of rewritten entries. This can be used to index appended entries
    // because rewritten entries are always at front.
    rewrite_count: usize,

    // key -> (value, queue, file_id)
    kvs: HashMap<Vec<u8>, (Vec<u8>, FileId)>,

    global_stats: Arc<GlobalStats>,
}

impl MemTable {
    pub fn new(region_id: u64, global_stats: Arc<GlobalStats>) -> MemTable {
        MemTable {
            region_id,
            entry_indexes: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            rewrite_count: 0,
            kvs: HashMap::default(),

            global_stats,
        }
    }

    pub fn get_entry(&self, index: u64) -> Option<EntryIndex> {
        if self.entry_indexes.is_empty() {
            return None;
        }

        let first_index = self.entry_indexes.front().unwrap().index;
        let last_index = self.entry_indexes.back().unwrap().index;
        if index < first_index || index > last_index {
            return None;
        }

        let ioffset = (index - first_index) as usize;
        let entry_index = self.entry_indexes[ioffset].clone();
        Some(entry_index)
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.kvs.get(key).map(|v| v.0.clone())
    }

    pub fn delete(&mut self, key: &[u8]) {
        if let Some(value) = self.kvs.remove(key) {
            if value.1.queue == LogQueue::Rewrite {
                self.global_stats.add_compacted_rewrite(1);
            }
        }
    }

    pub fn append(&mut self, entry_indexes: Vec<EntryIndex>) {
        if entry_indexes.is_empty() {
            return;
        }
        let first_index_to_add = entry_indexes[0].index;
        self.cut_entry_indexes(first_index_to_add);

        if let Some(index) = self.entry_indexes.back() {
            assert_eq!(
                index.index + 1,
                first_index_to_add,
                "memtable {} has a hole",
                self.region_id
            );
        }

        self.entry_indexes.extend(entry_indexes);
    }

    // This will only be called during recovery.
    pub fn append_rewrite(&mut self, entry_indexes: Vec<EntryIndex>) {
        self.global_stats.add_rewrite(entry_indexes.len());
        match (
            self.entry_indexes.back().map(|x| x.index),
            entry_indexes.first(),
        ) {
            (Some(back_idx), Some(first)) if back_idx + 1 < first.index => {
                // It's possible that a hole occurs in the rewrite queue.
                self.compact_to(back_idx + 1);
            }
            _ => {}
        }
        self.append(entry_indexes);
        self.rewrite_count = self.entry_indexes.len();
    }

    pub fn rewrite(&mut self, entry_indexes: Vec<EntryIndex>, latest_rewrite: Option<FileSeq>) {
        if entry_indexes.is_empty() {
            return;
        }

        // Update the counter of entries in the rewrite queue.
        self.global_stats.add_rewrite(entry_indexes.len());

        if self.entry_indexes.is_empty() {
            // All entries are compacted, update the counter.
            self.global_stats.add_compacted_rewrite(entry_indexes.len());
            return;
        }

        let self_ents_len = self.entry_indexes.len();
        let front = self.entry_indexes[0].index as usize;
        let back = self.entry_indexes[self_ents_len - 1].index as usize;

        let ents_len = entry_indexes.len();
        let first = cmp::max(entry_indexes[0].index as usize, front);
        let last = cmp::min(entry_indexes[ents_len - 1].index as usize, back);

        if last < first {
            // All entries are compacted, update the counter.
            self.global_stats.add_compacted_rewrite(entry_indexes.len());
            return;
        }

        // Some entries in `entry_indexes` are compacted or cut, update the counter.
        let strip_count = ents_len - (last + 1 - first);
        self.global_stats.add_compacted_rewrite(strip_count);

        let (entry_indexes, ents_len) = {
            let diff = first - entry_indexes[0].index as usize;
            let len = last - first + 1;
            (&entry_indexes[diff..diff + len], len)
        };

        let distance = (first - front) as usize;
        // No normal log entry mixed in rewritten entries at the front.
        debug_assert!(
            distance == 0
                || self.entry_indexes[distance - 1].entries.unwrap().id.queue == LogQueue::Rewrite
        );
        for (i, ei) in entry_indexes.iter().enumerate() {
            if let Some(latest_rewrite) = latest_rewrite {
                debug_assert_eq!(
                    self.entry_indexes[i + distance].entries.unwrap().id.queue,
                    LogQueue::Append
                );
                if self.entry_indexes[i + distance].entries.unwrap().id.seq > latest_rewrite {
                    // Some entries are overwritten by new appends.
                    self.global_stats.add_compacted_rewrite(ents_len - i);
                    self.rewrite_count = i + distance;
                    return;
                }
            } else {
                // It's a squeeze operation.
                debug_assert_eq!(
                    self.entry_indexes[i + distance].entries.unwrap().id.queue,
                    LogQueue::Rewrite
                );
            }

            if self.entry_indexes[i + distance].entries.unwrap().id.queue != LogQueue::Rewrite {
                debug_assert_eq!(ei.entries.unwrap().id.queue, LogQueue::Rewrite);
                self.entry_indexes[i + distance].entries.unwrap().id.queue = LogQueue::Rewrite;
            }

            self.entry_indexes[i + distance].compression_type = ei.compression_type;
            self.entry_indexes[i + distance].entries = ei.entries;
            self.entry_indexes[i + distance].entry_offset = ei.entry_offset;
            self.entry_indexes[i + distance].entry_len = ei.entry_len;
            debug_assert_eq!(&self.entry_indexes[i + distance], ei);
        }

        self.rewrite_count = distance + ents_len;
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, file_id: FileId) {
        if let Some(origin) = self.kvs.insert(key, (value, file_id)) {
            if origin.1.queue == LogQueue::Rewrite && file_id.queue == LogQueue::Append {
                self.global_stats.add_compacted_rewrite(1);
            }
        }
        if file_id.queue == LogQueue::Rewrite {
            self.global_stats.add_rewrite(1);
        }
    }

    pub fn rewrite_key(&mut self, key: Vec<u8>, latest_rewrite: Option<FileSeq>, seq: FileSeq) {
        self.global_stats.add_rewrite(1);
        if let Some(value) = self.kvs.get_mut(&key) {
            if value.1.queue == LogQueue::Append {
                if let Some(latest_rewrite) = latest_rewrite {
                    if value.1.seq <= latest_rewrite {
                        value.1 = FileId {
                            queue: LogQueue::Rewrite,
                            seq,
                        };
                    }
                } else {
                    // The rewritten key/value pair has been overwritten.
                    self.global_stats.add_compacted_rewrite(1);
                }
            } else {
                assert!(value.1.seq <= seq);
                value.1.seq = seq;
            }
        } else {
            // The rewritten key/value pair has been compacted.
            self.global_stats.add_compacted_rewrite(1);
        }
    }

    pub fn compact_to(&mut self, mut idx: u64) -> u64 {
        let first_idx = match self.entry_indexes.front() {
            Some(e) if e.index < idx => e.index,
            _ => return 0,
        };
        let last_idx = self.entry_indexes.back().unwrap().index;
        idx = cmp::min(last_idx + 1, idx);

        let drain_end = (idx - first_idx) as usize;
        self.entry_indexes.drain(..drain_end);
        self.maybe_shrink_entry_indexes();

        let rewrite_sub = cmp::min(drain_end, self.rewrite_count);
        self.rewrite_count -= rewrite_sub;
        self.global_stats.add_compacted_rewrite(rewrite_sub);
        drain_end as u64
    }

    // Removes all entry indexes with index greater than or equal to the given.
    // Returns the truncated amount.
    fn cut_entry_indexes(&mut self, index: u64) -> usize {
        if self.entry_indexes.is_empty() {
            return 0;
        }
        let last_index = self.entry_indexes.back().unwrap().index;
        let first_index = self.entry_indexes.front().unwrap().index;
        // Compacted entries can't be overwritten.
        assert!(first_index <= index, "corrupted raft {}", self.region_id);

        let to_truncate = if index <= last_index {
            (index - first_index) as usize
        } else {
            return 0;
        };
        self.entry_indexes.truncate(to_truncate);

        if self.rewrite_count > self.entry_indexes.len() {
            let diff = self.rewrite_count - self.entry_indexes.len();
            self.rewrite_count = self.entry_indexes.len();
            self.global_stats.add_compacted_rewrite(diff);
        }

        (last_index - index + 1) as usize
    }

    fn maybe_shrink_entry_indexes(&mut self) {
        if self.entry_indexes.capacity() > SHRINK_CACHE_LIMIT
            && self.entry_indexes.len() <= SHRINK_CACHE_CAPACITY
        {
            self.entry_indexes.shrink_to(SHRINK_CACHE_CAPACITY);
        }
    }

    /// Mrege from newer neighbor `rhs`.
    /// Only called during parllel recovery.
    pub fn merge_newer_neighbor(&mut self, rhs: &mut Self) {
        assert_eq!(self.region_id, rhs.region_id);
        if let (Some(last), Some(next)) = (self.entry_indexes.back(), rhs.entry_indexes.front()) {
            assert_eq!(last.index + 1, next.index);
        }
        self.entry_indexes.append(&mut rhs.entry_indexes);
        self.rewrite_count += rhs.rewrite_count;
        self.kvs.extend(rhs.kvs.drain());
        self.global_stats.merge(&rhs.global_stats);
    }

    /// Merge from `rhs`, which has a lower priority.
    pub fn merge_lower_prio(&mut self, rhs: &mut Self) {
        debug_assert_eq!(rhs.rewrite_count, rhs.entry_indexes.len());
        debug_assert_eq!(self.rewrite_count, 0);

        if !self.entry_indexes.is_empty() {
            if !rhs.entry_indexes.is_empty() {
                let front = self.entry_indexes[0].index;
                let self_back = rhs.entry_indexes.back().unwrap().index;
                let self_front = rhs.entry_indexes.front().unwrap().index;
                if front > self_back + 1 {
                    rhs.compact_to(self_back + 1);
                } else if front >= self_front {
                    rhs.cut_entry_indexes(front);
                } else {
                    unreachable!();
                }
            }
            rhs.entry_indexes.reserve(self.entry_indexes.len());
            for ei in std::mem::take(&mut self.entry_indexes) {
                rhs.entry_indexes.push_back(ei);
            }
        }

        for (key, (value, file_id)) in std::mem::take(&mut self.kvs) {
            rhs.kvs.insert(key, (value, file_id));
        }

        std::mem::swap(self, rhs);
    }

    pub fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<()> {
        if end <= begin {
            return Ok(());
        }
        if self.entry_indexes.is_empty() {
            return Err(Error::EntryNotFound(self.region_id));
        }
        let first_index = self.entry_indexes.front().unwrap().index;
        if begin < first_index {
            return Err(Error::EntryNotFound(self.region_id));
        }
        let last_index = self.entry_indexes.back().unwrap().index;
        if end > last_index + 1 {
            return Err(Error::EntryNotFound(self.region_id));
        }

        let start_pos = (begin - first_index) as usize;
        let end_pos = (end - begin) as usize + start_pos;

        let (first, second) = slices_in_range(&self.entry_indexes, start_pos, end_pos);
        if let Some(max_size) = max_size {
            let mut total_size = 0;
            for idx in first.iter().chain(second) {
                total_size += idx.entry_len;
                // No matter max_size's value, fetch one entry at least.
                if total_size as usize > max_size && total_size > idx.entry_len {
                    break;
                }
                vec_idx.push(idx.clone());
            }
        } else {
            vec_idx.extend_from_slice(first);
            vec_idx.extend_from_slice(second);
        }
        Ok(())
    }

    pub fn fetch_entry_indexes_before(
        &self,
        latest_rewrite: FileSeq,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<()> {
        let begin = self
            .entry_indexes
            .iter()
            .find(|e| e.entries.unwrap().id.queue == LogQueue::Append);
        let end = self
            .entry_indexes
            .iter()
            .rev()
            .find(|e| e.entries.unwrap().id.seq <= latest_rewrite);
        if let (Some(begin), Some(end)) = (begin, end) {
            if begin.index <= end.index {
                return self.fetch_entries_to(begin.index, end.index + 1, None, vec_idx);
            }
        }
        Ok(())
    }

    pub fn fetch_rewritten_entry_indexes(&self, vec_idx: &mut Vec<EntryIndex>) -> Result<()> {
        if self.rewrite_count > 0 {
            let end = self.entry_indexes[self.rewrite_count - 1].index + 1;
            let first = self.entry_indexes.front().unwrap().index;
            return self.fetch_entries_to(first, end, None, vec_idx);
        }
        Ok(())
    }

    pub fn fetch_kvs_before(&self, latest_rewrite: FileSeq, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, (value, file_id)) in &self.kvs {
            if file_id.queue == LogQueue::Append && file_id.seq <= latest_rewrite {
                vec.push((key.clone(), value.clone()));
            }
        }
    }

    pub fn fetch_rewritten_kvs(&self, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, (value, file_id)) in &self.kvs {
            if file_id.queue == LogQueue::Rewrite {
                vec.push((key.clone(), value.clone()));
            }
        }
    }

    pub fn min_file_seq(&self, queue: LogQueue) -> Option<FileSeq> {
        let entry = match queue {
            LogQueue::Append => self.entry_indexes.get(self.rewrite_count),
            LogQueue::Rewrite if self.rewrite_count == 0 => None,
            LogQueue::Rewrite => self.entry_indexes.front(),
        };
        let ents_min = entry.map(|e| e.entries.unwrap().id.seq);
        let kvs_min = self
            .kvs
            .values()
            .filter(|v| v.1.queue == queue)
            .fold(None, |min, v| {
                if let Some(min) = min {
                    Some(std::cmp::min(min, v.1.seq))
                } else {
                    Some(v.1.seq)
                }
            });
        match (ents_min, kvs_min) {
            (Some(ents_min), Some(kvs_min)) => Some(std::cmp::min(kvs_min, ents_min)),
            (Some(ents_min), None) => Some(ents_min),
            (None, Some(kvs_min)) => Some(kvs_min),
            (None, None) => None,
        }
    }

    pub fn entries_count(&self) -> usize {
        self.entry_indexes.len()
    }

    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    pub fn first_index(&self) -> Option<u64> {
        self.entry_indexes.front().map(|e| e.index)
    }

    pub fn last_index(&self) -> Option<u64> {
        self.entry_indexes.back().map(|e| e.index)
    }
}

type MemTables = HashMap<u64, Arc<RwLock<MemTable>>>;

/// Collection of MemTables, indexed by Raft group ID.
#[derive(Clone)]
pub struct MemTableAccessor {
    global_stats: Arc<GlobalStats>,

    slots: Vec<Arc<RwLock<MemTables>>>,
    // Deleted region memtables that are not yet rewritten.
    removed_memtables: Arc<Mutex<VecDeque<u64>>>,
}

impl MemTableAccessor {
    pub fn new(global_stats: Arc<GlobalStats>) -> MemTableAccessor {
        let mut slots = Vec::with_capacity(MEMTABLE_SLOT_COUNT);
        for _ in 0..MEMTABLE_SLOT_COUNT {
            slots.push(Arc::new(RwLock::new(MemTables::default())));
        }
        MemTableAccessor {
            global_stats,
            slots,
            removed_memtables: Default::default(),
        }
    }

    pub fn get_or_insert(&self, raft_group_id: u64) -> Arc<RwLock<MemTable>> {
        let global_stats = self.global_stats.clone();
        let mut memtables = self.slots[raft_group_id as usize % MEMTABLE_SLOT_COUNT].write();
        let memtable = memtables
            .entry(raft_group_id)
            .or_insert_with(|| Arc::new(RwLock::new(MemTable::new(raft_group_id, global_stats))));
        memtable.clone()
    }

    pub fn get(&self, raft_group_id: u64) -> Option<Arc<RwLock<MemTable>>> {
        self.slots[raft_group_id as usize % MEMTABLE_SLOT_COUNT]
            .read()
            .get(&raft_group_id)
            .cloned()
    }

    pub fn insert(&self, raft_group_id: u64, memtable: Arc<RwLock<MemTable>>) {
        self.slots[raft_group_id as usize % MEMTABLE_SLOT_COUNT]
            .write()
            .insert(raft_group_id, memtable);
    }

    pub fn remove(&self, raft_group_id: u64, queue: LogQueue) {
        self.slots[raft_group_id as usize % MEMTABLE_SLOT_COUNT]
            .write()
            .remove(&raft_group_id);
        if queue == LogQueue::Append {
            let mut removed_memtables = self.removed_memtables.lock();
            removed_memtables.push_back(raft_group_id);
        }
    }

    pub fn fold<B, F: Fn(B, &MemTable) -> B>(&self, mut init: B, fold: F) -> B {
        for tables in &self.slots {
            for memtable in tables.read().values() {
                init = fold(init, &*memtable.read());
            }
        }
        init
    }

    pub fn collect<F: FnMut(&MemTable) -> bool>(
        &self,
        mut condition: F,
    ) -> Vec<Arc<RwLock<MemTable>>> {
        let mut memtables = Vec::new();
        for tables in &self.slots {
            memtables.extend(tables.read().values().filter_map(|t| {
                if condition(&*t.read()) {
                    return Some(t.clone());
                }
                None
            }));
        }
        memtables
    }

    // Returns a `LogBatch` containing Clean commands for all the removed MemTables.
    pub fn take_cleaned_region_logs(&self) -> LogBatch {
        let mut log_batch = LogBatch::default();
        let mut removed_memtables = self.removed_memtables.lock();
        for id in removed_memtables.drain(..) {
            log_batch.add_command(id, Command::Clean);
        }
        log_batch
    }

    // Returns a `HashSet<u64>` containing ids for cleaned regions.
    // Only used for recover.
    pub fn cleaned_region_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::default();
        let removed_memtables = self.removed_memtables.lock();
        for raft_id in removed_memtables.iter() {
            ids.insert(*raft_id);
        }
        ids
    }

    /// Merge memtables from the next right one during segmented recovery.
    pub fn merge_newer_neighbor(&self, rhs: &mut Self) {
        for slot in rhs.slots.iter_mut() {
            for (raft_group_id, memtable) in slot.write().drain() {
                self.get_or_insert(raft_group_id)
                    .write()
                    .merge_newer_neighbor(memtable.write().borrow_mut());
            }
        }
        self.removed_memtables
            .lock()
            .append(&mut rhs.removed_memtables.lock());
    }

    pub fn merge_lower_prio(&self, rhs: &mut Self) {
        let ids = self.cleaned_region_ids();
        for slot in rhs.slots.iter_mut() {
            for (id, memtable) in std::mem::take(&mut *slot.write()) {
                if let Some(existing_memtable) = self.get(id) {
                    existing_memtable
                        .write()
                        .merge_lower_prio(&mut *memtable.write());
                } else if !ids.contains(&id) {
                    self.insert(id, memtable);
                }
            }
        }
    }

    pub fn apply(&self, log_items: LogItemDrain, queue: LogQueue) {
        for item in log_items {
            let raft = item.raft_group_id;
            let memtable = self.get_or_insert(raft);
            fail_point!("memtable_accessor::apply::region_3", raft == 3, |_| {});
            match item.content {
                LogItemContent::EntryIndexes(entries_to_add) => {
                    let entry_indexes = entries_to_add.0;
                    debug!(
                        "{} append to {:?}, Entries[{:?}, {:?}:{:?})",
                        raft,
                        queue,
                        entry_indexes.first().map(|x| x.entries.unwrap().id.queue),
                        entry_indexes.first().map(|x| x.index),
                        entry_indexes.last().map(|x| x.index + 1),
                    );
                    if queue == LogQueue::Rewrite {
                        memtable.write().append_rewrite(entry_indexes);
                    } else {
                        memtable.write().append(entry_indexes);
                    }
                }
                LogItemContent::Command(Command::Clean) => {
                    debug!("{} append to {:?}, Clean", raft, queue);
                    self.remove(raft, queue);
                }
                LogItemContent::Command(Command::Compact { index }) => {
                    debug!("{} append to {:?}, Compact({})", raft, queue, index);
                    memtable.write().compact_to(index);
                }
                LogItemContent::Kv(kv) => match kv.op_type {
                    OpType::Put => {
                        let value = kv.value.unwrap();
                        debug!("{} append to {:?}, Put", raft, queue);
                        memtable.write().put(kv.key, value, kv.file_id.unwrap());
                    }
                    OpType::Del => {
                        let key = kv.key;
                        debug!("{} append to {:?}, Del", raft, queue);
                        memtable.write().delete(key.as_slice());
                    }
                },
            }
        }
    }
}

pub struct MemTableRecoverContext {
    stats: Arc<GlobalStats>,
    log_batch: LogItemBatch,
    memtables: MemTableAccessor,
}

impl MemTableRecoverContext {
    pub fn finish(self) -> (MemTableAccessor, Arc<GlobalStats>) {
        (self.memtables, self.stats)
    }
}

impl Default for MemTableRecoverContext {
    fn default() -> Self {
        let stats = Arc::new(GlobalStats::default());
        Self {
            stats: stats.clone(),
            log_batch: LogItemBatch::default(),
            memtables: MemTableAccessor::new(stats),
        }
    }
}

impl ReplayMachine for MemTableRecoverContext {
    fn replay(&mut self, mut item_batch: LogItemBatch, file_id: FileId) -> Result<()> {
        for item in item_batch.iter() {
            match &item.content {
                LogItemContent::Command(Command::Clean)
                | LogItemContent::Command(Command::Compact { .. }) => {
                    self.log_batch.push((*item).clone());
                }
                LogItemContent::Kv(KeyValue { op_type, .. }) if *op_type == OpType::Del => {
                    self.log_batch.push((*item).clone());
                }
                _ => {}
            }
        }
        self.memtables.apply(item_batch.drain(), file_id.queue);
        Ok(())
    }

    fn merge(&mut self, mut rhs: Self, queue: LogQueue) -> Result<()> {
        self.log_batch.merge(&mut rhs.log_batch.clone());
        self.memtables.apply(rhs.log_batch.drain(), queue);
        self.memtables.merge_newer_neighbor(&mut rhs.memtables);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::generate_entry_indexes;

    impl MemTable {
        pub fn max_file_seq(&self, queue: LogQueue) -> Option<FileSeq> {
            let entry = match queue {
                LogQueue::Append if self.rewrite_count == self.entry_indexes.len() => None,
                LogQueue::Append => self.entry_indexes.back(),
                LogQueue::Rewrite if self.rewrite_count == 0 => None,
                LogQueue::Rewrite => self.entry_indexes.get(self.rewrite_count - 1),
            };
            let ents_max = entry.map(|e| e.entries.unwrap().id.seq);

            let kvs_max = self.kvs_max_file_seq(queue);
            match (ents_max, kvs_max) {
                (Some(ents_max), Some(kvs_max)) => Some(FileSeq::max(kvs_max, ents_max)),
                (Some(ents_max), None) => Some(ents_max),
                (None, Some(kvs_max)) => Some(kvs_max),
                (None, None) => None,
            }
        }

        pub fn kvs_max_file_seq(&self, queue: LogQueue) -> Option<FileSeq> {
            self.kvs
                .values()
                .filter(|v| v.1.queue == queue)
                .fold(None, |max, v| {
                    if let Some(max) = max {
                        Some(std::cmp::max(max, v.1.seq))
                    } else {
                        Some(v.1.seq)
                    }
                })
        }

        pub fn fetch_all(&self, vec_idx: &mut Vec<EntryIndex>) {
            if self.entry_indexes.is_empty() {
                return;
            }

            let begin = self.entry_indexes.front().unwrap().index;
            let end = self.entry_indexes.back().unwrap().index + 1;
            self.fetch_entries_to(begin, end, None, vec_idx).unwrap();
        }

        fn entries_size(&self) -> usize {
            self.entry_indexes
                .iter()
                .fold(0, |acc, e| acc + e.entry_len) as usize
        }

        fn check_entry_indexes(&self) {
            if self.entry_indexes.is_empty() {
                return;
            }

            let ei_first = self.entry_indexes.front().unwrap();
            let ei_last = self.entry_indexes.back().unwrap();
            assert_eq!(
                ei_last.index - ei_first.index + 1,
                self.entry_indexes.len() as u64
            );
        }
    }

    #[test]
    fn test_memtable_append() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::new(region_id, stats);

        // Append entries [10, 20) file_num = 1.
        // after appending
        // [10, 20) file_num = 1
        let ents_idx = generate_entry_indexes(
            10,
            20,
            FileId {
                queue: LogQueue::Append,
                seq: 1,
            },
        );
        memtable.append(ents_idx);
        assert_eq!(memtable.entries_size(), 10);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 1);
        memtable.check_entry_indexes();

        // Append entries [20, 30) file_num = 2.
        // after appending:
        // [10, 20) file_num = 1
        // [20, 30) file_num = 2
        let ents_idx = generate_entry_indexes(
            20,
            30,
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        memtable.append(ents_idx);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 2);
        memtable.check_entry_indexes();

        // Partial overlap Appending.
        // Append entries [25, 35) file_num = 3.
        // After appending:
        // [10, 20) file_num = 1
        // [20, 25) file_num = 2
        // [25, 35) file_num = 3
        let ents_idx = generate_entry_indexes(
            25,
            35,
            FileId {
                queue: LogQueue::Append,
                seq: 3,
            },
        );
        memtable.append(ents_idx);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        memtable.check_entry_indexes();

        // Full overlap Appending.
        // Append entries [10, 40) file_num = 4.
        // After appending:
        // [10, 40) file_num = 4
        let ents_idx = generate_entry_indexes(
            10,
            40,
            FileId {
                queue: LogQueue::Append,
                seq: 4,
            },
        );
        memtable.append(ents_idx);
        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        memtable.check_entry_indexes();
    }

    #[test]
    fn test_memtable_compact() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::new(region_id, stats);

        // After appending:
        // [0, 10) file_num = 1
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        let ents_idx = generate_entry_indexes(
            0,
            10,
            FileId {
                queue: LogQueue::Append,
                seq: 1,
            },
        );
        memtable.append(ents_idx);
        let ents_idx = generate_entry_indexes(
            10,
            15,
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        memtable.append(ents_idx);
        let ents_idx = generate_entry_indexes(
            15,
            20,
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        memtable.append(ents_idx);
        let ents_idx = generate_entry_indexes(
            20,
            25,
            FileId {
                queue: LogQueue::Append,
                seq: 3,
            },
        );
        memtable.append(ents_idx);

        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        memtable.check_entry_indexes();

        // Compact to 5.
        // Only index is needed to compact.
        assert_eq!(memtable.compact_to(5), 5);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        memtable.check_entry_indexes();

        // Compact to 20.
        assert_eq!(memtable.compact_to(20), 15);
        assert_eq!(memtable.entries_size(), 5);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 3);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        memtable.check_entry_indexes();

        // Compact to 20 or smaller index, nothing happens.
        assert_eq!(memtable.compact_to(20), 0);
        assert_eq!(memtable.compact_to(15), 0);
        assert_eq!(memtable.entries_size(), 5);
        memtable.check_entry_indexes();
    }

    #[test]
    fn test_memtable_fetch() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::new(region_id, stats);

        // After appending:
        // [0, 10) file_num = 1
        // [10, 15) file_num = 2
        // [15, 20) file_num = 2
        // [20, 25) file_num = 3
        let ents_idx = generate_entry_indexes(
            0,
            10,
            FileId {
                queue: LogQueue::Append,
                seq: 1,
            },
        );
        memtable.append(ents_idx);
        let ents_idx = generate_entry_indexes(
            10,
            20,
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        memtable.append(ents_idx);
        let ents_idx = generate_entry_indexes(
            20,
            25,
            FileId {
                queue: LogQueue::Append,
                seq: 3,
            },
        );
        memtable.append(ents_idx);

        // Fetching all
        let mut ents_idx = vec![];
        memtable.fetch_all(&mut ents_idx);
        assert_eq!(ents_idx.len(), 25);
        assert_eq!(ents_idx[0].index, 0);
        assert_eq!(ents_idx[24].index, 24);

        // After compact:
        // [10, 15) file_num = 2
        // [15, 20) file_num = 2
        // [20, 25) file_num = 3
        assert_eq!(memtable.compact_to(10), 10);

        // Out of range fetching.
        ents_idx.clear();
        assert!(memtable
            .fetch_entries_to(5, 15, None, &mut ents_idx)
            .is_err());

        // Out of range fetching.
        ents_idx.clear();
        assert!(memtable
            .fetch_entries_to(20, 30, None, &mut ents_idx)
            .is_err());

        ents_idx.clear();
        memtable
            .fetch_entries_to(20, 25, None, &mut ents_idx)
            .unwrap();
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 20);
        assert_eq!(ents_idx[4].index, 24);

        ents_idx.clear();
        memtable
            .fetch_entries_to(10, 15, None, &mut ents_idx)
            .unwrap();
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[4].index, 14);

        ents_idx.clear();
        memtable
            .fetch_entries_to(10, 25, None, &mut ents_idx)
            .unwrap();
        assert_eq!(ents_idx.len(), 15);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[14].index, 24);

        // Max size limitation range fetching.
        // Only can fetch [10, 20) because of size limitation,
        ents_idx.clear();
        let max_size = Some(10);
        memtable
            .fetch_entries_to(10, 25, max_size, &mut ents_idx)
            .unwrap();
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[9].index, 19);

        // Even max size limitation is 0, at least fetch one entry.
        ents_idx.clear();
        memtable
            .fetch_entries_to(20, 25, Some(0), &mut ents_idx)
            .unwrap();
        assert_eq!(ents_idx.len(), 1);
        assert_eq!(ents_idx[0].index, 20);
    }

    #[test]
    fn test_memtable_fetch_rewrite() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::new(region_id, stats);

        // After appending:
        // [0, 10) file_num = 1
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        let ents_idx = generate_entry_indexes(
            0,
            10,
            FileId {
                queue: LogQueue::Append,
                seq: 1,
            },
        );
        memtable.append(ents_idx);
        memtable.put(
            b"k1".to_vec(),
            b"v1".to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 1,
            },
        );
        let ents_idx = generate_entry_indexes(
            10,
            20,
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        memtable.append(ents_idx);
        memtable.put(
            b"k2".to_vec(),
            b"v2".to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        let ents_idx = generate_entry_indexes(
            20,
            25,
            FileId {
                queue: LogQueue::Append,
                seq: 3,
            },
        );
        memtable.append(ents_idx);
        memtable.put(
            b"k3".to_vec(),
            b"v3".to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 3,
            },
        );

        // After rewriting:
        // [0, 10) queue = rewrite, file_num = 50,
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        let ents_idx = generate_entry_indexes(
            0,
            10,
            FileId {
                queue: LogQueue::Rewrite,
                seq: 50,
            },
        );
        memtable.rewrite(ents_idx, Some(1));
        memtable.rewrite_key(b"k1".to_vec(), Some(1), 50);
        assert_eq!(memtable.entries_size(), 25);

        let mut ents_idx = vec![];
        assert!(memtable
            .fetch_entry_indexes_before(2, &mut ents_idx)
            .is_ok());
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx.last().unwrap().index, 19);

        ents_idx.clear();
        assert!(memtable
            .fetch_rewritten_entry_indexes(&mut ents_idx)
            .is_ok());
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx.first().unwrap().index, 0);
        assert_eq!(ents_idx.last().unwrap().index, 9);
    }

    #[test]
    fn test_memtable_kv_operations() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::new(region_id, stats);

        let (k1, v1) = (b"key1", b"value1");
        let (k5, v5) = (b"key5", b"value5");
        memtable.put(
            k1.to_vec(),
            v1.to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 1,
            },
        );
        memtable.put(
            k5.to_vec(),
            v5.to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 5,
            },
        );
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.get(k1.as_ref()), Some(v1.to_vec()));
        assert_eq!(memtable.get(k5.as_ref()), Some(v5.to_vec()));

        memtable.delete(k5.as_ref());
        assert_eq!(memtable.get(k5.as_ref()), None);
    }

    #[test]
    fn test_memtable_get_entry() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::new(region_id, stats);

        // [5, 10) file_num = 1
        // [10, 20) file_num = 2
        let ents_idx = generate_entry_indexes(
            5,
            10,
            FileId {
                queue: LogQueue::Append,
                seq: 1,
            },
        );
        memtable.append(ents_idx);
        let ents_idx = generate_entry_indexes(
            10,
            20,
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        memtable.append(ents_idx);

        // Not in range.
        assert_eq!(memtable.get_entry(2), None);
        assert_eq!(memtable.get_entry(25), None);

        let entry_idx = memtable.get_entry(5);
        assert_eq!(entry_idx.unwrap().index, 5);
    }

    #[test]
    fn test_memtable_rewrite() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::new(region_id, stats);

        // After appending and compacting:
        // [10, 20) file_num = 2
        // [20, 30) file_num = 3
        // [30, 40) file_num = 4
        let ents_idx = generate_entry_indexes(
            10,
            20,
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        memtable.append(ents_idx);
        memtable.put(
            b"kk1".to_vec(),
            b"vv1".to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 2,
            },
        );
        let ents_idx = generate_entry_indexes(
            20,
            30,
            FileId {
                queue: LogQueue::Append,
                seq: 3,
            },
        );
        memtable.append(ents_idx);
        memtable.put(
            b"kk2".to_vec(),
            b"vv2".to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 3,
            },
        );
        let ents_idx = generate_entry_indexes(
            30,
            40,
            FileId {
                queue: LogQueue::Append,
                seq: 4,
            },
        );
        memtable.append(ents_idx);
        memtable.put(
            b"kk3".to_vec(),
            b"vv3".to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 4,
            },
        );

        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 2);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        memtable.check_entry_indexes();

        // Rewrite compacted entries.
        let ents_idx = generate_entry_indexes(
            0,
            10,
            FileId {
                queue: LogQueue::Rewrite,
                seq: 50,
            },
        );
        memtable.rewrite(ents_idx, Some(1));
        memtable.rewrite_key(b"kk0".to_vec(), Some(1), 50);

        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 2);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        assert!(memtable.min_file_seq(LogQueue::Rewrite).is_none());
        assert!(memtable.max_file_seq(LogQueue::Rewrite).is_none());
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.get(b"kk0"), None);
        assert_eq!(memtable.global_stats.rewrite_operations(), 11);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 11);

        // Rewrite compacted entries + valid entries.
        let ents_idx = generate_entry_indexes(
            0,
            20,
            FileId {
                queue: LogQueue::Rewrite,
                seq: 100,
            },
        );
        memtable.rewrite(ents_idx, Some(2));
        assert_eq!(memtable.global_stats.rewrite_operations(), 31);
        memtable.rewrite_key(b"kk0".to_vec(), Some(1), 50);
        memtable.rewrite_key(b"kk1".to_vec(), Some(2), 100);

        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 3);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.rewrite_count, 10);
        assert_eq!(memtable.get(b"kk1"), Some(b"vv1".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 33);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 22);

        // Rewrite vaild entries.
        let ents_idx = generate_entry_indexes(
            20,
            30,
            FileId {
                queue: LogQueue::Rewrite,
                seq: 101,
            },
        );
        memtable.rewrite(ents_idx, Some(3));
        memtable.rewrite_key(b"kk2".to_vec(), Some(3), 101);

        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 101);
        assert_eq!(memtable.rewrite_count, 20);
        assert_eq!(memtable.get(b"kk2"), Some(b"vv2".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 44);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 22);

        // Put some entries overwritting entires in file 4.
        let ents_idx = generate_entry_indexes(
            35,
            36,
            FileId {
                queue: LogQueue::Append,
                seq: 5,
            },
        );
        memtable.append(ents_idx);
        memtable.put(
            b"kk3".to_vec(),
            b"vv33".to_vec(),
            FileId {
                queue: LogQueue::Append,
                seq: 5,
            },
        );
        assert_eq!(memtable.entry_indexes.back().unwrap().index, 35);

        // Rewrite valid + overwritten entries.
        let ents_idx = generate_entry_indexes(
            30,
            40,
            FileId {
                queue: LogQueue::Rewrite,
                seq: 102,
            },
        );
        memtable.rewrite(ents_idx, Some(4));
        memtable.rewrite_key(b"kk3".to_vec(), Some(4), 102);

        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 102);
        assert_eq!(memtable.rewrite_count, 25);
        assert_eq!(memtable.get(b"kk3"), Some(b"vv33".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 55);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 27);

        // Compact after rewrite.
        let ents_idx = generate_entry_indexes(
            35,
            50,
            FileId {
                queue: LogQueue::Append,
                seq: 6,
            },
        );
        memtable.append(ents_idx);
        memtable.compact_to(30);
        assert_eq!(memtable.entry_indexes.back().unwrap().index, 49);
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(memtable.global_stats.rewrite_operations(), 55);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 47);
        memtable.compact_to(40);
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 55);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 52);

        let ents_idx = generate_entry_indexes(
            30,
            36,
            FileId {
                queue: LogQueue::Rewrite,
                seq: 103,
            },
        );
        memtable.rewrite(ents_idx, Some(5));
        memtable.rewrite_key(b"kk3".to_vec(), Some(5), 103);

        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 6);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 6);
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 103);
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 62);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 58);

        // Rewrite after cut.
        let ents_idx = generate_entry_indexes(
            50,
            55,
            FileId {
                queue: LogQueue::Append,
                seq: 7,
            },
        );
        memtable.append(ents_idx);
        let ents_idx = generate_entry_indexes(
            30,
            50,
            FileId {
                queue: LogQueue::Rewrite,
                seq: 104,
            },
        );
        memtable.rewrite(ents_idx, Some(6));
        assert_eq!(memtable.rewrite_count, 10);
        assert_eq!(memtable.global_stats.rewrite_operations(), 82);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 68);

        let ents_idx = generate_entry_indexes(
            45,
            50,
            FileId {
                queue: LogQueue::Append,
                seq: 7,
            },
        );
        memtable.append(ents_idx);
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(memtable.global_stats.rewrite_operations(), 82);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 73);

        let ents_idx = generate_entry_indexes(
            40,
            50,
            FileId {
                queue: LogQueue::Append,
                seq: 7,
            },
        );
        memtable.append(ents_idx);
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 82);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 78);
    }

    #[test]
    fn test_memtable_recover() {
        let file_id = FileId::new(LogQueue::Append, 10);

        // ops:
        //
        // case 1:
        //      region 10: put (key1, v1) => del (key1) => put (key1, v2)
        // expect 1:
        //      get (key1) == v2
        //
        // case 2:
        //      region 11: put (k, _) => cleanup
        // expect 2:
        //      memtable 11 is none
        //
        // case 3:
        //      region 12: entries [1, 10) => compact 5 => entries [11, 20)
        // expect 3:
        //      [1, 5) is compacted, [5, 20) remains
        //

        let mut batches = vec![
            LogItemBatch::with_capacity(0),
            LogItemBatch::with_capacity(0),
            LogItemBatch::with_capacity(0),
        ];
        batches[0].put(10, b"key1".to_vec(), b"val1".to_vec());
        batches[1].delete(10, b"key1".to_vec());
        batches[2].put(10, b"key1".to_vec(), b"val2".to_vec());
        batches[0].put(11, b"key".to_vec(), b"ANYTHING".to_vec());
        batches[1].add_command(11, Command::Clean);
        batches[0].add_entry_indexes(12, generate_entry_indexes(1, 11, file_id));
        batches[1].add_command(12, Command::Compact { index: 5 });
        batches[2].add_entry_indexes(12, generate_entry_indexes(11, 21, file_id));
        for b in batches.iter_mut() {
            b.finish_write(FileBlockHandle::dummy(LogQueue::Append));
        }

        // reverse merge
        let mut ctxs = VecDeque::default();
        for batch in batches.clone() {
            let mut ctx = MemTableRecoverContext::default();
            ctx.replay(batch, file_id).unwrap();
            ctxs.push_back(ctx);
        }
        while ctxs.len() > 1 {
            let (y, mut x) = (ctxs.pop_back().unwrap(), ctxs.pop_back().unwrap());
            x.merge(y, LogQueue::Append).unwrap();
            ctxs.push_back(x);
        }
        let (merged_memtables, merged_global_stats) = ctxs.pop_front().unwrap().finish();
        // sequential apply
        let global_stats = Arc::new(GlobalStats::default());
        let memtables = MemTableAccessor::new(global_stats.clone());
        for mut batch in batches.clone() {
            memtables.apply(batch.drain(), LogQueue::Append);
        }

        // asserts
        assert_eq!(
            merged_memtables
                .get(10)
                .unwrap()
                .read()
                .get(b"key1")
                .unwrap(),
            b"val2".to_vec(),
        );
        assert!(merged_memtables.get(11).is_none());
        for i in 1..21 {
            let opt = merged_memtables.get(12).unwrap().read().get_entry(i);
            if i < 5 {
                assert!(opt.is_none());
            } else {
                assert!(opt.is_some());
            }
        }
        assert_eq!(
            global_stats.rewrite_operations(),
            merged_global_stats.rewrite_operations()
        );
        assert_eq!(
            global_stats.compacted_rewrite_operations(),
            merged_global_stats.compacted_rewrite_operations()
        );
    }
}
