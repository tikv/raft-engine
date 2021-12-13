// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::borrow::BorrowMut;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::path::Path;
use std::sync::Arc;

use fail::fail_point;
use hashbrown::HashMap;
use parking_lot::{Mutex, RwLock};

use crate::file_pipe_log::ReplayMachine;
use crate::log_batch::{
    Command, CompressionType, KeyValue, LogBatch, LogItemBatch, LogItemContent, LogItemDrain,
    OpType,
};
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue};
use crate::util::slices_in_range;
use crate::{Error, FileBuilder, GlobalStats, Result};

const SHRINK_CACHE_CAPACITY: usize = 64;
const SHRINK_CACHE_LIMIT: usize = 512;

const MEMTABLE_SLOT_COUNT: usize = 128;

#[derive(Debug, Copy, Clone, PartialEq)]
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

    /// Mrege from newer neighbor `rhs`.
    /// Only called during parllel recovery.
    pub fn merge_newer_neighbor(&mut self, rhs: &mut Self) {
        debug_assert_eq!(self.region_id, rhs.region_id);
        if let Some((rhs_first, _)) = rhs.span() {
            self.prepare_append(
                rhs_first,
                rhs.rewrite_count > 0, /*allow_hole*/
                true,                  /*allow_overwrite*/
            );
            self.global_stats.add(
                rhs.entry_indexes[0].entries.unwrap().id.queue,
                rhs.entry_indexes.len(),
            );
            self.rewrite_count += rhs.rewrite_count;
            self.entry_indexes.append(&mut rhs.entry_indexes);
        }

        for (key, (value, file_id)) in rhs.kvs.drain() {
            self.put(key, value, file_id);
        }

        let deleted = rhs.global_stats.deleted_rewrite_entries();
        self.global_stats.add(LogQueue::Rewrite, deleted);
        self.global_stats.delete(LogQueue::Rewrite, deleted);
    }

    /// Merge a counter part that contains all append data of current region.
    pub fn merge_append_table(&mut self, rhs: &mut Self) {
        debug_assert_eq!(self.rewrite_count, self.entry_indexes.len());
        debug_assert_eq!(rhs.rewrite_count, 0);

        if let Some((first, _)) = rhs.span() {
            self.prepare_append(first, true, true);
            self.global_stats.add(
                rhs.entry_indexes[0].entries.unwrap().id.queue,
                rhs.entry_indexes.len(),
            );
            self.entry_indexes.append(&mut rhs.entry_indexes);
        }

        for (key, (value, file_id)) in rhs.kvs.drain() {
            self.put(key, value, file_id);
        }

        let deleted = rhs.global_stats.deleted_rewrite_entries();
        self.global_stats.add(LogQueue::Rewrite, deleted);
        self.global_stats.delete(LogQueue::Rewrite, deleted);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.kvs.get(key).map(|v| v.0.clone())
    }

    pub fn delete(&mut self, key: &[u8]) {
        if let Some(value) = self.kvs.remove(key) {
            self.global_stats.delete(value.1.queue, 1);
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, file_id: FileId) {
        if let Some(origin) = self.kvs.insert(key, (value, file_id)) {
            self.global_stats.delete(origin.1.queue, 1);
        }
        self.global_stats.add(file_id.queue, 1);
    }

    pub fn rewrite_key(&mut self, key: Vec<u8>, gate: Option<FileSeq>, seq: FileSeq) {
        self.global_stats.add(LogQueue::Rewrite, 1);
        if let Some(origin) = self.kvs.get_mut(&key) {
            if origin.1.queue == LogQueue::Append {
                if let Some(gate) = gate {
                    if origin.1.seq <= gate {
                        origin.1 = FileId {
                            queue: LogQueue::Rewrite,
                            seq,
                        };
                        self.global_stats.delete(LogQueue::Append, 1);
                        return;
                    }
                }
            } else {
                assert!(origin.1.seq <= seq);
                origin.1.seq = seq;
            }
        }
        self.global_stats.delete(LogQueue::Rewrite, 1);
    }

    pub fn get_entry(&self, index: u64) -> Option<EntryIndex> {
        if let Some((first, last)) = self.span() {
            if index < first || index > last {
                return None;
            }

            let ioffset = (index - first) as usize;
            let entry_index = self.entry_indexes[ioffset];
            Some(entry_index)
        } else {
            None
        }
    }

    pub fn append(&mut self, entry_indexes: Vec<EntryIndex>) {
        let len = entry_indexes.len();
        if len > 0 {
            self.prepare_append(entry_indexes[0].index, false, false);
            self.global_stats.add(LogQueue::Append, len);
            // TODO: Optimize this.
            self.entry_indexes.extend(entry_indexes);
        }
    }

    // This will only be called during recovery.
    pub fn append_rewrite(&mut self, entry_indexes: Vec<EntryIndex>) {
        let len = entry_indexes.len();
        if len > 0 {
            debug_assert_eq!(self.rewrite_count, self.entry_indexes.len());
            self.prepare_append(entry_indexes[0].index, true, true);
            self.global_stats.add(LogQueue::Rewrite, len);
            self.entry_indexes.extend(entry_indexes);
            self.rewrite_count = self.entry_indexes.len();
        }
    }

    pub fn rewrite(&mut self, rewrite_indexes: Vec<EntryIndex>, gate: Option<FileSeq>) {
        if rewrite_indexes.is_empty() {
            return;
        }
        self.global_stats
            .add(LogQueue::Rewrite, rewrite_indexes.len());

        let len = self.entry_indexes.len();
        if len == 0 {
            self.global_stats
                .delete(LogQueue::Rewrite, rewrite_indexes.len());
            return;
        }

        let first = self.entry_indexes[0].index;
        let last = self.entry_indexes[len - 1].index;
        let rewrite_first = std::cmp::max(rewrite_indexes[0].index, first);
        let rewrite_last = std::cmp::min(rewrite_indexes[rewrite_indexes.len() - 1].index, last);
        let mut rewrite_len = (rewrite_last + 1).saturating_sub(rewrite_first) as usize;
        if rewrite_len == 0 {
            self.global_stats
                .delete(LogQueue::Rewrite, rewrite_indexes.len());
            return;
        }

        let pos = (rewrite_first - first) as usize;
        // No normal log entry mixed in rewritten entries at the front.
        assert!(
            pos == 0 || self.entry_indexes[pos - 1].entries.unwrap().id.queue == LogQueue::Rewrite
        );
        let rewrite_pos = (rewrite_first - rewrite_indexes[0].index) as usize;

        for (i, rindex) in rewrite_indexes[rewrite_pos..rewrite_pos + rewrite_len]
            .iter()
            .enumerate()
        {
            let index = &mut self.entry_indexes[i + pos];
            if let Some(gate) = gate {
                debug_assert_eq!(index.entries.unwrap().id.queue, LogQueue::Append);
                if index.entries.unwrap().id.seq > gate {
                    // Some entries are overwritten by new appends.
                    rewrite_len = i;
                    break;
                }
            } else if index.entries.unwrap().id.queue == LogQueue::Append {
                // Squeeze operation encounters a new append.
                rewrite_len = i;
                break;
            }

            *index = *rindex;
        }

        if gate.is_none() {
            self.global_stats
                .delete(LogQueue::Rewrite, rewrite_indexes.len());
        } else {
            self.global_stats.delete(LogQueue::Append, rewrite_len);
            self.global_stats
                .delete(LogQueue::Rewrite, rewrite_indexes.len() - rewrite_len);
        }

        self.rewrite_count = pos + rewrite_len;
    }

    // Removes all entry indexes with index smaller than to `index`.
    // Returns the number of deleted entries.
    pub fn compact_to(&mut self, index: u64) -> u64 {
        if self.entry_indexes.is_empty() {
            return 0;
        }
        let first = self.entry_indexes[0].index;
        if index <= first {
            return 0;
        }
        let count = std::cmp::min((index - first) as usize, self.entry_indexes.len());
        self.entry_indexes.drain(..count);
        self.maybe_shrink_entry_indexes();

        let compacted_rewrite = std::cmp::min(count, self.rewrite_count);
        self.rewrite_count -= compacted_rewrite;
        self.global_stats
            .delete(LogQueue::Rewrite, compacted_rewrite);
        self.global_stats
            .delete(LogQueue::Append, count - compacted_rewrite);
        count as u64
    }

    // Removes all entry indexes with index greater than or equal to `index`.
    // Returns the truncated amount.
    // Assumes index <= last.
    fn unsafe_truncate_back(&mut self, first: u64, index: u64, last: u64) -> usize {
        debug_assert!(index <= last);
        let len = self.entry_indexes.len();
        debug_assert_eq!(len as u64, last - first + 1);
        self.entry_indexes
            .truncate(index.saturating_sub(first) as usize);
        let new_len = self.entry_indexes.len();
        let truncated = len - new_len;

        if self.rewrite_count > new_len {
            let truncated_rewrite = self.rewrite_count - new_len;
            self.rewrite_count = new_len;
            self.global_stats
                .delete(LogQueue::Rewrite, truncated_rewrite);
            self.global_stats
                .delete(LogQueue::Append, truncated - truncated_rewrite);
        } else {
            self.global_stats.delete(LogQueue::Append, truncated);
        }
        truncated
    }

    #[inline]
    fn prepare_append(
        &mut self,
        first_index_to_add: u64,
        allow_hole: bool,
        allow_overwrite_compacted: bool,
    ) {
        if let Some((first, last)) = self.span() {
            if first_index_to_add < first {
                if allow_overwrite_compacted {
                    self.unsafe_truncate_back(first, 0, last);
                } else {
                    panic!(
                        "attempt to overwrite compacted entries in {}",
                        self.region_id
                    );
                }
            } else if last + 1 < first_index_to_add {
                if allow_hole {
                    self.unsafe_truncate_back(first, 0, last);
                } else {
                    panic!("memtable {} has a hole", self.region_id);
                }
            } else if first_index_to_add != last + 1 {
                self.unsafe_truncate_back(first, first_index_to_add, last);
            }
        }
    }

    fn maybe_shrink_entry_indexes(&mut self) {
        if self.entry_indexes.capacity() > SHRINK_CACHE_LIMIT
            && self.entry_indexes.len() <= SHRINK_CACHE_CAPACITY
        {
            self.entry_indexes.shrink_to(SHRINK_CACHE_CAPACITY);
        }
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
        let len = self.entry_indexes.len();
        if len == 0 {
            return Err(Error::EntryNotFound);
        }
        let first = self.entry_indexes[0].index;
        if begin < first {
            return Err(Error::EntryCompacted);
        }
        let last = self.entry_indexes[len - 1].index;
        if end > last + 1 {
            return Err(Error::EntryNotFound);
        }

        let start_pos = (begin - first) as usize;
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
                vec_idx.push(*idx);
            }
        } else {
            vec_idx.extend_from_slice(first);
            vec_idx.extend_from_slice(second);
        }
        Ok(())
    }

    // Inclusive.
    pub fn fetch_entry_indexes_before(
        &self,
        gate: FileSeq,
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
            .find(|e| e.entries.unwrap().id.seq <= gate);
        if let (Some(begin), Some(end)) = (begin, end) {
            if begin.index <= end.index {
                return self.fetch_entries_to(begin.index, end.index + 1, None, vec_idx);
            }
        }
        Ok(())
    }

    pub fn fetch_rewritten_entry_indexes(&self, vec_idx: &mut Vec<EntryIndex>) -> Result<()> {
        if self.rewrite_count > 0 {
            let first = self.entry_indexes[0].index;
            let end = self.entry_indexes[self.rewrite_count - 1].index + 1;
            self.fetch_entries_to(first, end, None, vec_idx)
        } else {
            Ok(())
        }
    }

    // Inclusive.
    pub fn fetch_kvs_before(&self, gate: FileSeq, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, (value, file_id)) in &self.kvs {
            if file_id.queue == LogQueue::Append && file_id.seq <= gate {
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

    pub fn entries_count_before(&self, mut gate: FileId) -> usize {
        gate.seq += 1;
        let idx = self
            .entry_indexes
            .binary_search_by_key(&gate, |ei| ei.entries.unwrap().id);
        match idx {
            Ok(idx) => idx,
            Err(idx) => idx,
        }
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

    #[inline]
    fn span(&self) -> Option<(u64, u64)> {
        let len = self.entry_indexes.len();
        if len > 0 {
            Some((
                self.entry_indexes[0].index,
                self.entry_indexes[len - 1].index,
            ))
        } else {
            None
        }
    }

    #[cfg(test)]
    fn consistency_check(&self) {
        let mut seen_append = false;
        let mut last_index = None;
        for idx in self.entry_indexes.iter() {
            // Check 1: indexes are contiguous.
            if let Some(last_index) = last_index {
                assert_eq!(idx.index, last_index + 1);
            }
            last_index = Some(idx.index);
            // Check 2: rewrites are at the front.
            let queue = idx.entries.unwrap().id.queue;
            if queue == LogQueue::Append {
                seen_append = true;
            }
            assert_eq!(
                queue,
                if seen_append {
                    LogQueue::Append
                } else {
                    LogQueue::Rewrite
                }
            );
        }
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
    #[allow(dead_code)]
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

    pub fn merge_append_table(&self, rhs: &mut Self) {
        for slot in rhs.slots.iter_mut() {
            for (id, memtable) in std::mem::take(&mut *slot.write()) {
                if let Some(existing_memtable) = self.get(id) {
                    existing_memtable
                        .write()
                        .merge_append_table(&mut *memtable.write());
                } else {
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
                    if queue == LogQueue::Rewrite {
                        memtable.write().append_rewrite(entry_indexes);
                    } else {
                        memtable.write().append(entry_indexes);
                    }
                }
                LogItemContent::Command(Command::Clean) => {
                    self.remove(raft, queue);
                }
                LogItemContent::Command(Command::Compact { index }) => {
                    memtable.write().compact_to(index);
                }
                LogItemContent::Kv(kv) => match kv.op_type {
                    OpType::Put => {
                        let value = kv.value.unwrap();
                        memtable.write().put(kv.key, value, kv.file_id.unwrap());
                    }
                    OpType::Del => {
                        let key = kv.key;
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

    pub fn merge_append_context(&self, mut append: MemTableRecoverContext) {
        self.memtables
            .apply(append.log_batch.clone().drain(), LogQueue::Append);
        self.memtables.merge_append_table(&mut append.memtables);
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

    fn end<B: FileBuilder>(&mut self, _path: &Path, _builder: Arc<B>) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_util::{catch_unwind_silent, generate_entry_indexes};

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
    }

    #[test]
    fn test_memtable_append() {
        let region_id = 8;
        let mut memtable = MemTable::new(region_id, Arc::new(GlobalStats::default()));

        // Append entries [10, 20) file_num = 1.
        // after appending
        // [10, 20) file_num = 1
        memtable.append(generate_entry_indexes(
            10,
            20,
            FileId::new(LogQueue::Append, 1),
        ));
        assert_eq!(memtable.entries_size(), 10);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 1);
        memtable.consistency_check();

        // Empty.
        memtable.append(Vec::new());

        // Hole.
        assert!(
            catch_unwind_silent(|| memtable.append(generate_entry_indexes(
                21,
                22,
                FileId::dummy(LogQueue::Append)
            )))
            .is_err()
        );
        memtable.consistency_check();

        // Append entries [20, 30) file_num = 2.
        // after appending:
        // [10, 20) file_num = 1
        // [20, 30) file_num = 2
        memtable.append(generate_entry_indexes(
            20,
            30,
            FileId::new(LogQueue::Append, 2),
        ));
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 2);
        memtable.consistency_check();

        // Partial overlap Appending.
        // Append entries [25, 35) file_num = 3.
        // After appending:
        // [10, 20) file_num = 1
        // [20, 25) file_num = 2
        // [25, 35) file_num = 3
        memtable.append(generate_entry_indexes(
            25,
            35,
            FileId::new(LogQueue::Append, 3),
        ));
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        memtable.consistency_check();

        // Full overlap Appending.
        // Append entries [10, 40) file_num = 4.
        // After appending:
        // [10, 40) file_num = 4
        memtable.append(generate_entry_indexes(
            10,
            40,
            FileId::new(LogQueue::Append, 4),
        ));
        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        memtable.consistency_check();
    }

    #[test]
    fn test_memtable_compact() {
        let region_id = 8;
        let mut memtable = MemTable::new(region_id, Arc::new(GlobalStats::default()));

        // After appending:
        // [0, 10) file_num = 1
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        memtable.append(generate_entry_indexes(
            0,
            10,
            FileId::new(LogQueue::Append, 1),
        ));
        memtable.append(generate_entry_indexes(
            10,
            15,
            FileId::new(LogQueue::Append, 2),
        ));
        memtable.append(generate_entry_indexes(
            15,
            20,
            FileId::new(LogQueue::Append, 2),
        ));
        memtable.append(generate_entry_indexes(
            20,
            25,
            FileId::new(LogQueue::Append, 3),
        ));

        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.first_index().unwrap(), 0);
        assert_eq!(memtable.last_index().unwrap(), 24);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        memtable.consistency_check();

        // Compact to 5.
        // Only index is needed to compact.
        assert_eq!(memtable.compact_to(5), 5);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.first_index().unwrap(), 5);
        assert_eq!(memtable.last_index().unwrap(), 24);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        // Can't override compacted entries.
        assert!(
            catch_unwind_silent(|| memtable.append(generate_entry_indexes(
                4,
                5,
                FileId::dummy(LogQueue::Append)
            )))
            .is_err()
        );
        memtable.consistency_check();

        // Compact to 20.
        assert_eq!(memtable.compact_to(20), 15);
        assert_eq!(memtable.entries_size(), 5);
        assert_eq!(memtable.first_index().unwrap(), 20);
        assert_eq!(memtable.last_index().unwrap(), 24);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 3);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 3);
        memtable.consistency_check();

        // Compact to 20 or smaller index, nothing happens.
        assert_eq!(memtable.compact_to(20), 0);
        assert_eq!(memtable.compact_to(15), 0);
        assert_eq!(memtable.entries_size(), 5);
        assert_eq!(memtable.first_index().unwrap(), 20);
        assert_eq!(memtable.last_index().unwrap(), 24);
        memtable.consistency_check();
    }

    #[test]
    fn test_memtable_fetch() {
        let region_id = 8;
        let mut memtable = MemTable::new(region_id, Arc::new(GlobalStats::default()));

        let mut ents_idx = vec![];

        // Fetch empty.
        memtable.fetch_all(&mut ents_idx);
        assert!(ents_idx.is_empty());
        memtable
            .fetch_entries_to(0, 0, None, &mut ents_idx)
            .unwrap();
        assert!(matches!(
            memtable
                .fetch_entries_to(0, 1, None, &mut ents_idx)
                .unwrap_err(),
            Error::EntryNotFound
        ));

        // After appending:
        // [0, 10) file_num = 1
        // [10, 15) file_num = 2
        // [15, 20) file_num = 2
        // [20, 25) file_num = 3
        memtable.append(generate_entry_indexes(
            0,
            10,
            FileId::new(LogQueue::Append, 1),
        ));
        memtable.append(generate_entry_indexes(
            10,
            20,
            FileId::new(LogQueue::Append, 2),
        ));
        memtable.append(generate_entry_indexes(
            20,
            25,
            FileId::new(LogQueue::Append, 3),
        ));

        // Fetching all
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
        assert!(matches!(
            memtable
                .fetch_entries_to(5, 15, None, &mut ents_idx)
                .unwrap_err(),
            Error::EntryCompacted
        ));

        // Out of range fetching.
        ents_idx.clear();
        assert!(matches!(
            memtable
                .fetch_entries_to(20, 30, None, &mut ents_idx)
                .unwrap_err(),
            Error::EntryNotFound
        ));

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
        let mut memtable = MemTable::new(region_id, Arc::new(GlobalStats::default()));
        let (k1, v1) = (b"key1", b"value1");
        let (k2, v2) = (b"key2", b"value2");
        let (k3, v3) = (b"key3", b"value3");

        // After appending:
        // [0, 10) file_num = 1
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        memtable.append(generate_entry_indexes(
            0,
            10,
            FileId::new(LogQueue::Append, 1),
        ));
        memtable.put(k1.to_vec(), v1.to_vec(), FileId::new(LogQueue::Append, 1));
        memtable.append(generate_entry_indexes(
            10,
            20,
            FileId::new(LogQueue::Append, 2),
        ));
        memtable.put(k2.to_vec(), v2.to_vec(), FileId::new(LogQueue::Append, 2));
        memtable.append(generate_entry_indexes(
            20,
            25,
            FileId::new(LogQueue::Append, 3),
        ));
        memtable.put(k3.to_vec(), v3.to_vec(), FileId::new(LogQueue::Append, 3));
        memtable.consistency_check();

        // Rewrite k1.
        memtable.rewrite_key(k1.to_vec(), Some(1), 50);
        let mut kvs = Vec::new();
        memtable.fetch_kvs_before(1, &mut kvs);
        assert!(kvs.is_empty());
        memtable.fetch_rewritten_kvs(&mut kvs);
        assert_eq!(kvs.len(), 1);
        assert_eq!(kvs.pop().unwrap(), (k1.to_vec(), v1.to_vec()));
        // Rewrite deleted k1.
        memtable.delete(k1.as_ref());
        assert_eq!(memtable.global_stats.deleted_rewrite_entries(), 1);
        memtable.rewrite_key(k1.to_vec(), Some(1), 50);
        assert_eq!(memtable.get(k1.as_ref()), None);
        memtable.fetch_rewritten_kvs(&mut kvs);
        assert!(kvs.is_empty());
        assert_eq!(memtable.global_stats.deleted_rewrite_entries(), 2);
        // Rewrite newer append k2/k3.
        memtable.rewrite_key(k2.to_vec(), Some(1), 50);
        memtable.fetch_rewritten_kvs(&mut kvs);
        assert!(kvs.is_empty());
        memtable.rewrite_key(k3.to_vec(), None, 50); // Rewrite encounters newer append.
        memtable.fetch_rewritten_kvs(&mut kvs);
        assert!(kvs.is_empty());
        assert_eq!(memtable.global_stats.deleted_rewrite_entries(), 4);
        // Rewrite k3 multiple times.
        memtable.rewrite_key(k3.to_vec(), Some(10), 50);
        memtable.rewrite_key(k3.to_vec(), None, 51);
        memtable.rewrite_key(k3.to_vec(), Some(11), 52);
        memtable.fetch_rewritten_kvs(&mut kvs);
        assert_eq!(kvs.len(), 1);
        assert_eq!(kvs.pop().unwrap(), (k3.to_vec(), v3.to_vec()));

        // Rewrite indexes:
        // [0, 10) queue = rewrite, file_num = 1,
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        let ents_idx = generate_entry_indexes(0, 10, FileId::new(LogQueue::Rewrite, 1));
        memtable.rewrite(ents_idx, Some(1));
        assert_eq!(memtable.entries_size(), 25);
        memtable.consistency_check();

        let mut ents_idx = vec![];
        assert!(memtable
            .fetch_entry_indexes_before(2, &mut ents_idx)
            .is_ok());
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx.last().unwrap().index, 19);
        ents_idx.clear();
        assert!(memtable
            .fetch_entry_indexes_before(1, &mut ents_idx)
            .is_ok());
        assert!(ents_idx.is_empty());

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
        let mut memtable = MemTable::new(region_id, Arc::new(GlobalStats::default()));

        let (k1, v1) = (b"key1", b"value1");
        let (k5, v5) = (b"key5", b"value5");
        memtable.put(k1.to_vec(), v1.to_vec(), FileId::new(LogQueue::Append, 1));
        memtable.put(k5.to_vec(), v5.to_vec(), FileId::new(LogQueue::Append, 5));
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.get(k1.as_ref()), Some(v1.to_vec()));
        assert_eq!(memtable.get(k5.as_ref()), Some(v5.to_vec()));

        memtable.delete(k5.as_ref());
        assert_eq!(memtable.get(k5.as_ref()), None);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 1);

        memtable.put(k1.to_vec(), v1.to_vec(), FileId::new(LogQueue::Rewrite, 2));
        memtable.put(k5.to_vec(), v5.to_vec(), FileId::new(LogQueue::Rewrite, 3));
        assert_eq!(memtable.min_file_seq(LogQueue::Append), None);
        assert_eq!(memtable.max_file_seq(LogQueue::Append), None);
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 2);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 3);
        assert_eq!(memtable.global_stats.rewrite_entries(), 2);

        memtable.delete(k1.as_ref());
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 3);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 3);
        assert_eq!(memtable.global_stats.deleted_rewrite_entries(), 1);

        memtable.put(k5.to_vec(), v5.to_vec(), FileId::new(LogQueue::Append, 7));
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite), None);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite), None);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 7);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 7);
        assert_eq!(memtable.global_stats.deleted_rewrite_entries(), 2);
    }

    #[test]
    fn test_memtable_get_entry() {
        let region_id = 8;
        let mut memtable = MemTable::new(region_id, Arc::new(GlobalStats::default()));

        assert_eq!(memtable.get_entry(0), None);

        // [5, 10) file_num = 1
        // [10, 20) file_num = 2
        memtable.append(generate_entry_indexes(
            5,
            10,
            FileId::new(LogQueue::Append, 1),
        ));
        memtable.append(generate_entry_indexes(
            10,
            20,
            FileId::new(LogQueue::Append, 2),
        ));

        // Not in range.
        assert_eq!(memtable.get_entry(2), None);
        assert_eq!(memtable.get_entry(25), None);

        let entry_idx = memtable.get_entry(5);
        assert_eq!(entry_idx.unwrap().index, 5);
    }

    #[test]
    fn test_memtable_rewrite() {
        let region_id = 8;
        let mut memtable = MemTable::new(region_id, Arc::new(GlobalStats::default()));
        let mut expected_append = 0;
        let mut expected_rewrite = 0;
        let mut expected_deleted_rewrite = 0;

        // Rewrite to empty table.
        let ents_idx = generate_entry_indexes(0, 10, FileId::new(LogQueue::Rewrite, 1));
        memtable.rewrite(ents_idx, Some(1));
        expected_rewrite += 10;
        expected_deleted_rewrite += 10;
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite), None);
        assert_eq!(
            memtable.global_stats.live_entries(LogQueue::Append),
            expected_append
        );
        assert_eq!(memtable.global_stats.rewrite_entries(), expected_rewrite);
        assert_eq!(
            memtable.global_stats.deleted_rewrite_entries(),
            expected_deleted_rewrite
        );

        // Append and compact:
        // [10, 20) file_num = 2
        // [20, 30) file_num = 3
        // [30, 40) file_num = 4
        // kk1 -> 2, kk2 -> 3, kk3 -> 4
        memtable.append(generate_entry_indexes(
            0,
            10,
            FileId::new(LogQueue::Append, 1),
        ));
        memtable.append(generate_entry_indexes(
            10,
            20,
            FileId::new(LogQueue::Append, 2),
        ));
        memtable.put(
            b"kk1".to_vec(),
            b"vv1".to_vec(),
            FileId::new(LogQueue::Append, 2),
        );
        memtable.append(generate_entry_indexes(
            20,
            30,
            FileId::new(LogQueue::Append, 3),
        ));
        memtable.put(
            b"kk2".to_vec(),
            b"vv2".to_vec(),
            FileId::new(LogQueue::Append, 3),
        );
        memtable.append(generate_entry_indexes(
            30,
            40,
            FileId::new(LogQueue::Append, 4),
        ));
        memtable.put(
            b"kk3".to_vec(),
            b"vv3".to_vec(),
            FileId::new(LogQueue::Append, 4),
        );
        expected_append += 4 * 10 + 3;
        memtable.compact_to(10);
        expected_append -= 10;
        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 2);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(
            memtable.global_stats.live_entries(LogQueue::Append),
            expected_append
        );
        memtable.consistency_check();

        // Rewrite compacted entries.
        // [10, 20) file_num = 2
        // [20, 30) file_num = 3
        // [30, 40) file_num = 4
        // kk1 -> 2, kk2 -> 3, kk3 -> 4
        let ents_idx = generate_entry_indexes(0, 10, FileId::new(LogQueue::Rewrite, 50));
        memtable.rewrite(ents_idx, Some(1));
        memtable.rewrite_key(b"kk0".to_vec(), Some(1), 50);
        expected_rewrite += 10 + 1;
        expected_deleted_rewrite += 10 + 1;
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 2);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        assert!(memtable.min_file_seq(LogQueue::Rewrite).is_none());
        assert!(memtable.max_file_seq(LogQueue::Rewrite).is_none());
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.get(b"kk0"), None);
        assert_eq!(
            memtable.global_stats.live_entries(LogQueue::Append),
            expected_append
        );
        assert_eq!(memtable.global_stats.rewrite_entries(), expected_rewrite);
        assert_eq!(
            memtable.global_stats.deleted_rewrite_entries(),
            expected_deleted_rewrite
        );
        memtable.consistency_check();

        // Mixed rewrite.
        // [10, 20) file_num = 100(r)
        // [20, 30) file_num = 101(r)
        // [30, 40) file_num = 4
        // kk1 -> 100(r), kk2 -> 101(r), kk3 -> 4
        let ents_idx = generate_entry_indexes(0, 20, FileId::new(LogQueue::Rewrite, 100));
        memtable.rewrite(ents_idx, Some(2));
        memtable.rewrite_key(b"kk0".to_vec(), Some(1), 50);
        memtable.rewrite_key(b"kk1".to_vec(), Some(2), 100);
        expected_append -= 10 + 1;
        expected_rewrite += 20 + 2;
        expected_deleted_rewrite += 10 + 1;
        let ents_idx = generate_entry_indexes(20, 30, FileId::new(LogQueue::Rewrite, 101));
        memtable.rewrite(ents_idx, Some(3));
        memtable.rewrite_key(b"kk2".to_vec(), Some(3), 101);
        expected_append -= 10 + 1;
        expected_rewrite += 10 + 1;
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 101);
        assert_eq!(memtable.rewrite_count, 20);
        assert_eq!(memtable.get(b"kk1"), Some(b"vv1".to_vec()));
        assert_eq!(
            memtable.global_stats.live_entries(LogQueue::Append),
            expected_append
        );
        assert_eq!(memtable.global_stats.rewrite_entries(), expected_rewrite);
        assert_eq!(
            memtable.global_stats.deleted_rewrite_entries(),
            expected_deleted_rewrite
        );
        memtable.consistency_check();

        // Put some entries overwritting entires in file 4. Then try to rewrite.
        // [10, 20) file_num = 100(r)
        // [20, 30) file_num = 101(r)
        // [30, 35) file_num = 4 -> 102(r)
        // 35 file_num = 5
        // kk1 -> 100(r), kk2 -> 101(r), kk3 -> 5
        memtable.append(generate_entry_indexes(
            35,
            36,
            FileId::new(LogQueue::Append, 5),
        ));
        expected_append -= 4;
        memtable.put(
            b"kk3".to_vec(),
            b"vv33".to_vec(),
            FileId::new(LogQueue::Append, 5),
        );
        assert_eq!(memtable.last_index().unwrap(), 35);
        memtable.consistency_check();
        let ents_idx = generate_entry_indexes(30, 40, FileId::new(LogQueue::Rewrite, 102));
        memtable.rewrite(ents_idx, Some(4));
        expected_append -= 5;
        expected_rewrite += 10;
        expected_deleted_rewrite += 5;
        assert_eq!(memtable.min_file_seq(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.max_file_seq(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.min_file_seq(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_seq(LogQueue::Rewrite).unwrap(), 102);
        assert_eq!(memtable.rewrite_count, 25);
        assert_eq!(memtable.get(b"kk3"), Some(b"vv33".to_vec()));
        assert_eq!(
            memtable.global_stats.live_entries(LogQueue::Append),
            expected_append
        );
        assert_eq!(memtable.global_stats.rewrite_entries(), expected_rewrite);
        assert_eq!(
            memtable.global_stats.deleted_rewrite_entries(),
            expected_deleted_rewrite
        );
        memtable.consistency_check();

        // Compact after rewrite.
        // [30, 35) file_num = 102(r)
        // [35, 50) file_num = 6
        // kk1 -> 100(r), kk2 -> 101(r), kk3 -> 5
        memtable.append(generate_entry_indexes(
            35,
            50,
            FileId::new(LogQueue::Append, 6),
        ));
        expected_append += 15 - 1;
        memtable.compact_to(30);
        expected_deleted_rewrite += 20;
        assert_eq!(memtable.last_index().unwrap(), 49);
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(
            memtable.global_stats.live_entries(LogQueue::Append),
            expected_append
        );
        assert_eq!(memtable.global_stats.rewrite_entries(), expected_rewrite);
        assert_eq!(
            memtable.global_stats.deleted_rewrite_entries(),
            expected_deleted_rewrite
        );
        memtable.consistency_check();

        // Squeeze some.
        // [30, 35) file_num = 103(r)
        // [35, 50) file_num = 6
        // kk1 -> 100(r), kk2 -> 101(r), kk3 -> 5
        let ents_idx = generate_entry_indexes(10, 60, FileId::new(LogQueue::Rewrite, 103));
        memtable.rewrite(ents_idx, None);
        expected_rewrite += 50;
        expected_deleted_rewrite += 50;
        assert_eq!(memtable.first_index().unwrap(), 30);
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(
            memtable.global_stats.live_entries(LogQueue::Append),
            expected_append
        );
        assert_eq!(memtable.global_stats.rewrite_entries(), expected_rewrite);
        assert_eq!(
            memtable.global_stats.deleted_rewrite_entries(),
            expected_deleted_rewrite
        );
        memtable.consistency_check();
    }

    #[test]
    fn test_memtable_merge_append() {
        let region_id = 7;
        fn empty_table(id: u64) -> MemTable {
            MemTable::new(id, Arc::new(GlobalStats::default()))
        }
        let cases = [
            |mut memtable: MemTable, on: Option<LogQueue>| -> MemTable {
                match on {
                    None => {
                        memtable.append(generate_entry_indexes(
                            0,
                            10,
                            FileId::new(LogQueue::Append, 1),
                        ));
                        memtable.append(generate_entry_indexes(
                            7,
                            15,
                            FileId::new(LogQueue::Append, 2),
                        ));
                        memtable.rewrite(
                            generate_entry_indexes(0, 10, FileId::new(LogQueue::Rewrite, 1)),
                            Some(1),
                        );
                    }
                    Some(LogQueue::Append) => {
                        memtable.append(generate_entry_indexes(
                            0,
                            10,
                            FileId::new(LogQueue::Append, 1),
                        ));
                        memtable.append(generate_entry_indexes(
                            7,
                            15,
                            FileId::new(LogQueue::Append, 2),
                        ));
                        memtable.compact_to(7);
                    }
                    Some(LogQueue::Rewrite) => {
                        memtable.append_rewrite(generate_entry_indexes(
                            0,
                            7,
                            FileId::new(LogQueue::Rewrite, 1),
                        ));
                    }
                }
                memtable
            },
            |mut memtable: MemTable, on: Option<LogQueue>| -> MemTable {
                match on {
                    None => {
                        memtable.append(generate_entry_indexes(
                            0,
                            10,
                            FileId::new(LogQueue::Append, 1),
                        ));
                        memtable.append(generate_entry_indexes(
                            7,
                            15,
                            FileId::new(LogQueue::Append, 2),
                        ));
                        memtable.rewrite(
                            generate_entry_indexes(0, 10, FileId::new(LogQueue::Rewrite, 1)),
                            Some(1),
                        );
                        memtable.compact_to(10);
                    }
                    Some(LogQueue::Append) => {
                        memtable.append(generate_entry_indexes(
                            0,
                            10,
                            FileId::new(LogQueue::Append, 1),
                        ));
                        memtable.append(generate_entry_indexes(
                            7,
                            15,
                            FileId::new(LogQueue::Append, 2),
                        ));
                        memtable.compact_to(10);
                    }
                    Some(LogQueue::Rewrite) => {
                        memtable.append_rewrite(generate_entry_indexes(
                            0,
                            7,
                            FileId::new(LogQueue::Rewrite, 1),
                        ));
                    }
                }
                memtable
            },
        ];

        // merge against empty table.
        for case in cases {
            let mut append = empty_table(region_id);
            let mut rewrite = case(empty_table(region_id), Some(LogQueue::Rewrite));
            rewrite.merge_append_table(&mut append);
            assert_eq!(
                rewrite.entry_indexes,
                case(empty_table(region_id), Some(LogQueue::Rewrite)).entry_indexes,
            );
            assert!(append.entry_indexes.is_empty());

            let mut append = case(empty_table(region_id), Some(LogQueue::Append));
            let mut rewrite = empty_table(region_id);
            rewrite.merge_append_table(&mut append);
            assert_eq!(
                rewrite.entry_indexes,
                case(empty_table(region_id), Some(LogQueue::Append)).entry_indexes
            );
            assert!(append.entry_indexes.is_empty());
        }

        for case in cases {
            let mut append = case(empty_table(region_id), Some(LogQueue::Append));
            let mut rewrite = case(empty_table(region_id), Some(LogQueue::Rewrite));
            rewrite.merge_append_table(&mut append);
            let expected = case(empty_table(region_id), None);
            assert_eq!(
                rewrite.global_stats.live_entries(LogQueue::Append),
                expected.global_stats.live_entries(LogQueue::Append)
            );
            assert_eq!(
                rewrite.global_stats.live_entries(LogQueue::Rewrite),
                expected.global_stats.live_entries(LogQueue::Rewrite)
            );
            assert_eq!(rewrite.entry_indexes, expected.entry_indexes);
            assert!(append.entry_indexes.is_empty());
        }
    }

    #[test]
    fn test_memtables_merge_append_neighbor() {
        let first_rid = 17;
        let mut last_rid = first_rid;

        let mut batches = vec![
            LogItemBatch::with_capacity(0),
            LogItemBatch::with_capacity(0),
            LogItemBatch::with_capacity(0),
        ];
        let files: Vec<_> = (0..batches.len())
            .map(|i| FileId::new(LogQueue::Append, 10 + i as u64))
            .collect();

        // put (key1, v1) => del (key1) => put (key1, v2)
        batches[0].put(last_rid, b"key1".to_vec(), b"val1".to_vec());
        batches[1].delete(last_rid, b"key1".to_vec());
        batches[2].put(last_rid, b"key1".to_vec(), b"val2".to_vec());

        // put (k, _) => cleanup
        last_rid += 1;
        batches[0].put(last_rid, b"key".to_vec(), b"ANYTHING".to_vec());
        batches[1].add_command(last_rid, Command::Clean);

        // entries [1, 10) => compact 5 => entries [11, 20)
        last_rid += 1;
        batches[0].add_entry_indexes(last_rid, generate_entry_indexes(1, 11, files[0]));
        batches[1].add_command(last_rid, Command::Compact { index: 5 });
        batches[2].add_entry_indexes(last_rid, generate_entry_indexes(11, 21, files[2]));

        for b in batches.iter_mut() {
            b.finish_write(FileBlockHandle::dummy(LogQueue::Append));
        }

        // reverse merge
        let mut ctxs = VecDeque::default();
        for (batch, file_id) in batches.clone().into_iter().zip(files) {
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
        let sequential_global_stats = Arc::new(GlobalStats::default());
        let sequential_memtables = MemTableAccessor::new(sequential_global_stats.clone());
        for mut batch in batches.clone() {
            sequential_memtables.apply(batch.drain(), LogQueue::Append);
        }

        for rid in first_rid..=last_rid {
            let m = merged_memtables.get(rid);
            let s = sequential_memtables.get(rid);
            if m.is_none() {
                assert!(s.is_none());
                continue;
            }
            let merged = m.as_ref().unwrap().read();
            let sequential = s.as_ref().unwrap().read();
            let mut merged_vec = Vec::new();
            let mut sequential_vec = Vec::new();
            merged
                .fetch_entry_indexes_before(u64::MAX, &mut merged_vec)
                .unwrap();
            sequential
                .fetch_entry_indexes_before(u64::MAX, &mut sequential_vec)
                .unwrap();
            assert_eq!(merged_vec, sequential_vec);
            merged_vec.clear();
            sequential_vec.clear();
            merged
                .fetch_rewritten_entry_indexes(&mut merged_vec)
                .unwrap();
            sequential
                .fetch_rewritten_entry_indexes(&mut sequential_vec)
                .unwrap();
            assert_eq!(merged_vec, sequential_vec);
            let mut merged_vec = Vec::new();
            let mut sequential_vec = Vec::new();
            merged.fetch_kvs_before(u64::MAX, &mut merged_vec);
            sequential.fetch_kvs_before(u64::MAX, &mut sequential_vec);
            assert_eq!(merged_vec, sequential_vec);
            merged_vec.clear();
            sequential_vec.clear();
            merged.fetch_rewritten_kvs(&mut merged_vec);
            sequential.fetch_rewritten_kvs(&mut sequential_vec);
            assert_eq!(merged_vec, sequential_vec);
        }
        assert_eq!(
            merged_global_stats.live_entries(LogQueue::Append),
            sequential_global_stats.live_entries(LogQueue::Append),
        );
        assert_eq!(
            merged_global_stats.rewrite_entries(),
            sequential_global_stats.rewrite_entries(),
        );
        assert_eq!(
            merged_global_stats.deleted_rewrite_entries(),
            sequential_global_stats.deleted_rewrite_entries(),
        );
    }
}
