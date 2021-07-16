use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{cmp, u64};

use protobuf::Message;

use crate::log_batch::{CompressionType, EntryExt};
use crate::pipe_log::{FileId, LogQueue};
use crate::util::{slices_in_range, HashMap};
use crate::{Error, GlobalStats, Result};

const SHRINK_CACHE_CAPACITY: usize = 64;
const SHRINK_CACHE_LIMIT: usize = 512;

#[derive(Debug, Clone, PartialEq)]
pub struct EntryIndex {
    pub index: u64,

    // Log batch physical position in file.
    pub queue: LogQueue,
    pub file_id: FileId,
    pub base_offset: u64,
    pub compression_type: CompressionType,
    pub batch_len: u64,

    // Entry position in log batch.
    pub offset: u64,
    pub len: u64,
}

impl Default for EntryIndex {
    fn default() -> EntryIndex {
        EntryIndex {
            index: 0,
            queue: LogQueue::Append,
            file_id: Default::default(),
            base_offset: 0,
            compression_type: CompressionType::None,
            batch_len: 0,
            offset: 0,
            len: 0,
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
pub struct MemTable<E: Message + Clone, W: EntryExt<E>> {
    region_id: u64,

    // All entries index
    entries_index: VecDeque<EntryIndex>,
    rewrite_count: usize,

    // Region scope key/value pairs
    // key -> (value, queue, file_id)
    kvs: HashMap<Vec<u8>, (Vec<u8>, LogQueue, FileId)>,

    global_stats: Arc<GlobalStats>,
    _phantom1: PhantomData<E>,
    _phantom2: PhantomData<W>,
}

impl<E: Message + Clone, W: EntryExt<E>> MemTable<E, W> {
    pub fn new(region_id: u64, global_stats: Arc<GlobalStats>) -> MemTable<E, W> {
        MemTable::<E, W> {
            region_id,
            entries_index: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            rewrite_count: 0,
            kvs: HashMap::default(),

            global_stats,
            _phantom1: PhantomData,
            _phantom2: PhantomData,
        }
    }

    // Remove all entry indexes with index greater than or equal to the given.
    fn cut_entries_index(&mut self, index: u64) -> usize {
        if self.entries_index.is_empty() {
            return 0;
        }
        let last_index = self.entries_index.back().unwrap().index;
        let first_index = self.entries_index.front().unwrap().index;
        // Compacted entries can't be overwritten.
        assert!(first_index <= index, "corrupted raft {}", self.region_id);

        let conflict = if index <= last_index {
            (index - first_index) as usize
        } else {
            return 0;
        };
        self.entries_index.truncate(conflict);

        if self.rewrite_count > self.entries_index.len() {
            let diff = self.rewrite_count - self.entries_index.len();
            self.rewrite_count = self.entries_index.len();
            self.global_stats.add_compacted_rewrite(diff);
        }

        (last_index - index + 1) as usize
    }

    fn shrink_entries_index(&mut self) {
        if self.entries_index.capacity() > SHRINK_CACHE_LIMIT
            && self.entries_index.len() <= SHRINK_CACHE_CAPACITY
        {
            self.entries_index.shrink_to(SHRINK_CACHE_CAPACITY);
        }
    }

    fn merge_higher_prio(&mut self, rhs: &mut Self) {
        debug_assert_eq!(self.rewrite_count, self.entries_index.len());
        debug_assert_eq!(rhs.rewrite_count, 0);

        if !rhs.entries_index.is_empty() {
            if !self.entries_index.is_empty() {
                let front = rhs.entries_index[0].index;
                let self_back = self.entries_index.back().unwrap().index;
                let self_front = self.entries_index.front().unwrap().index;
                if front > self_back + 1 {
                    self.compact_to(self_back + 1);
                } else if front >= self_front {
                    self.cut_entries_index(front);
                } else {
                    unreachable!();
                }
            }
            self.entries_index.reserve(rhs.entries_index.len());
            for ei in std::mem::take(&mut rhs.entries_index) {
                self.entries_index.push_back(ei);
            }
        }

        for (key, (value, queue, file_id)) in std::mem::take(&mut rhs.kvs) {
            self.kvs.insert(key, (value, queue, file_id));
        }
    }

    /// Merge from `rhs`, which has a lower priority.
    pub fn merge_lower_prio(&mut self, rhs: &mut Self) {
        rhs.merge_higher_prio(self);
        std::mem::swap(self, rhs);
    }

    pub fn append(&mut self, entries: Vec<E>, entries_index: Vec<EntryIndex>) {
        assert_eq!(entries.len(), entries_index.len());
        if entries.is_empty() {
            return;
        }

        let first_index_to_add = W::index(&entries[0]);
        self.cut_entries_index(first_index_to_add);

        if let Some(index) = self.entries_index.back().map(|e| e.index) {
            assert_eq!(
                index + 1,
                first_index_to_add,
                "memtable {} has a hole",
                self.region_id
            );
        }

        self.entries_index.extend(entries_index);
    }

    pub fn append_rewrite(&mut self, entries: Vec<E>, entries_index: Vec<EntryIndex>) {
        self.global_stats.add_rewrite(entries.len());
        match (
            self.entries_index.back().map(|x| x.index),
            entries_index.first(),
        ) {
            (Some(back_idx), Some(first)) if back_idx + 1 < first.index => {
                // It's possible that a hole occurs in the rewrite queue.
                self.compact_to(back_idx + 1);
            }
            _ => {}
        }
        self.append(entries, entries_index);
        self.rewrite_count = self.entries_index.len();
    }

    pub fn rewrite(&mut self, entries_index: Vec<EntryIndex>, latest_rewrite: Option<FileId>) {
        if entries_index.is_empty() {
            return;
        }

        // Update the counter of entries in the rewrite queue.
        self.global_stats.add_rewrite(entries_index.len());

        if self.entries_index.is_empty() {
            // All entries are compacted, update the counter.
            self.global_stats.add_compacted_rewrite(entries_index.len());
            return;
        }

        let self_ents_len = self.entries_index.len();
        let front = self.entries_index[0].index as usize;
        let back = self.entries_index[self_ents_len - 1].index as usize;

        let ents_len = entries_index.len();
        let first = cmp::max(entries_index[0].index as usize, front);
        let last = cmp::min(entries_index[ents_len - 1].index as usize, back);

        if last < front {
            // All entries are compacted, update the counter.
            self.global_stats.add_compacted_rewrite(entries_index.len());
            return;
        }

        // Some entries in `entries_index` are compacted or cut, update the counter.
        let strip_count = ents_len - (last + 1 - first);
        self.global_stats.add_compacted_rewrite(strip_count);

        let (entries_index, ents_len) = {
            let diff = first - entries_index[0].index as usize;
            let len = last - first + 1;
            (&entries_index[diff..diff + len], len)
        };

        let distance = (first - front) as usize;
        for (i, ei) in entries_index.iter().enumerate() {
            if let Some(latest_rewrite) = latest_rewrite {
                debug_assert_eq!(self.entries_index[i + distance].queue, LogQueue::Append);
                if self.entries_index[i + distance].file_id > latest_rewrite {
                    // Some entries are overwritten by new appends.
                    self.global_stats.add_compacted_rewrite(ents_len - i);
                    self.rewrite_count = i + distance;
                    return;
                }
            } else {
                // It's a squeeze operation.
                debug_assert_eq!(self.entries_index[i + distance].queue, LogQueue::Rewrite);
            }

            if self.entries_index[i + distance].queue != LogQueue::Rewrite {
                debug_assert_eq!(ei.queue, LogQueue::Rewrite);
                self.entries_index[i + distance].queue = LogQueue::Rewrite;
            }

            self.entries_index[i + distance].file_id = ei.file_id;
            self.entries_index[i + distance].base_offset = ei.base_offset;
            self.entries_index[i + distance].compression_type = ei.compression_type;
            self.entries_index[i + distance].batch_len = ei.batch_len;
            self.entries_index[i + distance].offset = ei.offset;
            self.entries_index[i + distance].len = ei.len;
        }

        self.rewrite_count = distance + ents_len;
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, file_id: FileId) {
        if let Some(origin) = self.kvs.insert(key, (value, LogQueue::Append, file_id)) {
            if origin.1 == LogQueue::Rewrite {
                self.global_stats.add_compacted_rewrite(1);
            }
        }
    }

    pub fn put_rewrite(&mut self, key: Vec<u8>, value: Vec<u8>, file_id: FileId) {
        self.kvs.insert(key, (value, LogQueue::Rewrite, file_id));
        self.global_stats.add_rewrite(1);
    }

    pub fn rewrite_key(&mut self, key: Vec<u8>, latest_rewrite: Option<FileId>, file_id: FileId) {
        self.global_stats.add_rewrite(1);
        if let Some(value) = self.kvs.get_mut(&key) {
            if value.1 == LogQueue::Append {
                if let Some(latest_rewrite) = latest_rewrite {
                    if value.2 <= latest_rewrite {
                        value.1 = LogQueue::Rewrite;
                        value.2 = file_id;
                    }
                } else {
                    // The rewritten key/value pair has been overwritten.
                    self.global_stats.add_compacted_rewrite(1);
                }
            } else {
                assert!(value.2 <= file_id);
                value.2 = file_id;
            }
        } else {
            // The rewritten key/value pair has been compacted.
            self.global_stats.add_compacted_rewrite(1);
        }
    }

    pub fn delete(&mut self, key: &[u8]) {
        if let Some(value) = self.kvs.remove(key) {
            if value.1 == LogQueue::Rewrite {
                self.global_stats.add_compacted_rewrite(1);
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.kvs.get(key).map(|v| v.0.clone())
    }

    pub fn compact_to(&mut self, mut idx: u64) -> u64 {
        let first_idx = match self.entries_index.front() {
            Some(e) if e.index < idx => e.index,
            _ => return 0,
        };
        let last_idx = self.entries_index.back().unwrap().index;
        idx = cmp::min(last_idx + 1, idx);

        let drain_end = (idx - first_idx) as usize;
        self.entries_index.drain(..drain_end);
        self.shrink_entries_index();

        let rewrite_sub = cmp::min(drain_end, self.rewrite_count);
        self.rewrite_count -= rewrite_sub;
        self.global_stats.add_compacted_rewrite(rewrite_sub);
        drain_end as u64
    }

    pub fn get_entry(&self, index: u64) -> Option<EntryIndex> {
        if self.entries_index.is_empty() {
            return None;
        }

        let first_index = self.entries_index.front().unwrap().index;
        let last_index = self.entries_index.back().unwrap().index;
        if index < first_index || index > last_index {
            return None;
        }

        let ioffset = (index - first_index) as usize;
        let entry_index = self.entries_index[ioffset].clone();
        Some(entry_index)
    }

    pub fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<()> {
        assert!(end > begin, "fetch_entries_to({}, {})", begin, end);

        if self.entries_index.is_empty() {
            return Err(Error::StorageUnavailable);
        }
        let first_index = self.entries_index.front().unwrap().index;
        if begin < first_index {
            return Err(Error::StorageCompacted);
        }
        let last_index = self.entries_index.back().unwrap().index;
        if end > last_index + 1 {
            return Err(Error::StorageUnavailable);
        }

        let start_pos = (begin - first_index) as usize;
        let mut end_pos = (end - begin) as usize + start_pos;

        // Check max size limitation.
        if let Some(max_size) = max_size {
            let count_limit = self.count_limit(start_pos, end_pos, max_size);
            end_pos = start_pos + count_limit;
        }

        let (first, second) = slices_in_range(&self.entries_index, start_pos, end_pos);
        vec_idx.extend_from_slice(first);
        vec_idx.extend_from_slice(second);
        Ok(())
    }

    pub fn fetch_rewrite_entries(
        &self,
        latest_rewrite: FileId,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<()> {
        let begin = self
            .entries_index
            .iter()
            .find(|e| e.queue == LogQueue::Append);
        let end = self
            .entries_index
            .iter()
            .rev()
            .find(|e| e.file_id <= latest_rewrite);
        if let (Some(begin), Some(end)) = (begin, end) {
            if begin.index <= end.index {
                return self.fetch_entries_to(begin.index, end.index + 1, None, vec_idx);
            }
        }
        Ok(())
    }

    pub fn fetch_entries_from_rewrite(&self, vec_idx: &mut Vec<EntryIndex>) -> Result<()> {
        if self.rewrite_count > 0 {
            let end = self.entries_index[self.rewrite_count - 1].index + 1;
            let first = self.entries_index.front().unwrap().index;
            return self.fetch_entries_to(first, end, None, vec_idx);
        }
        Ok(())
    }

    pub fn fetch_rewrite_kvs(&self, latest_rewrite: FileId, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, (value, queue, file_id)) in &self.kvs {
            if *queue == LogQueue::Append && *file_id <= latest_rewrite {
                vec.push((key.clone(), value.clone()));
            }
        }
    }

    pub fn fetch_kvs_from_rewrite(&self, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, (value, queue, _)) in &self.kvs {
            if *queue == LogQueue::Rewrite {
                vec.push((key.clone(), value.clone()));
            }
        }
    }

    pub fn min_file_id(&self, queue: LogQueue) -> Option<FileId> {
        let entry = match queue {
            LogQueue::Append => self.entries_index.get(self.rewrite_count),
            LogQueue::Rewrite if self.rewrite_count == 0 => None,
            LogQueue::Rewrite => self.entries_index.front(),
        };
        let ents_min = entry.map(|e| e.file_id);
        let kvs_min = self.kvs_min_file_id(queue);
        match (ents_min, kvs_min) {
            (Some(ents_min), Some(kvs_min)) => Some(FileId::min(kvs_min, ents_min)),
            (Some(ents_min), None) => Some(ents_min),
            (None, Some(kvs_min)) => Some(kvs_min),
            (None, None) => None,
        }
    }

    pub fn entries_count(&self) -> usize {
        self.entries_index.len()
    }

    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    pub fn first_index(&self) -> Option<u64> {
        self.entries_index.front().map(|e| e.index)
    }

    pub fn last_index(&self) -> Option<u64> {
        self.entries_index.back().map(|e| e.index)
    }

    pub fn kvs_min_file_id(&self, queue: LogQueue) -> Option<FileId> {
        self.kvs
            .values()
            .filter(|v| v.1 == queue)
            .fold(None, |min, v| {
                if let Some(min) = min {
                    Some(FileId::min(min, v.2))
                } else {
                    Some(v.2)
                }
            })
    }

    /// Returns (needs_rewrite, needs_force_compact).
    pub fn needs_rewrite_or_compact(
        &self,
        latest_rewrite: FileId,
        latest_compact: FileId,
        // Only entries with count is less than the limit can be rewritten.
        rewrite_count_limit: usize,
    ) -> (bool, bool) {
        debug_assert!(latest_compact <= latest_rewrite);
        let min_file_id = match self.min_file_id(LogQueue::Append) {
            Some(file_num) if file_num <= latest_rewrite => file_num,
            _ => return (false, false),
        };
        let entries_count = self.entries_count();

        if min_file_id < latest_compact && entries_count > rewrite_count_limit {
            // `rewrite_count_limit` is considered because in some raft applications,
            // log-compaction is implemented based on an special raft log, which means
            // there will always be at least 1 log left.
            return (false, true);
        }

        let needs_rewrite = entries_count < rewrite_count_limit;
        (needs_rewrite, false)
    }

    fn count_limit(&self, start_idx: usize, end_idx: usize, max_size: usize) -> usize {
        assert!(start_idx < end_idx);
        let (first, second) = slices_in_range(&self.entries_index, start_idx, end_idx);

        let (mut count, mut total_size) = (0, 0);
        for i in first.iter().chain(second) {
            count += 1;
            total_size += i.len;
            if total_size as usize > max_size {
                // No matter max_size's value, fetch one entry at lease.
                return cmp::max(count - 1, 1);
            }
        }
        count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::file_pipe_log::FilePipeLog;
    use crate::pipe_log::PipeLog;
    use raft::eraftpb::Entry;

    impl<E: Message + Clone, W: EntryExt<E>> MemTable<E, W> {
        pub fn max_file_id(&self, queue: LogQueue) -> Option<FileId> {
            let entry = match queue {
                LogQueue::Append if self.rewrite_count == self.entries_index.len() => None,
                LogQueue::Append => self.entries_index.back(),
                LogQueue::Rewrite if self.rewrite_count == 0 => None,
                LogQueue::Rewrite => self.entries_index.get(self.rewrite_count - 1),
            };
            let ents_max = entry.map(|e| e.file_id);

            let kvs_max = self.kvs_max_file_id(queue);
            match (ents_max, kvs_max) {
                (Some(ents_max), Some(kvs_max)) => Some(FileId::max(kvs_max, ents_max)),
                (Some(ents_max), None) => Some(ents_max),
                (None, Some(kvs_max)) => Some(kvs_max),
                (None, None) => None,
            }
        }

        pub fn kvs_max_file_id(&self, queue: LogQueue) -> Option<FileId> {
            self.kvs
                .values()
                .filter(|v| v.1 == queue)
                .fold(None, |max, v| {
                    if let Some(max) = max {
                        Some(FileId::max(max, v.2))
                    } else {
                        Some(v.2)
                    }
                })
        }

        pub fn fetch_all(&self, vec_idx: &mut Vec<EntryIndex>) {
            if self.entries_index.is_empty() {
                return;
            }

            let begin = self.entries_index.front().unwrap().index;
            let end = self.entries_index.back().unwrap().index + 1;
            self.fetch_entries_to(begin, end, None, vec_idx).unwrap();
        }

        fn entries_size(&self) -> usize {
            self.entries_index.iter().fold(0, |acc, e| acc + e.len) as usize
        }

        fn check_entries_index(&self) {
            if self.entries_index.is_empty() {
                return;
            }

            let ei_first = self.entries_index.front().unwrap();
            let ei_last = self.entries_index.back().unwrap();
            assert_eq!(
                ei_last.index - ei_first.index + 1,
                self.entries_index.len() as u64
            );
        }
    }

    #[test]
    fn test_memtable_append() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, stats.clone());

        // Append entries [10, 20) file_num = 1.
        // after appending
        // [10, 20) file_num = 1
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(10, 20, LogQueue::Append, 1.into());
        memtable.append(ents, ents_idx);
        assert_eq!(memtable.entries_size(), 10);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 1.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 1.into());
        memtable.check_entries_index();

        // Append entries [20, 30) file_num = 2.
        // after appending:
        // [10, 20) file_num = 1
        // [20, 30) file_num = 2
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(20, 30, LogQueue::Append, 2.into());
        memtable.append(ents, ents_idx);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 1.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 2.into());
        memtable.check_entries_index();

        // Partial overlap Appending.
        // Append entries [25, 35) file_num = 3.
        // After appending:
        // [10, 20) file_num = 1
        // [20, 25) file_num = 2
        // [25, 35) file_num = 3
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(25, 35, LogQueue::Append, 3.into());
        memtable.append(ents, ents_idx);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 1.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 3.into());
        memtable.check_entries_index();

        // Full overlap Appending.
        // Append entries [10, 40) file_num = 4.
        // After appending:
        // [10, 40) file_num = 4
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(10, 40, LogQueue::Append, 4.into());
        memtable.append(ents, ents_idx);
        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 4.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 4.into());
        memtable.check_entries_index();
    }

    #[test]
    fn test_memtable_compact() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, stats.clone());

        // After appending:
        // [0, 10) file_num = 1
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(0, 10, LogQueue::Append, 1.into());
        memtable.append(ents, ents_idx);
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(10, 15, LogQueue::Append, 2.into());
        memtable.append(ents, ents_idx);
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(15, 20, LogQueue::Append, 2.into());
        memtable.append(ents, ents_idx);
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(20, 25, LogQueue::Append, 3.into());
        memtable.append(ents, ents_idx);

        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 1.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 3.into());
        memtable.check_entries_index();

        // Compact to 5.
        // Only index is needed to compact.
        assert_eq!(memtable.compact_to(5), 5);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 1.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 3.into());
        memtable.check_entries_index();

        // Compact to 20.
        assert_eq!(memtable.compact_to(20), 15);
        assert_eq!(memtable.entries_size(), 5);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 3.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 3.into());
        memtable.check_entries_index();

        // Compact to 20 or smaller index, nothing happens.
        assert_eq!(memtable.compact_to(20), 0);
        assert_eq!(memtable.compact_to(15), 0);
        assert_eq!(memtable.entries_size(), 5);
        memtable.check_entries_index();
    }

    #[test]
    fn test_memtable_fetch() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, stats.clone());

        // After appending:
        // [0, 10) file_num = 1
        // [10, 15) file_num = 2
        // [15, 20) file_num = 2
        // [20, 25) file_num = 3
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(0, 10, LogQueue::Append, 1.into());
        memtable.append(ents, ents_idx);
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(10, 20, LogQueue::Append, 2.into());
        memtable.append(ents, ents_idx);
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(20, 25, LogQueue::Append, 3.into());
        memtable.append(ents, ents_idx);

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
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, stats.clone());

        // After appending:
        // [0, 10) file_num = 1
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(0, 10, LogQueue::Append, 1.into());
        memtable.append(ents, ents_idx);
        memtable.put(b"k1".to_vec(), b"v1".to_vec(), 1.into());
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(10, 20, LogQueue::Append, 2.into());
        memtable.append(ents, ents_idx);
        memtable.put(b"k2".to_vec(), b"v2".to_vec(), 2.into());
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(20, 25, LogQueue::Append, 3.into());
        memtable.append(ents, ents_idx);
        memtable.put(b"k3".to_vec(), b"v3".to_vec(), 3.into());

        // After rewriting:
        // [0, 10) queue = rewrite, file_num = 50,
        // [10, 20) file_num = 2
        // [20, 25) file_num = 3
        let (_, ents_idx) = generate_ents::<FilePipeLog>(0, 10, LogQueue::Rewrite, 50.into());
        memtable.rewrite(ents_idx, Some(1.into()));
        memtable.rewrite_key(b"k1".to_vec(), Some(1.into()), 50.into());
        assert_eq!(memtable.entries_size(), 25);

        let mut ents_idx = vec![];
        assert!(memtable
            .fetch_rewrite_entries(2.into(), &mut ents_idx)
            .is_ok());
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx.last().unwrap().index, 19);

        ents_idx.clear();
        assert!(memtable.fetch_entries_from_rewrite(&mut ents_idx).is_ok());
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx.first().unwrap().index, 0);
        assert_eq!(ents_idx.last().unwrap().index, 9);
    }

    #[test]
    fn test_memtable_kv_operations() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, stats);

        let (k1, v1) = (b"key1", b"value1");
        let (k5, v5) = (b"key5", b"value5");
        memtable.put(k1.to_vec(), v1.to_vec(), 1.into());
        memtable.put(k5.to_vec(), v5.to_vec(), 5.into());
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 1.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 5.into());
        assert_eq!(memtable.get(k1.as_ref()), Some(v1.to_vec()));
        assert_eq!(memtable.get(k5.as_ref()), Some(v5.to_vec()));

        memtable.delete(k5.as_ref());
        assert_eq!(memtable.get(k5.as_ref()), None);
    }

    #[test]
    fn test_memtable_get_entry() {
        let region_id = 8;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, stats.clone());

        // [5, 10) file_num = 1
        // [10, 20) file_num = 2
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(5, 10, LogQueue::Append, 1.into());
        memtable.append(ents, ents_idx);
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(10, 20, LogQueue::Append, 2.into());
        memtable.append(ents, ents_idx);

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
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, stats.clone());

        // After appending and compacting:
        // [10, 20) file_num = 2
        // [20, 30) file_num = 3
        // [30, 40) file_num = 4
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(10, 20, LogQueue::Append, 2.into());
        memtable.append(ents, ents_idx);
        memtable.put(b"kk1".to_vec(), b"vv1".to_vec(), 2.into());
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(20, 30, LogQueue::Append, 3.into());
        memtable.append(ents, ents_idx);
        memtable.put(b"kk2".to_vec(), b"vv2".to_vec(), 3.into());
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(30, 40, LogQueue::Append, 4.into());
        memtable.append(ents, ents_idx);
        memtable.put(b"kk3".to_vec(), b"vv3".to_vec(), 4.into());

        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 2.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 4.into());
        memtable.check_entries_index();

        // Rewrite compacted entries.
        let (_, ents_idx) = generate_ents::<FilePipeLog>(0, 10, LogQueue::Rewrite, 50.into());
        memtable.rewrite(ents_idx, Some(1.into()));
        memtable.rewrite_key(b"kk0".to_vec(), Some(1.into()), 50.into());

        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 2.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 4.into());
        assert!(memtable.min_file_id(LogQueue::Rewrite).is_none());
        assert!(memtable.max_file_id(LogQueue::Rewrite).is_none());
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.get(b"kk0"), None);
        assert_eq!(memtable.global_stats.rewrite_operations(), 11);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 11);

        // Rewrite compacted entries + valid entries.
        let (_, ents_idx) = generate_ents::<FilePipeLog>(0, 20, LogQueue::Rewrite, 100.into());
        memtable.rewrite(ents_idx, Some(2.into()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 31);
        memtable.rewrite_key(b"kk0".to_vec(), Some(1.into()), 50.into());
        memtable.rewrite_key(b"kk1".to_vec(), Some(2.into()), 100.into());

        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 3.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 4.into());
        assert_eq!(memtable.min_file_id(LogQueue::Rewrite).unwrap(), 100.into());
        assert_eq!(memtable.max_file_id(LogQueue::Rewrite).unwrap(), 100.into());
        assert_eq!(memtable.rewrite_count, 10);
        assert_eq!(memtable.get(b"kk1"), Some(b"vv1".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 33);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 22);

        // Rewrite vaild entries.
        let (_, ents_idx) = generate_ents::<FilePipeLog>(20, 30, LogQueue::Rewrite, 101.into());
        memtable.rewrite(ents_idx, Some(3.into()));
        memtable.rewrite_key(b"kk2".to_vec(), Some(3.into()), 101.into());

        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 4.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 4.into());
        assert_eq!(memtable.min_file_id(LogQueue::Rewrite).unwrap(), 100.into());
        assert_eq!(memtable.max_file_id(LogQueue::Rewrite).unwrap(), 101.into());
        assert_eq!(memtable.rewrite_count, 20);
        assert_eq!(memtable.get(b"kk2"), Some(b"vv2".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 44);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 22);

        // Put some entries overwritting entires in file 4.
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(35, 36, LogQueue::Append, 5.into());
        memtable.append(ents, ents_idx);
        memtable.put(b"kk3".to_vec(), b"vv33".to_vec(), 5.into());
        assert_eq!(memtable.entries_index.back().unwrap().index, 35);

        // Rewrite valid + overwritten entries.
        let (_, ents_idx) = generate_ents::<FilePipeLog>(30, 40, LogQueue::Rewrite, 102.into());
        memtable.rewrite(ents_idx, Some(4.into()));
        memtable.rewrite_key(b"kk3".to_vec(), Some(4.into()), 102.into());

        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 5.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 5.into());
        assert_eq!(memtable.min_file_id(LogQueue::Rewrite).unwrap(), 100.into());
        assert_eq!(memtable.max_file_id(LogQueue::Rewrite).unwrap(), 102.into());
        assert_eq!(memtable.rewrite_count, 25);
        assert_eq!(memtable.get(b"kk3"), Some(b"vv33".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 55);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 27);

        // Compact after rewrite.
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(35, 50, LogQueue::Append, 6.into());
        memtable.append(ents, ents_idx);
        memtable.compact_to(30);
        assert_eq!(memtable.entries_index.back().unwrap().index, 49);
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(memtable.global_stats.rewrite_operations(), 55);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 47);
        memtable.compact_to(40);
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 55);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 52);

        let (_, ents_idx) = generate_ents::<FilePipeLog>(30, 36, LogQueue::Rewrite, 103.into());
        memtable.rewrite(ents_idx, Some(5.into()));
        memtable.rewrite_key(b"kk3".to_vec(), Some(5.into()), 103.into());

        assert_eq!(memtable.min_file_id(LogQueue::Append).unwrap(), 6.into());
        assert_eq!(memtable.max_file_id(LogQueue::Append).unwrap(), 6.into());
        assert_eq!(memtable.min_file_id(LogQueue::Rewrite).unwrap(), 100.into());
        assert_eq!(memtable.max_file_id(LogQueue::Rewrite).unwrap(), 103.into());
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 62);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 58);

        // Rewrite after cut.
        let (ents, ents_idx) = generate_ents::<FilePipeLog>(50, 55, LogQueue::Append, 7.into());
        memtable.append(ents, ents_idx);
        let (_, ents_idx) = generate_ents::<FilePipeLog>(30, 50, LogQueue::Rewrite, 104.into());
        memtable.rewrite(ents_idx, Some(6.into()));
        assert_eq!(memtable.rewrite_count, 10);
        assert_eq!(memtable.global_stats.rewrite_operations(), 82);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 68);

        let (ents, ents_idx) = generate_ents::<FilePipeLog>(45, 50, LogQueue::Append, 7.into());
        memtable.append(ents, ents_idx);
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(memtable.global_stats.rewrite_operations(), 82);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 73);

        let (ents, ents_idx) = generate_ents::<FilePipeLog>(40, 50, LogQueue::Append, 7.into());
        memtable.append(ents, ents_idx);
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 82);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 78);
    }

    fn generate_ents<P: PipeLog>(
        begin_idx: u64,
        end_idx: u64,
        queue: LogQueue,
        file_id: FileId,
    ) -> (Vec<Entry>, Vec<EntryIndex>) {
        assert!(end_idx >= begin_idx);
        let (mut ents, mut ents_idx) = (vec![], vec![]);
        for idx in begin_idx..end_idx {
            let mut ent = Entry::new();
            ent.set_index(idx);
            ents.push(ent);

            let mut ent_idx = EntryIndex::default();
            ent_idx.index = idx;
            ent_idx.queue = queue;
            ent_idx.file_id = file_id;
            ent_idx.offset = idx; // fake offset
            ent_idx.len = 1; // fake size

            ents_idx.push(ent_idx);
        }
        (ents, ents_idx)
    }
}
