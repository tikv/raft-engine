use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::Arc;
use std::{cmp, u64};

use protobuf::Message;

use crate::cache_evict::CacheTracker;
use crate::log_batch::{CompressionType, EntryExt};
use crate::pipe_log::LogQueue;
use crate::util::{slices_in_range, HashMap};
use crate::{Error, GlobalStats, Result};

const SHRINK_CACHE_CAPACITY: usize = 64;
const SHRINK_CACHE_LIMIT: usize = 512;

#[derive(Debug, Clone, PartialEq)]
pub struct EntryIndex {
    pub index: u64,

    // Log batch physical position in file.
    pub queue: LogQueue,
    pub file_num: u64,
    pub base_offset: u64,
    pub compression_type: CompressionType,
    pub batch_len: u64,

    // Entry position in log batch.
    pub offset: u64,
    pub len: u64,

    // Take and drop the field when the entry is removed from entry cache.
    pub cache_tracker: Option<CacheTracker>,
}

impl Default for EntryIndex {
    fn default() -> EntryIndex {
        EntryIndex {
            index: 0,
            queue: LogQueue::Append,
            file_num: 0,
            base_offset: 0,
            compression_type: CompressionType::None,
            batch_len: 0,
            offset: 0,
            len: 0,
            cache_tracker: None,
        }
    }
}

/*
 * Each region has an individual `MemTable` to cache latest entries and all entries indices.
 * `MemTable` also have a map to store all key value pairs for this region.
 *
 * Latest N entries                    [**************************]
 *                                      ^                        ^
 *                                      |                        |
 *                             first entry in cache      last entry in cache
 * All entries indices [******************************************]
 *                      ^                                        ^
 *                      |                                        |
 *                 first entry                               last entry
 */
pub struct MemTable<E: Message + Clone, W: EntryExt<E>> {
    region_id: u64,

    // latest N entries
    entries_cache: VecDeque<E>,

    // All entries index
    entries_index: VecDeque<EntryIndex>,
    rewrite_count: usize,

    // Region scope key/value pairs
    // key -> (value, queue, file_num)
    kvs: HashMap<Vec<u8>, (Vec<u8>, LogQueue, u64)>,

    cache_size: usize,
    cache_limit: usize,
    global_stats: Arc<GlobalStats>,
    _phantom: PhantomData<W>,
}

impl<E: Message + Clone, W: EntryExt<E>> MemTable<E, W> {
    fn cache_distance(&self) -> usize {
        if self.entries_cache.is_empty() {
            return self.entries_index.len();
        }
        let distance = self.entries_index.len() - self.entries_cache.len();
        let cache_first = W::index(&self.entries_cache[0]);
        let index_first = self.entries_index[distance].index;
        assert_eq!(cache_first, index_first);
        distance
    }

    fn adjust_rewrite_count(&mut self, new_rewrite_count: usize) {
        let rewrite_delta = new_rewrite_count as i64 - self.rewrite_count as i64;
        self.rewrite_count = new_rewrite_count;
        if rewrite_delta >= 0 {
            self.global_stats.add_rewrite(rewrite_delta as usize);
        } else {
            let rewrite_delta = -rewrite_delta as usize;
            self.global_stats.add_compacted_rewrite(rewrite_delta);
        }
    }

    // Remove all cached entries with index greater than or equal to the given.
    fn cut_entries_cache(&mut self, index: u64) {
        if self.entries_cache.is_empty() {
            return;
        }
        let last_index = W::index(self.entries_cache.back().unwrap());
        let first_index = W::index(self.entries_cache.front().unwrap());
        let conflict = if index <= first_index {
            // All entries need to be removed.
            0
        } else if index <= last_index {
            // Entries after `index` (included) need to be removed.
            (index - first_index) as usize
        } else {
            // No entries need to be removed.
            return;
        };

        let distance = self.cache_distance();
        for offset in conflict..self.entries_cache.len() {
            let entry_index = &mut self.entries_index[distance + offset];
            entry_index.cache_tracker.take();
            self.cache_size -= entry_index.len as usize;
            self.global_stats.sub_mem_change(entry_index.len as usize);
        }

        self.entries_cache.truncate(conflict);
    }

    // Remove all entry indexes with index greater than or equal to the given.
    fn cut_entries_index(&mut self, index: u64) -> usize {
        if self.entries_index.is_empty() {
            return 0;
        }
        let last_index = self.entries_index.back().unwrap().index;
        let first_index = self.entries_index.front().unwrap().index;
        assert!(first_index <= index); // Compacted entries can't be overwritten.
        let conflict = if index <= last_index {
            (index - first_index) as usize
        } else {
            return 0;
        };
        self.entries_index.truncate(conflict);

        let new_rewrite_count = cmp::min(self.rewrite_count, self.entries_index.len());
        self.adjust_rewrite_count(new_rewrite_count);
        (last_index - index + 1) as usize
    }

    fn shrink_entries_cache(&mut self) {
        if self.entries_cache.capacity() > SHRINK_CACHE_LIMIT
            && self.entries_cache.len() <= SHRINK_CACHE_CAPACITY
        {
            self.entries_cache.shrink_to(SHRINK_CACHE_CAPACITY);
        }
    }

    fn shrink_entries_index(&mut self) {
        if self.entries_index.capacity() > SHRINK_CACHE_LIMIT
            && self.entries_index.len() <= SHRINK_CACHE_CAPACITY
        {
            self.entries_index.shrink_to(SHRINK_CACHE_CAPACITY);
        }
    }

    pub fn new(
        region_id: u64,
        cache_limit: usize,
        global_stats: Arc<GlobalStats>,
    ) -> MemTable<E, W> {
        MemTable::<E, W> {
            region_id,
            entries_cache: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            entries_index: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            rewrite_count: 0,
            kvs: HashMap::default(),

            cache_size: 0,
            cache_limit,
            global_stats,
            _phantom: PhantomData,
        }
    }

    pub fn append(&mut self, entries: Vec<E>, entries_index: Vec<EntryIndex>) {
        assert_eq!(entries.len(), entries_index.len());
        if entries.is_empty() {
            return;
        }

        let first_index_to_add = W::index(&entries[0]);
        self.cut_entries_cache(first_index_to_add);
        self.cut_entries_index(first_index_to_add);

        if let Some((queue, index)) = self.entries_index.back().map(|e| (e.queue, e.index)) {
            if first_index_to_add > index + 1 {
                if queue != LogQueue::Rewrite {
                    panic!("memtable {} has a hole", self.region_id);
                }
                self.compact_to(index + 1);
            }
        }

        let delta_size = entries_index.iter().fold(0, |acc, i| acc + i.len as usize);
        self.entries_index.extend(entries_index);
        if self.cache_limit > 0 {
            self.entries_cache.extend(entries);
            self.cache_size += delta_size;
        }

        // Evict front entries from cache when reaching cache size limitation.
        while self.cache_size > self.cache_limit && !self.entries_cache.is_empty() {
            let distance = self.cache_distance();
            self.entries_cache.pop_front().unwrap();
            let entry_index = &mut self.entries_index[distance];
            entry_index.cache_tracker.take();

            self.cache_size -= entry_index.len as usize;
            self.global_stats.sub_mem_change(entry_index.len as usize);
        }
    }

    pub fn append_rewrite(&mut self, entries: Vec<E>, mut entries_index: Vec<EntryIndex>) {
        for ei in &mut entries_index {
            ei.queue = LogQueue::Rewrite;
        }
        self.append(entries, entries_index);

        let new_rewrite_count = self.entries_index.len();
        self.adjust_rewrite_count(new_rewrite_count);
    }

    pub fn rewrite(&mut self, entries_index: Vec<EntryIndex>, latest_rewrite: Option<u64>) {
        if !entries_index.is_empty() {
            self.global_stats.add_rewrite(entries_index.len());
            let compacted_rewrite = self.rewrite_impl(entries_index, latest_rewrite);
            self.global_stats.add_compacted_rewrite(compacted_rewrite);
        }
    }

    // Try to rewrite some entries which have already been written into the rewrite queue.
    // However some are not necessary any more, so that return `compacted_rewrite_operations`.
    fn rewrite_impl(
        &mut self,
        mut entries_index: Vec<EntryIndex>,
        latest_rewrite: Option<u64>,
    ) -> usize {
        if self.entries_index.is_empty() {
            // All entries are compacted.
            return entries_index.len();
        }

        let mut compacted_rewrite_operations = 0;

        let front = self.entries_index.front().unwrap().index;
        let back = self.entries_index.back().unwrap().index;
        let mut first = entries_index.first().unwrap().index;
        debug_assert!(first <= self.entries_index[self.rewrite_count].index);
        let last = entries_index.last().unwrap().index;

        if last < front {
            // All rewritten entries are compacted.
            return entries_index.len();
        }

        if first < front {
            // Some of head entries in `entries_index` are compacted.
            let offset = (front - first) as usize;
            entries_index.drain(..offset);
            first = entries_index.first().unwrap().index;
            compacted_rewrite_operations += offset;
        }

        let distance = (first - front) as usize;
        let len = (cmp::min(last, back) - first + 1) as usize;
        for (i, ei) in entries_index.iter().take(len).enumerate() {
            if let Some(latest_rewrite) = latest_rewrite {
                debug_assert_eq!(self.entries_index[i + distance].queue, LogQueue::Append);
                if self.entries_index[i + distance].file_num > latest_rewrite {
                    // Some entries are overwritten by new appends.
                    compacted_rewrite_operations += entries_index.len() - i;
                    break;
                }
            } else {
                // It's a squeeze operation.
                debug_assert_eq!(self.entries_index[i + distance].queue, LogQueue::Rewrite);
            }

            if self.entries_index[i + distance].queue != LogQueue::Rewrite {
                debug_assert_eq!(ei.queue, LogQueue::Rewrite);
                self.entries_index[i + distance].queue = LogQueue::Rewrite;
                self.rewrite_count += 1;
            }

            self.entries_index[i + distance].file_num = ei.file_num;
            self.entries_index[i + distance].base_offset = ei.base_offset;
            self.entries_index[i + distance].compression_type = ei.compression_type;
            self.entries_index[i + distance].batch_len = ei.batch_len;
        }
        compacted_rewrite_operations
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, file_num: u64) {
        if let Some(origin) = self.kvs.insert(key, (value, LogQueue::Append, file_num)) {
            if origin.1 == LogQueue::Rewrite {
                self.global_stats.add_compacted_rewrite(1);
            }
        }
    }

    pub fn rewrite_key(&mut self, key: Vec<u8>, latest_rewrite: Option<u64>, file_num: u64) {
        self.global_stats.add_rewrite(1);
        if let Some(value) = self.kvs.get_mut(&key) {
            if value.1 == LogQueue::Append {
                if let Some(latest_rewrite) = latest_rewrite {
                    if value.2 <= latest_rewrite {
                        value.1 = LogQueue::Rewrite;
                        value.2 = file_num;
                    }
                } else {
                    // The rewritten key/value pair has been overwritten.
                    self.global_stats.add_compacted_rewrite(1);
                }
            } else {
                assert!(value.2 <= file_num);
                value.2 = file_num;
            }
        } else {
            // The rewritten key/value pair has been compacted.
            self.global_stats.add_compacted_rewrite(1);
        }
    }

    pub fn put_rewrite(&mut self, key: Vec<u8>, value: Vec<u8>, file_num: u64) {
        self.kvs.insert(key, (value, LogQueue::Rewrite, file_num));
        self.global_stats.add_rewrite(1);
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
        self.compact_cache_to(idx);

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
        self.adjust_rewrite_count(self.rewrite_count - rewrite_sub);
        drain_end as u64
    }

    pub fn compact_cache_to(&mut self, mut idx: u64) {
        let first_idx = match self.entries_cache.front() {
            Some(e) if W::index(e) < idx => W::index(e),
            _ => return,
        };
        let last_index = W::index(self.entries_cache.back().unwrap());
        assert!(last_index == self.entries_index.back().unwrap().index);
        idx = cmp::min(last_index + 1, idx);

        let distance = self.cache_distance();
        let drain_end = (idx - first_idx) as usize;
        self.entries_cache.drain(0..drain_end);

        for i in 0..drain_end {
            let entry_index = &mut self.entries_index[distance + i];
            entry_index.cache_tracker.take();
            self.cache_size -= entry_index.len as usize;
            self.global_stats.sub_mem_change(entry_index.len as usize);
        }
        self.shrink_entries_cache();
    }

    // If entry exist in cache, return (Entry, None).
    // If entry exist but not in cache, return (None, EntryIndex).
    // If entry not exist, return (None, None).
    pub fn get_entry(&self, index: u64) -> (Option<E>, Option<EntryIndex>) {
        if self.entries_index.is_empty() {
            return (None, None);
        }

        let first_index = self.entries_index.front().unwrap().index;
        let last_index = self.entries_index.back().unwrap().index;
        if index < first_index || index > last_index {
            return (None, None);
        }

        let ioffset = (index - first_index) as usize;
        let cache_distance = self.cache_distance();
        if ioffset < cache_distance {
            self.global_stats.add_cache_miss(1);
            let entry_index = self.entries_index[ioffset].clone();
            (None, Some(entry_index))
        } else {
            self.global_stats.add_cache_hit(1);
            let coffset = ioffset - cache_distance;
            let entry = self.entries_cache[coffset].clone();
            (Some(entry), None)
        }
    }

    pub fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<E>,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<()> {
        assert!(end > begin, "fetch_entries_to({}, {})", begin, end);
        let (vec_len, vec_idx_len) = (vec.len(), vec_idx.len());

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

        let cache_offset = self.cache_distance();
        if cache_offset < end_pos {
            if start_pos >= cache_offset {
                // All needed entries are in cache.
                let low = start_pos - cache_offset;
                let high = end_pos - cache_offset;
                let (first, second) = slices_in_range(&self.entries_cache, low, high);
                vec.extend_from_slice(first);
                vec.extend_from_slice(second);
            } else {
                // Partial needed entries are in cache.
                let high = end_pos - cache_offset;
                let (first, second) = slices_in_range(&self.entries_cache, 0, high);
                vec.extend_from_slice(first);
                vec.extend_from_slice(second);

                // Entries that not in cache should return their indices.
                let (first, second) = slices_in_range(&self.entries_index, start_pos, cache_offset);
                vec_idx.extend_from_slice(first);
                vec_idx.extend_from_slice(second);
            }
        } else {
            // All needed entries are not in cache
            let (first, second) = slices_in_range(&self.entries_index, start_pos, end_pos);
            vec_idx.extend_from_slice(first);
            vec_idx.extend_from_slice(second);
        }

        let (hit, miss) = (vec.len() - vec_len, vec_idx.len() - vec_idx_len);
        self.global_stats.add_cache_hit(hit);
        self.global_stats.add_cache_miss(miss);
        Ok(())
    }

    pub fn fetch_rewrite_entries(
        &self,
        latest_rewrite: u64,
        vec: &mut Vec<E>,
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
            .find(|e| e.file_num <= latest_rewrite);
        if let (Some(begin), Some(end)) = (begin, end) {
            if begin.index <= end.index {
                return self.fetch_entries_to(begin.index, end.index + 1, None, vec, vec_idx);
            }
        }
        Ok(())
    }

    pub fn fetch_entries_from_rewrite(
        &self,
        vec: &mut Vec<E>,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<()> {
        if self.rewrite_count > 0 {
            let end = self.entries_index[self.rewrite_count].index;
            let first = self.entries_index.front().unwrap().index;
            return self.fetch_entries_to(first, end, None, vec, vec_idx);
        }
        Ok(())
    }

    pub fn fetch_rewrite_kvs(&self, latest_rewrite: u64, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, (value, queue, file_num)) in &self.kvs {
            if *queue == LogQueue::Append && *file_num <= latest_rewrite {
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

    pub fn min_file_num(&self, queue: LogQueue) -> Option<u64> {
        let entry = match queue {
            LogQueue::Append => self.entries_index.get(self.rewrite_count),
            LogQueue::Rewrite if self.rewrite_count == 0 => None,
            LogQueue::Rewrite => self.entries_index.front(),
        };
        let ents_min = entry.map(|e| e.file_num);
        let kvs_min = self.kvs_min_file_num(queue);
        match (ents_min, kvs_min) {
            (Some(ents_min), Some(kvs_min)) => Some(cmp::min(ents_min, kvs_min)),
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

    pub fn kvs_min_file_num(&self, queue: LogQueue) -> Option<u64> {
        self.kvs
            .values()
            .filter(|v| v.1 == queue)
            .fold(None, |min, v| Some(cmp::min(min.unwrap_or(u64::MAX), v.2)))
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

impl<E: Message + Clone, W: EntryExt<E>> Drop for MemTable<E, W> {
    fn drop(&mut self) {
        // Drop `cache_tracker`s and sub mem change.
        self.entries_index.clear();
        self.global_stats.sub_mem_change(self.cache_size as usize);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use raft::eraftpb::Entry;

    impl<E: Message + Clone, W: EntryExt<E>> MemTable<E, W> {
        pub fn max_file_num(&self, queue: LogQueue) -> Option<u64> {
            let entry = match queue {
                LogQueue::Append if self.rewrite_count == self.entries_index.len() => None,
                LogQueue::Append => self.entries_index.back(),
                LogQueue::Rewrite if self.rewrite_count == 0 => None,
                LogQueue::Rewrite => self.entries_index.get(self.rewrite_count - 1),
            };
            let ents_max = entry.map(|e| e.file_num);

            let kvs_max = self.kvs_max_file_num(queue);
            match (ents_max, kvs_max) {
                (Some(ents_max), Some(kvs_max)) => Some(cmp::max(ents_max, kvs_max)),
                (Some(ents_max), None) => Some(ents_max),
                (None, Some(kvs_max)) => Some(kvs_max),
                (None, None) => None,
            }
        }

        pub fn kvs_max_file_num(&self, queue: LogQueue) -> Option<u64> {
            self.kvs
                .values()
                .filter(|v| v.1 == queue)
                .fold(None, |max, v| Some(cmp::max(max.unwrap_or(0), v.2)))
        }

        pub fn fetch_all(&self, vec: &mut Vec<E>, vec_idx: &mut Vec<EntryIndex>) {
            if self.entries_index.is_empty() {
                return;
            }

            let begin = self.entries_index.front().unwrap().index;
            let end = self.entries_index.back().unwrap().index + 1;
            self.fetch_entries_to(begin, end, None, vec, vec_idx)
                .unwrap();
        }

        fn entries_size(&self) -> usize {
            self.entries_index.iter().fold(0, |acc, e| acc + e.len) as usize
        }

        fn check_entries_index_and_cache(&self) {
            match (self.entries_index.back(), self.entries_cache.back()) {
                (Some(ei), Some(ec)) if ei.index != W::index(ec) => panic!(
                    "entries_index.last = {}, entries_cache.last = {}",
                    ei.index,
                    W::index(ec)
                ),
                (None, Some(_)) => panic!("entries_index is empty, but entries_cache isn't"),
                _ => return,
            }
        }
    }

    #[test]
    fn test_memtable_append() {
        let region_id = 8;
        let cache_limit = 15;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats);

        // Append entries [10, 20) file_num = 1 not over cache size limitation.
        // after appending
        // [10, 20) file_num = 1, in cache
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 1));
        assert_eq!(memtable.cache_size, 10);
        assert_eq!(memtable.entries_size(), 10);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 1);
        memtable.check_entries_index_and_cache();

        // Append entries [20, 30) file_num = 2, over cache size limitation 15,
        // after appending:
        // [10, 15) file_num = 1, not in cache
        // [15, 20) file_num = 1, in cache
        // [20, 30) file_num = 2, in cache
        memtable.append(generate_ents(20, 30), generate_ents_index(20, 30, 2));
        assert_eq!(memtable.cache_size, 15);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 20);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[14].get_index(), 29);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[19].index, 29);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 2);
        memtable.check_entries_index_and_cache();

        // Overlap Appending, partial overlap with cache.
        // Append entries [25, 35) file_num = 3, will truncate
        // tail entries from cache and indices.
        // After appending:
        // [10, 20) file_num = 1, not in cache
        // [20, 25) file_num = 2, in cache
        // [25, 35) file_num = 3, in cache
        memtable.append(generate_ents(25, 35), generate_ents_index(25, 35, 3));
        assert_eq!(memtable.cache_size, 15);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 25);
        assert_eq!(memtable.entries_cache[0].get_index(), 20);
        assert_eq!(memtable.entries_cache[14].get_index(), 34);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[24].index, 34);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 3);
        memtable.check_entries_index_and_cache();

        // Overlap Appending, whole overlap with cache.
        // Append entries [20, 40) file_num = 4.
        // After appending:
        // [10, 20) file_num = 1, not in cache
        // [20, 25) file_num = 4, not in cache
        // [25, 40) file_num = 4, in cache
        memtable.append(generate_ents(20, 40), generate_ents_index(20, 40, 4));
        assert_eq!(memtable.cache_size, 15);
        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 30);
        assert_eq!(memtable.entries_cache[0].get_index(), 25);
        assert_eq!(memtable.entries_cache[14].get_index(), 39);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[29].index, 39);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 4);
        memtable.check_entries_index_and_cache();

        // Overlap Appending, whole overlap with index.
        // Append entries [10, 30) file_num = 5.
        // After appending:
        // [10, 15) file_num = 5, not in cache
        // [15, 30) file_num = 5, in cache
        memtable.append(generate_ents(10, 30), generate_ents_index(10, 30, 5));
        assert_eq!(memtable.cache_size, 15);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 20);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[14].get_index(), 29);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[19].index, 29);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 5);
        memtable.check_entries_index_and_cache();

        // Cache with size limit 0.
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, 0, stats);
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 1));
        assert_eq!(memtable.cache_size, 0);
        assert_eq!(memtable.entries_cache.len(), 0);
        assert_eq!(memtable.entries_size(), 10);
        assert_eq!(memtable.entries_index.len(), 10);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[9].index, 19);
        memtable.check_entries_index_and_cache();
    }

    #[test]
    fn test_memtable_compact() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats);

        // After appending:
        // [0, 10) file_num = 1, not in cache
        // [10, 15) file_num = 2, not in cache
        // [15, 20) file_num = 2, in cache
        // [20, 25) file_num = 3, in cache
        memtable.append(generate_ents(0, 10), generate_ents_index(0, 10, 1));
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));
        memtable.append(generate_ents(20, 25), generate_ents_index(20, 25, 3));
        assert_eq!(memtable.cache_size, 10);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.entries_index.len(), 25);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[9].get_index(), 24);
        assert_eq!(memtable.entries_index[0].index, 0);
        assert_eq!(memtable.entries_index[24].index, 24);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 3);
        memtable.check_entries_index_and_cache();

        // Compact to 5.
        // Only index is needed to compact.
        assert_eq!(memtable.compact_to(5), 5);
        assert_eq!(memtable.cache_size, 10);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.entries_index.len(), 20);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[9].get_index(), 24);
        assert_eq!(memtable.entries_index[0].index, 5);
        assert_eq!(memtable.entries_index[19].index, 24);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 3);
        memtable.check_entries_index_and_cache();

        // Compact to 20.
        // Both index and cache  need compaction.
        assert_eq!(memtable.compact_to(20), 15);
        assert_eq!(memtable.entries_size(), memtable.cache_size);
        assert_eq!(memtable.entries_size(), 5);
        assert_eq!(memtable.entries_cache.len(), 5);
        assert_eq!(memtable.entries_index.len(), 5);
        assert_eq!(memtable.entries_cache[0].get_index(), 20);
        assert_eq!(memtable.entries_cache[4].get_index(), 24);
        assert_eq!(memtable.entries_index[0].index, 20);
        assert_eq!(memtable.entries_index[4].index, 24);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 3);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 3);
        memtable.check_entries_index_and_cache();

        // Compact to 20 or smaller index, nothing happens.
        assert_eq!(memtable.compact_to(20), 0);
        assert_eq!(memtable.compact_to(15), 0);
        memtable.check_entries_index_and_cache();
    }

    #[test]
    fn test_memtable_compact_cache() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats);

        // After appending:
        // [0, 10) file_num = 1, not in cache
        // [10, 15) file_num = 2, not in cache
        // [15, 20) file_num = 2, in cache
        // [20, 25) file_num = 3, in cache
        memtable.append(generate_ents(0, 10), generate_ents_index(0, 10, 1));
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));
        memtable.append(generate_ents(20, 25), generate_ents_index(20, 25, 3));
        assert_eq!(memtable.cache_size, 10);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.entries_index.len(), 25);
        memtable.check_entries_index_and_cache();

        // Compact cache to 15, nothing needs to be changed.
        memtable.compact_cache_to(15);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.cache_size, 10);
        memtable.check_entries_index_and_cache();

        // Compact cache to 20.
        memtable.compact_to(20);
        assert_eq!(memtable.entries_cache.len(), 5);
        assert_eq!(memtable.cache_size, 5);
        memtable.check_entries_index_and_cache();

        // Compact cache to 25
        memtable.compact_cache_to(25);
        assert_eq!(memtable.entries_cache.len(), 0);
        assert_eq!(memtable.cache_size, 0);
        memtable.check_entries_index_and_cache();
    }

    #[test]
    fn test_memtable_fetch() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats.clone());

        // After appending:
        // [0, 10) file_num = 1, not in cache
        // [10, 15) file_num = 2, not in cache
        // [15, 20) file_num = 2, in cache
        // [20, 25) file_num = 3, in cache
        memtable.append(generate_ents(0, 10), generate_ents_index(0, 10, 1));
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));
        memtable.append(generate_ents(20, 25), generate_ents_index(20, 25, 3));

        // Fetching all
        // Only latest 10 entries are in cache.
        let mut ents = vec![];
        let mut ents_idx = vec![];
        memtable.fetch_all(&mut ents, &mut ents_idx);
        assert_eq!(ents.len(), 10);
        assert_eq!(ents[0].get_index(), 15);
        assert_eq!(ents[9].get_index(), 24);
        assert_eq!(ents_idx.len(), 15);
        assert_eq!(ents_idx[0].index, 0);
        assert_eq!(ents_idx[14].index, 14);
        assert_eq!(stats.cache_hit(), 10);
        assert_eq!(stats.cache_miss(), 15);

        // After compact:
        // [10, 15) file_num = 2, not in cache
        // [15, 20) file_num = 2, in cache
        // [20, 25) file_num = 3, in cache
        assert_eq!(memtable.compact_to(10), 10);

        // Out of range fetching
        ents.clear();
        ents_idx.clear();
        assert!(memtable
            .fetch_entries_to(5, 15, None, &mut ents, &mut ents_idx)
            .is_err());

        // Out of range fetching
        ents.clear();
        ents_idx.clear();
        assert!(memtable
            .fetch_entries_to(20, 30, None, &mut ents, &mut ents_idx)
            .is_err());

        // All needed entries are in cache.
        ents.clear();
        ents_idx.clear();
        stats.reset_cache();
        memtable
            .fetch_entries_to(20, 25, None, &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 5);
        assert_eq!(ents[0].get_index(), 20);
        assert_eq!(ents[4].get_index(), 24);
        assert!(ents_idx.is_empty());
        assert_eq!(stats.cache_hit(), 5);

        // All needed entries are not in cache.
        ents.clear();
        ents_idx.clear();
        stats.reset_cache();
        memtable
            .fetch_entries_to(10, 15, None, &mut ents, &mut ents_idx)
            .unwrap();
        assert!(ents.is_empty());
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[4].index, 14);
        assert_eq!(stats.cache_miss(), 5);

        // Some needed entries are in cache, the others are not.
        ents.clear();
        ents_idx.clear();
        stats.reset_cache();
        memtable
            .fetch_entries_to(10, 25, None, &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 10);
        assert_eq!(ents[0].get_index(), 15);
        assert_eq!(ents[9].get_index(), 24);
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[4].index, 14);
        assert_eq!(stats.cache_hit(), 10);
        assert_eq!(stats.cache_miss(), 5);

        // Max size limitation range fetching.
        // Only can fetch [10, 20) because of size limitation,
        // and [10, 15) is not in cache, [15, 20) is in cache.
        ents.clear();
        ents_idx.clear();
        let max_size = Some(10);
        stats.reset_cache();
        memtable
            .fetch_entries_to(10, 25, max_size, &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 5);
        assert_eq!(ents[0].get_index(), 15);
        assert_eq!(ents[4].get_index(), 19);
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[4].index, 14);
        assert_eq!(stats.cache_hit(), 5);
        assert_eq!(stats.cache_miss(), 5);

        // Even max size limitation is 0, at least fetch one entry.
        ents.clear();
        ents_idx.clear();
        stats.reset_cache();
        memtable
            .fetch_entries_to(20, 25, Some(0), &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 1);
        assert_eq!(ents[0].get_index(), 20);
        assert!(ents_idx.is_empty());
        assert_eq!(stats.cache_hit(), 1);
    }

    #[test]
    fn test_memtable_fetch_rewrite() {
        let region_id = 8;
        let cache_limit = 0;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats.clone());

        // After appending:
        // [0, 10) file_num = 1
        // [10, 20) file_num = 2
        // [20, 30) file_num = 3
        memtable.append(generate_ents(0, 10), generate_ents_index(0, 10, 1));
        memtable.put(b"k1".to_vec(), b"v1".to_vec(), 1);
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));
        memtable.put(b"k2".to_vec(), b"v2".to_vec(), 2);
        memtable.append(generate_ents(20, 25), generate_ents_index(20, 25, 3));
        memtable.put(b"k3".to_vec(), b"v3".to_vec(), 3);

        // After rewriting:
        // [0, 10) queue = rewrite, file_num = 50,
        // [10, 20) file_num = 2
        // [20, 30) file_num = 3
        memtable.rewrite(generate_rewrite_ents_index(0, 10, 50), Some(1));
        memtable.rewrite_key(b"k1".to_vec(), Some(1), 50);

        let (mut ents, mut ents_idx) = (vec![], vec![]);

        assert!(memtable
            .fetch_rewrite_entries(2, &mut ents, &mut ents_idx)
            .is_ok());
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx.first().unwrap().index, 10);
        assert_eq!(ents_idx.last().unwrap().index, 19);

        ents.clear();
        ents_idx.clear();
        assert!(memtable
            .fetch_entries_from_rewrite(&mut ents, &mut ents_idx)
            .is_ok());
        assert_eq!(ents_idx.len(), 10);
        assert_eq!(ents_idx.first().unwrap().index, 0);
        assert_eq!(ents_idx.last().unwrap().index, 9);
    }

    #[test]
    fn test_memtable_kv_operations() {
        let region_id = 8;
        let cache_limit = 1024;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats);

        let (k1, v1) = (b"key1", b"value1");
        let (k5, v5) = (b"key5", b"value5");
        memtable.put(k1.to_vec(), v1.to_vec(), 1);
        memtable.put(k5.to_vec(), v5.to_vec(), 5);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 1);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.get(k1.as_ref()), Some(v1.to_vec()));
        assert_eq!(memtable.get(k5.as_ref()), Some(v5.to_vec()));

        memtable.delete(k5.as_ref());
        assert_eq!(memtable.get(k5.as_ref()), None);
    }

    #[test]
    fn test_memtable_get_entry() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats);

        // [5, 10) file_num = 1, not in cache
        // [10, 20) file_num = 2, in cache
        memtable.append(generate_ents(5, 10), generate_ents_index(5, 10, 1));
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));

        // Not in range.
        assert_eq!(memtable.get_entry(2), (None, None));
        assert_eq!(memtable.get_entry(25), (None, None));

        // In cache.
        let (entry, _) = memtable.get_entry(10);
        assert_eq!(entry.unwrap().get_index(), 10);

        // Not in cache.
        let (_, entry_idx) = memtable.get_entry(5);
        assert_eq!(entry_idx.unwrap().index, 5);
    }

    #[test]
    fn test_memtable_rewrite() {
        let region_id = 8;
        let cache_limit = 15;
        let stats = Arc::new(GlobalStats::default());
        let mut memtable = MemTable::<Entry, Entry>::new(region_id, cache_limit, stats);

        // after appending
        // [10, 20) file_num = 2, not in cache
        // [20, 30) file_num = 3, 5 not in cache, 5 in cache
        // [30, 40) file_num = 4, in cache
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));
        memtable.put(b"kk1".to_vec(), b"vv1".to_vec(), 2);
        memtable.append(generate_ents(20, 30), generate_ents_index(20, 30, 3));
        memtable.put(b"kk2".to_vec(), b"vv2".to_vec(), 3);
        memtable.append(generate_ents(30, 40), generate_ents_index(30, 40, 4));
        memtable.put(b"kk3".to_vec(), b"vv3".to_vec(), 4);

        assert_eq!(memtable.cache_size, 15);
        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 2);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 4);
        memtable.check_entries_index_and_cache();

        // Rewrite compacted entries.
        memtable.rewrite(generate_rewrite_ents_index(0, 10, 50), Some(1));
        memtable.rewrite_key(b"kk0".to_vec(), Some(1), 50);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 2);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 4);
        assert!(memtable.min_file_num(LogQueue::Rewrite).is_none());
        assert!(memtable.max_file_num(LogQueue::Rewrite).is_none());
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.get(b"kk0"), None);
        assert_eq!(memtable.global_stats.rewrite_operations(), 11);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 11);

        // Rewrite compacted entries + valid entries.
        memtable.rewrite(generate_rewrite_ents_index(0, 20, 100), Some(2));
        memtable.rewrite_key(b"kk1".to_vec(), Some(2), 100);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 3);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.min_file_num(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_num(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.rewrite_count, 10);
        assert_eq!(memtable.get(b"kk1"), Some(b"vv1".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 32);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 21);

        // Rewrite vaild entries, some of them are in cache.
        memtable.rewrite(generate_rewrite_ents_index(20, 30, 101), Some(3));
        memtable.rewrite_key(b"kk2".to_vec(), Some(3), 101);
        assert_eq!(memtable.cache_size, 15);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 4);
        assert_eq!(memtable.min_file_num(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_num(LogQueue::Rewrite).unwrap(), 101);
        assert_eq!(memtable.rewrite_count, 20);
        assert_eq!(memtable.get(b"kk2"), Some(b"vv2".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 43);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 21);

        // Rewrite valid + overwritten entries.
        memtable.append(generate_ents(35, 36), generate_ents_index(35, 36, 5));
        memtable.put(b"kk2".to_vec(), b"vv4".to_vec(), 5);
        assert_eq!(memtable.cache_size, 11);
        assert_eq!(memtable.entries_index.back().unwrap().index, 35);
        assert_eq!(memtable.global_stats.rewrite_operations(), 43);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 22);
        memtable.rewrite(generate_rewrite_ents_index(30, 40, 102), Some(4));
        memtable.rewrite_key(b"kk3".to_vec(), Some(4), 102);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 5);
        assert_eq!(memtable.min_file_num(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_num(LogQueue::Rewrite).unwrap(), 102);
        assert_eq!(memtable.rewrite_count, 25);
        assert_eq!(memtable.get(b"kk2"), Some(b"vv4".to_vec()));
        assert_eq!(memtable.global_stats.rewrite_operations(), 54);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 27);

        // Rewrite after compact.
        memtable.append(generate_ents(36, 50), generate_ents_index(36, 50, 6));
        memtable.compact_to(30);
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(memtable.global_stats.rewrite_operations(), 54);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 47);
        memtable.compact_to(40);
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 54);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 52);

        assert_eq!(memtable.cache_size, 10);
        assert_eq!(memtable.entries_index.back().unwrap().index, 49);
        memtable.rewrite(generate_rewrite_ents_index(30, 36, 103), Some(5));
        memtable.rewrite_key(b"kk2".to_vec(), Some(5), 103);
        assert_eq!(memtable.min_file_num(LogQueue::Append).unwrap(), 6);
        assert_eq!(memtable.max_file_num(LogQueue::Append).unwrap(), 6);
        assert_eq!(memtable.min_file_num(LogQueue::Rewrite).unwrap(), 100);
        assert_eq!(memtable.max_file_num(LogQueue::Rewrite).unwrap(), 103);
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 61);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 58);

        // Rewrite after cut.
        memtable.append(generate_ents(50, 55), generate_ents_index(50, 55, 7));
        memtable.rewrite(generate_rewrite_ents_index(30, 50, 104), Some(6));
        assert_eq!(memtable.rewrite_count, 10);
        assert_eq!(memtable.global_stats.rewrite_operations(), 81);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 68);
        memtable.append(generate_ents(45, 50), generate_ents_index(45, 50, 7));
        assert_eq!(memtable.rewrite_count, 5);
        assert_eq!(memtable.global_stats.rewrite_operations(), 81);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 73);
        memtable.append(generate_ents(40, 50), generate_ents_index(40, 50, 7));
        assert_eq!(memtable.rewrite_count, 0);
        assert_eq!(memtable.global_stats.rewrite_operations(), 81);
        assert_eq!(memtable.global_stats.compacted_rewrite_operations(), 78);
    }

    fn generate_ents(begin_idx: u64, end_idx: u64) -> Vec<Entry> {
        assert!(end_idx >= begin_idx);
        let mut ents = vec![];
        for idx in begin_idx..end_idx {
            let mut ent = Entry::new();
            ent.set_index(idx);
            ents.push(ent);
        }
        ents
    }

    fn generate_ents_index(begin_idx: u64, end_idx: u64, file_num: u64) -> Vec<EntryIndex> {
        assert!(end_idx >= begin_idx);
        let mut ents_idx = vec![];
        for idx in begin_idx..end_idx {
            let mut ent_idx = EntryIndex::default();
            ent_idx.index = idx;
            ent_idx.file_num = file_num;
            ent_idx.offset = idx; // fake offset
            ent_idx.len = 1; // fake size
            ents_idx.push(ent_idx);
        }
        ents_idx
    }

    fn generate_rewrite_ents_index(begin_idx: u64, end_idx: u64, file_num: u64) -> Vec<EntryIndex> {
        assert!(end_idx >= begin_idx);
        let mut ents_idx = vec![];
        for idx in begin_idx..end_idx {
            let mut ent_idx = EntryIndex::default();
            ent_idx.index = idx;
            ent_idx.queue = LogQueue::Rewrite;
            ent_idx.file_num = file_num;
            ent_idx.offset = idx; // fake offset
            ent_idx.len = 1; // fake size
            ents_idx.push(ent_idx);
        }
        ents_idx
    }
}
