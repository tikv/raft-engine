use std::collections::VecDeque;
use std::sync::Arc;
use std::{cmp, u64};

use raft::{eraftpb::Entry, StorageError};

use crate::engine::SharedCacheStats;
use crate::log_batch::CompressionType;
use crate::util::{slices_in_range, HashMap};
use crate::{Error, Result};

const SHRINK_CACHE_CAPACITY: usize = 64;

#[derive(Debug, Clone, PartialEq)]
pub struct EntryIndex {
    pub index: u64,

    // Log batch physical position in file.
    pub file_num: u64,
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
            file_num: 0,
            base_offset: 0,
            compression_type: CompressionType::None,
            batch_len: 0,
            offset: 0,
            len: 0,
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

pub struct MemTable {
    region_id: u64,

    // latest N entries
    entries_cache: VecDeque<Entry>,

    // All entries index
    entries_index: VecDeque<EntryIndex>,

    // Region scope key/value pairs
    // key -> (value, file_num)
    kvs: HashMap<Vec<u8>, (Vec<u8>, u64)>,

    cache_size: u64,
    cache_limit: u64,
    cache_stats: Arc<SharedCacheStats>,
}

impl MemTable {
    fn cache_distance(&self) -> usize {
        if self.entries_cache.is_empty() {
            return self.entries_index.len();
        }
        let distance = self.entries_index.len() - self.entries_cache.len();
        let cache_first = self.entries_cache[0].index;
        let index_first = self.entries_index[distance].index;
        assert_eq!(cache_first, index_first);
        distance
    }

    // Remove all cached entries with index greater than or equal to the given.
    fn cut_entries_cache(&mut self, index: u64) {
        if self.entries_cache.is_empty() {
            return;
        }
        let last_index = self.entries_cache.back().unwrap().index;
        let first_index = self.entries_cache.front().unwrap().index;
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
            let delta = self.entries_index[distance + offset].len;
            self.cache_size -= delta;
            self.cache_stats.sub_mem_change(delta);
        }

        self.entries_cache.truncate(conflict);
    }

    // Remove all entry indexes with index greater than or equal to the given.
    fn cut_entries_index(&mut self, index: u64) {
        if self.entries_index.is_empty() {
            return;
        }
        let last_index = self.entries_index.back().unwrap().index;
        let first_index = self.entries_index.front().unwrap().index;
        assert!(first_index <= index); // Compacted entries can't be overwritten.
        let conflict = if index <= last_index {
            (index - first_index) as usize
        } else {
            return;
        };

        let mut total_size_delta = 0;
        for offset in conflict..self.entries_index.len() {
            total_size_delta += self.entries_index[offset].len;
        }
        self.cache_stats.sub_total_size(total_size_delta);

        self.entries_index.truncate(conflict);
    }

    pub fn new(region_id: u64, cache_limit: u64, cache_stats: Arc<SharedCacheStats>) -> MemTable {
        MemTable {
            region_id,
            entries_cache: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            entries_index: VecDeque::with_capacity(SHRINK_CACHE_CAPACITY),
            kvs: HashMap::default(),

            cache_size: 0,
            cache_limit,
            cache_stats: cache_stats,
        }
    }

    pub fn append(&mut self, entries: Vec<Entry>, entries_index: Vec<EntryIndex>) {
        assert_eq!(entries.len(), entries_index.len());
        if entries.is_empty() {
            return;
        }

        let first_index_to_add = entries[0].index;
        self.cut_entries_cache(first_index_to_add);
        self.cut_entries_index(first_index_to_add);

        let delta_size = entries_index.iter().fold(0, |acc, i| acc + i.len);
        self.entries_index.extend(entries_index);
        self.cache_stats.add_total_size(delta_size);
        if self.cache_limit > 0 {
            self.entries_cache.extend(entries);
            self.cache_size += delta_size;
            self.cache_stats.add_mem_change(delta_size);
        }

        // Evict front entries from cache when reaching cache size limitation.
        while self.cache_size > self.cache_limit && !self.entries_cache.is_empty() {
            let distance = self.cache_distance();
            self.entries_cache.pop_front().unwrap();
            let delta = self.entries_index[distance].len;
            self.cache_size -= delta;
            self.cache_stats.sub_mem_change(delta);
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, file_num: u64) {
        self.kvs.insert(key, (value, file_num));
    }

    pub fn delete(&mut self, key: &[u8]) {
        self.kvs.remove(key);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        self.kvs.get(key).map(|v| v.0.clone())
    }

    /// # Panics
    ///
    /// This method will panic if `idx` is greater than `last_idx + 1`.
    pub fn compact_to(&mut self, idx: u64) -> u64 {
        self.compact_cache_to(idx);

        let first_idx = match self.entries_index.front() {
            Some(e) if e.index < idx => e.index,
            _ => return 0,
        };
        let last_idx = self.entries_index.back().unwrap().index;
        assert!(idx <= last_idx + 1);
        let drain_end = (idx - first_idx) as usize;

        let mut total_size_delta = 0;
        for e in self.entries_index.drain(..drain_end) {
            total_size_delta += e.len;
        }
        self.cache_stats.add_compacted_size(total_size_delta);

        drain_end as u64
    }

    /// # Panics
    ///
    /// This method will panic if `idx` is greater than `last_idx + 1`.
    pub fn compact_cache_to(&mut self, idx: u64) {
        let first_idx = match self.entries_cache.front() {
            Some(e) if e.index < idx => e.index,
            _ => return,
        };
        let last_index = self.entries_index.back().unwrap().index;
        assert!(idx <= last_index + 1);

        let distance = self.cache_distance();
        let drain_end = (idx - first_idx) as usize;
        self.entries_cache.drain(0..drain_end);

        for i in 0..drain_end {
            let delta = self.entries_index[distance + i].len;
            self.cache_size -= delta;
            self.cache_stats.sub_mem_change(delta);
        }
    }

    // If entry exist in cache, return (Entry, None).
    // If entry exist but not in cache, return (None, EntryIndex).
    // If entry not exist, return (None, None).
    pub fn get_entry(&self, index: u64) -> (Option<Entry>, Option<EntryIndex>) {
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
            self.cache_stats.miss_cache(1);
            let entry_index = self.entries_index[ioffset].clone();
            (None, Some(entry_index))
        } else {
            self.cache_stats.hit_cache(1);
            let coffset = ioffset - cache_distance;
            let entry = self.entries_cache[coffset].clone();
            (Some(entry), None)
        }
    }

    pub(crate) fn fetch_entries_to(
        &self,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<Entry>,
        vec_idx: &mut Vec<EntryIndex>,
    ) -> Result<()> {
        assert!(end > begin, "fetch_entries_to({}, {})", begin, end);
        let (vec_len, vec_idx_len) = (vec.len(), vec_idx.len());

        if self.entries_index.is_empty() {
            return Err(Error::Storage(StorageError::Unavailable));
        }
        let first_index = self.entries_index.front().unwrap().index;
        if begin < first_index {
            return Err(Error::Storage(StorageError::Compacted));
        }
        let last_index = self.entries_index.back().unwrap().index;
        if end > last_index + 1 {
            return Err(Error::Storage(StorageError::Unavailable));
        }

        let start_pos = (begin - first_index) as usize;
        let mut end_pos = (end - begin) as usize + start_pos;

        // Check max size limitation.
        if let Some(max_size) = max_size {
            let count_limit = self.count_limit(start_pos, end_pos, max_size);
            end_pos = start_pos + count_limit;
        }

        let cache_first_index = self.entries_cache.front().unwrap().get_index();
        let cache_offset = (cache_first_index - first_index) as usize;
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
        self.cache_stats.hit_cache(vec.len() - vec_len);
        self.cache_stats.miss_cache(vec_idx.len() - vec_idx_len);
        Ok(())
    }

    pub fn fetch_all(&self, vec: &mut Vec<Entry>, vec_idx: &mut Vec<EntryIndex>) {
        if self.entries_index.is_empty() {
            return;
        }

        let begin = self.entries_index.front().unwrap().index;
        let end = self.entries_index.back().unwrap().index + 1;
        self.fetch_entries_to(begin, end, None, vec, vec_idx)
            .unwrap();
    }

    pub fn fetch_all_kvs(&self, vec: &mut Vec<(Vec<u8>, Vec<u8>)>) {
        for (key, value) in &self.kvs {
            vec.push((key.clone(), value.0.clone()));
        }
    }

    pub fn min_file_num(&self) -> Option<u64> {
        let ents_min = self.entries_index.front().map(|idx| idx.file_num);
        let kvs_min = self.kvs_min_file_num();
        match (ents_min, kvs_min) {
            (Some(ents_min), Some(kvs_min)) => Some(cmp::min(ents_min, kvs_min)),
            (Some(ents_min), None) => Some(ents_min),
            (None, Some(kvs_min)) => Some(kvs_min),
            (None, None) => None,
        }
    }

    pub fn max_file_num(&self) -> Option<u64> {
        let ents_max = self.entries_index.back().map(|idx| idx.file_num);
        let kvs_max = self.kvs_max_file_num();
        match (ents_max, kvs_max) {
            (Some(ents_max), Some(kvs_max)) => Some(cmp::max(ents_max, kvs_max)),
            (Some(ents_max), None) => Some(ents_max),
            (None, Some(kvs_max)) => Some(kvs_max),
            (None, None) => None,
        }
    }

    pub fn kvs_total_count(&self) -> usize {
        self.kvs.len()
    }

    pub fn entries_count(&self) -> usize {
        self.entries_index.len()
    }

    pub fn cache_size(&self) -> u64 {
        self.cache_size
    }

    pub fn region_id(&self) -> u64 {
        self.region_id
    }

    pub fn first_index(&self) -> Option<u64> {
        self.entries_index.front().map(|e| e.index)
    }

    fn kvs_min_file_num(&self) -> Option<u64> {
        if self.kvs.is_empty() {
            return None;
        }
        Some(
            self.kvs
                .values()
                .fold(u64::MAX, |min, v| cmp::min(min, v.1)),
        )
    }

    fn kvs_max_file_num(&self) -> Option<u64> {
        if self.kvs.is_empty() {
            return None;
        }
        Some(self.kvs.values().fold(0, |max, v| cmp::max(max, v.1)))
    }

    fn count_limit(&self, start_idx: usize, end_idx: usize, max_size: usize) -> usize {
        assert!(start_idx < end_idx);
        let (first, second) = slices_in_range(&self.entries_index, start_idx, end_idx);

        let mut count = 0;
        let mut total_size = 0;
        for i in first {
            count += 1;
            total_size += i.len;
            if total_size as usize > max_size {
                // No matter max_size's value, fetch one entry at lease.
                return if count > 1 { count - 1 } else { count };
            }
        }
        for i in second {
            count += 1;
            total_size += i.len;
            if total_size as usize > max_size {
                return if count > 1 { count - 1 } else { count };
            }
        }
        count
    }

    #[cfg(test)]
    fn entries_size(&self) -> u64 {
        self.entries_index.iter().fold(0, |acc, e| acc + e.len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use raft::eraftpb::Entry;

    #[test]
    fn test_memtable_append() {
        let region_id = 8;
        let cache_limit = 15;
        let stats = Arc::new(SharedCacheStats::default());
        let mut memtable = MemTable::new(region_id, cache_limit, stats);

        // Append entries [10, 20) file_num = 1 not over cache size limitation.
        // after appending
        // [10, 20) file_num = 1, in cache
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 1));
        assert_eq!(memtable.cache_size(), 10);
        assert_eq!(memtable.entries_size(), 10);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 1);

        // Append entries [20, 30) file_num = 2, over cache size limitation 15,
        // after appending:
        // [10, 15) file_num = 1, not in cache
        // [15, 20) file_num = 1, in cache
        // [20, 30) file_num = 2, in cache
        memtable.append(generate_ents(20, 30), generate_ents_index(20, 30, 2));
        assert_eq!(memtable.cache_size(), 15);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 20);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[14].get_index(), 29);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[19].index, 29);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 2);

        // Overlap Appending, partial overlap with cache.
        // Append entries [25, 35) file_num = 3, will truncate
        // tail entries from cache and indices.
        // After appending:
        // [10, 20) file_num = 1, not in cache
        // [20, 25) file_num = 2, in cache
        // [25, 35) file_num = 3, in cache
        memtable.append(generate_ents(25, 35), generate_ents_index(25, 35, 3));
        assert_eq!(memtable.cache_size(), 15);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 25);
        assert_eq!(memtable.entries_cache[0].get_index(), 20);
        assert_eq!(memtable.entries_cache[14].get_index(), 34);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[24].index, 34);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // Overlap Appending, whole overlap with cache.
        // Append entries [20, 40) file_num = 4.
        // After appending:
        // [10, 20) file_num = 1, not in cache
        // [20, 25) file_num = 4, not in cache
        // [25, 40) file_num = 4, in cache
        memtable.append(generate_ents(20, 40), generate_ents_index(20, 40, 4));
        assert_eq!(memtable.cache_size(), 15);
        assert_eq!(memtable.entries_size(), 30);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 30);
        assert_eq!(memtable.entries_cache[0].get_index(), 25);
        assert_eq!(memtable.entries_cache[14].get_index(), 39);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[29].index, 39);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 4);

        // Overlap Appending, whole overlap with index.
        // Append entries [10, 30) file_num = 5.
        // After appending:
        // [10, 15) file_num = 5, not in cache
        // [15, 30) file_num = 5, in cache
        memtable.append(generate_ents(10, 30), generate_ents_index(10, 30, 5));
        assert_eq!(memtable.cache_size(), 15);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.entries_cache.len(), 15);
        assert_eq!(memtable.entries_index.len(), 20);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[14].get_index(), 29);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[19].index, 29);
        assert_eq!(memtable.min_file_num().unwrap(), 5);
        assert_eq!(memtable.max_file_num().unwrap(), 5);

        // Cache with size limit 0.
        let stats = Arc::new(SharedCacheStats::default());
        let mut memtable = MemTable::new(region_id, 0, stats);
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 1));
        assert_eq!(memtable.cache_size(), 0);
        assert_eq!(memtable.entries_cache.len(), 0);
        assert_eq!(memtable.entries_size(), 10);
        assert_eq!(memtable.entries_index.len(), 10);
        assert_eq!(memtable.entries_index[0].index, 10);
        assert_eq!(memtable.entries_index[9].index, 19);
    }

    #[test]
    fn test_memtable_compact() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(SharedCacheStats::default());
        let mut memtable = MemTable::new(region_id, cache_limit, stats);

        // After appending:
        // [0, 10) file_num = 1, not in cache
        // [10, 15) file_num = 2, not in cache
        // [15, 20) file_num = 2, in cache
        // [20, 25) file_num = 3, in cache
        memtable.append(generate_ents(0, 10), generate_ents_index(0, 10, 1));
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));
        memtable.append(generate_ents(20, 25), generate_ents_index(20, 25, 3));
        assert_eq!(memtable.cache_size(), 10);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.entries_index.len(), 25);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[9].get_index(), 24);
        assert_eq!(memtable.entries_index[0].index, 0);
        assert_eq!(memtable.entries_index[24].index, 24);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // Compact to 5.
        // Only index is needed to compact.
        assert_eq!(memtable.compact_to(5), 5);
        assert_eq!(memtable.cache_size(), 10);
        assert_eq!(memtable.entries_size(), 20);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.entries_index.len(), 20);
        assert_eq!(memtable.entries_cache[0].get_index(), 15);
        assert_eq!(memtable.entries_cache[9].get_index(), 24);
        assert_eq!(memtable.entries_index[0].index, 5);
        assert_eq!(memtable.entries_index[19].index, 24);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // Compact to 20.
        // Both index and cache  need compaction.
        assert_eq!(memtable.compact_to(20), 15);
        assert_eq!(memtable.entries_size(), memtable.cache_size());
        assert_eq!(memtable.entries_size(), 5);
        assert_eq!(memtable.entries_cache.len(), 5);
        assert_eq!(memtable.entries_index.len(), 5);
        assert_eq!(memtable.entries_cache[0].get_index(), 20);
        assert_eq!(memtable.entries_cache[4].get_index(), 24);
        assert_eq!(memtable.entries_index[0].index, 20);
        assert_eq!(memtable.entries_index[4].index, 24);
        assert_eq!(memtable.min_file_num().unwrap(), 3);
        assert_eq!(memtable.max_file_num().unwrap(), 3);

        // Compact to 20 or smaller index, nothing happens.
        assert_eq!(memtable.compact_to(20), 0);
        assert_eq!(memtable.compact_to(15), 0);
    }

    #[test]
    fn test_memtable_compact_cache() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(SharedCacheStats::default());
        let mut memtable = MemTable::new(region_id, cache_limit, stats);

        // After appending:
        // [0, 10) file_num = 1, not in cache
        // [10, 15) file_num = 2, not in cache
        // [15, 20) file_num = 2, in cache
        // [20, 25) file_num = 3, in cache
        memtable.append(generate_ents(0, 10), generate_ents_index(0, 10, 1));
        memtable.append(generate_ents(10, 20), generate_ents_index(10, 20, 2));
        memtable.append(generate_ents(20, 25), generate_ents_index(20, 25, 3));
        assert_eq!(memtable.cache_size(), 10);
        assert_eq!(memtable.entries_size(), 25);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.entries_index.len(), 25);

        // Compact cache to 15, nothing needs to be changed.
        memtable.compact_cache_to(15);
        assert_eq!(memtable.entries_cache.len(), 10);
        assert_eq!(memtable.cache_size(), 10);

        // Compact cache to 20.
        memtable.compact_to(20);
        assert_eq!(memtable.entries_cache.len(), 5);
        assert_eq!(memtable.cache_size(), 5);

        // Compact cache to 25
        memtable.compact_cache_to(25);
        assert_eq!(memtable.entries_cache.len(), 0);
        assert_eq!(memtable.cache_size(), 0);
    }

    #[test]
    fn test_memtable_fetch() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(SharedCacheStats::default());
        let mut memtable = MemTable::new(region_id, cache_limit, stats.clone());

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
        assert_eq!(stats.hit_times(), 10);
        assert_eq!(stats.miss_times(), 15);

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
        stats.reset();
        memtable
            .fetch_entries_to(20, 25, None, &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 5);
        assert_eq!(ents[0].get_index(), 20);
        assert_eq!(ents[4].get_index(), 24);
        assert!(ents_idx.is_empty());
        assert_eq!(stats.hit_times(), 5);

        // All needed entries are not in cache.
        ents.clear();
        ents_idx.clear();
        stats.reset();
        memtable
            .fetch_entries_to(10, 15, None, &mut ents, &mut ents_idx)
            .unwrap();
        assert!(ents.is_empty());
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[4].index, 14);
        assert_eq!(stats.miss_times(), 5);

        // Some needed entries are in cache, the others are not.
        ents.clear();
        ents_idx.clear();
        stats.reset();
        memtable
            .fetch_entries_to(10, 25, None, &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 10);
        assert_eq!(ents[0].get_index(), 15);
        assert_eq!(ents[9].get_index(), 24);
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[4].index, 14);
        assert_eq!(stats.hit_times(), 10);
        assert_eq!(stats.miss_times(), 5);

        // Max size limitation range fetching.
        // Only can fetch [10, 20) because of size limitation,
        // and [10, 15) is not in cache, [15, 20) is in cache.
        ents.clear();
        ents_idx.clear();
        let max_size = Some(10);
        stats.reset();
        memtable
            .fetch_entries_to(10, 25, max_size, &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 5);
        assert_eq!(ents[0].get_index(), 15);
        assert_eq!(ents[4].get_index(), 19);
        assert_eq!(ents_idx.len(), 5);
        assert_eq!(ents_idx[0].index, 10);
        assert_eq!(ents_idx[4].index, 14);
        assert_eq!(stats.hit_times(), 5);
        assert_eq!(stats.miss_times(), 5);

        // Even max size limitation is 0, at least fetch one entry.
        ents.clear();
        ents_idx.clear();
        stats.reset();
        memtable
            .fetch_entries_to(20, 25, Some(0), &mut ents, &mut ents_idx)
            .unwrap();
        assert_eq!(ents.len(), 1);
        assert_eq!(ents[0].get_index(), 20);
        assert!(ents_idx.is_empty());
        assert_eq!(stats.hit_times(), 1);
    }

    #[test]
    fn test_memtable_kv_operations() {
        let region_id = 8;
        let cache_limit = 1024;
        let stats = Arc::new(SharedCacheStats::default());
        let mut memtable = MemTable::new(region_id, cache_limit, stats);

        let (k1, v1) = (b"key1", b"value1");
        let (k5, v5) = (b"key5", b"value5");
        memtable.put(k1.to_vec(), v1.to_vec(), 1);
        memtable.put(k5.to_vec(), v5.to_vec(), 5);
        assert_eq!(memtable.min_file_num().unwrap(), 1);
        assert_eq!(memtable.max_file_num().unwrap(), 5);
        assert_eq!(memtable.get(k1.as_ref()), Some(v1.to_vec()));
        assert_eq!(memtable.get(k5.as_ref()), Some(v5.to_vec()));

        memtable.delete(k5.as_ref());
        assert_eq!(memtable.get(k5.as_ref()), None);
    }

    #[test]
    fn test_memtable_get_entry() {
        let region_id = 8;
        let cache_limit = 10;
        let stats = Arc::new(SharedCacheStats::default());
        let mut memtable = MemTable::new(region_id, cache_limit, stats);

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
}
