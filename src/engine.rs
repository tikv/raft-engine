use std::io::BufRead;
use std::sync::atomic::{AtomicIsize, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;
use std::{cmp, fmt, u64};

use protobuf::Message as PbMsg;
use raft::eraftpb::Entry;

use crate::util::{HashMap, RAFT_LOG_STATE_KEY};

use crate::config::{Config, RecoveryMode};
use crate::log_batch::{
    self, Command, CompressionType, LogBatch, LogItemType, OpType, CHECKSUM_LEN, HEADER_LEN,
};
use crate::memtable::{EntryIndex, MemTable};
use crate::pipe_log::{PipeLog, FILE_MAGIC_HEADER, VERSION};
use crate::{codec, CacheStats, RaftEngine, RaftLocalState, Result};

const SLOTS_COUNT: usize = 128;

// If a region has some very old raft logs less than this threshold,
// rewrite them to clean stale log files ASAP.
const REWRITE_ENTRY_COUNT_THRESHOLD: usize = 128;

struct FileEngineInner {
    cfg: Config,

    // Multiple slots
    // region_id -> MemTable.
    memtables: Vec<RwLock<HashMap<u64, MemTable>>>,

    // Persistent entries.
    pipe_log: PipeLog,

    cache_stats: Arc<SharedCacheStats>,

    // To protect concurrent calls of `gc`.
    purge_mutex: Mutex<()>,
}

impl FileEngineInner {
    // recover from disk.
    fn recover(&mut self, recovery_mode: RecoveryMode) -> Result<()> {
        // Get first file number and last file number.
        let (first_file_num, active_file_num) = {
            (
                self.pipe_log.first_file_num(),
                self.pipe_log.active_file_num(),
            )
        };

        let start = Instant::now();

        // Iterate files one by one
        let mut current_read_file = first_file_num;
        loop {
            if current_read_file > active_file_num {
                break;
            }

            // Read a file
            let content = {
                self.pipe_log
                    .read_next_file()
                    .unwrap_or_else(|e| {
                        panic!(
                            "Read content of file {} failed, error {:?}",
                            current_read_file, e
                        )
                    })
                    .unwrap_or_else(|| panic!("Expect has content, but get None"))
            };

            // Verify file header
            let mut buf = content.as_slice();
            if buf.len() < FILE_MAGIC_HEADER.len() || !buf.starts_with(FILE_MAGIC_HEADER) {
                if current_read_file != active_file_num {
                    panic!("Raft log file {} is corrupted.", current_read_file);
                } else {
                    self.pipe_log.truncate_active_log(0).unwrap();
                    break;
                }
            }

            // Iterate all LogBatch in one file
            let start_ptr = buf.as_ptr();
            buf.consume(FILE_MAGIC_HEADER.len() + VERSION.len());
            let mut offset = (FILE_MAGIC_HEADER.len() + VERSION.len()) as u64;
            loop {
                match LogBatch::from_bytes(&mut buf, current_read_file, offset) {
                    Ok(Some(log_batch)) => {
                        self.apply_to_memtable(log_batch, current_read_file);
                        offset = unsafe { buf.as_ptr().offset_from(start_ptr) as u64 };
                    }
                    Ok(None) => {
                        info!("Recovered raft log file {}.", current_read_file);
                        break;
                    }
                    Err(e) => {
                        // There may be a pre-allocated space at the tail of the active log.
                        if current_read_file == active_file_num {
                            match recovery_mode {
                                RecoveryMode::TolerateCorruptedTailRecords => {
                                    warn!(
                                        "Encounter err {:?}, incomplete batch in last log file {}, \
                                         offset {}, truncate it in TolerateCorruptedTailRecords \
                                         recovery mode.",
                                        e,
                                        current_read_file,
                                        offset
                                    );
                                    self.pipe_log.truncate_active_log(offset as usize).unwrap();
                                    break;
                                }
                                RecoveryMode::AbsoluteConsistency => {
                                    panic!(
                                        "Encounter err {:?}, incomplete batch in last log file {}, \
                                         offset {}, panic in AbsoluteConsistency recovery mode.",
                                        e,
                                        current_read_file,
                                        offset
                                    );
                                }
                            }
                        } else {
                            panic!("Corruption occur in middle log file {}", current_read_file);
                        }
                    }
                }
            }

            current_read_file += 1;
        }

        info!("Recover raft log takes {:?}", start.elapsed());
        Ok(())
    }

    fn apply_to_memtable(&self, log_batch: LogBatch, file_num: u64) {
        for item in log_batch.items.borrow_mut().drain(..) {
            match item.item_type {
                LogItemType::Entries => {
                    let entries_to_add = item.entries.unwrap();
                    let region_id = entries_to_add.region_id;
                    let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                        .write()
                        .unwrap();
                    let memtable = memtables.entry(region_id).or_insert_with(|| {
                        let cache_limit = self.cfg.cache_limit_per_raft.0;
                        let cache_stats = self.cache_stats.clone();
                        MemTable::new(region_id, cache_limit, cache_stats)
                    });
                    memtable.append(
                        entries_to_add.entries,
                        entries_to_add.entries_index.into_inner(),
                    );
                }
                LogItemType::CMD => {
                    let command = item.command.unwrap();
                    match command {
                        Command::Clean { region_id } => {
                            let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                                .write()
                                .unwrap();
                            memtables.remove(&region_id);
                        }
                        Command::Compact { region_id, index } => {
                            let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                                .write()
                                .unwrap();
                            if let Some(memtable) = memtables.get_mut(&region_id) {
                                memtable.compact_to(index);
                            }
                        }
                    }
                }
                LogItemType::KV => {
                    let kv = item.kv.unwrap();
                    let mut memtables = self.memtables[kv.region_id as usize % SLOTS_COUNT]
                        .write()
                        .unwrap();
                    let memtable = memtables.entry(kv.region_id).or_insert_with(|| {
                        let cache_limit = self.cfg.cache_limit_per_raft.0;
                        let stats = self.cache_stats.clone();
                        MemTable::new(kv.region_id, cache_limit, stats)
                    });
                    match kv.op_type {
                        OpType::Put => {
                            memtable.put(kv.key, kv.value.unwrap(), file_num);
                        }
                        OpType::Del => {
                            memtable.delete(kv.key.as_slice());
                        }
                    }
                }
            }
        }
    }

    // 0 means no inactive file.
    fn max_inactive_file_num(&self) -> u64 {
        let total_size = self.pipe_log.total_size();
        let garbage_ratio = self.cache_stats.garbage_ratio();
        let rewrite_limit = (total_size as f64 * (1.0 - garbage_ratio)) as usize;
        self.pipe_log.last_file_before(rewrite_limit)
    }

    // Scan all regions and for every one
    // 1. return it if raft logs size between committed index and last index is greater than
    //    `force_compact_threshold`;
    // 2. rewrite inactive logs if it has less entries than `REWRITE_ENTRY_COUNT_THRESHOLD`.
    fn regions_rewrite_or_force_compact(
        &self,
        last_inactive: u64,
        force_compact_threshold: usize,
        will_force_compact: &mut Vec<u64>,
    ) {
        let (mut log_batches, mut cache) = (Vec::new(), HashMap::default());
        for memtables in &self.memtables {
            for memtable in memtables.read().unwrap().values() {
                let region_id = memtable.region_id();

                // Check the memtable needs force compaction or not.
                if let Some(value) = memtable.get(RAFT_LOG_STATE_KEY) {
                    let mut raft_state = RaftLocalState::new();
                    raft_state.merge_from_bytes(&value).unwrap();
                    let committed = raft_state.get_hard_state().commit;
                    let last = raft_state.get_last_index();
                    let delay_size = memtable.entries_size_in(committed + 1, last + 1);
                    if delay_size > force_compact_threshold {
                        // The region needs force compaction, so skip to rewrite logs for it.
                        will_force_compact.push(region_id);
                        continue;
                    }
                }

                let min_file_num = memtable.min_file_num().unwrap_or(u64::MAX);
                let entries_count = memtable.entries_count();
                if min_file_num > last_inactive || entries_count > REWRITE_ENTRY_COUNT_THRESHOLD {
                    // TODO: maybe it's not necessary to rewrite all logs for a region.
                    continue;
                }

                let log_batch = LogBatch::new();
                log_batch.clean_region(region_id);

                let mut ents = Vec::with_capacity(entries_count);
                let mut ents_idx = Vec::with_capacity(entries_count);
                memtable.fetch_all(&mut ents, &mut ents_idx);
                let mut all_ents = Vec::with_capacity(entries_count);
                for ei in ents_idx {
                    let e = self.read_entry_from_file(&ei, Some(&mut cache)).unwrap();
                    all_ents.push(e);
                }
                all_ents.extend(ents.into_iter());
                log_batch.add_entries(region_id, all_ents);

                let mut kvs = Vec::new();
                memtable.fetch_all_kvs(&mut kvs);
                for (key, value) in kvs {
                    log_batch.put(region_id, &key, &value);
                }

                log_batches.push(log_batch);
            }
        }
        for log_batch in log_batches {
            self.write(log_batch, false).unwrap();
        }
        will_force_compact.sort();
    }

    fn purge_expired_files(&self, last_inactive: u64) -> Result<()> {
        let first_file_num = self.pipe_log.first_file_num();

        let mut min_file_num = u64::MAX;
        for memtables in &self.memtables {
            let memtables = memtables.read().unwrap();
            for memtable in memtables.values() {
                if let Some(file_num) = memtable.min_file_num() {
                    min_file_num = cmp::min(min_file_num, file_num);
                }
            }
        }

        let expected = (last_inactive + 1 - first_file_num) as usize;
        let purged = self.pipe_log.purge_to(min_file_num)?;
        self.cache_stats.on_purge(purged, expected);
        info!(
            "puerge_expired_fies deletes {} files, {} is best",
            purged, expected
        );
        Ok(())
    }

    fn first_index(&self, region_id: u64) -> Option<u64> {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            return memtable.first_index();
        }
        None
    }

    fn compact_to(&self, region_id: u64, index: u64) -> u64 {
        let first_index = match self.first_index(region_id) {
            Some(index) => index,
            None => return 0,
        };

        let log_batch = LogBatch::new();
        log_batch.add_command(Command::Compact { region_id, index });
        self.write(log_batch, false).map(|_| ()).unwrap();

        self.first_index(region_id).unwrap_or(index) - first_index
    }

    fn compact_cache_to(&self, region_id: u64, index: u64) {
        let mut memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .write()
            .unwrap();
        if let Some(memtable) = memtables.get_mut(&region_id) {
            memtable.compact_cache_to(index);
        }
    }

    fn write(&self, log_batch: LogBatch, sync: bool) -> Result<usize> {
        let mut file_num = 0;
        let bytes = self
            .pipe_log
            .append_log_batch(&log_batch, sync, &mut file_num)?;
        self.post_append_to_file(log_batch, file_num);
        Ok(bytes)
    }

    fn sync(&self) -> Result<()> {
        self.pipe_log.sync();
        Ok(())
    }

    #[allow(dead_code)]
    fn kv_count(&self, region_id: u64) -> usize {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            return memtable.kvs_total_count();
        }
        0
    }

    fn put_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8], m: &M) -> Result<()> {
        let log_batch = LogBatch::new();
        log_batch.put_msg(region_id, key, m)?;
        self.write(log_batch, false).map(|_| ())
    }

    fn get(&self, region_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            Ok(memtable.get(key))
        } else {
            Ok(None)
        }
    }

    fn get_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8]) -> Result<Option<M>> {
        match self.get(region_id, key)? {
            Some(value) => {
                let mut m = M::new();
                m.merge_from_bytes(&value)?;
                Ok(Some(m))
            }
            None => Ok(None),
        }
    }

    fn get_entry(&self, region_id: u64, log_idx: u64) -> Result<Option<Entry>> {
        // Fetch from cache
        let entry_idx = {
            let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
                .read()
                .unwrap();
            if let Some(memtable) = memtables.get(&region_id) {
                match memtable.get_entry(log_idx) {
                    (Some(entry), _) => return Ok(Some(entry)),
                    (None, Some(idx)) => idx,
                    (None, None) => return Ok(None),
                }
            } else {
                return Ok(None);
            }
        };

        // Read from file
        let entry = self.read_entry_from_file(&entry_idx, None).unwrap();
        Ok(Some(entry))
    }

    fn read_entry_from_file(
        &self,
        entry_index: &EntryIndex,
        _: Option<&mut HashMap<(u64, u64), Vec<u8>>>,
    ) -> Result<Entry> {
        let file_num = entry_index.file_num;
        let base_offset = entry_index.base_offset;
        let batch_len = entry_index.batch_len;
        let offset = entry_index.offset;
        let len = entry_index.len;

        let entry_content = match entry_index.compression_type {
            CompressionType::None => {
                let offset = base_offset + offset;
                self.pipe_log.fread(file_num, offset, len)?
            }
            CompressionType::Lz4 => {
                let read_len = batch_len + HEADER_LEN as u64;
                let compressed = self.pipe_log.fread(file_num, base_offset, read_len)?;
                let mut reader = compressed.as_ref();
                let header = codec::decode_u64(&mut reader)?;
                assert_eq!(header >> 8, batch_len);

                log_batch::test_batch_checksum(reader)?;
                let buf = log_batch::decompress(&reader[..batch_len as usize - CHECKSUM_LEN]);
                let start = offset as usize - HEADER_LEN;
                let end = (offset + len) as usize - HEADER_LEN;
                buf[start..end].to_vec()
            }
        };

        let mut e = Entry::new();
        e.merge_from_bytes(&entry_content)?;
        assert_eq!(e.get_index(), entry_index.index);
        Ok(e)
    }

    pub fn fetch_entries_to(
        &self,
        region_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<Entry>,
    ) -> Result<usize> {
        let memtables = self.memtables[region_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        if let Some(memtable) = memtables.get(&region_id) {
            let mut entries = Vec::with_capacity((end - begin) as usize);
            let mut entries_idx = Vec::with_capacity((end - begin) as usize);
            memtable.fetch_entries_to(begin, end, max_size, &mut entries, &mut entries_idx)?;
            let count = entries.len() + entries_idx.len();
            for idx in &entries_idx {
                let e = self.read_entry_from_file(idx, None)?;
                vec.push(e);
            }
            vec.extend(entries.into_iter());
            return Ok(count);
        }
        Ok(0)
    }

    fn post_append_to_file(&self, log_batch: LogBatch, file_num: u64) {
        // 0 means write nothing.
        if file_num == 0 {
            return;
        }
        self.apply_to_memtable(log_batch, file_num);
    }
}

#[derive(Default)]
pub struct SharedCacheStats {
    hit: AtomicUsize,
    miss: AtomicUsize,
    mem_size_change: AtomicIsize,
    // Size of all entries which have not been purged.
    total_size: AtomicUsize,
    // Size of all entries which are compacted but not purged.
    compacted_size: AtomicUsize,
}

impl SharedCacheStats {
    pub fn sub_mem_change(&self, bytes: u64) {
        self.mem_size_change
            .fetch_sub(bytes as isize, Ordering::Relaxed);
    }
    pub fn add_mem_change(&self, bytes: u64) {
        self.mem_size_change
            .fetch_add(bytes as isize, Ordering::Relaxed);
    }
    pub fn hit_cache(&self, count: usize) {
        self.hit.fetch_add(count, Ordering::Relaxed);
    }
    pub fn miss_cache(&self, count: usize) {
        self.miss.fetch_add(count, Ordering::Relaxed);
    }
    pub fn add_total_size(&self, size: u64) {
        self.total_size.fetch_add(size as usize, Ordering::Relaxed);
    }
    pub fn sub_total_size(&self, size: u64) {
        self.total_size.fetch_sub(size as usize, Ordering::Relaxed);
    }
    pub fn add_compacted_size(&self, size: u64) {
        self.compacted_size
            .fetch_add(size as usize, Ordering::Relaxed);
    }

    pub fn hit_times(&self) -> usize {
        self.hit.load(Ordering::Relaxed)
    }
    pub fn miss_times(&self) -> usize {
        self.miss.load(Ordering::Relaxed)
    }
    pub fn garbage_ratio(&self) -> f64 {
        let compacted = self.compacted_size.load(Ordering::Acquire);
        let total = self.total_size.load(Ordering::Acquire);
        compacted as f64 / total as f64
    }
    pub fn on_purge(&self, purged: usize, expected: usize) {
        let x1 = self.compacted_size.load(Ordering::SeqCst);
        let x2 = (x1 as f64 * purged as f64 / expected as f64) as usize;
        let compacted = cmp::min(x1, x2);
        self.compacted_size.fetch_sub(compacted, Ordering::SeqCst);
        self.total_size.fetch_sub(compacted, Ordering::SeqCst);
    }

    #[cfg(test)]
    pub fn reset(&self) {
        self.hit.store(0, Ordering::Relaxed);
        self.miss.store(0, Ordering::Relaxed);
        self.mem_size_change.store(0, Ordering::Relaxed);
    }
}

#[derive(Clone)]
pub struct FileEngine {
    inner: Arc<FileEngineInner>,
}

impl fmt::Debug for FileEngine {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileEngineInner dir: {}", self.inner.cfg.dir)
    }
}

impl FileEngine {
    pub fn new(cfg: Config) -> FileEngine {
        let cache_stats = Arc::new(SharedCacheStats::default());

        let pipe_log = PipeLog::open(
            &cfg.dir,
            cfg.bytes_per_sync.0 as usize,
            cfg.target_file_size.0 as usize,
        )
        .unwrap_or_else(|e| panic!("Open raft log failed, error: {:?}", e));
        let mut memtables = Vec::with_capacity(SLOTS_COUNT);
        for _ in 0..SLOTS_COUNT {
            memtables.push(RwLock::new(HashMap::default()));
        }
        let mut engine = FileEngineInner {
            cfg,
            memtables,
            pipe_log,
            cache_stats,
            purge_mutex: Mutex::new(()),
        };
        let recovery_mode = RecoveryMode::from(engine.cfg.recovery_mode);
        engine
            .recover(recovery_mode)
            .unwrap_or_else(|e| panic!("Recover raft log failed, error: {:?}", e));

        FileEngine {
            inner: Arc::new(engine),
        }
    }

    fn needs_purge_log_files(&self) -> bool {
        let total_size = self.inner.pipe_log.total_size();
        let purge_threshold = self.inner.cfg.purge_threshold.0 as usize;
        total_size > purge_threshold
    }

    #[cfg(test)]
    fn commit_to(&self, raft_group_id: u64, index: u64) {
        let mut raft_state = self.get_raft_state(raft_group_id).unwrap().unwrap();
        raft_state.mut_hard_state().commit = index;
        self.put_raft_state(raft_group_id, &raft_state).unwrap();
    }
}

impl RaftEngine for FileEngine {
    type LogBatch = LogBatch;

    fn log_batch(&self, _capacity: usize) -> Self::LogBatch {
        LogBatch::default()
    }

    fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    fn get_raft_state(&self, raft_group_id: u64) -> Result<Option<RaftLocalState>> {
        self.inner.get_msg(raft_group_id, RAFT_LOG_STATE_KEY)
    }

    fn get_entry(&self, raft_group_id: u64, index: u64) -> Result<Option<Entry>> {
        self.inner.get_entry(raft_group_id, index)
    }

    fn fetch_entries_to(
        &self,
        raft_group_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        to: &mut Vec<Entry>,
    ) -> Result<usize> {
        self.inner
            .fetch_entries_to(raft_group_id, begin, end, max_size, to)
    }

    fn consume(&self, batch: &mut Self::LogBatch, sync: bool) -> Result<usize> {
        self.inner.write(std::mem::take(batch), sync)
    }

    fn consume_and_shrink(
        &self,
        batch: &mut Self::LogBatch,
        sync: bool,
        _: usize,
        _: usize,
    ) -> Result<usize> {
        self.consume(batch, sync)
    }

    fn clean(&self, raft_group_id: u64, _: &RaftLocalState, batch: &mut LogBatch) -> Result<()> {
        batch.clean_region(raft_group_id);
        Ok(())
    }

    fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
        let batch = LogBatch::default();
        batch.add_entries(raft_group_id, entries);
        self.inner.write(batch, false)
    }

    fn put_raft_state(&self, raft_group_id: u64, state: &RaftLocalState) -> Result<()> {
        self.inner.put_msg(raft_group_id, RAFT_LOG_STATE_KEY, state)
    }

    fn gc(&self, raft_group_id: u64, _from: u64, to: u64) -> usize {
        self.inner.compact_to(raft_group_id, to) as usize
    }

    fn purge_expired_files(&self, force_compact_threshold: usize) -> Vec<u64> {
        if let Ok(_x) = self.inner.purge_mutex.try_lock() {
            if self.needs_purge_log_files() {
                let last_inactive = self.inner.max_inactive_file_num();
                let mut will_force_compact = Vec::new();
                self.inner.regions_rewrite_or_force_compact(
                    last_inactive,
                    force_compact_threshold,
                    &mut will_force_compact,
                );
                self.inner.purge_expired_files(last_inactive).unwrap();
                return will_force_compact;
            }
        }
        vec![]
    }

    fn has_builtin_entry_cache(&self) -> bool {
        true
    }

    fn gc_entry_cache(&self, raft_group_id: u64, to: u64) {
        self.inner.compact_cache_to(raft_group_id, to)
    }

    fn flush_stats(&self) -> CacheStats {
        let inner = &self.inner;
        CacheStats {
            hit: inner.cache_stats.hit.swap(0, Ordering::SeqCst),
            miss: inner.cache_stats.miss.swap(0, Ordering::SeqCst),
            mem_size_change: inner.cache_stats.mem_size_change.swap(0, Ordering::SeqCst),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::ReadableSize;

    fn append_log(engine: &FileEngine, raft: u64, entry: &Entry) {
        engine.append(raft, vec![entry.clone()]).unwrap();
        let mut state = RaftLocalState::new();
        state.last_index = entry.index;
        engine.put_raft_state(raft, &state).unwrap();
    }

    #[test]
    fn test_get_entry_from_file() {
        let normal_batch_size = 10;
        let compressed_batch_size = 5120;
        for &entry_size in &[normal_batch_size, compressed_batch_size] {
            let dir = tempfile::Builder::new()
                .prefix("test_engine")
                .tempdir()
                .unwrap();

            let mut cfg = Config::default();
            cfg.dir = dir.path().to_str().unwrap().to_owned();

            let engine = FileEngine::new(cfg.clone());
            let mut entry = Entry::new();
            entry.set_data(vec![b'x'; entry_size]);
            for i in 10..20 {
                entry.set_index(i);
                engine.append(i, vec![entry.clone()]).unwrap();
                entry.set_index(i + 1);
                engine.append(i, vec![entry.clone()]).unwrap();
            }

            for i in 10..20 {
                // Test get_entry from cache.
                entry.set_index(i + 1);
                assert_eq!(engine.get_entry(i, i + 1).unwrap(), Some(entry.clone()));

                // Test get_entry from file.
                entry.set_index(i);
                assert_eq!(engine.get_entry(i, i).unwrap(), Some(entry.clone()));
            }

            drop(engine);

            // Recover the engine.
            let engine = FileEngine::new(cfg.clone());
            for i in 10..20 {
                entry.set_index(i + 1);
                assert_eq!(engine.get_entry(i, i + 1).unwrap(), Some(entry.clone()));

                entry.set_index(i);
                assert_eq!(engine.get_entry(i, i).unwrap(), Some(entry.clone()));
            }
        }
    }

    // Test whether GC works fine or not, and purge should be triggered correctly.
    #[test]
    fn test_gc_and_purge() {
        let dir = tempfile::Builder::new()
            .prefix("test_engine")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.target_file_size = ReadableSize::kb(5);
        cfg.purge_threshold = ReadableSize::kb(150);

        let engine = FileEngine::new(cfg.clone());
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024]);
        for i in 0..100 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC all log entries.
        engine.commit_to(1, 99);
        let count = engine.gc(1, 0, 100);
        assert_eq!(count, 100);
        assert!(!engine.needs_purge_log_files());

        // Append more logs to make total size greater than `purge_threshold`.
        for i in 100..250 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC first 101 log entries.
        engine.commit_to(1, 100);
        let count = engine.gc(1, 0, 101);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.needs_purge_log_files());

        let old_min_file_num = engine.inner.pipe_log.first_file_num();
        let will_force_compact = engine.purge_expired_files(200 * 1024);
        let new_min_file_num = engine.inner.pipe_log.first_file_num();
        // Some entries are rewritten.
        assert!(new_min_file_num > old_min_file_num);
        // No regions need to be force compacted because the threshold is not reached.
        assert!(will_force_compact.is_empty());
        // After purge, entries and raft state are still available.
        assert!(engine.get_entry(1, 101).unwrap().is_some());
        assert!(engine.get_raft_state(1).unwrap().is_some());

        engine.commit_to(1, 101);
        let count = engine.gc(1, 0, 102);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.needs_purge_log_files());
        let old_min_file_num = engine.inner.pipe_log.first_file_num();
        let will_force_compact = engine.purge_expired_files(100 * 1024);
        let new_min_file_num = engine.inner.pipe_log.first_file_num();
        // No entries are rewritten.
        assert_eq!(new_min_file_num, old_min_file_num);
        // The region needs to be force compacted because the threshold is reached.
        assert!(!will_force_compact.is_empty());
        assert_eq!(will_force_compact[0], 1);
    }
}
