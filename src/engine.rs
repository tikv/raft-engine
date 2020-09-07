use std::io::BufRead;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockWriteGuard};
use std::time::{Duration, Instant};
use std::{cmp, fmt, u64};

use crate::cache_evict::{
    CacheSubmitor, CacheTask, Runner as CacheEvictRunner, DEFAULT_CACHE_CHUNK_SIZE,
};
use crate::config::{Config, RecoveryMode};
use crate::log_batch::{
    self, Command, CompressionType, EntryExt, LogBatch, LogItemContent, OpType, CHECKSUM_LEN,
    HEADER_LEN,
};
use crate::memtable::{EntryIndex, MemTable};
use crate::pipe_log::{PipeLog, PipeLogImpl, FILE_MAGIC_HEADER, VERSION};
use crate::util::{HandyRwLock, HashMap, Worker};
use crate::{codec, CacheStats, Result};
use protobuf::Message;

const SLOTS_COUNT: usize = 128;

// If a region has some very old raft logs less than this threshold,
// rewrite them to clean stale log files ASAP.
const REWRITE_ENTRY_COUNT_THRESHOLD: usize = 32;
const REWRITE_INACTIVE_RATIO: f64 = 0.7;
const FORCE_COMPACT_RATIO: f64 = 0.2;

// When add/delete a memtable, a write lock is required.
// We can use LRU cache or slots to reduce conflicts.
type MemTables<E, W> = HashMap<u64, Arc<RwLock<MemTable<E, W>>>>;
pub struct MemTableAccessor<E: Message, W: EntryExt<E>> {
    slots: Vec<Arc<RwLock<MemTables<E, W>>>>,
    creator: Arc<dyn Fn(u64) -> MemTable<E, W> + Send + Sync>,
}

impl<E, W> Clone for MemTableAccessor<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    fn clone(&self) -> Self {
        Self {
            slots: self.slots.clone(),
            creator: self.creator.clone(),
        }
    }
}

impl<E, W> MemTableAccessor<E, W>
where
    E: Message,
    W: EntryExt<E>,
{
    fn new(creator: Arc<dyn Fn(u64) -> MemTable<E, W> + Send + Sync>) -> MemTableAccessor<E, W> {
        let mut slots = Vec::with_capacity(SLOTS_COUNT);
        for _ in 0..SLOTS_COUNT {
            slots.push(Arc::new(RwLock::new(MemTables::default())));
        }
        MemTableAccessor { slots, creator }
    }

    pub fn get_or_insert(&self, raft_group_id: u64) -> Arc<RwLock<MemTable<E, W>>> {
        let mut memtables = self.slots[raft_group_id as usize % SLOTS_COUNT]
            .write()
            .unwrap();
        let memtable = memtables
            .entry(raft_group_id)
            .or_insert_with(|| Arc::new(RwLock::new((self.creator)(raft_group_id))));
        memtable.clone()
    }

    pub fn get(&self, raft_group_id: u64) -> Option<Arc<RwLock<MemTable<E, W>>>> {
        let memtables = self.slots[raft_group_id as usize % SLOTS_COUNT]
            .read()
            .unwrap();
        memtables.get(&raft_group_id).cloned()
    }

    pub fn remove(&self, raft_group_id: u64) {
        let mut memtables = self.slots[raft_group_id as usize % SLOTS_COUNT]
            .write()
            .unwrap();
        memtables.remove(&raft_group_id);
    }

    pub fn fold<B, F: Fn(B, &MemTable<E, W>) -> B>(&self, mut init: B, fold: F) -> B {
        for tables in &self.slots {
            for memtable in tables.rl().values() {
                init = fold(init, &*memtable.rl());
            }
        }
        init
    }

    pub fn collect<F: FnMut(&MemTable<E, W>) -> bool>(
        &self,
        mut condition: F,
    ) -> Vec<Arc<RwLock<MemTable<E, W>>>> {
        let mut memtables = Vec::new();
        for tables in &self.slots {
            memtables.extend(tables.rl().values().filter_map(|t| {
                if condition(&*t.rl()) {
                    return Some(t.clone());
                }
                None
            }));
        }
        memtables
    }
}

#[derive(Clone)]
pub struct FileEngine<E, W, P>
where
    E: Message,
    W: EntryExt<E>,
    P: PipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor<E, W>,
    pipe_log: P,
    cache_stats: Arc<SharedCacheStats>,

    workers: Arc<RwLock<Workers>>,

    // To protect concurrent calls of `purge_expired_files`.
    purge_mutex: Arc<Mutex<()>>,
}

impl<E, W, P> FileEngine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
    P: PipeLog,
{
    // recover from disk.
    fn recover(
        pipe_log: &mut P,
        memtables: &MemTableAccessor<E, W>,
        recovery_mode: RecoveryMode,
    ) -> Result<()> {
        // Get first file number and last file number.
        let first_file_num = pipe_log.first_file_num();
        let active_file_num = pipe_log.active_file_num();

        let start = Instant::now();

        // Iterate files one by one
        let mut current_read_file = first_file_num;
        while current_read_file <= active_file_num {
            // Read a file
            let content = pipe_log
                .read_next_file()
                .unwrap_or_else(|e| {
                    panic!(
                        "Read content of file {} failed, error {:?}",
                        current_read_file, e
                    )
                })
                .unwrap_or_else(|| panic!("Expect has content, but get None"));

            // Verify file header
            let mut buf = content.as_slice();
            if buf.len() < FILE_MAGIC_HEADER.len() || !buf.starts_with(FILE_MAGIC_HEADER) {
                if current_read_file != active_file_num {
                    panic!("Raft log file {} is corrupted.", current_read_file);
                } else {
                    pipe_log.truncate_active_log(0).unwrap();
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
                        let entries_size = log_batch.entries_size();
                        if let Some(tracker) = pipe_log.cache_submitor().get_cache_tracker(
                            current_read_file,
                            offset,
                            entries_size,
                        ) {
                            for item in &log_batch.items {
                                if let LogItemContent::Entries(ref entries) = item.content {
                                    entries.attach_cache_tracker(tracker.clone());
                                }
                            }
                        }
                        Self::apply_to_memtable(memtables, log_batch, current_read_file);
                        offset = (buf.as_ptr() as usize - start_ptr as usize) as u64;
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
                                    pipe_log.truncate_active_log(offset as usize).unwrap();
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

    fn apply_log_item_to_memtable(
        content: LogItemContent<E>,
        memtable: &mut MemTable<E, W>,
        file_num: u64,
    ) {
        match content {
            LogItemContent::Entries(entries_to_add) => {
                memtable.append(
                    entries_to_add.entries,
                    entries_to_add.entries_index.into_inner(),
                );
            }
            LogItemContent::Command(Command::Clean) => {
                memtable.remove();
            }
            LogItemContent::Command(Command::Compact { index }) => {
                memtable.compact_to(index);
            }
            LogItemContent::Kv(kv) => match kv.op_type {
                OpType::Put => memtable.put(kv.key, kv.value.unwrap(), file_num),
                OpType::Del => memtable.delete(kv.key.as_slice()),
            },
        }
    }

    fn apply_to_memtable(
        memtables: &MemTableAccessor<E, W>,
        log_batch: LogBatch<E, W>,
        file_num: u64,
    ) {
        for item in log_batch.items {
            let m = memtables.get_or_insert(item.raft_group_id);
            Self::apply_log_item_to_memtable(item.content, &mut *m.wl(), file_num);
        }
    }

    // Returns (`latest_needs_rewrite`, `latest_needs_force_compact`).
    fn latest_inactive_file_num(&self) -> (u64, u64) {
        let total_size = self.pipe_log.total_size() as f64;
        let rewrite_limit = (total_size * (1.0 - REWRITE_INACTIVE_RATIO)) as usize;
        let compact_limit = (total_size * (1.0 - FORCE_COMPACT_RATIO)) as usize;
        let latest_needs_rewrite = self.pipe_log.latest_file_before(rewrite_limit);
        let latest_needs_compact = self.pipe_log.latest_file_before(compact_limit);
        (latest_needs_rewrite, latest_needs_compact)
    }

    fn regions_rewrite_or_force_compact(
        &self,
        rewrite_latest_file_num: u64,
        compact_latest_file_num: u64,
        will_force_compact: &mut Vec<u64>,
    ) {
        assert!(compact_latest_file_num <= rewrite_latest_file_num);
        let memtables = self.memtables.collect(|t| {
            let min_file_num = t.min_file_num().unwrap_or(u64::MAX);
            if min_file_num <= compact_latest_file_num {
                will_force_compact.push(t.region_id());
                return false;
            }
            min_file_num <= rewrite_latest_file_num
        });

        let mut cache = HashMap::default();
        for m in memtables {
            let memtable = m.wl();
            let region_id = memtable.region_id();

            let entries_count = memtable.entries_count();
            if entries_count > REWRITE_ENTRY_COUNT_THRESHOLD {
                continue;
            }

            // TODO: maybe it's not necessary to rewrite all logs for a region.
            let mut ents = Vec::with_capacity(entries_count);
            let mut ents_idx = Vec::with_capacity(entries_count);
            memtable.fetch_all(&mut ents, &mut ents_idx);
            let mut all_ents = Vec::with_capacity(entries_count);
            for ei in ents_idx {
                let e = self.read_entry_from_file(&ei, Some(&mut cache)).unwrap();
                all_ents.push(e);
            }
            all_ents.extend(ents.into_iter());

            let mut kvs = Vec::new();
            memtable.fetch_all_kvs(&mut kvs);
            self.rewrite(region_id, all_ents, kvs, memtable).unwrap();
        }
    }

    // Write a batch needs 3 steps:
    // 1. find all involved raft groups and then lock their memtables;
    // 2. append the log batch to pipe log;
    // 3. update all involved memtables.
    // The lock logic is a little complex. However it's necessary because
    // 1. "Inactive log rewrite" needs to keep logs on pipe log order;
    // 2. Users can call `append` on one raft group concurrently.
    // Maybe we can improve the implement of "inactive log rewrite" and
    // forbid concurrent `append` to remove locks here.
    fn write_impl(&self, log_batch: &mut LogBatch<E, W>, sync: bool) -> Result<usize> {
        let mut rafts = Vec::with_capacity(log_batch.items.len());
        for raft in log_batch.items.iter().map(|item| item.raft_group_id) {
            rafts.push(raft);
        }
        rafts.sort();
        rafts.dedup();

        let mut memtables = Vec::with_capacity(rafts.len());
        let mut locked = Vec::with_capacity(rafts.len());
        for raft in &rafts {
            memtables.push(self.memtables.get_or_insert(*raft));
            let x = memtables.last_mut().unwrap().wl();
            unsafe {
                // Unsafe block because `locked` has mutable references to `memtables`.
                let x: RwLockWriteGuard<'static, MemTable<E, W>> = std::mem::transmute(x);
                locked.push(x);
            }
        }

        let mut file_num = 0;
        let bytes = self.pipe_log.write(log_batch, sync, &mut file_num)?;
        if file_num > 0 {
            for item in log_batch.items.drain(..) {
                let offset = rafts.binary_search(&item.raft_group_id).unwrap();
                let m = &mut locked[offset];
                Self::apply_log_item_to_memtable(item.content, &mut *m, file_num);
            }
        }
        for memtable in locked {
            if memtable.uninitialized() {
                self.memtables.remove(memtable.region_id());
            }
        }
        Ok(bytes)
    }

    fn rewrite(
        &self,
        raft_group_id: u64,
        entries: Vec<E>,
        kvs: Vec<(Vec<u8>, Vec<u8>)>,
        mut m: RwLockWriteGuard<MemTable<E, W>>,
    ) -> Result<()> {
        let mut log_batch = LogBatch::<E, W>::new();
        log_batch.add_command(raft_group_id, Command::Clean);
        log_batch.add_entries(raft_group_id, entries);
        for (k, v) in kvs {
            log_batch.put(raft_group_id, k, v);
        }

        let mut file_num = 0;
        self.pipe_log.write(&log_batch, false, &mut file_num)?;
        if file_num > 0 {
            for item in log_batch.items {
                Self::apply_log_item_to_memtable(item.content, &mut *m, file_num);
            }
        }
        Ok(())
    }

    fn read_entry_from_file(
        &self,
        entry_index: &EntryIndex,
        _: Option<&mut HashMap<(u64, u64), Vec<u8>>>,
    ) -> Result<E> {
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

        let mut e = E::new();
        e.merge_from_bytes(&entry_content)?;
        assert_eq!(W::index(&e), entry_index.index);
        Ok(e)
    }

    fn needs_purge_log_files(&self) -> bool {
        let total_size = self.pipe_log.total_size();
        let purge_threshold = self.cfg.purge_threshold.0 as usize;
        total_size > purge_threshold
    }
}

#[derive(Default)]
pub struct SharedCacheStats {
    hit: AtomicUsize,
    miss: AtomicUsize,
    cache_size: AtomicUsize,
}

impl SharedCacheStats {
    pub fn sub_mem_change(&self, bytes: usize) {
        self.cache_size.fetch_sub(bytes, Ordering::Release);
    }
    pub fn add_mem_change(&self, bytes: usize) {
        self.cache_size.fetch_add(bytes, Ordering::Release);
    }
    pub fn hit_cache(&self, count: usize) {
        self.hit.fetch_add(count, Ordering::Relaxed);
    }
    pub fn miss_cache(&self, count: usize) {
        self.miss.fetch_add(count, Ordering::Relaxed);
    }

    pub fn hit_times(&self) -> usize {
        self.hit.load(Ordering::Relaxed)
    }
    pub fn miss_times(&self) -> usize {
        self.miss.load(Ordering::Relaxed)
    }
    pub fn cache_size(&self) -> usize {
        self.cache_size.load(Ordering::Acquire)
    }

    #[cfg(test)]
    pub fn reset(&self) {
        self.hit.store(0, Ordering::Relaxed);
        self.miss.store(0, Ordering::Relaxed);
        self.cache_size.store(0, Ordering::Relaxed);
    }
}

struct Workers {
    cache_evict: Worker<CacheTask>,
}

impl<E, W, P> fmt::Debug for FileEngine<E, W, P>
where
    E: Message,
    W: EntryExt<E>,
    P: PipeLog,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FileEngine dir: {}", self.cfg.dir)
    }
}

impl<E, W> FileEngine<E, W, PipeLogImpl>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
{
    pub fn new_impl(cfg: Config, chunk_limit: usize) -> FileEngine<E, W, PipeLogImpl> {
        let cache_limit = cfg.cache_limit.0 as usize;
        let cache_stats = Arc::new(SharedCacheStats::default());

        let mut cache_evict_worker = Worker::new("cache_evict".to_owned(), None);

        let mut pipe_log = PipeLogImpl::open(
            &cfg,
            CacheSubmitor::new(
                cache_limit,
                chunk_limit,
                cache_evict_worker.scheduler(),
                cache_stats.clone(),
            ),
        )
        .expect("Open raft log");
        pipe_log.cache_submitor().block_on_full();

        let memtables = {
            let stats = cache_stats.clone();
            MemTableAccessor::<E, W>::new(Arc::new(move |id: u64| {
                MemTable::new(id, cache_limit, stats.clone())
            }))
        };

        let cache_evict_runner = CacheEvictRunner::new(
            cache_limit,
            cache_stats.clone(),
            chunk_limit,
            memtables.clone(),
            pipe_log.clone(),
        );
        cache_evict_worker.start(cache_evict_runner, Some(Duration::from_secs(1)));

        let recovery_mode = cfg.recovery_mode;
        FileEngine::recover(&mut pipe_log, &memtables, recovery_mode).unwrap();
        pipe_log.cache_submitor().nonblock_on_full();

        FileEngine {
            cfg: Arc::new(cfg),
            memtables,
            pipe_log,
            cache_stats,
            workers: Arc::new(RwLock::new(Workers {
                cache_evict: cache_evict_worker,
            })),
            purge_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub fn new(cfg: Config) -> FileEngine<E, W, PipeLogImpl> {
        Self::new_impl(cfg, DEFAULT_CACHE_CHUNK_SIZE)
    }
}

impl<E, W, P> FileEngine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
    P: PipeLog + 'static,
{
    /// Synchronize the Raft engine.
    pub fn sync(&self) -> Result<()> {
        self.pipe_log.sync()
    }

    pub fn put(&self, region_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let mut log_batch = LogBatch::new();
        log_batch.put(region_id, key.to_vec(), value.to_vec());
        self.write(&mut log_batch, false).map(|_| ())
    }

    pub fn put_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8], m: &M) -> Result<()> {
        let mut log_batch = LogBatch::new();
        log_batch.put_msg(region_id, key.to_vec(), m)?;
        self.write(&mut log_batch, false).map(|_| ())
    }

    pub fn get(&self, region_id: u64, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let memtable = self.memtables.get(region_id);
        Ok(memtable.and_then(|t| t.rl().get(key)))
    }

    pub fn get_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8]) -> Result<Option<M>> {
        match self.get(region_id, key)? {
            Some(value) => {
                let mut m = M::new();
                m.merge_from_bytes(&value)?;
                Ok(Some(m))
            }
            None => Ok(None),
        }
    }

    pub fn get_entry(&self, region_id: u64, log_idx: u64) -> Result<Option<E>> {
        // Fetch from cache
        let entry_idx = {
            if let Some(memtable) = self.memtables.get(region_id) {
                match memtable.rl().get_entry(log_idx) {
                    (Some(entry), _) => return Ok(Some(entry)),
                    (None, Some(idx)) => idx,
                    (None, None) => return Ok(None),
                }
            } else {
                return Ok(None);
            }
        };

        // Read from file
        let entry = self.read_entry_from_file(&entry_idx, None)?;
        Ok(Some(entry))
    }

    /// Purge expired logs files and return a set of Raft group ids
    /// which needs to be compacted ASAP.
    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let _purge_mutex = match self.purge_mutex.try_lock() {
            Ok(locked) if self.needs_purge_log_files() => locked,
            _ => return Ok(vec![]),
        };

        let (rewrite_limit, compact_limit) = self.latest_inactive_file_num();
        let mut will_force_compact = Vec::new();
        self.regions_rewrite_or_force_compact(
            rewrite_limit,
            compact_limit,
            &mut will_force_compact,
        );

        let min_file_num = self.memtables.fold(u64::MAX, |min, t| {
            cmp::min(min, t.min_file_num().unwrap_or(u64::MAX))
        });
        let purged = self.pipe_log.purge_to(min_file_num)?;
        info!("purged {} expired log files", purged);

        Ok(will_force_compact)
    }

    /// Return count of fetched entries.
    pub fn fetch_entries_to(
        &self,
        region_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<E>,
    ) -> Result<usize> {
        if let Some(memtable) = self.memtables.get(region_id) {
            let memtable = memtable.rl();
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

    pub fn first_index(&self, region_id: u64) -> Option<u64> {
        if let Some(memtable) = self.memtables.get(region_id) {
            return memtable.rl().first_index();
        }
        None
    }

    pub fn last_index(&self, region_id: u64) -> Option<u64> {
        if let Some(memtable) = self.memtables.get(region_id) {
            return memtable.rl().last_index();
        }
        None
    }

    /// Like `cut_logs` but the range could be very large. Return the deleted count.
    /// Generally, `from` can be passed in `0`.
    pub fn compact_to(&self, region_id: u64, index: u64) -> u64 {
        let first_index = match self.first_index(region_id) {
            Some(index) => index,
            None => return 0,
        };

        let mut log_batch = LogBatch::new();
        log_batch.add_command(region_id, Command::Compact { index });
        self.write(&mut log_batch, false).map(|_| ()).unwrap();

        self.first_index(region_id).unwrap_or(index) - first_index
    }

    pub fn compact_cache_to(&self, region_id: u64, index: u64) {
        if let Some(memtable) = self.memtables.get(region_id) {
            memtable.wl().compact_cache_to(index);
        }
    }

    /// Write the content of LogBatch into the engine and return written bytes.
    /// If set sync true, the data will be persisted on disk by `fsync`.
    pub fn write(&self, log_batch: &mut LogBatch<E, W>, sync: bool) -> Result<usize> {
        self.write_impl(log_batch, sync)
    }

    /// Flush stats about EntryCache.
    pub fn flush_stats(&self) -> CacheStats {
        CacheStats {
            hit: self.cache_stats.hit.swap(0, Ordering::SeqCst),
            miss: self.cache_stats.miss.swap(0, Ordering::SeqCst),
            cache_size: self.cache_stats.cache_size.load(Ordering::SeqCst),
        }
    }

    /// Stop background thread which will keep trying evict caching.
    pub fn stop(&self) {
        let mut workers = self.workers.wl();
        workers.cache_evict.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pipe_log::PipeLogImpl;
    use crate::util::ReadableSize;
    use raft::eraftpb::Entry;

    impl EntryExt<Entry> for Entry {
        fn index(e: &Entry) -> u64 {
            e.get_index()
        }
    }

    type RaftLogEngine = FileEngine<Entry, Entry, PipeLogImpl>;
    impl RaftLogEngine {
        fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
            let mut batch = LogBatch::default();
            batch.add_entries(raft_group_id, entries);
            self.write(&mut batch, false)
        }
    }

    fn append_log(engine: &RaftLogEngine, raft: u64, entry: &Entry) {
        engine.append(raft, vec![entry.clone()]).unwrap();
    }

    #[test]
    fn test_get_entry_from_file() {
        let normal_batch_size = 10;
        let compressed_batch_size = 5120;
        for &entry_size in &[normal_batch_size, compressed_batch_size] {
            let dir = tempfile::Builder::new()
                .prefix("test_get_entry_from_file")
                .tempdir()
                .unwrap();

            let mut cfg = Config::default();
            cfg.dir = dir.path().to_str().unwrap().to_owned();

            let engine = FileEngine::<Entry, Entry, PipeLogImpl>::new(cfg.clone());
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
            let engine = FileEngine::<Entry, Entry, PipeLogImpl>::new(cfg.clone());
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
            .prefix("test_gc_and_purge")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.target_file_size = ReadableSize::kb(5);
        cfg.purge_threshold = ReadableSize::kb(150);

        let engine = FileEngine::<Entry, Entry, PipeLogImpl>::new(cfg.clone());
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024]);
        for i in 0..100 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC all log entries. Won't trigger purge because total size is not enough.
        let count = engine.compact_to(1, 100);
        assert_eq!(count, 100);
        assert!(!engine.needs_purge_log_files());

        // Append more logs to make total size greater than `purge_threshold`.
        for i in 100..250 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC first 101 log entries.
        let count = engine.compact_to(1, 101);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.needs_purge_log_files());

        let old_min_file_num = engine.pipe_log.first_file_num();
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_num = engine.pipe_log.first_file_num();
        // Some entries are rewritten.
        assert!(new_min_file_num > old_min_file_num);
        // No regions need to be force compacted because the threshold is not reached.
        assert!(will_force_compact.is_empty());
        // After purge, entries and raft state are still available.
        assert!(engine.get_entry(1, 101).unwrap().is_some());

        let count = engine.compact_to(1, 102);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.needs_purge_log_files());
        let old_min_file_num = engine.pipe_log.first_file_num();
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_num = engine.pipe_log.first_file_num();
        // No entries are rewritten.
        assert_eq!(new_min_file_num, old_min_file_num);
        // The region needs to be force compacted because the threshold is reached.
        assert!(!will_force_compact.is_empty());
        assert_eq!(will_force_compact[0], 1);
    }

    #[test]
    fn test_cache_limit() {
        let dir = tempfile::Builder::new()
            .prefix("test_recover_with_cache_limit")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.purge_threshold = ReadableSize::mb(70);
        cfg.target_file_size = ReadableSize::mb(8);
        cfg.cache_limit = ReadableSize::mb(10);

        let engine = FileEngine::<Entry, Entry, PipeLogImpl>::new_impl(cfg.clone(), 512 * 1024);

        // Append some entries with total size 100M.
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024]);
        for idx in 1..=10 {
            for raft_id in 1..=10000 {
                entry.set_index(idx);
                append_log(&engine, raft_id, &entry);
            }
        }

        let cache_size = engine.cache_stats.cache_size();
        assert!(cache_size <= 10 * 1024 * 1024);

        // Recover from log files.
        engine.stop();
        drop(engine);
        let engine = FileEngine::<Entry, Entry, PipeLogImpl>::new_impl(cfg.clone(), 512 * 1024);
        let cache_size = engine.cache_stats.cache_size();
        assert!(cache_size <= 10 * 1024 * 1024);

        // Rewrite inactive logs.
        for raft_id in 1..=10000 {
            engine.compact_to(raft_id, 8);
        }
        let ret = engine.purge_expired_files().unwrap();
        if !ret.is_empty() {
            for x in ret.iter() {
                println!("region: {}", *x);
            }
        }
        assert!(ret.is_empty());
        let cache_size = engine.cache_stats.cache_size();
        assert!(cache_size <= 10 * 1024 * 1024);
    }
}
