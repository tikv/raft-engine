use std::collections::HashSet;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, Instant};
use std::{mem, u64};

use log::{debug, info, trace};
use protobuf::Message;

use crate::cache_evict::{
    CacheSubmitor, CacheTask, Runner as CacheEvictRunner, DEFAULT_CACHE_CHUNK_SIZE,
};
use crate::config::{Config, RecoveryMode};
use crate::log_batch::{
    self, Command, CompressionType, EntryExt, LogBatch, LogItemContent, OpType, CHECKSUM_LEN,
    HEADER_LEN,
};
use crate::memtable::{EntryIndex, MemTable};
use crate::pipe_log::{GenericFileId, GenericPipeLog, LogQueue, PipeLog, PipeLogHook};
use crate::purge::{PurgeHook, PurgeManager};
use crate::util::{HandyRwLock, HashMap, Worker};
use crate::{codec, CacheStats, GlobalStats, Result};

const SLOTS_COUNT: usize = 128;

// Modifying MemTables collection requires a write lock.
type MemTables<E, W, P> = HashMap<u64, Arc<RwLock<MemTable<E, W, P>>>>;

/// Collection of MemTables, indexed by Raft group ID.
#[derive(Clone)]
pub struct MemTableAccessor<E: Message + Clone, W: EntryExt<E>, P: GenericPipeLog> {
    slots: Vec<Arc<RwLock<MemTables<E, W, P>>>>,
    initializer: Arc<dyn Fn(u64) -> MemTable<E, W, P> + Send + Sync>,

    #[allow(clippy::type_complexity)]
    removed_memtables: Arc<Mutex<HashMap<P::FileId, u64>>>,
}

impl<E, W, P> MemTableAccessor<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E>,
    P: GenericPipeLog,
{
    pub fn new(
        initializer: Arc<dyn Fn(u64) -> MemTable<E, W, P> + Send + Sync>,
    ) -> MemTableAccessor<E, W, P> {
        let mut slots = Vec::with_capacity(SLOTS_COUNT);
        for _ in 0..SLOTS_COUNT {
            slots.push(Arc::new(RwLock::new(MemTables::default())));
        }
        MemTableAccessor {
            slots,
            initializer,
            removed_memtables: Default::default(),
        }
    }

    pub fn get_or_insert(&self, raft_group_id: u64) -> Arc<RwLock<MemTable<E, W, P>>> {
        let mut memtables = self.slots[raft_group_id as usize % SLOTS_COUNT].wl();
        let memtable = memtables
            .entry(raft_group_id)
            .or_insert_with(|| Arc::new(RwLock::new((self.initializer)(raft_group_id))));
        memtable.clone()
    }

    pub fn get(&self, raft_group_id: u64) -> Option<Arc<RwLock<MemTable<E, W, P>>>> {
        self.slots[raft_group_id as usize % SLOTS_COUNT]
            .rl()
            .get(&raft_group_id)
            .cloned()
    }

    pub fn insert(&self, raft_group_id: u64, memtable: Arc<RwLock<MemTable<E, W, P>>>) {
        self.slots[raft_group_id as usize % SLOTS_COUNT]
            .wl()
            .insert(raft_group_id, memtable);
    }

    pub fn remove(&self, raft_group_id: u64, queue: LogQueue, file_id: P::FileId) {
        self.slots[raft_group_id as usize % SLOTS_COUNT]
            .wl()
            .remove(&raft_group_id);
        if queue == LogQueue::Append {
            let mut removed_memtables = self.removed_memtables.lock().unwrap();
            removed_memtables.insert(file_id, raft_group_id);
        }
    }

    pub fn fold<B, F: Fn(B, &MemTable<E, W, P>) -> B>(&self, mut init: B, fold: F) -> B {
        for tables in &self.slots {
            for memtable in tables.rl().values() {
                init = fold(init, &*memtable.rl());
            }
        }
        init
    }

    pub fn collect<F: FnMut(&MemTable<E, W, P>) -> bool>(
        &self,
        mut condition: F,
    ) -> Vec<Arc<RwLock<MemTable<E, W, P>>>> {
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

    pub fn clean_regions_before(&self, rewrite_file_id: P::FileId) -> LogBatch<E, W, P::FileId> {
        let mut log_batch = LogBatch::<E, W, P::FileId>::default();
        let mut removed_memtables = self.removed_memtables.lock().unwrap();
        removed_memtables.retain(|&file_id, raft_id| {
            if file_id <= rewrite_file_id {
                log_batch.clean_region(*raft_id);
                false
            } else {
                true
            }
        });
        log_batch
    }

    // Only used for recover.
    pub fn cleaned_region_ids(&self) -> HashSet<u64> {
        let mut ids = HashSet::default();
        let removed_memtables = self.removed_memtables.lock().unwrap();
        for (_, raft_id) in removed_memtables.iter() {
            ids.insert(*raft_id);
        }
        ids
    }
}

#[derive(Clone)]
pub struct Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
{
    cfg: Arc<Config>,
    pub(crate) memtables: MemTableAccessor<E, W, P>,
    pipe_log: P,
    global_stats: Arc<GlobalStats>,
    purge_manager: PurgeManager<E, W, P>,

    workers: Arc<RwLock<Workers<P>>>,
}

impl<E, W, P> Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone + 'static,
    P: GenericPipeLog,
{
    fn apply_to_memtable(
        memtables: &MemTableAccessor<E, W, P>,
        log_batch: &mut LogBatch<E, W, P::FileId>,
        queue: LogQueue,
        file_id: P::FileId,
    ) {
        for item in log_batch.items.drain(..) {
            let raft = item.raft_group_id;
            let memtable = memtables.get_or_insert(raft);
            match item.content {
                LogItemContent::Entries(entries_to_add) => {
                    let entries = entries_to_add.entries;
                    let entries_index = entries_to_add.entries_index;
                    debug!(
                        "{} append to {:?}.{}, Entries[{:?}, {:?}:{:?})",
                        raft,
                        queue,
                        file_id,
                        entries_index.first().map(|x| x.queue),
                        entries_index.first().map(|x| x.index),
                        entries_index.last().map(|x| x.index + 1),
                    );
                    if queue == LogQueue::Rewrite {
                        memtable.wl().append_rewrite(entries, entries_index);
                    } else {
                        memtable.wl().append(entries, entries_index);
                    }
                }
                LogItemContent::Command(Command::Clean) => {
                    debug!("{} append to {:?}.{}, Clean", raft, queue, file_id);
                    memtables.remove(raft, queue, file_id);
                }
                LogItemContent::Command(Command::Compact { index }) => {
                    debug!(
                        "{} append to {:?}.{}, Compact({})",
                        raft, queue, file_id, index
                    );
                    memtable.wl().compact_to(index);
                }
                LogItemContent::Kv(kv) => match kv.op_type {
                    OpType::Put => {
                        let (key, value) = (kv.key, kv.value.unwrap());
                        debug!(
                            "{} append to {:?}.{}, Put({}, {})",
                            raft,
                            queue,
                            file_id,
                            hex::encode(&key),
                            hex::encode(&value)
                        );
                        match queue {
                            LogQueue::Append => memtable.wl().put(key, value, file_id),
                            LogQueue::Rewrite => memtable.wl().put_rewrite(key, value, file_id),
                        }
                    }
                    OpType::Del => {
                        let key = kv.key;
                        debug!(
                            "{} append to {:?}.{}, Del({})",
                            raft,
                            queue,
                            file_id,
                            hex::encode(&key),
                        );
                        memtable.wl().delete(key.as_slice());
                    }
                },
            }
        }
    }

    fn write_impl(&self, log_batch: &mut LogBatch<E, W, P::FileId>, sync: bool) -> Result<usize> {
        if log_batch.items.is_empty() {
            if sync {
                self.pipe_log.sync(LogQueue::Append)?;
            }
            Ok(0)
        } else {
            let (file_id, bytes) = self.pipe_log.append(LogQueue::Append, log_batch, sync)?;
            if file_id.valid() {
                Self::apply_to_memtable(&self.memtables, log_batch, LogQueue::Append, file_id);
                for hook in self.pipe_log.hooks() {
                    hook.post_apply_memtables(LogQueue::Append, file_id);
                }
            }
            Ok(bytes)
        }
    }
}

struct Workers<P>
where
    P: GenericPipeLog,
{
    cache_evict: Worker<CacheTask<P::FileId>>,
}

// An internal structure for tests.
struct EngineOptions {
    chunk_limit: usize,
    hooks: Vec<Arc<dyn PipeLogHook<PipeLog>>>,
}

impl Default for EngineOptions {
    fn default() -> EngineOptions {
        EngineOptions {
            chunk_limit: DEFAULT_CACHE_CHUNK_SIZE,
            hooks: vec![],
        }
    }
}

impl<E, W> Engine<E, W, PipeLog>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone + 'static,
{
    pub fn new(cfg: Config) -> Engine<E, W, PipeLog> {
        let options = Default::default();
        Self::with_options(cfg, options).unwrap()
    }

    fn with_options(cfg: Config, mut options: EngineOptions) -> Result<Engine<E, W, PipeLog>> {
        let cache_limit = cfg.cache_limit.0 as usize;
        let global_stats = Arc::new(GlobalStats::default());

        let mut cache_evict_worker = Worker::new("cache_evict".to_owned(), None);

        let mut hooks = vec![Arc::new(PurgeHook::new()) as Arc<dyn PipeLogHook<PipeLog>>];
        hooks.extend(options.hooks.drain(..));
        let pipe_log = PipeLog::open(
            &cfg,
            CacheSubmitor::new(
                cache_limit,
                options.chunk_limit,
                cache_evict_worker.scheduler(),
                global_stats.clone(),
            ),
            hooks,
        )
        .expect("Open raft log");

        let memtables = {
            let stats = global_stats.clone();
            MemTableAccessor::<E, W, PipeLog>::new(Arc::new(move |id: u64| {
                MemTable::new(id, stats.clone())
            }))
        };

        let cache_evict_runner = CacheEvictRunner::new(
            cache_limit,
            options.chunk_limit,
            global_stats.clone(),
            memtables.clone(),
            pipe_log.clone(),
        );
        cache_evict_worker.start(cache_evict_runner, Some(Duration::from_secs(1)));

        let cfg = Arc::new(cfg);
        let purge_manager = PurgeManager::new(
            cfg.clone(),
            memtables.clone(),
            pipe_log.clone(),
            global_stats.clone(),
        );

        let mut engine = Engine {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            purge_manager,
            workers: Arc::new(RwLock::new(Workers {
                cache_evict: cache_evict_worker,
            })),
        };

        engine.pipe_log.set_recovery_mode(true);
        engine.recover_from_queues()?;
        engine.pipe_log.set_recovery_mode(false);

        Ok(engine)
    }

    fn recover_from_queues(&mut self) -> Result<()> {
        let mut rewrite_memtables = MemTableAccessor::new(self.memtables.initializer.clone());
        Self::recover(
            &mut rewrite_memtables,
            &mut self.pipe_log,
            LogQueue::Rewrite,
            RecoveryMode::TolerateCorruptedTailRecords,
        )?;

        Self::recover(
            &mut self.memtables,
            &mut self.pipe_log,
            LogQueue::Append,
            self.cfg.recovery_mode,
        )?;

        let ids = self.memtables.cleaned_region_ids();
        for slot in rewrite_memtables.slots.into_iter() {
            for (id, raft_rewrite) in mem::take(&mut *slot.wl()) {
                if let Some(raft_append) = self.memtables.get(id) {
                    raft_append.wl().merge_lower_prio(&mut *raft_rewrite.wl());
                } else if !ids.contains(&id) {
                    self.memtables.insert(id, raft_rewrite);
                }
            }
        }

        Ok(())
    }

    // Recover from disk.
    fn recover(
        memtables: &mut MemTableAccessor<E, W, PipeLog>,
        pipe_log: &mut PipeLog,
        queue: LogQueue,
        recovery_mode: RecoveryMode,
    ) -> Result<()> {
        // Get first file number and last file number.
        let first_file = pipe_log.first_file_id(queue);
        let active_file = pipe_log.active_file_id(queue);
        trace!("recovering queue {:?}", queue);

        // Iterate and recover from files one by one.
        let start = Instant::now();
        let mut batches = Vec::new();
        let mut file_id = first_file;
        while file_id <= active_file {
            pipe_log.read_file(queue, file_id, recovery_mode, &mut batches)?;
            for mut batch in batches.drain(..) {
                Self::apply_to_memtable(memtables, &mut batch, queue, file_id);
            }
            file_id = file_id.forward(1);
        }
        info!("Recover raft log takes {:?}", start.elapsed());
        Ok(())
    }
}

impl<E, W, P> Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone + 'static,
    P: GenericPipeLog + 'static,
{
    /// Synchronize the Raft engine.
    pub fn sync(&self) -> Result<()> {
        self.pipe_log.sync(LogQueue::Append)
    }

    pub fn put(&self, region_id: u64, key: &[u8], value: &[u8]) -> Result<()> {
        let mut log_batch = LogBatch::default();
        log_batch.put(region_id, key.to_vec(), value.to_vec());
        self.write(&mut log_batch, false).map(|_| ())
    }

    pub fn put_msg<M: protobuf::Message>(&self, region_id: u64, key: &[u8], m: &M) -> Result<()> {
        let mut log_batch = LogBatch::default();
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
        let entry = read_entry_from_file::<_, W, _>(&self.pipe_log, &entry_idx)?;
        Ok(Some(entry))
    }

    /// Purge expired logs files and return a set of Raft group ids
    /// which needs to be compacted ASAP.
    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        self.purge_manager.purge_expired_files()
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
            let old_len = vec.len();
            fetch_entries(
                &self.pipe_log,
                &memtable,
                vec,
                (end - begin) as usize,
                |t, ents, ents_idx| t.fetch_entries_to(begin, end, max_size, ents, ents_idx),
            )?;
            return Ok(vec.len() - old_len);
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

        let mut log_batch = LogBatch::default();
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
    pub fn write(&self, log_batch: &mut LogBatch<E, W, P::FileId>, sync: bool) -> Result<usize> {
        self.write_impl(log_batch, sync)
    }

    /// Flush stats about EntryCache.
    pub fn flush_cache_stats(&self) -> CacheStats {
        self.global_stats.flush_cache_stats()
    }

    /// Stop background thread which will keep trying evict caching.
    pub fn stop(&self) {
        let mut workers = self.workers.wl();
        workers.cache_evict.stop();
    }

    pub fn raft_groups(&self) -> Vec<u64> {
        self.memtables.fold(vec![], |mut v, m| {
            v.push(m.region_id());
            v
        })
    }
}

pub fn fetch_entries<E, W, P, F>(
    pipe_log: &P,
    memtable: &RwLock<MemTable<E, W, P>>,
    vec: &mut Vec<E>,
    count: usize,
    mut fetch: F,
) -> Result<()>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
    F: FnMut(&MemTable<E, W, P>, &mut Vec<E>, &mut Vec<EntryIndex<P::FileId>>) -> Result<()>,
{
    let mut ents = Vec::with_capacity(count);
    let mut ents_idx = Vec::with_capacity(count);
    fetch(&*memtable.rl(), &mut ents, &mut ents_idx)?;

    for ei in ents_idx {
        vec.push(read_entry_from_file::<_, W, _>(pipe_log, &ei)?);
    }
    vec.extend(ents.into_iter());
    Ok(())
}

pub fn read_entry_from_file<E, W, P>(pipe_log: &P, entry_index: &EntryIndex<P::FileId>) -> Result<E>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
{
    let queue = entry_index.queue;
    let file_id = entry_index.file_id;
    let base_offset = entry_index.base_offset;
    let batch_len = entry_index.batch_len;
    let offset = entry_index.offset;
    let len = entry_index.len;

    let entry_content = match entry_index.compression_type {
        CompressionType::None => {
            let offset = base_offset + offset;
            pipe_log.read_bytes(queue, file_id, offset, len)?
        }
        CompressionType::Lz4 => {
            let read_len = batch_len + HEADER_LEN as u64;
            let compressed = pipe_log.read_bytes(queue, file_id, base_offset, read_len)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::ReadableSize;
    use raft::eraftpb::Entry;

    impl<E, W, P> Engine<E, W, P>
    where
        E: Message + Clone,
        W: EntryExt<E> + Clone + 'static,
        P: GenericPipeLog,
    {
        pub fn purge_manager(&self) -> &PurgeManager<E, W, P> {
            &self.purge_manager
        }
    }

    type RaftLogEngine = Engine<Entry, Entry, PipeLog>;
    impl RaftLogEngine {
        fn append(&self, raft_group_id: u64, entries: Vec<Entry>) -> Result<usize> {
            let mut batch = LogBatch::default();
            batch.add_entries(raft_group_id, entries);
            self.write(&mut batch, false)
        }
    }

    fn append_log(engine: &RaftLogEngine, raft: u64, entry: &Entry) {
        let mut log_batch = LogBatch::default();
        log_batch.add_entries(raft, vec![entry.clone()]);
        let last_index = format!("{}", entry.index).into_bytes();
        log_batch.put(raft, b"last_index".to_vec(), last_index);
        engine.write(&mut log_batch, false).unwrap();
    }

    fn last_index(engine: &RaftLogEngine, raft: u64) -> u64 {
        let s = engine.get(raft, b"last_index").unwrap().unwrap();
        std::str::from_utf8(&s).unwrap().parse().unwrap()
    }

    #[test]
    fn test_clean_memtable() {
        let dir = tempfile::Builder::new()
            .prefix("test_clean_memtable")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.target_file_size = ReadableSize::kb(5);
        cfg.purge_threshold = ReadableSize::kb(80);
        let engine = RaftLogEngine::new(cfg.clone());

        append_log(&engine, 1, &Entry::new());
        assert!(engine.memtables.get(1).is_some());

        let mut log_batch = LogBatch::default();
        log_batch.clean_region(1);
        engine.write(&mut log_batch, false).unwrap();
        assert!(engine.memtables.get(1).is_none());
    }

    #[test]
    fn test_get_entry_from_file_id() {
        let normal_batch_size = 10;
        let compressed_batch_size = 5120;
        for &entry_size in &[normal_batch_size, compressed_batch_size] {
            let dir = tempfile::Builder::new()
                .prefix("test_get_entry_from_file")
                .tempdir()
                .unwrap();

            let mut cfg = Config::default();
            cfg.dir = dir.path().to_str().unwrap().to_owned();

            let engine = RaftLogEngine::new(cfg.clone());
            let mut entry = Entry::new();
            entry.set_data(vec![b'x'; entry_size].into());
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
            let engine = RaftLogEngine::new(cfg.clone());
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

        let engine = RaftLogEngine::new(cfg.clone());
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());
        for i in 0..100 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC all log entries. Won't trigger purge because total size is not enough.
        let count = engine.compact_to(1, 100);
        assert_eq!(count, 100);
        assert!(!engine.purge_manager.needs_purge_log_files(LogQueue::Append));

        // Append more logs to make total size greater than `purge_threshold`.
        for i in 100..250 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC first 101 log entries.
        let count = engine.compact_to(1, 101);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));

        let old_min_file_id = engine.pipe_log.first_file_id(LogQueue::Append);
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_id = engine.pipe_log.first_file_id(LogQueue::Append);
        // Some entries are rewritten.
        assert!(new_min_file_id > old_min_file_id);
        // No regions need to be force compacted because the threshold is not reached.
        assert!(will_force_compact.is_empty());
        // After purge, entries and raft state are still available.
        assert!(engine.get_entry(1, 101).unwrap().is_some());

        let count = engine.compact_to(1, 102);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        let old_min_file_id = engine.pipe_log.first_file_id(LogQueue::Append);
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_id = engine.pipe_log.first_file_id(LogQueue::Append);
        // No entries are rewritten.
        assert_eq!(new_min_file_id, old_min_file_id);
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

        let mut options = EngineOptions::default();
        options.chunk_limit = 512 * 1024;
        let engine = RaftLogEngine::with_options(cfg.clone(), options).unwrap();

        // Append some entries with total size 100M.
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());
        for idx in 1..=10 {
            for raft_id in 1..=10000 {
                entry.set_index(idx);
                append_log(&engine, raft_id, &entry);
            }
        }
        let cache_size = engine.global_stats.cache_size();
        assert!(
            cache_size <= 10 * 1024 * 1024,
            "cache size {} > 10M",
            cache_size
        );

        // Recover from log files.
        engine.stop();
        drop(engine);
        let mut options = EngineOptions::default();
        options.chunk_limit = 512 * 1024;
        let engine = RaftLogEngine::with_options(cfg.clone(), options).unwrap();
        let cache_size = engine.global_stats.cache_size();
        assert!(cache_size <= 10 * 1024 * 1024);

        // Rewrite inactive logs.
        for raft_id in 1..=10000 {
            engine.compact_to(raft_id, 8);
        }
        let ret = engine.purge_expired_files().unwrap();
        assert!(ret.is_empty());
        let cache_size = engine.global_stats.cache_size();
        assert!(cache_size <= 10 * 1024 * 1024);
    }

    #[test]
    fn test_rewrite_and_recover() {
        let dir = tempfile::Builder::new()
            .prefix("test_rewrite_and_recover")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.cache_limit = ReadableSize::kb(0);
        cfg.target_file_size = ReadableSize::kb(5);
        cfg.purge_threshold = ReadableSize::kb(80);
        let engine = RaftLogEngine::new(cfg.clone());

        // Put 100 entries into 10 regions.
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());
        for i in 1..=10 {
            for j in 1..=10 {
                entry.set_index(i);
                append_log(&engine, j, &entry);
            }
        }

        // The engine needs purge, and all old entries should be rewritten.
        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.first_file_id(LogQueue::Append) > 1);

        let active_num = engine.pipe_log.active_file_id(LogQueue::Rewrite);
        let active_len = engine
            .pipe_log
            .file_len(LogQueue::Rewrite, active_num)
            .unwrap();
        assert!(active_num > 1 || active_len > 59); // The rewrite queue isn't empty.

        // All entries should be available.
        for i in 1..=10 {
            for j in 1..=10 {
                let e = engine.get_entry(j, i).unwrap().unwrap();
                assert_eq!(e.get_data(), entry.get_data());
                assert_eq!(last_index(&engine, j), 10);
            }
        }

        // Recover with rewrite queue and append queue.
        drop(engine);
        let engine = RaftLogEngine::new(cfg.clone());
        for i in 1..=10 {
            for j in 1..=10 {
                let e = engine.get_entry(j, i).unwrap().unwrap();
                assert_eq!(e.get_data(), entry.get_data());
                assert_eq!(last_index(&engine, j), 10);
            }
        }

        // Rewrite again to check the rewrite queue is healthy.
        for i in 11..=20 {
            for j in 1..=10 {
                entry.set_index(i);
                append_log(&engine, j, &entry);
            }
        }

        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());

        let new_active_num = engine.pipe_log.active_file_id(LogQueue::Rewrite);
        let new_active_len = engine
            .pipe_log
            .file_len(LogQueue::Rewrite, active_num)
            .unwrap();
        assert!(
            new_active_num > active_num
                || (new_active_num == active_num && new_active_len > active_len)
        );
    }

    // Raft groups can be removed when they only have entries in the rewrite queue.
    // We need to ensure that these raft groups won't appear again after recover.
    fn test_clean_raft_with_only_rewrite(purge_before_recover: bool) {
        let dir = tempfile::Builder::new()
            .prefix("test_clean_raft_with_rewrite")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.cache_limit = ReadableSize::kb(0);
        cfg.target_file_size = ReadableSize::kb(128);
        cfg.purge_threshold = ReadableSize::kb(512);
        let engine = RaftLogEngine::new(cfg.clone());

        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());

        // Layout of region 1 in file 1:
        // entries[1..10], Clean, entries[2..11]
        for j in 1..=10 {
            entry.set_index(j);
            append_log(&engine, 1, &entry);
        }
        let mut log_batch = LogBatch::with_capacity(1);
        log_batch.clean_region(1);
        engine.write(&mut log_batch, false).unwrap();
        assert!(engine.memtables.get(1).is_none());

        entry.set_data(vec![b'y'; 1024].into());
        for j in 2..=11 {
            entry.set_index(j);
            append_log(&engine, 1, &entry);
        }

        assert_eq!(engine.pipe_log.active_file_id(LogQueue::Append), 1);

        // Put more raft logs to trigger purge.
        for i in 2..64 {
            for j in 1..=10 {
                entry.set_index(j);
                append_log(&engine, i, &entry);
            }
        }

        // The engine needs purge, and all old entries should be rewritten.
        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.first_file_id(LogQueue::Append) > 1);

        // All entries of region 1 has been rewritten.
        let memtable_1 = engine.memtables.get(1).unwrap();
        assert!(memtable_1.rl().max_file_id(LogQueue::Append).is_none());
        assert!(memtable_1.rl().kvs_max_file_id(LogQueue::Append).is_none());
        // Entries of region 1 after the clean command should be still valid.
        for j in 2..=11 {
            let entry_j = engine.get_entry(1, j).unwrap().unwrap();
            assert_eq!(entry_j.get_data(), entry.get_data());
        }

        // Clean the raft group again.
        let mut log_batch = LogBatch::with_capacity(1);
        log_batch.clean_region(1);
        engine.write(&mut log_batch, false).unwrap();
        assert!(engine.memtables.get(1).is_none());

        // Put more raft logs and then recover.
        let active_file = engine.pipe_log.active_file_id(LogQueue::Append);
        for i in 64..=128 {
            for j in 1..=10 {
                entry.set_index(j);
                append_log(&engine, i, &entry);
            }
        }

        if purge_before_recover {
            assert!(engine.purge_expired_files().unwrap().is_empty());
            assert!(engine.pipe_log.first_file_id(LogQueue::Append) > active_file);
        }

        // After the engine recovers, the removed raft group shouldn't appear again.
        drop(engine);
        let engine = RaftLogEngine::new(cfg.clone());
        assert!(engine.memtables.get(1).is_none());
    }

    // Test `purge` should copy `LogBatch::Clean` to rewrite queue from append queue.
    // So that after recover the cleaned raft group won't appear again.
    #[test]
    fn test_clean_raft_with_only_rewrite_1() {
        test_clean_raft_with_only_rewrite(true);
    }

    // Test `recover` can handle `LogBatch::Clean` in append queue correctly.
    #[test]
    fn test_clean_raft_with_only_rewrite_2() {
        test_clean_raft_with_only_rewrite(false);
    }

    #[test]
    fn test_pipe_log_hooks() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::sync::mpsc;
        use std::time::Duration;

        #[derive(Default)]
        struct QueueHook {
            files: AtomicUsize,
            batches: AtomicUsize,
            applys: AtomicUsize,
            purged: AtomicU64,
        }
        impl QueueHook {
            fn files(&self) -> usize {
                self.files.load(Ordering::Acquire)
            }
            fn batches(&self) -> usize {
                self.batches.load(Ordering::Acquire)
            }
            fn applys(&self) -> usize {
                self.applys.load(Ordering::Acquire)
            }
            fn purged(&self) -> u64 {
                self.purged.load(Ordering::Acquire)
            }
        }

        struct Hook(HashMap<LogQueue, QueueHook>);
        impl Default for Hook {
            fn default() -> Hook {
                let mut hash = HashMap::default();
                hash.insert(LogQueue::Append, QueueHook::default());
                hash.insert(LogQueue::Rewrite, QueueHook::default());
                Hook(hash)
            }
        }

        impl PipeLogHook<PipeLog> for Hook {
            fn post_new_log_file(&self, queue: LogQueue, _: u64) {
                self.0[&queue].files.fetch_add(1, Ordering::Release);
            }

            fn on_append_log_file(&self, queue: LogQueue, _: u64) {
                self.0[&queue].batches.fetch_add(1, Ordering::Release);
            }

            fn post_apply_memtables(&self, queue: LogQueue, _: u64) {
                self.0[&queue].applys.fetch_add(1, Ordering::Release);
            }

            fn ready_for_purge(&self, _: LogQueue, _: u64) -> bool {
                // To test the default hook's logic.
                true
            }

            fn post_purge(&self, queue: LogQueue, file_id: u64) {
                self.0[&queue].purged.store(file_id, Ordering::Release);
            }
        }

        let dir = tempfile::Builder::new()
            .prefix("test_clean_raft_with_rewrite")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.target_file_size = ReadableSize::kb(128);
        cfg.purge_threshold = ReadableSize::kb(512);
        cfg.cache_limit = ReadableSize::kb(0);
        cfg.batch_compression_threshold = ReadableSize::kb(0);

        let hook = Arc::new(Hook::default());

        let mut options = EngineOptions::default();
        options.hooks = vec![hook.clone()];
        let engine = RaftLogEngine::with_options(cfg.clone(), options).unwrap();
        assert_eq!(hook.0[&LogQueue::Append].files(), 1);
        assert_eq!(hook.0[&LogQueue::Rewrite].files(), 1);

        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 64 * 1024].into());

        // Append 10 logs for region 1, 10 logs for region 2.
        for i in 1..=20 {
            let region_id = (i as u64 - 1) % 2 + 1;
            entry.set_index((i as u64 + 1) / 2);
            append_log(&engine, region_id, &entry);
            assert_eq!(hook.0[&LogQueue::Append].batches(), i);
            assert_eq!(hook.0[&LogQueue::Append].applys(), i);
        }
        assert_eq!(hook.0[&LogQueue::Append].files(), 10);

        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        engine.purge_manager.purge_expired_files().unwrap();
        assert_eq!(hook.0[&LogQueue::Append].purged(), 8);

        // All things in a region will in one write batch.
        assert_eq!(hook.0[&LogQueue::Rewrite].files(), 2);
        assert_eq!(hook.0[&LogQueue::Rewrite].batches(), 2);
        assert_eq!(hook.0[&LogQueue::Rewrite].applys(), 2);

        // Compact so that almost all content of rewrite queue will become garbage.
        for i in 21..=30 {
            let region_id = (i as u64 - 1) % 2 + 1;
            entry.set_index((i as u64 + 1) / 2);
            append_log(&engine, region_id, &entry);
            assert_eq!(hook.0[&LogQueue::Append].batches(), i);
            assert_eq!(hook.0[&LogQueue::Append].applys(), i);
        }
        engine.compact_to(1, 28);
        engine.compact_to(2, 28);
        assert_eq!(hook.0[&LogQueue::Append].batches(), 32);
        assert_eq!(hook.0[&LogQueue::Append].applys(), 32);
        assert!(engine.purge_manager.rewrite_queue_needs_squeeze());

        engine.purge_manager.purge_expired_files().unwrap();
        assert_eq!(hook.0[&LogQueue::Append].purged(), 13);
        assert_eq!(hook.0[&LogQueue::Rewrite].purged(), 2);

        // Use an another thread to lock memtable of region 3.
        let (tx, rx) = mpsc::sync_channel(1);
        let table_3 = engine.memtables.get_or_insert(3);
        let th1 = std::thread::spawn(move || {
            let x = table_3.rl();
            let _ = rx.recv().unwrap();
            drop(x);
        });

        std::thread::sleep(Duration::from_millis(200));

        // Use an another thread to push something to region 3.
        let engine_clone = engine.clone();
        let mut entry_clone = entry.clone();
        let th2 = std::thread::spawn(move || {
            entry_clone.set_index(1);
            append_log(&engine_clone, 3, &entry_clone);
        });

        // Sleep a while to wait the log batch `Append(3, [1])` to get written.
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(hook.0[&LogQueue::Append].batches(), 33);
        assert_eq!(hook.0[&LogQueue::Append].applys(), 32);

        for i in 31..=40 {
            let region_id = (i as u64 - 1) % 2 + 1;
            entry.set_index((i as u64 + 1) / 2);
            append_log(&engine, region_id, &entry);
            assert_eq!(hook.0[&LogQueue::Append].batches(), i + 3);
            assert_eq!(hook.0[&LogQueue::Append].applys(), i + 2);
        }

        // Can't purge because `purge_pender` is still not written to memtables.
        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        let old_first = engine.pipe_log.first_file_id(LogQueue::Append);
        engine.purge_manager.purge_expired_files().unwrap();
        let new_first = engine.pipe_log.first_file_id(LogQueue::Append);
        assert_eq!(old_first, new_first);

        // Release the lock on region 3. Then can purge.
        tx.send(0i32).unwrap();
        th1.join().unwrap();
        th2.join().unwrap();

        std::thread::sleep(Duration::from_millis(200));
        engine.purge_manager.purge_expired_files().unwrap();
        let new_first = engine.pipe_log.first_file_id(LogQueue::Append);
        assert_ne!(old_first, new_first);

        // Drop and then recover.
        drop(engine);
        drop(RaftLogEngine::new(cfg));
    }
}
