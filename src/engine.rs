use std::io::BufRead;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use std::{fmt, u64};

use log::{debug, info, trace, warn};
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
use crate::pipe_log::{GenericPipeLog, LogQueue, PipeLog, PipeLogHook, FILE_MAGIC_HEADER, VERSION};
use crate::purge::{PurgeHook, PurgeManager};
use crate::util::{HandyRwLock, HashMap, Worker};
use crate::{codec, CacheStats, GlobalStats, Result};

const SLOTS_COUNT: usize = 128;

// When add/delete a memtable, a write lock is required.
// We can use LRU cache or slots to reduce conflicts.
type MemTables<E, W> = HashMap<u64, Arc<RwLock<MemTable<E, W>>>>;

pub struct MemTableAccessor<E: Message + Clone, W: EntryExt<E>> {
    slots: Vec<Arc<RwLock<MemTables<E, W>>>>,
    creator: Arc<dyn Fn(u64) -> MemTable<E, W> + Send + Sync>,
}

impl<E, W> Clone for MemTableAccessor<E, W>
where
    E: Message + Clone,
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
    E: Message + Clone,
    W: EntryExt<E>,
{
    pub fn new(
        creator: Arc<dyn Fn(u64) -> MemTable<E, W> + Send + Sync>,
    ) -> MemTableAccessor<E, W> {
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

    pub fn remove(&self, raft_group_id: u64) -> Option<Arc<RwLock<MemTable<E, W>>>> {
        let mut memtables = self.slots[raft_group_id as usize % SLOTS_COUNT]
            .write()
            .unwrap();
        memtables.remove(&raft_group_id)
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
pub struct Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor<E, W>,
    pipe_log: P,
    global_stats: Arc<GlobalStats>,
    purge_manager: PurgeManager<E, W, P>,

    workers: Arc<RwLock<Workers>>,
}

impl<E, W, P> Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone + 'static,
    P: GenericPipeLog,
{
    fn apply_to_memtable(&self, log_batch: &mut LogBatch<E, W>, queue: LogQueue, file_num: u64) {
        for item in log_batch.items.drain(..) {
            let raft = item.raft_group_id;
            let memtable = self.memtables.get_or_insert(raft);
            match item.content {
                LogItemContent::Entries(entries_to_add) => {
                    let entries = entries_to_add.entries;
                    let entries_index = entries_to_add.entries_index;
                    debug!(
                        "{} append to {:?}.{}, Entries[{:?}, {:?}:{:?})",
                        raft,
                        queue,
                        file_num,
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
                    debug!("{} append to {:?}.{}, Clean", raft, queue, file_num);
                    if self.memtables.remove(raft).is_some() {
                        self.purge_manager.remove_memtable(file_num, raft);
                    }
                }
                LogItemContent::Command(Command::Compact { index }) => {
                    debug!(
                        "{} append to {:?}.{}, Compact({})",
                        raft, queue, file_num, index
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
                            file_num,
                            hex::encode(&key),
                            hex::encode(&value)
                        );
                        match queue {
                            LogQueue::Append => memtable.wl().put(key, value, file_num),
                            LogQueue::Rewrite => memtable.wl().put_rewrite(key, value, file_num),
                        }
                    }
                    OpType::Del => {
                        let key = kv.key;
                        debug!(
                            "{} append to {:?}.{}, Del({})",
                            raft,
                            queue,
                            file_num,
                            hex::encode(&key),
                        );
                        memtable.wl().delete(key.as_slice());
                    }
                },
            }
        }
    }

    fn write_impl(&self, log_batch: &mut LogBatch<E, W>, sync: bool) -> Result<usize> {
        if log_batch.items.is_empty() {
            if sync {
                self.pipe_log.sync(LogQueue::Append)?;
            }
            return Ok(0);
        }

        let mut file_num = 0;
        let bytes = self.pipe_log.write(log_batch, sync, &mut file_num)?;
        if file_num > 0 {
            self.apply_to_memtable(log_batch, LogQueue::Append, file_num);
            for hook in self.pipe_log.hooks() {
                hook.post_apply_memtables(LogQueue::Append, file_num);
            }
        }
        Ok(bytes)
    }
}

impl<E, W, P> fmt::Debug for Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Engine dir: {}", self.cfg.dir)
    }
}

struct Workers {
    cache_evict: Worker<CacheTask>,
}

// An internal structure for tests.
struct EngineOptions {
    chunk_limit: usize,
    hooks: Vec<Arc<dyn PipeLogHook>>,
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

        let mut hooks = vec![Arc::new(PurgeHook::default()) as Arc<dyn PipeLogHook>];
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
            MemTableAccessor::<E, W>::new(Arc::new(move |id: u64| MemTable::new(id, stats.clone())))
        };

        let cache_evict_runner = CacheEvictRunner::new(
            cache_limit,
            global_stats.clone(),
            options.chunk_limit,
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

        let engine = Engine {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            purge_manager,
            workers: Arc::new(RwLock::new(Workers {
                cache_evict: cache_evict_worker,
            })),
        };

        engine.pipe_log.cache_submitor().block_on_full();
        engine.recover_from_queues()?;
        engine.pipe_log.cache_submitor().nonblock_on_full();

        Ok(engine)
    }

    fn recover_from_queues(&self) -> Result<()> {
        // Recover the rewrite queue then recover the append queue.
        let mut engine = self.clone();
        engine.memtables = MemTableAccessor::new(self.memtables.creator.clone());
        let rewrite_recover_mode = RecoveryMode::TolerateCorruptedTailRecords;
        engine.recover(LogQueue::Rewrite, rewrite_recover_mode)?;

        self.recover(LogQueue::Append, self.cfg.recovery_mode)?;

        let (appends, rewrites) = (&self.memtables, &engine.memtables);
        for i in 0..SLOTS_COUNT {
            for (id, raft) in rewrites.slots[i].rl().iter() {
                let mut raft_rewrite = raft.wl();
                let raft_append = appends.get_or_insert(*id);
                raft_append.wl().merge_lower_prio(&mut *raft_rewrite);
            }
        }

        Ok(())
    }

    // Recover from disk.
    fn recover(&self, queue: LogQueue, recovery_mode: RecoveryMode) -> Result<()> {
        // Get first file number and last file number.
        let first_file_num = self.pipe_log.first_file_num(queue);
        let active_file_num = self.pipe_log.active_file_num(queue);
        trace!("recovering queue {:?}", queue);

        // Iterate and recover from files one by one.
        let start = Instant::now();
        for file_num in first_file_num..=active_file_num {
            // Read a file.
            let content = self.pipe_log.read_whole_file(queue, file_num)?;

            // Verify file header.
            let mut buf = content.as_slice();
            if !buf.starts_with(FILE_MAGIC_HEADER) {
                if file_num != active_file_num {
                    warn!("Raft log header is corrupted at {:?}.{}", queue, file_num);
                    return Err(box_err!("Raft log file header is corrupted"));
                } else {
                    self.pipe_log.truncate_active_log(queue, Some(0)).unwrap();
                    break;
                }
            }

            // Iterate all LogBatch in one file.
            let start_ptr = buf.as_ptr();
            buf.consume(FILE_MAGIC_HEADER.len() + VERSION.len());
            let mut offset = (FILE_MAGIC_HEADER.len() + VERSION.len()) as u64;
            loop {
                debug!("recovering log batch at {}.{}", file_num, offset);
                let mut log_batch = match LogBatch::from_bytes(&mut buf, file_num, offset) {
                    Ok(Some(log_batch)) => log_batch,
                    Ok(None) => {
                        info!("Recovered raft log {:?}.{}.", queue, file_num);
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "Raft log content is corrupted at {:?}.{}:{}, error: {}",
                            queue, file_num, offset, e
                        );
                        // There may be a pre-allocated space at the tail of the active log.
                        if file_num == active_file_num
                            && recovery_mode == RecoveryMode::TolerateCorruptedTailRecords
                        {
                            let offset = Some(offset as usize);
                            self.pipe_log.truncate_active_log(queue, offset)?;
                            break;
                        }
                        return Err(box_err!("Raft log content is corrupted"));
                    }
                };
                if queue == LogQueue::Append {
                    let size = log_batch.entries_size();
                    let mut submitor = self.pipe_log.cache_submitor();
                    if let Some(tracker) = submitor.get_cache_tracker(file_num, offset, size) {
                        for item in log_batch.items.iter_mut() {
                            if let LogItemContent::Entries(entries) = &mut item.content {
                                entries.attach_cache_tracker(tracker.clone());
                            }
                        }
                    }
                } else {
                    // By default the queue is `LogQueue::Append`, so it's unnecessary.
                    for item in log_batch.items.iter_mut() {
                        if let LogItemContent::Entries(entries) = &mut item.content {
                            entries.set_queue_when_recover(LogQueue::Rewrite);
                        }
                    }
                }

                self.apply_to_memtable(&mut log_batch, queue, file_num);
                offset = (buf.as_ptr() as usize - start_ptr as usize) as u64;
            }
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
    pub fn write(&self, log_batch: &mut LogBatch<E, W>, sync: bool) -> Result<usize> {
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
}

pub fn fetch_entries<E, W, P, F>(
    pipe_log: &P,
    memtable: &RwLock<MemTable<E, W>>,
    vec: &mut Vec<E>,
    count: usize,
    mut fetch: F,
) -> Result<()>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
    F: FnMut(&MemTable<E, W>, &mut Vec<E>, &mut Vec<EntryIndex>) -> Result<()>,
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

pub fn read_entry_from_file<E, W, P>(pipe_log: &P, entry_index: &EntryIndex) -> Result<E>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
{
    let queue = entry_index.queue;
    let file_num = entry_index.file_num;
    let base_offset = entry_index.base_offset;
    let batch_len = entry_index.batch_len;
    let offset = entry_index.offset;
    let len = entry_index.len;

    let entry_content = match entry_index.compression_type {
        CompressionType::None => {
            let offset = base_offset + offset;
            pipe_log.fread(queue, file_num, offset, len)?
        }
        CompressionType::Lz4 => {
            let read_len = batch_len + HEADER_LEN as u64;
            let compressed = pipe_log.fread(queue, file_num, base_offset, read_len)?;
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

            let engine = RaftLogEngine::new(cfg.clone());
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
        entry.set_data(vec![b'x'; 1024]);
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

        let old_min_file_num = engine.pipe_log.first_file_num(LogQueue::Append);
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_num = engine.pipe_log.first_file_num(LogQueue::Append);
        // Some entries are rewritten.
        assert!(new_min_file_num > old_min_file_num);
        // No regions need to be force compacted because the threshold is not reached.
        assert!(will_force_compact.is_empty());
        // After purge, entries and raft state are still available.
        assert!(engine.get_entry(1, 101).unwrap().is_some());

        let count = engine.compact_to(1, 102);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        let old_min_file_num = engine.pipe_log.first_file_num(LogQueue::Append);
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_num = engine.pipe_log.first_file_num(LogQueue::Append);
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

        let mut options = EngineOptions::default();
        options.chunk_limit = 512 * 1024;
        let engine = RaftLogEngine::with_options(cfg.clone(), options).unwrap();

        // Append some entries with total size 100M.
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024]);
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
        entry.set_data(vec![b'x'; 1024]);
        for i in 1..=10 {
            for j in 1..=10 {
                entry.set_index(i);
                append_log(&engine, j, &entry);
            }
        }

        // The engine needs purge, and all old entries should be rewritten.
        assert!(engine.purge_manager.needs_purge_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.first_file_num(LogQueue::Append) > 1);

        let active_num = engine.pipe_log.active_file_num(LogQueue::Rewrite);
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

        let new_active_num = engine.pipe_log.active_file_num(LogQueue::Rewrite);
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
    #[test]
    fn test_clean_raft_with_rewrite() {
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
        entry.set_data(vec![b'x'; 1024]);

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

        entry.set_data(vec![b'y'; 1024]);
        for j in 2..=11 {
            entry.set_index(j);
            append_log(&engine, 1, &entry);
        }

        assert_eq!(engine.pipe_log.active_file_num(LogQueue::Append), 1);

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
        assert!(engine.pipe_log.first_file_num(LogQueue::Append) > 1);

        // All entries of region 1 has been rewritten.
        let memtable_1 = engine.memtables.get(1).unwrap();
        assert!(memtable_1.rl().max_file_num(LogQueue::Append).is_none());
        assert!(memtable_1.rl().kvs_max_file_num(LogQueue::Append).is_none());
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
        let active_file_num = engine.pipe_log.active_file_num(LogQueue::Append);
        for i in 64..=128 {
            for j in 1..=10 {
                entry.set_index(j);
                append_log(&engine, i, &entry);
            }
        }
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.first_file_num(LogQueue::Append) > active_file_num);

        // After the engine recovers, the removed raft group shouldn't appear again.
        drop(engine);
        let engine = RaftLogEngine::new(cfg.clone());
        assert!(engine.memtables.get(1).is_none());
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

        impl PipeLogHook for Hook {
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

            fn post_purge(&self, queue: LogQueue, file_num: u64) {
                self.0[&queue].purged.store(file_num, Ordering::Release);
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
        entry.set_data(vec![b'x'; 64 * 1024]);

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
        let old_first = engine.pipe_log.first_file_num(LogQueue::Append);
        engine.purge_manager.purge_expired_files().unwrap();
        let new_first = engine.pipe_log.first_file_num(LogQueue::Append);
        assert_eq!(old_first, new_first);

        // Release the lock on region 3. Then can purge.
        tx.send(0i32).unwrap();
        th1.join().unwrap();
        th2.join().unwrap();

        std::thread::sleep(Duration::from_millis(200));
        engine.purge_manager.purge_expired_files().unwrap();
        let new_first = engine.pipe_log.first_file_num(LogQueue::Append);
        assert_ne!(old_first, new_first);

        // Drop and then recover.
        drop(engine);
        drop(RaftLogEngine::new(cfg));
    }
}
