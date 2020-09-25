use std::io::BufRead;
use std::sync::mpsc::{channel, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};
use std::{fmt, u64};

use futures::channel as future_channel;
use futures::executor::block_on;

use log::{info, warn};
use protobuf::Message;

use crate::cache_evict::{
    CacheSubmitor, CacheTask, Runner as CacheEvictRunner, DEFAULT_CACHE_CHUNK_SIZE,
};
use crate::config::{Config, RecoveryMode};
use crate::errors::Error;
use crate::log_batch::{
    self, Command, CompressionType, EntryExt, LogBatch, LogItem, LogItemContent, OpType,
    CHECKSUM_LEN, HEADER_LEN,
};
use crate::memtable::{EntryIndex, MemTable};
use crate::pipe_log::{GenericPipeLog, LogQueue, PipeLog, FILE_MAGIC_HEADER, VERSION};
use crate::purge::PurgeManager;
use crate::util::{HandyRwLock, HashMap, Worker};
use crate::wal::{LogMsg, WalRunner, WriteTask};
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
pub struct Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E>,
    P: GenericPipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor<E, W>,
    pipe_log: P,
    global_stats: Arc<GlobalStats>,
    purge_manager: PurgeManager<E, W, P>,
    wal_sender: Sender<LogMsg>,

    workers: Arc<RwLock<Workers>>,
}

impl<E, W, P> Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
    P: GenericPipeLog,
{
    fn apply_to_memtable(&self, item: LogItem<E>, queue: LogQueue, file_num: u64) {
        let memtable = self.memtables.get_or_insert(item.raft_group_id);
        match item.content {
            LogItemContent::Entries(entries_to_add) => {
                let entries = entries_to_add.entries;
                let entries_index = entries_to_add.entries_index.into_inner();
                if queue == LogQueue::Rewrite {
                    memtable.wl().append_rewrite(entries, entries_index);
                } else {
                    memtable.wl().append(entries, entries_index);
                }
            }
            LogItemContent::Command(Command::Clean) => {
                self.memtables.remove(item.raft_group_id);
            }
            LogItemContent::Command(Command::Compact { index }) => {
                memtable.wl().compact_to(index);
            }
            LogItemContent::Kv(kv) => match kv.op_type {
                OpType::Put => {
                    let (key, value) = (kv.key, kv.value.unwrap());
                    match queue {
                        LogQueue::Append => memtable.wl().put(key, value, file_num),
                        LogQueue::Rewrite => memtable.wl().put_rewrite(key, value, file_num),
                    }
                }
                OpType::Del => memtable.wl().delete(kv.key.as_slice()),
            },
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
    async fn write_impl(&self, log_batch: &mut LogBatch<E, W>, sync: bool) -> Result<usize> {
        let mut entries_size = 0;
        if let Some(content) = log_batch.encode_to_bytes(&mut entries_size) {
            let (sender, r) = future_channel::oneshot::channel();
            let bytes = content.len();
            let task = WriteTask {
                content,
                sync,
                entries_size,
                sender,
            };
            self.wal_sender
                .send(LogMsg::Write(task))
                .map_err(|_| Error::Stop)?;
            let (file_num, offset, tracker) = r.await?;
            if file_num > 0 {
                for item in log_batch.items.drain(..) {
                    if let LogItemContent::Entries(ref entries) = item.content {
                        entries.update_position(LogQueue::Append, file_num, offset, &tracker);
                    }
                    self.apply_to_memtable(item, LogQueue::Append, file_num);
                }
            }
            return Ok(bytes);
        }
        Ok(0)
    }
}

impl<E, W, P> fmt::Debug for Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E>,
    P: GenericPipeLog,
{
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Engine dir: {}", self.cfg.dir)
    }
}

struct Workers {
    cache_evict: Worker<CacheTask>,
    wal: Option<JoinHandle<()>>,
}

impl<E, W> Engine<E, W, PipeLog>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
{
    pub fn new(cfg: Config) -> Engine<E, W, PipeLog> {
        Self::new_impl(cfg, DEFAULT_CACHE_CHUNK_SIZE).unwrap()
    }

    fn new_impl(cfg: Config, chunk_limit: usize) -> Result<Engine<E, W, PipeLog>> {
        let cache_limit = cfg.cache_limit.0 as usize;
        let global_stats = Arc::new(GlobalStats::default());

        let mut cache_evict_worker = Worker::new("cache_evict".to_owned(), None);

        let pipe_log = PipeLog::open(&cfg).expect("Open raft log");

        let memtables = {
            let stats = global_stats.clone();
            MemTableAccessor::<E, W>::new(Arc::new(move |id: u64| {
                MemTable::new(id, cache_limit, stats.clone())
            }))
        };

        let cache_evict_runner = CacheEvictRunner::new(
            cache_limit,
            global_stats.clone(),
            chunk_limit,
            memtables.clone(),
            pipe_log.clone(),
        );
        cache_evict_worker.start(cache_evict_runner, Some(Duration::from_secs(1)));

        let mut cache_submitor = CacheSubmitor::new(
            cache_limit,
            chunk_limit,
            cache_evict_worker.scheduler(),
            global_stats.clone(),
        );

        let cfg = Arc::new(cfg);
        let purge_manager = PurgeManager::new(cfg.clone(), memtables.clone(), pipe_log.clone());
        let (wal_sender, wal_receiver) = channel();
        let engine = Engine {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            purge_manager,
            wal_sender,
            workers: Arc::new(RwLock::new(Workers {
                cache_evict: cache_evict_worker,
                wal: None,
            })),
        };
        cache_submitor.block_on_full();
        engine.recover(
            &mut cache_submitor,
            LogQueue::Rewrite,
            RecoveryMode::TolerateCorruptedTailRecords,
        )?;
        engine.recover(
            &mut cache_submitor,
            LogQueue::Append,
            engine.cfg.recovery_mode,
        )?;
        cache_submitor.nonblock_on_full();

        let mut wal_runner = WalRunner::new(cache_submitor, engine.pipe_log.clone(), wal_receiver);
        let th = ThreadBuilder::new()
            .name("wal".to_string())
            .spawn(move || {
                wal_runner
                    .run()
                    .unwrap_or_else(|e| warn!("run error because: {}", e))
            })
            .unwrap();
        engine.workers.wl().wal = Some(th);
        Ok(engine)
    }
    // Recover from disk.
    fn recover(
        &self,
        cache_submitor: &mut CacheSubmitor,
        queue: LogQueue,
        recovery_mode: RecoveryMode,
    ) -> Result<()> {
        // Get first file number and last file number.
        let first_file_num = self.pipe_log.first_file_num(queue);
        let active_file_num = self.pipe_log.active_file_num(queue);

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
                match LogBatch::<E, W>::from_bytes(&mut buf, file_num, offset) {
                    Ok(Some(mut log_batch)) => {
                        let mut encoded_size = 0;
                        for item in &log_batch.items {
                            if let LogItemContent::Entries(ref entries) = item.content {
                                encoded_size += entries.encoded_size.get();
                            }
                        }

                        if let Some(tracker) = cache_submitor.get_cache_tracker(file_num, offset) {
                            for item in &log_batch.items {
                                if let LogItemContent::Entries(ref entries) = item.content {
                                    entries.attach_cache_tracker(tracker.clone());
                                }
                            }
                        }
                        cache_submitor.fill_chunk(encoded_size);
                        for item in log_batch.items.drain(..) {
                            self.apply_to_memtable(item, queue, file_num);
                        }
                        offset = (buf.as_ptr() as usize - start_ptr as usize) as u64;
                    }
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
                            self.pipe_log
                                .truncate_active_log(queue, Some(offset as usize))?;
                            break;
                        }
                        return Err(box_err!("Raft log content is corrupted"));
                    }
                }
            }
        }
        info!("Recover raft log takes {:?}", start.elapsed());
        Ok(())
    }
}

impl<E, W, P> Engine<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + 'static,
    P: GenericPipeLog + 'static,
{
    /// Synchronize the Raft engine.
    pub fn sync(&self) -> Result<()> {
        self.pipe_log.sync(LogQueue::Append)
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
        block_on(self.write_impl(log_batch, sync))
    }

    pub async fn async_write(&self, log_batch: &mut LogBatch<E, W>, sync: bool) -> Result<usize> {
        self.write_impl(log_batch, sync).await
    }

    /// Flush stats about EntryCache.
    pub fn flush_cache_stats(&self) -> CacheStats {
        self.global_stats.flush_cache_stats()
    }

    /// Stop background thread which will keep trying evict caching.
    pub fn stop(&self) {
        let mut workers = self.workers.wl();
        self.wal_sender.send(LogMsg::Stop).unwrap();
        workers.cache_evict.stop();
        if let Some(wal) = workers.wal.take() {
            wal.join().unwrap();
        }
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
    W: EntryExt<E>,
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
    W: EntryExt<E>,
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

    impl EntryExt<Entry> for Entry {
        fn index(e: &Entry) -> u64 {
            e.get_index()
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
        engine.append(raft, vec![entry.clone()]).unwrap();
        let last_index = format!("{}", entry.index).into_bytes();
        engine.put(raft, b"last_index", &last_index).unwrap();
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

        let mut log_batch = LogBatch::new();
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
        assert!(!engine.purge_manager.needs_purge_log_files());

        // Append more logs to make total size greater than `purge_threshold`.
        for i in 100..250 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC first 101 log entries.
        let count = engine.compact_to(1, 101);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine.purge_manager.needs_purge_log_files());

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
        assert!(engine.purge_manager.needs_purge_log_files());
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

        let engine = RaftLogEngine::new_impl(cfg.clone(), 512 * 1024).unwrap();

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
        assert!(cache_size <= 10 * 1024 * 1024);

        // Recover from log files.
        engine.stop();
        drop(engine);
        let engine = RaftLogEngine::new_impl(cfg.clone(), 512 * 1024).unwrap();
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
        assert!(engine.purge_manager.needs_purge_log_files());
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.first_file_num(LogQueue::Append) > 1);

        let active_num = engine.pipe_log.active_file_num(LogQueue::Rewrite);
        let active_len = engine.pipe_log.file_len(LogQueue::Rewrite, active_num);
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

        assert!(engine.purge_manager.needs_purge_log_files());
        assert!(engine.purge_expired_files().unwrap().is_empty());

        let new_active_num = engine.pipe_log.active_file_num(LogQueue::Rewrite);
        let new_active_len = engine.pipe_log.file_len(LogQueue::Rewrite, active_num);
        assert!(
            new_active_num > active_num
                || (new_active_num == active_num && new_active_len > active_len)
        );
    }
}
