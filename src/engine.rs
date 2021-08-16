// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::{HashSet, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use std::{mem, u64};

use fail::fail_point;
use log::{debug, error, info, trace};
use parking_lot::{Mutex, RwLock};
use protobuf::Message;

use crate::config::{Config, RecoveryMode};
use crate::event_listener::EventListener;
use crate::file_pipe_log::FilePipeLog;
use crate::log_batch::{
    self, Command, CompressionType, LogBatch, LogItemBatch, LogItemContent, MessageExt, OpType,
    CHECKSUM_LEN,
};
use crate::memtable::{EntryIndex, MemTable};
use crate::metrics::*;
use crate::pipe_log::{FileId, LogQueue, PipeLog};
use crate::purge::{PurgeHook, PurgeManager};
use crate::util::{HashMap, InstantExt};
use crate::{GlobalStats, Result};

const SLOTS_COUNT: usize = 128;

// Modifying MemTables collection requires a write lock.
type MemTables = HashMap<u64, Arc<RwLock<MemTable>>>;

/// Collection of MemTables, indexed by Raft group ID.
#[derive(Clone)]
pub struct MemTableAccessor {
    slots: Vec<Arc<RwLock<MemTables>>>,
    initializer: Arc<dyn Fn(u64) -> MemTable + Send + Sync>,

    // Deleted region memtables that are not yet rewritten.
    removed_memtables: Arc<Mutex<VecDeque<u64>>>,
}

impl MemTableAccessor {
    pub fn new(initializer: Arc<dyn Fn(u64) -> MemTable + Send + Sync>) -> MemTableAccessor {
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

    pub fn get_or_insert(&self, raft_group_id: u64) -> Arc<RwLock<MemTable>> {
        let mut memtables = self.slots[raft_group_id as usize % SLOTS_COUNT].write();
        let memtable = memtables
            .entry(raft_group_id)
            .or_insert_with(|| Arc::new(RwLock::new((self.initializer)(raft_group_id))));
        memtable.clone()
    }

    pub fn get(&self, raft_group_id: u64) -> Option<Arc<RwLock<MemTable>>> {
        self.slots[raft_group_id as usize % SLOTS_COUNT]
            .read()
            .get(&raft_group_id)
            .cloned()
    }

    pub fn insert(&self, raft_group_id: u64, memtable: Arc<RwLock<MemTable>>) {
        self.slots[raft_group_id as usize % SLOTS_COUNT]
            .write()
            .insert(raft_group_id, memtable);
    }

    pub fn remove(&self, raft_group_id: u64, queue: LogQueue, _: FileId) {
        self.slots[raft_group_id as usize % SLOTS_COUNT]
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
    pub fn take_cleaned_region_logs<M>(&self) -> LogBatch<M>
    where
        M: MessageExt,
    {
        let mut log_batch = LogBatch::<M>::default();
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
}

#[derive(Clone)]
pub struct Engine<M, P>
where
    M: MessageExt + Clone,
    P: PipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor,
    pipe_log: P,
    global_stats: Arc<GlobalStats>,
    purge_manager: PurgeManager<M, P>,

    listeners: Vec<Arc<dyn EventListener>>,

    _phantom: PhantomData<M>,
}

impl<M> Engine<M, FilePipeLog>
where
    M: MessageExt + Clone,
{
    pub fn open(cfg: Config) -> Result<Engine<M, FilePipeLog>> {
        Self::open_with_listeners(cfg, vec![])
    }

    fn open_with_listeners(
        cfg: Config,
        mut listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<Engine<M, FilePipeLog>> {
        listeners.push(Arc::new(PurgeHook::new()) as Arc<dyn EventListener>);

        let global_stats = Arc::new(GlobalStats::default());

        let global_stats2 = global_stats.clone();
        let memtables = MemTableAccessor::new(Arc::new(move |id: u64| {
            MemTable::new(id, global_stats2.clone())
        }));

        let pipe_log = FilePipeLog::open(&cfg, listeners.clone())?;

        let cfg = Arc::new(cfg);
        let purge_manager = PurgeManager::new(
            cfg.clone(),
            memtables.clone(),
            pipe_log.clone(),
            global_stats.clone(),
            listeners.clone(),
        );

        let mut engine = Engine {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            purge_manager,
            listeners,
            _phantom: PhantomData,
        };

        engine.recover()?;
        Ok(engine)
    }
}

impl<M, P> Engine<M, P>
where
    M: MessageExt + Clone,
    P: PipeLog,
{
    fn recover(&mut self) -> Result<()> {
        let mut rewrite_memtables = MemTableAccessor::new(self.memtables.initializer.clone());
        Self::recover_queue(
            &mut rewrite_memtables,
            &mut self.pipe_log,
            LogQueue::Rewrite,
            RecoveryMode::TolerateCorruptedTailRecords,
        )?;

        Self::recover_queue(
            &mut self.memtables,
            &mut self.pipe_log,
            LogQueue::Append,
            self.cfg.recovery_mode,
        )?;

        let ids = self.memtables.cleaned_region_ids();
        for slot in rewrite_memtables.slots.into_iter() {
            for (id, raft_rewrite) in mem::take(&mut *slot.write()) {
                if let Some(raft_append) = self.memtables.get(id) {
                    raft_append
                        .write()
                        .merge_lower_prio(&mut *raft_rewrite.write());
                } else if !ids.contains(&id) {
                    self.memtables.insert(id, raft_rewrite);
                }
            }
        }

        Ok(())
    }

    fn recover_queue(
        memtables: &mut MemTableAccessor,
        pipe_log: &mut P,
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
            pipe_log.read_file_into_log_item_batch(queue, file_id, recovery_mode, &mut batches)?;
            for mut item_batch in batches.drain(..) {
                Self::apply_to_memtable(memtables, &mut item_batch, queue, file_id);
            }
            file_id = file_id.forward(1);
        }
        info!(
            "Recovering raft logs from {:?} queue takes {:?}",
            queue,
            start.elapsed()
        );
        Ok(())
    }

    fn apply_to_memtable(
        memtables: &MemTableAccessor,
        log_item_batch: &mut LogItemBatch<M>,
        queue: LogQueue,
        file_id: FileId,
    ) {
        for item in log_item_batch.drain() {
            let raft = item.raft_group_id;
            let memtable = memtables.get_or_insert(raft);
            fail_point!("apply_memtable_region_3", raft == 3, |_| {});
            match item.content {
                LogItemContent::EntriesIndex(entries_to_add) => {
                    let entries_index = entries_to_add.0;
                    debug!(
                        "{} append to {:?}.{:?}, Entries[{:?}, {:?}:{:?})",
                        raft,
                        queue,
                        file_id,
                        entries_index.first().map(|x| x.queue),
                        entries_index.first().map(|x| x.index),
                        entries_index.last().map(|x| x.index + 1),
                    );
                    if queue == LogQueue::Rewrite {
                        memtable.write().append_rewrite(entries_index);
                    } else {
                        memtable.write().append(entries_index);
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
                    memtable.write().compact_to(index);
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
                            LogQueue::Append => memtable.write().put(key, value, file_id),
                            LogQueue::Rewrite => memtable.write().put_rewrite(key, value, file_id),
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
                        memtable.write().delete(key.as_slice());
                    }
                },
            }
        }
    }

    /// Write the content of LogBatch into the engine and return written bytes.
    /// If set sync true, the data will be persisted on disk by `fsync`.
    pub fn write(&self, log_batch: &mut LogBatch<M>, sync: bool) -> Result<usize> {
        let start = Instant::now();
        let bytes = if log_batch.is_empty() {
            if sync {
                self.pipe_log.sync(LogQueue::Append)?;
            }
            0
        } else {
            let (file_id, bytes) = self.pipe_log.append(LogQueue::Append, log_batch, sync)?;
            if file_id.valid() {
                Self::apply_to_memtable(
                    &self.memtables,
                    log_batch.items_batch(),
                    LogQueue::Append,
                    file_id,
                );
                for listener in &self.listeners {
                    listener.post_apply_memtables(LogQueue::Append, file_id);
                }
            }
            bytes
        };
        ENGINE_WRITE_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        ENGINE_WRITE_SIZE_HISTOGRAM.observe(bytes as f64);
        Ok(bytes)
    }

    /// Synchronize the Raft engine.
    pub fn sync(&self) -> Result<()> {
        self.pipe_log.sync(LogQueue::Append)
    }

    pub fn put_message<S: Message>(&self, region_id: u64, key: &[u8], m: &S) -> Result<()> {
        let mut log_batch = LogBatch::default();
        log_batch.put_message(region_id, key.to_vec(), m)?;
        self.write(&mut log_batch, false).map(|_| ())
    }

    pub fn get_message<S: Message>(&self, region_id: u64, key: &[u8]) -> Result<Option<S>> {
        if let Some(memtable) = self.memtables.get(region_id) {
            if let Some(value) = memtable.read().get(key) {
                let mut m = S::new();
                m.merge_from_bytes(&value)?;
                return Ok(Some(m));
            }
        }
        Ok(None)
    }

    pub fn get_entry(&self, region_id: u64, log_idx: u64) -> Result<Option<M::Entry>> {
        let start = Instant::now();
        let mut entry = None;
        if let Some(memtable) = self.memtables.get(region_id) {
            if let Some(idx) = memtable.read().get_entry(log_idx) {
                entry = Some(read_entry_from_file::<M, _>(&self.pipe_log, &idx)?);
            }
        }
        ENGINE_READ_ENTRY_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        Ok(entry)
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
        vec: &mut Vec<M::Entry>,
    ) -> Result<usize> {
        let start = Instant::now();
        if let Some(memtable) = self.memtables.get(region_id) {
            let old_len = vec.len();
            let mut ents_idx: Vec<EntryIndex> = Vec::with_capacity((end - begin) as usize);
            memtable
                .read()
                .fetch_entries_to(begin, end, max_size, &mut ents_idx)?;
            for i in ents_idx {
                vec.push(read_entry_from_file::<M, _>(&self.pipe_log, &i)?);
            }
            ENGINE_READ_ENTRY_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
            return Ok(vec.len() - old_len);
        }
        Ok(0)
    }

    pub fn first_index(&self, region_id: u64) -> Option<u64> {
        if let Some(memtable) = self.memtables.get(region_id) {
            return memtable.read().first_index();
        }
        None
    }

    pub fn last_index(&self, region_id: u64) -> Option<u64> {
        if let Some(memtable) = self.memtables.get(region_id) {
            return memtable.read().last_index();
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
        if let Err(e) = self.write(&mut log_batch, false) {
            error!("Failed to write Compact command: {}", e);
        }

        self.first_index(region_id).unwrap_or(index) - first_index
    }

    pub fn raft_groups(&self) -> Vec<u64> {
        self.memtables.fold(vec![], |mut v, m| {
            v.push(m.region_id());
            v
        })
    }
}

pub fn read_entry_from_file<M, P>(pipe_log: &P, entry_index: &EntryIndex) -> Result<M::Entry>
where
    M: MessageExt + Clone,
    P: PipeLog,
{
    let queue = entry_index.queue;
    let file_id = entry_index.file_id;
    let entries_offset = entry_index.entries_offset;
    let entries_len = entry_index.entries_len;
    let entry_offset = entry_index.entry_offset;
    let entry_len = entry_index.entry_len;

    let buf = pipe_log.read_bytes(queue, file_id, entries_offset, entries_len as u64)?;
    log_batch::test_checksum(&buf)?;

    let entry_content = match entry_index.compression_type {
        CompressionType::None => {
            buf[entry_offset as usize..entry_offset as usize + entry_len].to_owned()
        }
        CompressionType::Lz4 => {
            let reader = &buf[..];
            let decompressed = log_batch::decompress(&reader[..entries_len - CHECKSUM_LEN]);
            decompressed[entry_offset as usize..entry_offset as usize + entry_len].to_vec()
        }
    };

    let mut e = M::Entry::new();
    e.merge_from_bytes(&entry_content)?;
    assert_eq!(M::index(&e), entry_index.index);
    Ok(e)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::ReadableSize;
    use kvproto::raft_serverpb::RaftLocalState;
    use raft::eraftpb::Entry;

    impl<M, P> Engine<M, P>
    where
        M: MessageExt + Clone,
        P: PipeLog,
    {
        pub fn purge_manager(&self) -> &PurgeManager<M, P> {
            &self.purge_manager
        }
    }

    type RaftLogEngine = Engine<Entry, FilePipeLog>;
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
        log_batch
            .put_message(
                raft,
                b"last_index".to_vec(),
                &RaftLocalState {
                    last_index: entry.index,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.write(&mut log_batch, false).unwrap();
    }

    fn last_index(engine: &RaftLogEngine, raft: u64) -> u64 {
        engine
            .get_message::<RaftLocalState>(raft, b"last_index")
            .unwrap()
            .unwrap()
            .last_index
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
        let engine = RaftLogEngine::open(cfg.clone()).unwrap();

        append_log(&engine, 1, &Entry::new());
        assert!(engine.memtables.get(1).is_some());

        let mut log_batch = LogBatch::default();
        log_batch.add_command(1, Command::Clean);
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

            let engine = RaftLogEngine::open(cfg.clone()).unwrap();
            let mut entry = Entry::new();
            entry.set_data(vec![b'x'; entry_size].into());
            for i in 10..20 {
                entry.set_index(i);
                engine.append(i, vec![entry.clone()]).unwrap();
                entry.set_index(i + 1);
                engine.append(i, vec![entry.clone()]).unwrap();
            }

            for i in 10..20 {
                // Test get_entry from file.
                entry.set_index(i);
                assert_eq!(engine.get_entry(i, i).unwrap(), Some(entry.clone()));
                entry.set_index(i + 1);
                assert_eq!(engine.get_entry(i, i + 1).unwrap(), Some(entry.clone()));
            }

            drop(engine);

            // Recover the engine.
            let engine = RaftLogEngine::open(cfg.clone()).unwrap();
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

        let engine = RaftLogEngine::open(cfg.clone()).unwrap();
        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());
        for i in 0..100 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC all log entries. Won't trigger purge because total size is not enough.
        let count = engine.compact_to(1, 100);
        assert_eq!(count, 100);
        assert!(!engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));

        // Append more logs to make total size greater than `purge_threshold`.
        for i in 100..250 {
            entry.set_index(i);
            append_log(&engine, 1, &entry);
        }

        // GC first 101 log entries.
        let count = engine.compact_to(1, 101);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));

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
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
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
    fn test_rewrite_and_recover() {
        let dir = tempfile::Builder::new()
            .prefix("test_rewrite_and_recover")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.target_file_size = ReadableSize::kb(5);
        cfg.purge_threshold = ReadableSize::kb(80);
        let engine = RaftLogEngine::open(cfg.clone()).unwrap();

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
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.first_file_id(LogQueue::Append) > 1.into());

        let active_num = engine.pipe_log.active_file_id(LogQueue::Rewrite);
        let active_len = engine
            .pipe_log
            .file_size(LogQueue::Rewrite, active_num)
            .unwrap();
        assert!(active_num > 1.into() || active_len > 59); // The rewrite queue isn't empty.

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
        let engine = RaftLogEngine::open(cfg.clone()).unwrap();
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

        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());

        let new_active_num = engine.pipe_log.active_file_id(LogQueue::Rewrite);
        let new_active_len = engine
            .pipe_log
            .file_size(LogQueue::Rewrite, active_num)
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
            .prefix("test_clean_raft_with_only_rewrite")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.target_file_size = ReadableSize::kb(128);
        cfg.purge_threshold = ReadableSize::kb(512);
        let engine = RaftLogEngine::open(cfg.clone()).unwrap();

        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 1024].into());

        // Layout of region 1 in file 1:
        // entries[1..10], Clean, entries[2..11]
        for j in 1..=10 {
            entry.set_index(j);
            append_log(&engine, 1, &entry);
        }
        let mut log_batch = LogBatch::with_capacity(1);
        log_batch.add_command(1, Command::Clean);
        engine.write(&mut log_batch, false).unwrap();
        assert!(engine.memtables.get(1).is_none());

        entry.set_data(vec![b'y'; 1024].into());
        for j in 2..=11 {
            entry.set_index(j);
            append_log(&engine, 1, &entry);
        }

        assert_eq!(engine.pipe_log.active_file_id(LogQueue::Append), 1.into());

        // Put more raft logs to trigger purge.
        for i in 2..64 {
            for j in 1..=10 {
                entry.set_index(j);
                append_log(&engine, i, &entry);
            }
        }

        // The engine needs purge, and all old entries should be rewritten.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.first_file_id(LogQueue::Append) > 1.into());

        // All entries of region 1 has been rewritten.
        let memtable_1 = engine.memtables.get(1).unwrap();
        assert!(memtable_1.read().max_file_id(LogQueue::Append).is_none());
        assert!(memtable_1
            .read()
            .kvs_max_file_id(LogQueue::Append)
            .is_none());
        // Entries of region 1 after the clean command should be still valid.
        for j in 2..=11 {
            let entry_j = engine.get_entry(1, j).unwrap().unwrap();
            assert_eq!(entry_j.get_data(), entry.get_data());
        }

        // Clean the raft group again.
        let mut log_batch = LogBatch::with_capacity(1);
        log_batch.add_command(1, Command::Clean);
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
        let engine = RaftLogEngine::open(cfg.clone()).unwrap();
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
    #[cfg(feature = "failpoints")]
    fn test_pipe_log_listeners() {
        use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
        use std::time::Duration;

        #[derive(Default)]
        struct QueueHook {
            files: AtomicUsize,
            appends: AtomicUsize,
            applys: AtomicUsize,
            purged: AtomicU64,
        }
        impl QueueHook {
            fn files(&self) -> usize {
                self.files.load(Ordering::Acquire)
            }
            fn appends(&self) -> usize {
                self.appends.load(Ordering::Acquire)
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

        impl EventListener for Hook {
            fn post_new_log_file(&self, queue: LogQueue, _: FileId) {
                self.0[&queue].files.fetch_add(1, Ordering::Release);
            }

            fn on_append_log_file(&self, queue: LogQueue, _: FileId) {
                self.0[&queue].appends.fetch_add(1, Ordering::Release);
            }

            fn post_apply_memtables(&self, queue: LogQueue, _: FileId) {
                self.0[&queue].applys.fetch_add(1, Ordering::Release);
            }

            fn first_file_not_ready_for_purge(&self, _: LogQueue) -> FileId {
                Default::default()
            }

            fn post_purge(&self, queue: LogQueue, file_id: FileId) {
                self.0[&queue]
                    .purged
                    .store(file_id.as_u64(), Ordering::Release);
            }
        }

        let dir = tempfile::Builder::new()
            .prefix("test_pipe_log_listeners")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.target_file_size = ReadableSize::kb(128);
        cfg.purge_threshold = ReadableSize::kb(512);
        cfg.batch_compression_threshold = ReadableSize::kb(0);

        let hook = Arc::new(Hook::default());
        let engine = RaftLogEngine::open_with_listeners(cfg.clone(), vec![hook.clone()]).unwrap();
        assert_eq!(hook.0[&LogQueue::Append].files(), 1);
        assert_eq!(hook.0[&LogQueue::Rewrite].files(), 1);

        let mut entry = Entry::new();
        entry.set_data(vec![b'x'; 64 * 1024].into());

        // Append 10 logs for region 1, 10 logs for region 2.
        for i in 1..=20 {
            let region_id = (i as u64 - 1) % 2 + 1;
            entry.set_index((i as u64 + 1) / 2);
            append_log(&engine, region_id, &entry);
            assert_eq!(hook.0[&LogQueue::Append].appends(), i);
            assert_eq!(hook.0[&LogQueue::Append].applys(), i);
        }
        assert_eq!(hook.0[&LogQueue::Append].files(), 10);

        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        engine.purge_manager.purge_expired_files().unwrap();
        assert_eq!(hook.0[&LogQueue::Append].purged(), 8);

        // All things in a region will in one write batch.
        assert_eq!(hook.0[&LogQueue::Rewrite].files(), 2);
        assert_eq!(hook.0[&LogQueue::Rewrite].appends(), 2);
        assert_eq!(hook.0[&LogQueue::Rewrite].applys(), 2);

        // Append 5 logs for region 1, 5 logs for region 2.
        for i in 21..=30 {
            let region_id = (i as u64 - 1) % 2 + 1;
            entry.set_index((i as u64 + 1) / 2);
            append_log(&engine, region_id, &entry);
            assert_eq!(hook.0[&LogQueue::Append].appends(), i);
            assert_eq!(hook.0[&LogQueue::Append].applys(), i);
        }
        // Compact so that almost all content of rewrite queue will become garbage.
        engine.compact_to(1, 14);
        engine.compact_to(2, 14);
        assert_eq!(hook.0[&LogQueue::Append].appends(), 32);
        assert_eq!(hook.0[&LogQueue::Append].applys(), 32);

        engine.purge_manager.purge_expired_files().unwrap();
        assert_eq!(hook.0[&LogQueue::Append].purged(), 13);
        assert_eq!(hook.0[&LogQueue::Rewrite].purged(), 2);

        // Write region 3 without applying.
        let apply_memtable_region_3_fp = "apply_memtable_region_3";
        fail::cfg(apply_memtable_region_3_fp, "pause").unwrap();
        let engine_clone = engine.clone();
        let mut entry_clone = entry.clone();
        let th = std::thread::spawn(move || {
            entry_clone.set_index(1);
            append_log(&engine_clone, 3, &entry_clone);
        });

        // Sleep a while to wait the log batch `Append(3, [1])` to get written.
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(hook.0[&LogQueue::Append].appends(), 33);
        let file_not_applied = engine.pipe_log.active_file_id(LogQueue::Append);
        assert_eq!(hook.0[&LogQueue::Append].applys(), 32);

        for i in 31..=40 {
            let region_id = (i as u64 - 1) % 2 + 1;
            entry.set_index((i as u64 + 1) / 2);
            append_log(&engine, region_id, &entry);
            assert_eq!(hook.0[&LogQueue::Append].appends(), i + 3);
            assert_eq!(hook.0[&LogQueue::Append].applys(), i + 2);
        }

        // Can't purge because region 3 is not yet applied.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        engine.purge_manager.purge_expired_files().unwrap();
        let first = engine.pipe_log.first_file_id(LogQueue::Append);
        assert_eq!(file_not_applied, first);

        // Resume write on region 3.
        fail::remove(apply_memtable_region_3_fp);
        th.join().unwrap();

        std::thread::sleep(Duration::from_millis(200));
        engine.purge_manager.purge_expired_files().unwrap();
        let new_first = engine.pipe_log.first_file_id(LogQueue::Append);
        assert_ne!(file_not_applied, new_first);

        // Drop and then recover.
        drop(engine);
        RaftLogEngine::open(cfg).unwrap();
    }
}
