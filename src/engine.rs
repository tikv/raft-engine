// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use std::u64;

use log::{error, info};
use protobuf::{parse_from_bytes, Message};

use crate::config::Config;
use crate::consistency::ConsistencyChecker;
use crate::event_listener::EventListener;
use crate::file_builder::*;
use crate::file_pipe_log::{FilePipeLog, ReplayMachine};
use crate::log_batch::{Command, LogBatch, MessageExt};
use crate::memtable::{EntryIndex, MemTableAccessor, MemTableRecoverContext};
use crate::metrics::*;
use crate::pipe_log::{FileId, LogQueue, PipeLog};
use crate::purge::{PurgeHook, PurgeManager};
use crate::util::InstantExt;
use crate::write_barrier::{WriteBarrier, Writer};
use crate::{Error, Result};

pub struct Engine<B = DefaultFileBuilder, P = FilePipeLog<B>>
where
    B: FileBuilder,
    P: PipeLog,
{
    cfg: Arc<Config>,

    memtables: MemTableAccessor,
    pipe_log: Arc<P>,
    purge_manager: PurgeManager<P>,

    write_barrier: WriteBarrier<LogBatch, Result<(FileId, u64)>>,

    listeners: Vec<Arc<dyn EventListener>>,

    _phantom: PhantomData<B>,
}

impl Engine<DefaultFileBuilder, FilePipeLog<DefaultFileBuilder>> {
    pub fn open(
        cfg: Config,
    ) -> Result<Engine<DefaultFileBuilder, FilePipeLog<DefaultFileBuilder>>> {
        Self::open_with_listeners(cfg, vec![])
    }

    pub fn open_with_listeners(
        cfg: Config,
        listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<Engine<DefaultFileBuilder, FilePipeLog<DefaultFileBuilder>>> {
        Self::open_with(cfg, Arc::new(DefaultFileBuilder {}), listeners)
    }
}

impl<B> Engine<B, FilePipeLog<B>>
where
    B: FileBuilder,
{
    pub fn open_with_file_builder(
        cfg: Config,
        file_builder: Arc<B>,
    ) -> Result<Engine<B, FilePipeLog<B>>> {
        Self::open_with(cfg, file_builder, vec![])
    }

    pub fn open_with(
        mut cfg: Config,
        file_builder: Arc<B>,
        mut listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<Engine<B, FilePipeLog<B>>> {
        cfg.sanitize()?;
        listeners.push(Arc::new(PurgeHook::new()) as Arc<dyn EventListener>);

        let start = Instant::now();
        let (pipe_log, append, rewrite) =
            FilePipeLog::open::<MemTableRecoverContext>(&cfg, file_builder, listeners.clone())?;
        let pipe_log = Arc::new(pipe_log);
        info!("Recovering raft logs takes {:?}", start.elapsed());

        let (memtables, global_stats) = append.finish();
        let (mut memtables_rewrite, rewrite_stats) = rewrite.finish();
        global_stats.merge(&rewrite_stats);

        memtables.merge_lower_prio(&mut memtables_rewrite);

        let cfg = Arc::new(cfg);
        let purge_manager = PurgeManager::new(
            cfg.clone(),
            memtables.clone(),
            pipe_log.clone(),
            global_stats,
            listeners.clone(),
        );

        Ok(Self {
            cfg,
            memtables,
            pipe_log,
            purge_manager,
            write_barrier: Default::default(),
            listeners,
            _phantom: PhantomData,
        })
    }
}

impl<B, P> Engine<B, P>
where
    B: FileBuilder,
    P: PipeLog,
{
    /// Write the content of LogBatch into the engine and return written bytes.
    /// If `sync` is true, the write will be followed by a call to `fdatasync` on
    /// the log file.
    pub fn write(&self, log_batch: &mut LogBatch, mut sync: bool) -> Result<usize> {
        let start = Instant::now();
        let len = log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        let (file_id, offset) = {
            let mut writer = Writer::new(log_batch as &_, sync);
            if let Some(mut group) = self.write_barrier.enter(&mut writer) {
                for writer in group.iter_mut() {
                    sync |= writer.is_sync();
                    let log_batch = writer.get_payload();
                    let res = if !log_batch.is_empty() {
                        self.pipe_log.append(
                            LogQueue::Append,
                            log_batch.encoded_bytes(),
                            false, /*sync*/
                        )
                    } else {
                        Ok((FileId::default(), 0))
                    };
                    writer.set_output(res);
                }
                if sync {
                    // fsync() is not retryable, a failed attempt could result in
                    // unrecoverable loss of data written after last successful
                    // fsync(). See [PostgreSQL's fsync() surprise]
                    // (https://lwn.net/Articles/752063/) for more details.
                    if let Err(e) = self.pipe_log.sync(LogQueue::Append) {
                        for writer in group.iter_mut() {
                            writer.set_output(Err(Error::Fsync(e.to_string())));
                        }
                    }
                }
            }
            writer.finish()?
        };

        if len > 0 {
            assert!(file_id.valid());
            log_batch.finish_write(LogQueue::Append, file_id, offset);
            self.memtables.apply(log_batch.drain(), LogQueue::Append);
            for listener in &self.listeners {
                listener.post_apply_memtables(LogQueue::Append, file_id);
            }
        }

        ENGINE_WRITE_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        ENGINE_WRITE_SIZE_HISTOGRAM.observe(len as f64);
        Ok(len)
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
                return Ok(Some(parse_from_bytes(&value)?));
            }
        }
        Ok(None)
    }

    pub fn get_entry<M: MessageExt>(
        &self,
        region_id: u64,
        log_idx: u64,
    ) -> Result<Option<M::Entry>> {
        let start = Instant::now();
        let mut entry = None;
        if let Some(memtable) = self.memtables.get(region_id) {
            if let Some(idx) = memtable.read().get_entry(log_idx) {
                entry = Some(read_entry_from_file::<M, _>(self.pipe_log.as_ref(), &idx)?);
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
    pub fn fetch_entries_to<M: MessageExt>(
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
                vec.push(read_entry_from_file::<M, _>(self.pipe_log.as_ref(), &i)?);
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

impl<B> Engine<B, FilePipeLog<B>>
where
    B: FileBuilder,
{
    /// Return a list of corrupted Raft groups, including their id and last unaffected
    /// log index.
    pub fn consistency_check(cfg: Config, file_builder: Arc<B>) -> Result<Vec<(u64, u64)>> {
        let (_, mut append, rewrite) =
            FilePipeLog::open::<ConsistencyChecker>(&cfg, file_builder, vec![])?;
        append.merge(rewrite, LogQueue::Rewrite)?;
        Ok(append.finish())
    }

    pub fn unsafe_truncate_raft_groups(
        _cfg: Config,
        _file_builder: Arc<B>,
        _raft_groups: Vec<(u64, Option<u64>)>,
    ) -> Result<()> {
        todo!()
    }
}

pub fn read_entry_from_file<M, P>(pipe_log: &P, ent_idx: &EntryIndex) -> Result<M::Entry>
where
    M: MessageExt,
    P: PipeLog,
{
    let buf = pipe_log.read_bytes(
        ent_idx.queue,
        ent_idx.file_id,
        ent_idx.entries_offset,
        ent_idx.entries_len as u64,
    )?;
    let e = LogBatch::parse_entry::<M>(&buf, ent_idx)?;
    assert_eq!(M::index(&e), ent_idx.index);
    Ok(e)
}

pub fn read_entry_bytes_from_file<P>(pipe_log: &P, ent_idx: &EntryIndex) -> Result<Vec<u8>>
where
    P: PipeLog,
{
    let entries_buf = pipe_log.read_bytes(
        ent_idx.queue,
        ent_idx.file_id,
        ent_idx.entries_offset,
        ent_idx.entries_len as u64,
    )?;
    LogBatch::parse_entry_bytes(&entries_buf, ent_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::ReadableSize;
    use hashbrown::HashMap;
    use kvproto::raft_serverpb::RaftLocalState;
    use raft::eraftpb::Entry;

    type RaftLogEngine = Engine;
    impl RaftLogEngine {
        fn append(&self, raft_group_id: u64, entries: &[Entry]) -> Result<usize> {
            let mut batch = LogBatch::default();
            batch.add_entries::<Entry>(raft_group_id, entries)?;
            self.write(&mut batch, false)
        }
    }

    fn append_log(engine: &RaftLogEngine, raft: u64, entry: &Entry) {
        let mut log_batch = LogBatch::default();
        log_batch
            .add_entries::<Entry>(raft, &[entry.clone()])
            .unwrap();
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

        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::kb(5),
            purge_threshold: ReadableSize::kb(80),
            ..Default::default()
        };
        let engine = RaftLogEngine::open(cfg).unwrap();

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

            let cfg = Config {
                dir: dir.path().to_str().unwrap().to_owned(),
                ..Default::default()
            };

            let engine = RaftLogEngine::open(cfg.clone()).unwrap();
            let mut entry = Entry::new();
            entry.set_data(vec![b'x'; entry_size].into());
            for i in 10..20 {
                entry.set_index(i);
                engine.append(i, &[entry.clone()]).unwrap();
                entry.set_index(i + 1);
                engine.append(i, &[entry.clone()]).unwrap();
            }

            for i in 10..20 {
                // Test get_entry from file.
                entry.set_index(i);
                assert_eq!(
                    engine.get_entry::<Entry>(i, i).unwrap(),
                    Some(entry.clone())
                );
                entry.set_index(i + 1);
                assert_eq!(
                    engine.get_entry::<Entry>(i, i + 1).unwrap(),
                    Some(entry.clone())
                );
            }

            drop(engine);

            // Recover the engine.
            let engine = RaftLogEngine::open(cfg.clone()).unwrap();
            for i in 10..20 {
                entry.set_index(i + 1);
                assert_eq!(
                    engine.get_entry::<Entry>(i, i + 1).unwrap(),
                    Some(entry.clone())
                );

                entry.set_index(i);
                assert_eq!(
                    engine.get_entry::<Entry>(i, i).unwrap(),
                    Some(entry.clone())
                );
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

        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::kb(5),
            purge_threshold: ReadableSize::kb(150),
            ..Default::default()
        };

        let engine = RaftLogEngine::open(cfg).unwrap();
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

        let old_min_file_id = engine.pipe_log.file_span(LogQueue::Append).0;
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_id = engine.pipe_log.file_span(LogQueue::Append).0;
        // Some entries are rewritten.
        assert!(new_min_file_id > old_min_file_id);
        // No regions need to be force compacted because the threshold is not reached.
        assert!(will_force_compact.is_empty());
        // After purge, entries and raft state are still available.
        assert!(engine.get_entry::<Entry>(1, 101).unwrap().is_some());

        let count = engine.compact_to(1, 102);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        let old_min_file_id = engine.pipe_log.file_span(LogQueue::Append).0;
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_id = engine.pipe_log.file_span(LogQueue::Append).0;
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

        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::kb(5),
            purge_threshold: ReadableSize::kb(80),
            ..Default::default()
        };
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
        assert!(engine.pipe_log.file_span(LogQueue::Append).0 > 1.into());

        let rewrite_file_size = engine.pipe_log.total_size(LogQueue::Rewrite);
        assert!(rewrite_file_size > 59); // The rewrite queue isn't empty.

        // All entries should be available.
        for i in 1..=10 {
            for j in 1..=10 {
                let e = engine.get_entry::<Entry>(j, i).unwrap().unwrap();
                assert_eq!(e.get_data(), entry.get_data());
                assert_eq!(last_index(&engine, j), 10);
            }
        }

        // Recover with rewrite queue and append queue.
        let cleaned_region_ids = engine.memtables.cleaned_region_ids();
        drop(engine);
        let engine = RaftLogEngine::open(cfg).unwrap();
        assert_eq!(engine.memtables.cleaned_region_ids(), cleaned_region_ids);
        for i in 1..=10 {
            for j in 1..=10 {
                let e = engine.get_entry::<Entry>(j, i).unwrap().unwrap();
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
    }

    // Raft groups can be removed when they only have entries in the rewrite queue.
    // We need to ensure that these raft groups won't appear again after recover.
    fn test_clean_raft_with_only_rewrite(purge_before_recover: bool) {
        let dir = tempfile::Builder::new()
            .prefix("test_clean_raft_with_only_rewrite")
            .tempdir()
            .unwrap();

        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::kb(128),
            purge_threshold: ReadableSize::kb(512),
            ..Default::default()
        };
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

        assert_eq!(engine.pipe_log.file_span(LogQueue::Append).1, 1.into());

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
        assert!(engine.pipe_log.file_span(LogQueue::Append).0 > 1.into());

        // All entries of region 1 has been rewritten.
        let memtable_1 = engine.memtables.get(1).unwrap();
        assert!(memtable_1.read().max_file_id(LogQueue::Append).is_none());
        assert!(memtable_1
            .read()
            .kvs_max_file_id(LogQueue::Append)
            .is_none());
        assert_eq!(engine.get_entry::<Entry>(1, 1).unwrap(), None);
        // Entries of region 1 after the clean command should be still valid.
        for j in 2..=11 {
            let entry_j = engine.get_entry::<Entry>(1, j).unwrap().unwrap();
            assert_eq!(entry_j.get_data(), entry.get_data());
        }

        // Clean the raft group again.
        let mut log_batch = LogBatch::with_capacity(1);
        log_batch.add_command(1, Command::Clean);
        engine.write(&mut log_batch, false).unwrap();
        assert!(engine.memtables.get(1).is_none());

        // Put more raft logs and then recover.
        let active_file = engine.pipe_log.file_span(LogQueue::Append).1;
        for i in 64..=128 {
            for j in 1..=10 {
                entry.set_index(j);
                append_log(&engine, i, &entry);
            }
        }

        if purge_before_recover {
            assert!(engine.purge_expired_files().unwrap().is_empty());
            assert!(engine.pipe_log.file_span(LogQueue::Append).0 > active_file);
        }

        // After the engine recovers, the removed raft group shouldn't appear again.
        drop(engine);
        let engine = RaftLogEngine::open(cfg).unwrap();
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

            fn on_append_log_file(&self, queue: LogQueue, _: FileId, _: usize) {
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

        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::kb(128),
            purge_threshold: ReadableSize::kb(512),
            batch_compression_threshold: ReadableSize::kb(0),
            ..Default::default()
        };

        let hook = Arc::new(Hook::default());
        let engine =
            Arc::new(RaftLogEngine::open_with_listeners(cfg.clone(), vec![hook.clone()]).unwrap());
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
        let apply_memtable_region_3_fp = "memtable_accessor::apply::region_3";
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
        let file_not_applied = engine.pipe_log.file_span(LogQueue::Append).1;
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
        let first = engine.pipe_log.file_span(LogQueue::Append).0;
        assert_eq!(file_not_applied, first);

        // Resume write on region 3.
        fail::remove(apply_memtable_region_3_fp);
        th.join().unwrap();

        std::thread::sleep(Duration::from_millis(200));
        engine.purge_manager.purge_expired_files().unwrap();
        let new_first = engine.pipe_log.file_span(LogQueue::Append).0;
        assert_ne!(file_not_applied, new_first);

        // Drop and then recover.
        drop(engine);
        RaftLogEngine::open(cfg).unwrap();
    }

    #[test]
    fn test_empty_protobuf_message() {
        let dir = tempfile::Builder::new()
            .prefix("test_empty_protobuf_message")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Arc::new(RaftLogEngine::open(cfg.clone()).unwrap());

        let mut log_batch = LogBatch::default();
        let empty_entry = Entry::new();
        assert_eq!(empty_entry.compute_size(), 0);
        log_batch
            .add_entries::<Entry>(0, &[empty_entry.clone()])
            .unwrap();
        engine.write(&mut log_batch, false).unwrap();
        let empty_state = RaftLocalState::new();
        assert_eq!(empty_state.compute_size(), 0);
        log_batch
            .put_message(1, b"last_index".to_vec(), &empty_state)
            .unwrap();
        engine.write(&mut log_batch, false).unwrap();
        log_batch
            .add_entries::<Entry>(2, &[empty_entry.clone()])
            .unwrap();
        log_batch
            .put_message(2, b"last_index".to_vec(), &empty_state)
            .unwrap();
        engine.write(&mut log_batch, true).unwrap();
        drop(engine);

        let engine = RaftLogEngine::open(cfg).unwrap();
        assert_eq!(
            engine.get_entry::<Entry>(0, 0).unwrap().unwrap(),
            empty_entry
        );
        assert_eq!(
            engine.get_entry::<Entry>(2, 0).unwrap().unwrap(),
            empty_entry
        );
        assert_eq!(
            engine
                .get_message::<RaftLocalState>(1, b"last_index")
                .unwrap()
                .unwrap(),
            empty_state
        );
        assert_eq!(
            engine
                .get_message::<RaftLocalState>(2, b"last_index")
                .unwrap()
                .unwrap(),
            empty_state
        );
    }

    #[cfg(feature = "failpoints")]
    struct ConcurrentWriteContext {
        engine: Arc<RaftLogEngine>,
        ths: Vec<std::thread::JoinHandle<()>>,
    }

    #[cfg(feature = "failpoints")]
    impl ConcurrentWriteContext {
        fn new(engine: Arc<RaftLogEngine>) -> Self {
            Self {
                engine,
                ths: Vec::new(),
            }
        }

        fn leader_write(&mut self, mut log_batch: LogBatch) {
            if self.ths.is_empty() {
                fail::cfg("write_barrier::leader_exit", "pause").unwrap();
                let engine_clone = self.engine.clone();
                self.ths.push(
                    std::thread::Builder::new()
                        .spawn(move || {
                            engine_clone.write(&mut LogBatch::default(), true).unwrap();
                        })
                        .unwrap(),
                );
            }
            let engine_clone = self.engine.clone();
            self.ths.push(
                std::thread::Builder::new()
                    .spawn(move || {
                        engine_clone.write(&mut log_batch, true).unwrap();
                    })
                    .unwrap(),
            );
        }

        fn follower_write(&mut self, mut log_batch: LogBatch) {
            assert!(self.ths.len() == 2);
            let engine_clone = self.engine.clone();
            self.ths.push(
                std::thread::Builder::new()
                    .spawn(move || {
                        engine_clone.write(&mut log_batch, true).unwrap();
                    })
                    .unwrap(),
            );
        }

        fn join(&mut self) {
            fail::remove("write_barrier::leader_exit");
            for t in self.ths.drain(..) {
                t.join().unwrap();
            }
        }
    }

    #[test]
    #[cfg(feature = "failpoints")]
    fn test_concurrent_write_empty_log_batch() {
        let dir = tempfile::Builder::new()
            .prefix("test_concurrent_write_empty_log_batch")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Arc::new(RaftLogEngine::open(cfg.clone()).unwrap());
        let mut ctx = ConcurrentWriteContext::new(engine.clone());

        let some_entries = vec![
            Entry::new(),
            Entry {
                index: 1,
                ..Default::default()
            },
        ];

        ctx.leader_write(LogBatch::default());
        let mut log_batch = LogBatch::default();
        log_batch.add_entries::<Entry>(1, &some_entries).unwrap();
        ctx.follower_write(log_batch);
        ctx.join();

        let mut log_batch = LogBatch::default();
        log_batch.add_entries::<Entry>(2, &some_entries).unwrap();
        ctx.leader_write(log_batch);
        ctx.follower_write(LogBatch::default());
        ctx.join();
        drop(ctx);
        drop(engine);

        let engine = RaftLogEngine::open(cfg).unwrap();
        let mut entries = Vec::new();
        engine
            .fetch_entries_to::<Entry>(
                1,    /*region*/
                0,    /*begin*/
                2,    /*end*/
                None, /*max_size*/
                &mut entries,
            )
            .unwrap();
        assert_eq!(entries, some_entries);
        entries.clear();
        engine
            .fetch_entries_to::<Entry>(
                2,    /*region*/
                0,    /*begin*/
                2,    /*end*/
                None, /*max_size*/
                &mut entries,
            )
            .unwrap();
        assert_eq!(entries, some_entries);
    }
}
