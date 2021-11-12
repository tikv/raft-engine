// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Instant;
use std::u64;

use log::{error, info};
use protobuf::{parse_from_bytes, Message};

use crate::config::{Config, RecoveryMode};
use crate::consistency::ConsistencyChecker;
use crate::event_listener::EventListener;
use crate::file_builder::*;
use crate::file_pipe_log::FilePipeLog;
use crate::log_batch::{Command, LogBatch, LogItem, MessageExt};
use crate::memtable::{EntryIndex, MemTableAccessor, MemTableRecoverContext};
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, LogQueue, PipeLog};
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

    write_barrier: WriteBarrier<LogBatch, Result<FileBlockHandle>>,

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

        memtables.merge_rewrite_table(&mut memtables_rewrite);

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
        let block_handle = {
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
                        // TODO(tabokie)
                        Ok(FileBlockHandle {
                            id: FileId::new(LogQueue::Append, 0),
                            offset: 0,
                            len: 0,
                        })
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
            log_batch.finish_write(block_handle);
            self.memtables.apply(log_batch.drain(), LogQueue::Append);
            for listener in &self.listeners {
                listener.post_apply_memtables(block_handle.id);
            }
        }

        ENGINE_WRITE_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        ENGINE_WRITE_SIZE_HISTOGRAM.observe(len as f64);
        Ok(len)
    }

    /// Synchronize the Raft engine.
    pub fn sync(&self) -> Result<()> {
        // TODO(tabokie): use writer.
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

    pub fn file_span(&self, queue: LogQueue) -> (u64, u64) {
        self.pipe_log.file_span(queue)
    }
}

impl Engine<DefaultFileBuilder, FilePipeLog<DefaultFileBuilder>> {
    pub fn consistency_check(path: &std::path::Path) -> Result<Vec<(u64, u64)>> {
        Self::consistency_check_with(path, Arc::new(DefaultFileBuilder {}))
    }

    // Repair log entry holes by fill in empty message
    /// queue: accept "append", "rewrite", "all"
    #[allow(unused_variables)]
    pub fn auto_fill(path: &std::path::Path, queue: &str, raft_groups: &[u64]) -> Result<()> {
        todo!()
    }

    // Trunate all files unsafely
    /// mode: accept "front", "back", "all"
    /// queue: accept "append", "rewrite", "all"
    #[allow(unused_variables)]
    pub fn truncate(
        path: &std::path::Path,
        mode: &str,
        queue: &str,
        raft_groups: &[u64],
    ) -> Result<()> {
        todo!()
    }

    // Dump all all operations in log files
    #[allow(unused_variables)]
    pub fn dump(path: &std::path::Path, raft_groups: &[u64]) -> Result<Vec<LogItem>> {
        todo!()
    }
}

impl<B> Engine<B, FilePipeLog<B>>
where
    B: FileBuilder,
{
    /// Returns a list of corrupted Raft groups, including their id and last unaffected
    /// log index. Head or tail corruption might not be detected.
    pub fn consistency_check_with(
        path: &std::path::Path,
        file_builder: Arc<B>,
    ) -> Result<Vec<(u64, u64)>> {
        if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "raft-engine directory '{}' does not exist.",
                path.to_str().unwrap().to_owned()
            )));
        }

        let cfg = Config {
            dir: path.to_str().unwrap().to_owned(),
            recovery_mode: RecoveryMode::TolerateAnyCorruption,
            ..Default::default()
        };
        let (_, append, rewrite) =
            FilePipeLog::open::<ConsistencyChecker>(&cfg, file_builder, vec![])?;
        let mut map = rewrite.finish();
        for (id, index) in append.finish() {
            map.entry(id).or_insert(index);
        }
        let mut list: Vec<(u64, u64)> = map.into_iter().collect();
        list.sort_unstable();
        Ok(list)
    }
}

pub fn read_entry_from_file<M, P>(pipe_log: &P, ent_idx: &EntryIndex) -> Result<M::Entry>
where
    M: MessageExt,
    P: PipeLog,
{
    let buf = pipe_log.read_bytes(ent_idx.entries.unwrap())?;
    let e = LogBatch::parse_entry::<M>(&buf, ent_idx)?;
    assert_eq!(M::index(&e), ent_idx.index);
    Ok(e)
}

pub fn read_entry_bytes_from_file<P>(pipe_log: &P, ent_idx: &EntryIndex) -> Result<Vec<u8>>
where
    P: PipeLog,
{
    let entries_buf = pipe_log.read_bytes(ent_idx.entries.unwrap())?;
    LogBatch::parse_entry_bytes(&entries_buf, ent_idx)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::ReadableSize;
    use kvproto::raft_serverpb::RaftLocalState;
    use raft::eraftpb::Entry;

    type RaftLogEngine = Engine;
    impl RaftLogEngine {
        fn append(&self, raft_group_id: u64, entries: &[Entry]) {
            if !entries.is_empty() {
                let mut batch = LogBatch::default();
                batch.add_entries::<Entry>(raft_group_id, entries).unwrap();
                batch
                    .put_message(
                        raft_group_id,
                        b"last_index".to_vec(),
                        &RaftLocalState {
                            last_index: entries[entries.len() - 1].index,
                            ..Default::default()
                        },
                    )
                    .unwrap();
                self.write(&mut batch, true).unwrap();
            }
        }

        fn append_one(&self, raft_group_id: u64, entry: &Entry) {
            self.append(raft_group_id, &[entry.clone()])
        }

        fn last_index_slow(&self, raft_group_id: u64) -> u64 {
            self.get_message::<RaftLocalState>(raft_group_id, b"last_index")
                .unwrap()
                .unwrap()
                .last_index
        }
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

        engine.append_one(1, &Entry::new());
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
                engine.append_one(i, &entry);
                entry.set_index(i + 1);
                engine.append_one(i, &entry);
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
            engine.append_one(1, &entry);
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
            engine.append_one(1, &entry);
        }

        // GC first 101 log entries.
        let count = engine.compact_to(1, 101);
        assert_eq!(count, 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));

        let old_min_file_seq = engine.pipe_log.file_span(LogQueue::Append).0;
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_seq = engine.pipe_log.file_span(LogQueue::Append).0;
        // Some entries are rewritten.
        assert!(new_min_file_seq > old_min_file_seq);
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
        let old_min_file_seq = engine.pipe_log.file_span(LogQueue::Append).0;
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_seq = engine.pipe_log.file_span(LogQueue::Append).0;
        // No entries are rewritten.
        assert_eq!(new_min_file_seq, old_min_file_seq);
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
                engine.append_one(j, &entry);
            }
        }

        // The engine needs purge, and all old entries should be rewritten.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.file_span(LogQueue::Append).0 > 1);

        let rewrite_file_size = engine.pipe_log.total_size(LogQueue::Rewrite);
        assert!(rewrite_file_size > 59); // The rewrite queue isn't empty.

        // All entries should be available.
        for i in 1..=10 {
            for j in 1..=10 {
                let e = engine.get_entry::<Entry>(j, i).unwrap().unwrap();
                assert_eq!(e.get_data(), entry.get_data());
                assert_eq!(engine.last_index_slow(j), 10);
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
                assert_eq!(engine.last_index_slow(j), 10);
            }
        }

        // Rewrite again to check the rewrite queue is healthy.
        for i in 11..=20 {
            for j in 1..=10 {
                entry.set_index(i);
                engine.append_one(j, &entry);
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
            engine.append_one(1, &entry);
        }
        let mut log_batch = LogBatch::with_capacity(1);
        log_batch.add_command(1, Command::Clean);
        engine.write(&mut log_batch, false).unwrap();
        assert!(engine.memtables.get(1).is_none());

        entry.set_data(vec![b'y'; 1024].into());
        for j in 2..=11 {
            entry.set_index(j);
            engine.append_one(1, &entry);
        }

        assert_eq!(engine.pipe_log.file_span(LogQueue::Append).1, 1);

        // Put more raft logs to trigger purge.
        for i in 2..64 {
            for j in 1..=10 {
                entry.set_index(j);
                engine.append_one(i, &entry);
            }
        }

        // The engine needs purge, and all old entries should be rewritten.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.pipe_log.file_span(LogQueue::Append).0 > 1);

        // All entries of region 1 has been rewritten.
        let memtable_1 = engine.memtables.get(1).unwrap();
        assert!(memtable_1.read().max_file_seq(LogQueue::Append).is_none());
        assert!(memtable_1
            .read()
            .kvs_max_file_seq(LogQueue::Append)
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
                engine.append_one(i, &entry);
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
}
