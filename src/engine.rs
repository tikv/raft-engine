// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::cell::{Cell, RefCell};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use log::{error, info};
use protobuf::{parse_from_bytes, Message};

use crate::config::{Config, RecoveryMode};
use crate::consistency::ConsistencyChecker;
use crate::env::{DefaultFileSystem, FileSystem};
use crate::event_listener::EventListener;
use crate::file_pipe_log::debug::LogItemReader;
use crate::file_pipe_log::{DefaultMachineFactory, FilePipeLog, FilePipeLogBuilder};
use crate::log_batch::{Command, LogBatch, MessageExt};
use crate::memtable::{EntryIndex, MemTableAccessor, MemTableRecoverContext};
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, LogQueue, PipeLog};
use crate::purge::{PurgeHook, PurgeManager};
use crate::write_barrier::{WriteBarrier, Writer};
use crate::{Error, GlobalStats, Result};

pub struct Engine<F = DefaultFileSystem, P = FilePipeLog<F>>
where
    F: FileSystem,
    P: PipeLog,
{
    cfg: Arc<Config>,
    listeners: Vec<Arc<dyn EventListener>>,

    stats: Arc<GlobalStats>,
    memtables: MemTableAccessor,
    pipe_log: Arc<P>,
    purge_manager: PurgeManager<P>,

    write_barrier: WriteBarrier<LogBatch, Result<FileBlockHandle>>,

    _phantom: PhantomData<F>,
}

impl Engine<DefaultFileSystem, FilePipeLog<DefaultFileSystem>> {
    pub fn open(cfg: Config) -> Result<Engine<DefaultFileSystem, FilePipeLog<DefaultFileSystem>>> {
        Self::open_with_listeners(cfg, vec![])
    }

    pub fn open_with_listeners(
        cfg: Config,
        listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<Engine<DefaultFileSystem, FilePipeLog<DefaultFileSystem>>> {
        Self::open_with(cfg, Arc::new(DefaultFileSystem), listeners)
    }
}

impl<F> Engine<F, FilePipeLog<F>>
where
    F: FileSystem,
{
    pub fn open_with_file_system(
        cfg: Config,
        file_system: Arc<F>,
    ) -> Result<Engine<F, FilePipeLog<F>>> {
        Self::open_with(cfg, file_system, vec![])
    }

    pub fn open_with(
        mut cfg: Config,
        file_system: Arc<F>,
        mut listeners: Vec<Arc<dyn EventListener>>,
    ) -> Result<Engine<F, FilePipeLog<F>>> {
        cfg.sanitize()?;
        listeners.push(Arc::new(PurgeHook::new()) as Arc<dyn EventListener>);

        let start = Instant::now();
        let mut builder = FilePipeLogBuilder::new(cfg.clone(), file_system, listeners.clone());
        builder.scan()?;
        let (append, rewrite) =
            builder.recover(&DefaultMachineFactory::<MemTableRecoverContext>::default())?;
        let pipe_log = Arc::new(builder.finish()?);
        rewrite.merge_append_context(append);
        let (memtables, stats) = rewrite.finish();
        info!("Recovering raft logs takes {:?}", start.elapsed());

        let cfg = Arc::new(cfg);
        let purge_manager = PurgeManager::new(
            cfg.clone(),
            memtables.clone(),
            pipe_log.clone(),
            stats.clone(),
            listeners.clone(),
        );

        Ok(Self {
            cfg,
            listeners,
            stats,
            memtables,
            pipe_log,
            purge_manager,
            write_barrier: Default::default(),
            _phantom: PhantomData,
        })
    }
}

impl<F, P> Engine<F, P>
where
    F: FileSystem,
    P: PipeLog,
{
    /// Writes the content of `log_batch` into the engine and returns written
    /// bytes. If `sync` is true, the write will be followed by a call to
    /// `fdatasync` on the log file.
    pub fn write(&self, log_batch: &mut LogBatch, mut sync: bool) -> Result<usize> {
        let start = Instant::now();
        let len = log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        let block_handle = {
            let mut writer = Writer::new(log_batch, sync, start);
            if let Some(mut group) = self.write_barrier.enter(&mut writer) {
                let now = Instant::now();
                let _t = StopWatch::new_with(&ENGINE_WRITE_LEADER_DURATION_HISTOGRAM, now);
                for writer in group.iter_mut() {
                    ENGINE_WRITE_PREPROCESS_DURATION_HISTOGRAM.observe(
                        now.saturating_duration_since(writer.start_time)
                            .as_secs_f64(),
                    );
                    sync |= writer.sync;
                    let log_batch = writer.get_payload();
                    let res = if !log_batch.is_empty() {
                        self.pipe_log
                            .append(LogQueue::Append, log_batch.encoded_bytes())
                    } else {
                        // TODO(tabokie): use Option<FileBlockHandle> instead.
                        Ok(FileBlockHandle {
                            id: FileId::new(LogQueue::Append, 0),
                            offset: 0,
                            len: 0,
                        })
                    };
                    writer.set_output(res);
                }
                if let Err(e) = self.pipe_log.maybe_sync(LogQueue::Append, sync) {
                    panic!(
                        "Cannot sync {:?} queue due to IO error: {}",
                        LogQueue::Append,
                        e
                    );
                }
            }
            writer.finish()?
        };

        let mut now = Instant::now();
        if len > 0 {
            log_batch.finish_write(block_handle);
            self.memtables.apply_append_writes(log_batch.drain());
            for listener in &self.listeners {
                listener.post_apply_memtables(block_handle.id);
            }
            let end = Instant::now();
            ENGINE_WRITE_APPLY_DURATION_HISTOGRAM
                .observe(end.saturating_duration_since(now).as_secs_f64());
            now = end;
        }
        ENGINE_WRITE_DURATION_HISTOGRAM.observe(now.saturating_duration_since(start).as_secs_f64());
        ENGINE_WRITE_SIZE_HISTOGRAM.observe(len as f64);
        Ok(len)
    }

    /// Synchronizes the Raft engine.
    pub fn sync(&self) -> Result<()> {
        // TODO(tabokie): use writer.
        self.pipe_log.maybe_sync(LogQueue::Append, true)
    }

    pub fn get_message<S: Message>(&self, region_id: u64, key: &[u8]) -> Result<Option<S>> {
        let _t = StopWatch::new(&ENGINE_READ_MESSAGE_DURATION_HISTOGRAM);
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
        let _t = StopWatch::new(&ENGINE_READ_ENTRY_DURATION_HISTOGRAM);
        if let Some(memtable) = self.memtables.get(region_id) {
            if let Some(idx) = memtable.read().get_entry(log_idx) {
                ENGINE_READ_ENTRY_COUNT_HISTOGRAM.observe(1.0);
                return Ok(Some(read_entry_from_file::<M, _>(
                    self.pipe_log.as_ref(),
                    &idx,
                )?));
            }
        }
        Ok(None)
    }

    /// Purges expired logs files and returns a set of Raft group ids that need
    /// to be compacted.
    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        // TODO: Move this to a dedicated thread.
        self.stats.flush_metrics();
        self.purge_manager.purge_expired_files()
    }

    /// Returns count of fetched entries.
    pub fn fetch_entries_to<M: MessageExt>(
        &self,
        region_id: u64,
        begin: u64,
        end: u64,
        max_size: Option<usize>,
        vec: &mut Vec<M::Entry>,
    ) -> Result<usize> {
        let _t = StopWatch::new(&ENGINE_READ_ENTRY_DURATION_HISTOGRAM);
        if let Some(memtable) = self.memtables.get(region_id) {
            let mut ents_idx: Vec<EntryIndex> = Vec::with_capacity((end - begin) as usize);
            memtable
                .read()
                .fetch_entries_to(begin, end, max_size, &mut ents_idx)?;
            for i in ents_idx.iter() {
                vec.push(read_entry_from_file::<M, _>(self.pipe_log.as_ref(), i)?);
            }
            ENGINE_READ_ENTRY_COUNT_HISTOGRAM.observe(ents_idx.len() as f64);
            return Ok(ents_idx.len());
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

    /// Deletes log entries before `index` in the specified Raft group. Returns
    /// the number of deleted entries.
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

    // For testing.
    pub fn file_span(&self, queue: LogQueue) -> (u64, u64) {
        self.pipe_log.file_span(queue)
    }

    pub fn get_used_size(&self) -> usize {
        self.pipe_log.total_size(LogQueue::Append) + self.pipe_log.total_size(LogQueue::Rewrite)
    }
}

impl Engine<DefaultFileSystem, FilePipeLog<DefaultFileSystem>> {
    pub fn consistency_check(path: &Path) -> Result<Vec<(u64, u64)>> {
        Self::consistency_check_with_file_system(path, Arc::new(DefaultFileSystem))
    }

    #[cfg(feature = "scripting")]
    pub fn unsafe_repair(path: &Path, queue: Option<LogQueue>, script: String) -> Result<()> {
        Self::unsafe_repair_with_file_system(path, queue, script, Arc::new(DefaultFileSystem))
    }

    pub fn dump(path: &Path) -> Result<LogItemReader<DefaultFileSystem>> {
        Self::dump_with_file_system(path, Arc::new(DefaultFileSystem))
    }
}

impl<F> Engine<F, FilePipeLog<F>>
where
    F: FileSystem,
{
    /// Returns a list of corrupted Raft groups, including their ids and last
    /// valid log index. Head or tail corruption cannot be detected.
    pub fn consistency_check_with_file_system(
        path: &Path,
        file_system: Arc<F>,
    ) -> Result<Vec<(u64, u64)>> {
        if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "raft-engine directory '{}' does not exist.",
                path.to_str().unwrap()
            )));
        }

        let cfg = Config {
            dir: path.to_str().unwrap().to_owned(),
            recovery_mode: RecoveryMode::TolerateAnyCorruption,
            ..Default::default()
        };
        let mut builder = FilePipeLogBuilder::new(cfg, file_system, Vec::new());
        builder.scan()?;
        let (append, rewrite) =
            builder.recover(&DefaultMachineFactory::<ConsistencyChecker>::default())?;
        let mut map = rewrite.finish();
        for (id, index) in append.finish() {
            map.entry(id).or_insert(index);
        }
        let mut list: Vec<(u64, u64)> = map.into_iter().collect();
        list.sort_unstable();
        Ok(list)
    }

    #[cfg(feature = "scripting")]
    pub fn unsafe_repair_with_file_system(
        path: &Path,
        queue: Option<LogQueue>,
        script: String,
        file_system: Arc<F>,
    ) -> Result<()> {
        use crate::file_pipe_log::ReplayMachine;

        if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "raft-engine directory '{}' does not exist.",
                path.to_str().unwrap()
            )));
        }

        let cfg = Config {
            dir: path.to_str().unwrap().to_owned(),
            recovery_mode: RecoveryMode::TolerateAnyCorruption,
            ..Default::default()
        };

        let mut builder = FilePipeLogBuilder::new(cfg, file_system.clone(), Vec::new());
        builder.scan()?;
        let factory = crate::filter::RhaiFilterMachineFactory::from_script(script);
        let mut machine = None;
        if queue.is_none() || queue.unwrap() == LogQueue::Append {
            machine = Some(builder.recover_queue(LogQueue::Append, &factory, 1)?);
        }
        if queue.is_none() || queue.unwrap() == LogQueue::Rewrite {
            let machine2 = builder.recover_queue(LogQueue::Rewrite, &factory, 1)?;
            if let Some(machine) = &mut machine {
                machine.merge(machine2, LogQueue::Rewrite)?;
            }
        }
        if let Some(machine) = machine {
            machine.finish(file_system.as_ref(), path)?;
        }
        Ok(())
    }

    /// Dumps all operations.
    pub fn dump_with_file_system(path: &Path, file_system: Arc<F>) -> Result<LogItemReader<F>> {
        if !path.exists() {
            return Err(Error::InvalidArgument(format!(
                "raft-engine directory or file '{}' does not exist.",
                path.to_str().unwrap()
            )));
        }

        if path.is_dir() {
            LogItemReader::new_directory_reader(file_system, path)
        } else {
            LogItemReader::new_file_reader(file_system, path)
        }
    }
}

struct BlockCache {
    key: Cell<FileBlockHandle>,
    block: RefCell<Vec<u8>>,
}

impl BlockCache {
    fn new() -> Self {
        BlockCache {
            key: Cell::new(FileBlockHandle {
                id: FileId::new(LogQueue::Append, 0),
                offset: 0,
                len: 0,
            }),
            block: RefCell::new(Vec::new()),
        }
    }

    fn insert(&self, key: FileBlockHandle, block: Vec<u8>) {
        self.key.set(key);
        self.block.replace(block);
    }
}

thread_local! {
    static BLOCK_CACHE: BlockCache = BlockCache::new();
}

pub(crate) fn read_entry_from_file<M, P>(pipe_log: &P, idx: &EntryIndex) -> Result<M::Entry>
where
    M: MessageExt,
    P: PipeLog,
{
    BLOCK_CACHE.with(|cache| {
        if cache.key.get() != idx.entries.unwrap() {
            cache.insert(
                idx.entries.unwrap(),
                LogBatch::decode_entries_block(
                    &pipe_log.read_bytes(idx.entries.unwrap())?,
                    idx.entries.unwrap(),
                    idx.compression_type,
                )?,
            );
        }
        let e = parse_from_bytes(
            &cache.block.borrow()
                [idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len],
        )?;
        assert_eq!(M::index(&e), idx.index);
        Ok(e)
    })
}

pub(crate) fn read_entry_bytes_from_file<P>(pipe_log: &P, idx: &EntryIndex) -> Result<Vec<u8>>
where
    P: PipeLog,
{
    BLOCK_CACHE.with(|cache| {
        if cache.key.get() != idx.entries.unwrap() {
            cache.insert(
                idx.entries.unwrap(),
                LogBatch::decode_entries_block(
                    &pipe_log.read_bytes(idx.entries.unwrap())?,
                    idx.entries.unwrap(),
                    idx.compression_type,
                )?,
            );
        }
        Ok(cache.block.borrow()
            [idx.entry_offset as usize..idx.entry_offset as usize + idx.entry_len]
            .to_owned())
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::env::ObfuscatedFileSystem;
    use crate::file_pipe_log::FileNameExt;
    use crate::test_util::{generate_entries, PanicGuard};
    use crate::util::ReadableSize;
    use kvproto::raft_serverpb::RaftLocalState;
    use raft::eraftpb::Entry;
    use std::fs::OpenOptions;
    use std::path::PathBuf;

    type RaftLogEngine<F = DefaultFileSystem> = Engine<F>;
    impl<F: FileSystem> RaftLogEngine<F> {
        fn append(&self, rid: u64, start_index: u64, end_index: u64, data: Option<&[u8]>) {
            let entries = generate_entries(start_index, end_index, data);
            if !entries.is_empty() {
                let mut batch = LogBatch::default();
                batch.add_entries::<Entry>(rid, &entries).unwrap();
                batch
                    .put_message(
                        rid,
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

        fn get(&self, rid: u64, key: &[u8]) -> Option<Vec<u8>> {
            if let Some(memtable) = self.memtables.get(rid) {
                if let Some(value) = memtable.read().get(key) {
                    return Some(value);
                }
            }
            None
        }

        fn clean(&self, rid: u64) {
            let mut log_batch = LogBatch::default();
            log_batch.add_command(rid, Command::Clean);
            self.write(&mut log_batch, true).unwrap();
        }

        fn decode_last_index(&self, rid: u64) -> Option<u64> {
            self.get_message::<RaftLocalState>(rid, b"last_index")
                .unwrap()
                .map(|s| s.last_index)
        }

        fn reopen(self) -> Self {
            let cfg: Config = self.cfg.as_ref().clone();
            let file_system = self.pipe_log.file_system();
            let mut listeners = self.listeners.clone();
            listeners.pop();
            drop(self);
            RaftLogEngine::open_with(cfg, file_system, listeners).unwrap()
        }

        fn scan<FR: Fn(u64, LogQueue, &[u8])>(&self, rid: u64, start: u64, end: u64, reader: FR) {
            let mut entries = Vec::new();
            self.fetch_entries_to::<Entry>(
                rid,
                self.first_index(rid).unwrap(),
                self.last_index(rid).unwrap() + 1,
                None,
                &mut entries,
            )
            .unwrap();
            assert_eq!(entries.first().unwrap().index, start);
            assert_eq!(
                entries.last().unwrap().index,
                self.decode_last_index(rid).unwrap()
            );
            assert_eq!(entries.last().unwrap().index + 1, end);
            for e in entries.iter() {
                let entry_index = self
                    .memtables
                    .get(rid)
                    .unwrap()
                    .read()
                    .get_entry(e.index)
                    .unwrap();
                assert_eq!(&self.get_entry::<Entry>(rid, e.index).unwrap().unwrap(), e);
                reader(e.index, entry_index.entries.unwrap().id.queue, &e.data);
            }
        }
    }

    #[test]
    fn test_empty_engine() {
        let dir = tempfile::Builder::new()
            .prefix("test_empty_engine")
            .tempdir()
            .unwrap();
        let mut sub_dir = PathBuf::from(dir.as_ref());
        sub_dir.push("raft-engine");
        let cfg = Config {
            dir: sub_dir.to_str().unwrap().to_owned(),
            ..Default::default()
        };
        RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new())).unwrap();
    }

    #[test]
    fn test_get_entry() {
        let normal_batch_size = 10;
        let compressed_batch_size = 5120;
        for &entry_size in &[normal_batch_size, compressed_batch_size] {
            let dir = tempfile::Builder::new()
                .prefix("test_get_entry")
                .tempdir()
                .unwrap();
            let cfg = Config {
                dir: dir.path().to_str().unwrap().to_owned(),
                target_file_size: ReadableSize(1),
                ..Default::default()
            };

            let engine = RaftLogEngine::open_with_file_system(
                cfg.clone(),
                Arc::new(ObfuscatedFileSystem::new()),
            )
            .unwrap();
            let data = vec![b'x'; entry_size];
            for i in 10..20 {
                let rid = i;
                let index = i;
                engine.append(rid, index, index + 2, Some(&data));
            }
            for i in 10..20 {
                let rid = i;
                let index = i;
                engine.scan(rid, index, index + 2, |_, q, d| {
                    assert_eq!(q, LogQueue::Append);
                    assert_eq!(d, &data);
                });
            }

            // Recover the engine.
            let engine = engine.reopen();
            for i in 10..20 {
                let rid = i;
                let index = i;
                engine.scan(rid, index, index + 2, |_, q, d| {
                    assert_eq!(q, LogQueue::Append);
                    assert_eq!(d, &data);
                });
            }
        }
    }

    #[test]
    fn test_clean_raft_group() {
        fn run_steps(steps: &[Option<(u64, u64)>]) {
            let rid = 1;
            let data = vec![b'x'; 1024];

            for rewrite_step in 1..=steps.len() {
                for exit_purge in [None, Some(1), Some(2)] {
                    let _guard = PanicGuard::with_prompt(format!(
                        "case: [{:?}, {}, {:?}]",
                        steps, rewrite_step, exit_purge
                    ));
                    let dir = tempfile::Builder::new()
                        .prefix("test_clean_raft_group")
                        .tempdir()
                        .unwrap();
                    let cfg = Config {
                        dir: dir.path().to_str().unwrap().to_owned(),
                        target_file_size: ReadableSize(1),
                        ..Default::default()
                    };
                    let engine = RaftLogEngine::open_with_file_system(
                        cfg.clone(),
                        Arc::new(ObfuscatedFileSystem::new()),
                    )
                    .unwrap();

                    for (i, step) in steps.iter().enumerate() {
                        if let Some((start, end)) = *step {
                            engine.append(rid, start, end, Some(&data));
                        } else {
                            engine.clean(rid);
                        }
                        if i + 1 == rewrite_step {
                            engine
                                .purge_manager
                                .must_rewrite_append_queue(None, exit_purge);
                        }
                    }

                    let engine = engine.reopen();
                    if let Some((start, end)) = *steps.last().unwrap() {
                        engine.scan(rid, start, end, |_, _, d| {
                            assert_eq!(d, &data);
                        });
                    } else {
                        assert!(engine.raft_groups().is_empty());
                    }

                    engine.purge_manager.must_rewrite_append_queue(None, None);
                    let engine = engine.reopen();
                    if let Some((start, end)) = *steps.last().unwrap() {
                        engine.scan(rid, start, end, |_, _, d| {
                            assert_eq!(d, &data);
                        });
                    } else {
                        assert!(engine.raft_groups().is_empty());
                    }
                }
            }
        }

        run_steps(&[Some((1, 5)), None, Some((2, 6)), None, Some((3, 7)), None]);
        run_steps(&[Some((1, 5)), None, Some((2, 6)), None, Some((3, 7))]);
        run_steps(&[Some((1, 5)), None, Some((2, 6)), None]);
        run_steps(&[Some((1, 5)), None, Some((2, 6))]);
        run_steps(&[Some((1, 5)), None]);
    }

    #[test]
    fn test_delete_key_value() {
        let dir = tempfile::Builder::new()
            .prefix("test_delete_key_value")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize(1),
            ..Default::default()
        };
        let rid = 1;
        let key = b"key".to_vec();
        let (v1, v2) = (b"v1".to_vec(), b"v2".to_vec());
        let mut batch_1 = LogBatch::default();
        batch_1.put(rid, key.clone(), v1);
        let mut batch_2 = LogBatch::default();
        batch_2.put(rid, key.clone(), v2.clone());
        let mut delete_batch = LogBatch::default();
        delete_batch.delete(rid, key.clone());

        // put | delete
        //     ^ rewrite
        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine.purge_manager.must_rewrite_append_queue(None, None);
        engine.write(&mut delete_batch.clone(), true).unwrap();
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key), None);
        // Incomplete purge.
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        engine.write(&mut delete_batch.clone(), true).unwrap();
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key), None);

        // TODO: Preserve kv tombstone during rewrite and activate this test case.
        // put | delete |
        //              ^ rewrite
        // let engine = engine.reopen();
        // engine.write(&mut batch_1.clone(), true).unwrap();
        // engine.write(&mut delete_batch.clone(), true).unwrap();
        // engine.purge_manager.must_rewrite_append_queue(None, None);
        // let engine = engine.reopen();
        // assert_eq!(engine.get(rid, &key), None);

        // put | delete | put
        //     ^ rewrite
        let engine = engine.reopen();
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine.purge_manager.must_rewrite_append_queue(None, None);
        engine.write(&mut delete_batch.clone(), true).unwrap();
        engine.write(&mut batch_2.clone(), true).unwrap();
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key).unwrap(), v2);
        // Incomplete purge.
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        engine.write(&mut delete_batch.clone(), true).unwrap();
        engine.write(&mut batch_2.clone(), true).unwrap();
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key).unwrap(), v2);

        // put | delete | put
        //              ^ rewrite
        let engine = engine.reopen();
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine.write(&mut delete_batch.clone(), true).unwrap();
        engine.purge_manager.must_rewrite_append_queue(None, None);
        engine.write(&mut batch_2.clone(), true).unwrap();
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key).unwrap(), v2);
        // Incomplete purge.
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine.write(&mut delete_batch.clone(), true).unwrap();
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        engine.write(&mut batch_2.clone(), true).unwrap();
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key).unwrap(), v2);

        // put | delete | put |
        //                    ^ rewrite
        let engine = engine.reopen();
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine.write(&mut delete_batch.clone(), true).unwrap();
        engine.write(&mut batch_2.clone(), true).unwrap();
        engine.purge_manager.must_rewrite_append_queue(None, None);
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key).unwrap(), v2);
        // Incomplete purge.
        let engine = engine.reopen();
        engine.write(&mut batch_1.clone(), true).unwrap();
        engine.write(&mut delete_batch.clone(), true).unwrap();
        engine.write(&mut batch_2.clone(), true).unwrap();
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        let engine = engine.reopen();
        assert_eq!(engine.get(rid, &key).unwrap(), v2);
    }

    #[test]
    fn test_compact_raft_group() {
        let dir = tempfile::Builder::new()
            .prefix("test_compact_raft_group")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize(1),
            ..Default::default()
        };
        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        let data = vec![b'x'; 1024];

        // rewrite:[1  ..10]
        // append:   [5..10]
        let mut rid = 7;
        engine.append(rid, 1, 10, Some(&data));
        // Files are not purged.
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        let mut compact_log = LogBatch::default();
        compact_log.add_command(rid, Command::Compact { index: 5 });
        engine.write(&mut compact_log, true).unwrap();
        let engine = engine.reopen();
        engine.scan(rid, 5, 10, |_, q, d| {
            assert_eq!(q, LogQueue::Append);
            assert_eq!(d, &data);
        });

        // rewrite:   [20..25]
        // append: [10   ..25]
        rid += 1;
        engine.append(rid, 5, 15, Some(&data));
        let mut compact_log = LogBatch::default();
        compact_log.add_command(rid, Command::Compact { index: 10 });
        engine.write(&mut compact_log, true).unwrap();
        engine.append(rid, 15, 25, Some(&data));
        // Files are not purged.
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        // Simulate loss of buffered write.
        let mut compact_log = LogBatch::default();
        compact_log.add_command(rid, Command::Compact { index: 20 });
        engine.memtables.apply_append_writes(compact_log.drain());
        engine.purge_manager.must_rewrite_rewrite_queue();
        let engine = engine.reopen();
        engine.scan(rid, 10, 25, |_, q, d| {
            assert_eq!(q, LogQueue::Append);
            assert_eq!(d, &data);
        });
        // rewrite: [20..25][10..25]
        // append: [10..25]
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        let engine = engine.reopen();
        engine.scan(rid, 10, 25, |_, q, d| {
            assert_eq!(q, LogQueue::Append);
            assert_eq!(d, &data);
        });

        // rewrite:[10..15][15  ..25]
        // append:           [20..25]
        rid += 1;
        engine.append(rid, 5, 15, Some(&data));
        let mut compact_log = LogBatch::default();
        compact_log.add_command(rid, Command::Compact { index: 10 });
        engine.write(&mut compact_log, true).unwrap();
        engine.purge_manager.must_rewrite_append_queue(None, None);
        engine.append(rid, 15, 25, Some(&data));
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        let mut compact_log = LogBatch::default();
        compact_log.add_command(rid, Command::Compact { index: 20 });
        engine.write(&mut compact_log, true).unwrap();
        let engine = engine.reopen();
        engine.scan(rid, 20, 25, |_, q, d| {
            assert_eq!(q, LogQueue::Append);
            assert_eq!(d, &data);
        });

        // rewrite:[1..5] [10..15]
        // append:        [10..15]
        rid += 1;
        engine.append(rid, 1, 5, Some(&data));
        engine.purge_manager.must_rewrite_append_queue(None, None);
        engine.append(rid, 5, 15, Some(&data));
        let mut compact_log = LogBatch::default();
        compact_log.add_command(rid, Command::Compact { index: 10 });
        engine.write(&mut compact_log, true).unwrap();
        // Files are not purged.
        engine
            .purge_manager
            .must_rewrite_append_queue(None, Some(2));
        let engine = engine.reopen();
        engine.scan(rid, 10, 15, |_, q, d| {
            assert_eq!(q, LogQueue::Append);
            assert_eq!(d, &data);
        });
    }

    #[test]
    fn test_purge_triggered_by_compact() {
        let dir = tempfile::Builder::new()
            .prefix("test_purge_triggered_by_compact")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::kb(5),
            purge_threshold: ReadableSize::kb(150),
            ..Default::default()
        };

        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        let data = vec![b'x'; 1024];
        for index in 0..100 {
            engine.append(1, index, index + 1, Some(&data));
        }

        // GC all log entries. Won't trigger purge because total size is not enough.
        let count = engine.compact_to(1, 100);
        assert_eq!(count, 100);
        assert!(!engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));

        // Append more logs to make total size greater than `purge_threshold`.
        for index in 100..250 {
            engine.append(1, index, index + 1, Some(&data));
        }

        // GC first 101 log entries.
        assert_eq!(engine.compact_to(1, 101), 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));

        let old_min_file_seq = engine.file_span(LogQueue::Append).0;
        let will_force_compact = engine.purge_expired_files().unwrap();
        let new_min_file_seq = engine.file_span(LogQueue::Append).0;
        // Some entries are rewritten.
        assert!(new_min_file_seq > old_min_file_seq);
        // No regions need to be force compacted because the threshold is not reached.
        assert!(will_force_compact.is_empty());
        // After purge, entries and raft state are still available.
        assert!(engine.get_entry::<Entry>(1, 101).unwrap().is_some());

        assert_eq!(engine.compact_to(1, 102), 1);
        // Needs to purge because the total size is greater than `purge_threshold`.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        let will_force_compact = engine.purge_expired_files().unwrap();
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
        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        let data = vec![b'x'; 1024];

        // Put 100 entries into 10 regions.
        for index in 1..=10 {
            for rid in 1..=10 {
                engine.append(rid, index, index + 1, Some(&data));
            }
        }

        // The engine needs purge, and all old entries should be rewritten.
        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
        assert!(engine.file_span(LogQueue::Append).0 > 1);

        let rewrite_file_size = engine.pipe_log.total_size(LogQueue::Rewrite);
        assert!(rewrite_file_size > 59); // The rewrite queue isn't empty.

        // All entries should be available.
        for rid in 1..=10 {
            engine.scan(rid, 1, 11, |_, _, d| {
                assert_eq!(d, &data);
            });
        }

        // Recover with rewrite queue and append queue.
        let cleaned_region_ids = engine.memtables.cleaned_region_ids();

        let engine = engine.reopen();
        assert_eq!(engine.memtables.cleaned_region_ids(), cleaned_region_ids);

        for rid in 1..=10 {
            engine.scan(rid, 1, 11, |_, _, d| {
                assert_eq!(d, &data);
            });
        }

        // Rewrite again to check the rewrite queue is healthy.
        for index in 11..=20 {
            for rid in 1..=10 {
                engine.append(rid, index, index + 1, Some(&data));
            }
        }

        assert!(engine
            .purge_manager
            .needs_rewrite_log_files(LogQueue::Append));
        assert!(engine.purge_expired_files().unwrap().is_empty());
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
        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();

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
            .put_message(1, b"key".to_vec(), &empty_state)
            .unwrap();
        engine.write(&mut log_batch, false).unwrap();
        log_batch
            .add_entries::<Entry>(2, &[empty_entry.clone()])
            .unwrap();
        log_batch
            .put_message(2, b"key".to_vec(), &empty_state)
            .unwrap();
        engine.write(&mut log_batch, true).unwrap();

        let engine = engine.reopen();
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
                .get_message::<RaftLocalState>(1, b"key")
                .unwrap()
                .unwrap(),
            empty_state
        );
        assert_eq!(
            engine
                .get_message::<RaftLocalState>(2, b"key")
                .unwrap()
                .unwrap(),
            empty_state
        );
    }

    #[test]
    fn test_dirty_recovery() {
        let dir = tempfile::Builder::new()
            .prefix("test_dirty_recovery")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        let data = vec![b'x'; 1024];

        for rid in 1..21 {
            engine.append(rid, 1, 21, Some(&data));
        }

        // Create an unrelated sub-directory.
        std::fs::create_dir(dir.path().join(Path::new("random_dir"))).unwrap();
        // Create an unrelated file.
        let _f = std::fs::File::create(dir.path().join(Path::new("random_file"))).unwrap();

        let engine = engine.reopen();
        for rid in 1..21 {
            engine.scan(rid, 1, 21, |_, _, d| {
                assert_eq!(d, &data);
            });
        }
    }

    #[test]
    fn test_large_rewrite_batch() {
        let dir = tempfile::Builder::new()
            .prefix("test_large_rewrite_batch")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize(1),
            ..Default::default()
        };
        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        let data = vec![b'x'; 2 * 1024 * 1024];

        for rid in 1..=3 {
            engine.append(rid, 1, 11, Some(&data));
        }

        let old_active_file = engine.file_span(LogQueue::Append).1;
        engine.purge_manager.must_rewrite_append_queue(None, None);
        assert_eq!(engine.file_span(LogQueue::Append).0, old_active_file + 1);
        let old_active_file = engine.file_span(LogQueue::Rewrite).1;
        engine.purge_manager.must_rewrite_rewrite_queue();
        assert_eq!(engine.file_span(LogQueue::Rewrite).0, old_active_file + 1);

        let engine = engine.reopen();
        for rid in 1..=3 {
            engine.scan(rid, 1, 11, |_, _, d| {
                assert_eq!(d, &data);
            });
        }
    }

    /// Test cases related to tools ///

    #[test]
    fn test_dump_file_or_directory() {
        let dir = tempfile::Builder::new()
            .prefix("test_dump_file_or_directory")
            .tempdir()
            .unwrap();
        let entry_data = vec![b'x'; 1024];

        let mut batches = vec![vec![LogBatch::default()]];
        let mut batch = LogBatch::default();
        batch
            .add_entries::<Entry>(7, &generate_entries(1, 11, Some(&entry_data)))
            .unwrap();
        batch.add_command(7, Command::Clean);
        batch.put(7, b"key".to_vec(), b"value".to_vec());
        batch.delete(7, b"key2".to_vec());
        batches.push(vec![batch.clone()]);
        let mut batch2 = LogBatch::default();
        batch2.put(8, b"key3".to_vec(), b"value".to_vec());
        batch2
            .add_entries::<Entry>(8, &generate_entries(5, 15, Some(&entry_data)))
            .unwrap();
        batches.push(vec![batch, batch2]);

        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };

        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        for bs in batches.iter_mut() {
            for batch in bs.iter_mut() {
                engine.write(batch, false).unwrap();
            }

            engine.sync().unwrap();
        }

        drop(engine);
        //dump dir with raft groups. 8 element in raft groups 7 and 2 elements in raft
        // groups 8
        let dump_it =
            Engine::dump_with_file_system(dir.path(), Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        let total = dump_it
            .inspect(|i| {
                i.as_ref().unwrap();
            })
            .count();
        assert!(total == 10);

        //dump file
        let file_id = FileId {
            queue: LogQueue::Rewrite,
            seq: 1,
        };
        let dump_it = Engine::dump_with_file_system(
            file_id.build_file_path(dir.path()).as_path(),
            Arc::new(ObfuscatedFileSystem::new()),
        )
        .unwrap();
        let total = dump_it
            .inspect(|i| {
                i.as_ref().unwrap();
            })
            .count();
        assert!(0 == total);

        //dump dir that does not exists
        assert!(Engine::dump_with_file_system(
            Path::new("/not_exists_dir"),
            Arc::new(ObfuscatedFileSystem::new())
        )
        .is_err());

        //dump file that does not exists
        let mut not_exists_file = PathBuf::from(dir.as_ref());
        not_exists_file.push("not_exists_file");
        assert!(Engine::dump_with_file_system(
            not_exists_file.as_path(),
            Arc::new(ObfuscatedFileSystem::new())
        )
        .is_err());
    }

    #[cfg(feature = "scripting")]
    #[test]
    fn test_repair_default() {
        let dir = tempfile::Builder::new()
            .prefix("test_repair_default")
            .tempdir()
            .unwrap();
        let entry_data = vec![b'x'; 128];
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize(1), // Create lots of files.
            ..Default::default()
        };

        let engine = RaftLogEngine::open_with_file_system(
            cfg.clone(),
            Arc::new(ObfuscatedFileSystem::new()),
        )
        .unwrap();
        for rid in 1..=50 {
            engine.append(rid, 1, 6, Some(&entry_data));
        }
        for rid in 25..=50 {
            engine.append(rid, 6, 11, Some(&entry_data));
        }
        drop(engine);

        let script1 = "".to_owned();
        RaftLogEngine::unsafe_repair_with_file_system(
            dir.path(),
            None, /* queue */
            script1,
            Arc::new(ObfuscatedFileSystem::new()),
        )
        .unwrap();
        let script2 = "
            fn filter_append(id, first, count, rewrite_count, queue, ifirst, ilast) {
                0
            }
            fn filter_compact(id, first, count, rewrite_count, queue, compact_to) {
                0
            }
            fn filter_clean(id, first, count, rewrite_count, queue) {
                0
            }
        "
        .to_owned();
        RaftLogEngine::unsafe_repair_with_file_system(
            dir.path(),
            None, /* queue */
            script2,
            Arc::new(ObfuscatedFileSystem::new()),
        )
        .unwrap();

        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        for rid in 1..25 {
            engine.scan(rid, 1, 6, |_, _, d| {
                assert_eq!(d, &entry_data);
            });
        }
        for rid in 25..=50 {
            engine.scan(rid, 1, 11, |_, _, d| {
                assert_eq!(d, &entry_data);
            });
        }
    }

    #[cfg(feature = "scripting")]
    #[test]
    fn test_repair_discard_entries() {
        let dir = tempfile::Builder::new()
            .prefix("test_repair_discard")
            .tempdir()
            .unwrap();
        let entry_data = vec![b'x'; 128];
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize(1), // Create lots of files.
            ..Default::default()
        };

        let engine = RaftLogEngine::open_with_file_system(
            cfg.clone(),
            Arc::new(ObfuscatedFileSystem::new()),
        )
        .unwrap();
        for rid in 1..=50 {
            engine.append(rid, 1, 6, Some(&entry_data));
        }
        for rid in 25..=50 {
            engine.append(rid, 6, 11, Some(&entry_data));
        }
        drop(engine);

        let incoming_emptied = [1, 25];
        let existing_emptied = [2, 26];
        let script = "
            fn filter_append(id, first, count, rewrite_count, queue, ifirst, ilast) {
                if id == 1 {
                    return 1;
                } else if id == 2 {
                    return 2;
                } else if id == 25 {
                    return 1;
                } else if id == 26 {
                    return 2;
                }
                0 // default
            }
        "
        .to_owned();
        RaftLogEngine::unsafe_repair_with_file_system(
            dir.path(),
            None, /* queue */
            script,
            Arc::new(ObfuscatedFileSystem::new()),
        )
        .unwrap();

        let engine =
            RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new()))
                .unwrap();
        for rid in 1..25 {
            if existing_emptied.contains(&rid) || incoming_emptied.contains(&rid) {
                continue;
            }
            engine.scan(rid, 1, 6, |_, _, d| {
                assert_eq!(d, &entry_data);
            });
        }
        for rid in 25..=50 {
            if existing_emptied.contains(&rid) || incoming_emptied.contains(&rid) {
                continue;
            }
            engine.scan(rid, 1, 11, |_, _, d| {
                assert_eq!(d, &entry_data);
            });
        }
        for rid in existing_emptied {
            let first_index = if rid < 25 { 1 } else { 6 };
            let last_index = if rid < 25 { 5 } else { 10 };
            engine.scan(rid, first_index, last_index + 1, |_, _, d| {
                assert_eq!(d, &entry_data);
            });
        }
        for rid in incoming_emptied {
            let last_index = if rid < 25 { 5 } else { 10 };
            assert_eq!(engine.first_index(rid), None);
            assert_eq!(engine.last_index(rid), None);
            assert_eq!(engine.decode_last_index(rid), Some(last_index));
        }
    }

    #[test]
    fn test_tail_corruption() {
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption")
            .tempdir()
            .unwrap();
        let entry_data = vec![b'x'; 16];
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            // One big file.
            target_file_size: ReadableSize::gb(10),
            ..Default::default()
        };

        let engine = RaftLogEngine::open_with_file_system(
            cfg.clone(),
            Arc::new(ObfuscatedFileSystem::new()),
        )
        .unwrap();
        for rid in 1..=50 {
            engine.append(rid, 1, 6, Some(&entry_data));
        }
        for rid in 25..=50 {
            engine.append(rid, 6, 11, Some(&entry_data));
        }
        let (_, last_file_seq) = engine.file_span(LogQueue::Append);
        drop(engine);

        let last_file = FileId {
            queue: LogQueue::Append,
            seq: last_file_seq,
        };
        let f = OpenOptions::new()
            .write(true)
            .open(last_file.build_file_path(dir.path()))
            .unwrap();

        // Corrupt a log batch.
        f.set_len(f.metadata().unwrap().len() - 1).unwrap();
        RaftLogEngine::open_with_file_system(cfg.clone(), Arc::new(ObfuscatedFileSystem::new()))
            .unwrap();

        // Corrupt the file header.
        f.set_len(1).unwrap();
        RaftLogEngine::open_with_file_system(cfg, Arc::new(ObfuscatedFileSystem::new())).unwrap();
    }

    #[cfg(feature = "nightly")]
    #[bench]
    fn bench_engine_fetch_entries(b: &mut test::Bencher) {
        use rand::{thread_rng, Rng};

        let dir = tempfile::Builder::new()
            .prefix("bench_engine_fetch_entries")
            .tempdir()
            .unwrap();
        let entry_data = vec![b'x'; 1024];
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = RaftLogEngine::open(cfg).unwrap();
        for i in 0..10 {
            for rid in 1..=100 {
                engine.append(rid, 1 + i * 10, 1 + i * 10 + 10, Some(&entry_data));
            }
        }
        let mut vec: Vec<Entry> = Vec::new();
        b.iter(move || {
            let region_id = thread_rng().gen_range(1..=100);
            engine
                .fetch_entries_to::<Entry>(region_id, 1, 101, None, &mut vec)
                .unwrap();
            vec.clear();
        });
    }
}
