use std::cmp;
use std::collections::VecDeque;
use std::marker::PhantomData;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use log::{debug, info};
use protobuf::Message;

use crate::config::Config;
use crate::engine::{read_entry_from_file, MemTableAccessor};
use crate::event_listener::EventListener;
use crate::log_batch::{Command, EntryExt, LogBatch, LogItemContent, OpType};
use crate::memtable::MemTable;
use crate::pipe_log::{FileId, LogQueue, PipeLog};
use crate::util::HandyRwLock;
use crate::{GlobalStats, Result};

// Force compact region with oldest 20% logs.
const FORCE_COMPACT_RATIO: f64 = 0.2;
// Only rewrite region with oldest 70% logs.
const REWRITE_RATIO: f64 = 0.7;
// Only rewrite region with stale logs less than this threshold.
const MAX_REWRITE_ENTRIES_PER_REGION: usize = 32;
const MAX_REWRITE_BATCH_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct PurgeManager<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: PipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor,
    pipe_log: P,
    global_stats: Arc<GlobalStats>,
    listeners: Vec<Arc<dyn EventListener>>,

    // Only one thread can run `purge_expired_files` at a time.
    purge_mutex: Arc<Mutex<()>>,

    _phantom1: PhantomData<E>,
    _phantom2: PhantomData<W>,
}

impl<E, W, P> PurgeManager<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: PipeLog,
{
    pub fn new(
        cfg: Arc<Config>,
        memtables: MemTableAccessor,
        pipe_log: P,
        global_stats: Arc<GlobalStats>,
        listeners: Vec<Arc<dyn EventListener>>,
    ) -> PurgeManager<E, W, P> {
        PurgeManager {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            listeners,
            purge_mutex: Arc::new(Mutex::new(())),
            _phantom1: PhantomData,
            _phantom2: PhantomData,
        }
    }

    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let guard = self.purge_mutex.try_lock();
        if guard.is_err() || !self.needs_purge_log_files(LogQueue::Append) {
            return Ok(vec![]);
        }

        let (rewrite_watermark, compact_watermark) = self.watermark_file_ids();
        if !self
            .listeners
            .iter()
            .all(|l| l.ready_for_purge(LogQueue::Append, rewrite_watermark))
        {
            // Generally it means some entries or kvs are not written into memtables, skip.
            debug!("Append file {} is not ready for purge", rewrite_watermark);
            return Ok(vec![]);
        }

        let should_force_compact =
            self.rewrite_or_force_compact(rewrite_watermark, compact_watermark)?;

        if self.needs_purge_log_files(LogQueue::Rewrite) {
            self.compact_rewrite_queue()?;
        }

        let (min_file_1, min_file_2) = self.memtables.fold(
            (
                rewrite_watermark.forward(1),
                self.pipe_log.active_file_id(LogQueue::Rewrite),
            ),
            |(min1, min2), t| {
                (
                    FileId::min(min1, t.min_file_id(LogQueue::Append).unwrap_or_default()),
                    FileId::min(min2, t.min_file_id(LogQueue::Rewrite).unwrap_or_default()),
                )
            },
        );
        assert!(min_file_1.valid() && min_file_2.valid());

        for (purge_to, queue) in &[
            (min_file_1, LogQueue::Append),
            (min_file_2, LogQueue::Rewrite),
        ] {
            let purged = self.pipe_log.purge_to(*queue, *purge_to)?;
            info!("purged {} expired log files for queue {:?}", purged, *queue);
            for listener in &self.listeners {
                listener.post_purge(*queue, purge_to.backward(1));
            }
        }

        Ok(should_force_compact)
    }

    pub(crate) fn needs_purge_log_files(&self, queue: LogQueue) -> bool {
        let active_file = self.pipe_log.active_file_id(queue);
        let first_file = self.pipe_log.first_file_id(queue);
        if active_file == first_file {
            return false;
        }

        let total_size = self.pipe_log.total_size(queue);
        let purge_threshold = self.cfg.purge_threshold.0 as usize;
        match queue {
            LogQueue::Append => total_size > purge_threshold,
            LogQueue::Rewrite => {
                let compacted_rewrites_ratio = self.global_stats.compacted_rewrite_operations()
                    as f64
                    / self.global_stats.rewrite_operations() as f64;
                total_size * 10 > purge_threshold && compacted_rewrites_ratio > 0.5
            }
        }
    }

    // Returns (rewrite_watermark, compact_watermark).
    // Files older than compact_watermark should be compacted;
    // Files between compact_watermark and rewrite_watermark should be rewritten.
    fn watermark_file_ids(&self) -> (FileId, FileId) {
        let queue = LogQueue::Append;

        let first_file = self.pipe_log.first_file_id(queue);
        let active_file = self.pipe_log.active_file_id(queue);
        if active_file == first_file {
            // Can't rewrite or force compact the active file.
            return (Default::default(), Default::default());
        }

        let rewrite_watermark = self.pipe_log.file_at(queue, REWRITE_RATIO);
        let compact_watermark = self.pipe_log.file_at(queue, FORCE_COMPACT_RATIO);
        (
            FileId::min(rewrite_watermark, active_file.backward(1)),
            FileId::min(compact_watermark, active_file.backward(1)),
        )
    }

    fn rewrite_or_force_compact(
        &self,
        rewrite_watermark: FileId,
        compact_watermark: FileId,
    ) -> Result<Vec<u64>> {
        assert!(compact_watermark <= rewrite_watermark);
        let mut should_force_compact = Vec::new();

        let mut log_batch = self
            .memtables
            .take_clean_region_logs_before(rewrite_watermark);
        self.rewrite_impl(&mut log_batch, None /**/, true /*sync*/)?;

        let memtables = self.memtables.collect(|t| {
            if let Some(f) = t.min_file_id(LogQueue::Append) {
                let sparse = t.entries_count() < MAX_REWRITE_ENTRIES_PER_REGION;
                if f < compact_watermark && !sparse {
                    should_force_compact.push(t.region_id());
                } else if f < rewrite_watermark {
                    return sparse;
                }
            }
            false
        });

        self.rewrite_memtables(
            memtables,
            MAX_REWRITE_ENTRIES_PER_REGION, /*expect_count*/
            Some(rewrite_watermark),
        )?;

        Ok(should_force_compact)
    }

    // Compact the entire rewrite queue into new rewrite files.
    fn compact_rewrite_queue(&self) -> Result<()> {
        self.pipe_log.new_log_file(LogQueue::Rewrite)?;

        let memtables = self
            .memtables
            .collect(|t| t.min_file_id(LogQueue::Rewrite).is_some());

        self.rewrite_memtables(memtables, 0 /*expect_count*/, None)
    }

    fn rewrite_memtables(
        &self,
        memtables: Vec<Arc<RwLock<MemTable>>>,
        expect_count: usize,
        rewrite: Option<FileId>,
    ) -> Result<()> {
        let mut log_batch = LogBatch::<E, W>::default();
        let mut total_size = 0;
        for memtable in memtables {
            let mut entry_indexes = Vec::with_capacity(expect_count);
            let mut entries = Vec::with_capacity(expect_count);
            let mut kvs = Vec::new();
            let region_id = {
                let m = memtable.rl();
                if let Some(rewrite) = rewrite {
                    m.fetch_entry_indexes_before(rewrite, &mut entry_indexes)?;
                    m.fetch_kvs_before(rewrite, &mut kvs);
                } else {
                    m.fetch_rewritten_entry_indexes(&mut entry_indexes)?;
                    m.fetch_rewritten_kvs(&mut kvs);
                }
                m.region_id()
            };

            for i in entry_indexes {
                let entry = read_entry_from_file::<_, W, _>(&self.pipe_log, &i)?;
                total_size += entry.compute_size();
                entries.push(entry);
            }
            log_batch.add_entries(region_id, entries);
            for (k, v) in kvs {
                log_batch.put(region_id, k, v);
            }

            let target_file_size = self.cfg.target_file_size.0 as usize;
            if total_size as usize > cmp::min(MAX_REWRITE_BATCH_BYTES, target_file_size) {
                self.rewrite_impl(&mut log_batch, rewrite, false)?;
                total_size = 0;
            }
        }
        self.rewrite_impl(&mut log_batch, rewrite, true)
    }

    fn rewrite_impl(
        &self,
        log_batch: &mut LogBatch<E, W>,
        rewrite_watermark: Option<FileId>,
        sync: bool,
    ) -> Result<()> {
        if log_batch.is_empty() {
            if sync {
                self.pipe_log.sync(LogQueue::Rewrite)?;
            }
            return Ok(());
        }

        let (file_id, _) = self.pipe_log.append(LogQueue::Rewrite, log_batch, sync)?;
        if file_id.valid() {
            let queue = LogQueue::Rewrite;
            for item in log_batch.items.drain(..) {
                let raft = item.raft_group_id;
                let memtable = self.memtables.get_or_insert(raft);
                match item.content {
                    LogItemContent::Entries(entries_to_add) => {
                        let entries_index = entries_to_add.entries_index;
                        debug!(
                            "{} append to {:?}.{}, Entries[{:?}:{:?})",
                            raft,
                            queue,
                            file_id,
                            entries_index.first().map(|x| x.index),
                            entries_index.last().map(|x| x.index + 1),
                        );
                        memtable.wl().rewrite(entries_index, rewrite_watermark);
                    }
                    LogItemContent::Kv(kv) => match kv.op_type {
                        OpType::Put => {
                            let key = kv.key;
                            debug!(
                                "{} append to {:?}.{}, Put({})",
                                raft,
                                queue,
                                file_id,
                                hex::encode(&key),
                            );
                            memtable.wl().rewrite_key(key, rewrite_watermark, file_id);
                        }
                        _ => unreachable!(),
                    },
                    LogItemContent::Command(Command::Clean) => {
                        debug!("{} append to {:?}.{}, Clean", raft, queue, file_id);
                    }
                    _ => unreachable!(),
                }
            }
            for listener in &self.listeners {
                listener.post_apply_memtables(LogQueue::Rewrite, file_id);
            }
        }
        Ok(())
    }
}

pub struct PurgeHook {
    // There could be a gap between a write batch is written into a file and applied to memtables.
    // If a log-rewrite happens at the time, entries and kvs in the batch can be omited. So after
    // the log file gets purged, those entries and kvs will be lost.
    //
    // Maintain a per-file reference counter to forbid this bad case. Log files `active_log_files`
    // can't be purged.
    // It's only used for the append queue. The rewrite queue doesn't need this.
    active_log_files: RwLock<VecDeque<(FileId, AtomicUsize)>>,
}

impl PurgeHook {
    pub fn new() -> Self {
        PurgeHook {
            active_log_files: Default::default(),
        }
    }
}

impl EventListener for PurgeHook {
    fn post_new_log_file(&self, queue: LogQueue, file_id: FileId) {
        if queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.wl();
            if let Some(id) = active_log_files.back().map(|x| x.0) {
                assert_eq!(
                    id.forward(1),
                    file_id,
                    "active log files should be contiguous"
                );
            }
            let counter = AtomicUsize::new(0);
            active_log_files.push_back((file_id, counter));
        }
    }

    fn on_append_log_file(&self, queue: LogQueue, file_id: FileId) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[file_id.step_after(&front).unwrap()].1;
            counter.fetch_add(1, Ordering::Release);
        }
    }

    fn post_apply_memtables(&self, queue: LogQueue, file_id: FileId) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[file_id.step_after(&front).unwrap()].1;
            counter.fetch_sub(1, Ordering::Release);
        }
    }

    fn ready_for_purge(&self, queue: LogQueue, file_id: FileId) -> bool {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            for (id, counter) in active_log_files.iter() {
                if *id > file_id {
                    break;
                } else if counter.load(Ordering::Acquire) > 0 {
                    return false;
                }
            }
        }
        true
    }

    fn post_purge(&self, queue: LogQueue, file_id: FileId) {
        if queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.wl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            if front <= file_id {
                let mut purged = active_log_files.drain(0..=file_id.step_after(&front).unwrap());
                assert_eq!(purged.next_back().unwrap().0, file_id);
            }
        }
    }
}
