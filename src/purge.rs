use std::cmp;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use log::{debug, info};
use protobuf::Message;

use crate::config::Config;
use crate::engine::{fetch_entries, MemTableAccessor};
use crate::event_listener::EventListener;
use crate::log_batch::{Command, EntryExt, LogBatch, LogItemContent, OpType};
use crate::memtable::{EntryIndex, MemTable};
use crate::pipe_log::{FileId, LogQueue, PipeLog};
use crate::util::HandyRwLock;
use crate::{GlobalStats, Result};

// If a region has some very old raft logs less than this threshold,
// rewrite them to clean stale log files ASAP.
const REWRITE_ENTRY_COUNT_THRESHOLD: usize = 32;
const REWRITE_INACTIVE_RATIO: f64 = 0.7;
const FORCE_COMPACT_RATIO: f64 = 0.2;
const REWRITE_BATCH_SIZE: usize = 1024 * 1024;

#[derive(Clone)]
pub struct PurgeManager<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: PipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor<E, W>,
    pipe_log: P,
    global_stats: Arc<GlobalStats>,
    listeners: Vec<Arc<dyn EventListener>>,

    // Only one thread can run `purge_expired_files` at a time.
    purge_mutex: Arc<Mutex<()>>,
}

impl<E, W, P> PurgeManager<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: PipeLog,
{
    pub fn new(
        cfg: Arc<Config>,
        memtables: MemTableAccessor<E, W>,
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
        }
    }

    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let guard = self.purge_mutex.try_lock();
        if guard.is_err() {
            return Ok(vec![]);
        }

        if !self.needs_purge_log_files(LogQueue::Append) {
            return Ok(vec![]);
        }

        let (rewrite_limit, compact_limit) = self.latest_inactive_file_id();
        if !self
            .listeners
            .iter()
            .all(|h| h.ready_for_purge(LogQueue::Append, rewrite_limit))
        {
            // Generally it means some entries or kvs are not written into memtables, skip.
            debug!("Append file {} is not ready for purge", rewrite_limit);
            return Ok(vec![]);
        }

        let mut will_force_compact = Vec::new();
        self.regions_rewrite_or_force_compact(
            rewrite_limit,
            compact_limit,
            &mut will_force_compact,
        );

        if self.rewrite_queue_needs_squeeze() && self.needs_purge_log_files(LogQueue::Rewrite) {
            self.squeeze_rewrite_queue();
        }

        let (min_file_1, min_file_2) = self.memtables.fold(
            (
                rewrite_limit.forward(1),
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

        Ok(will_force_compact)
    }

    pub fn needs_purge_log_files(&self, queue: LogQueue) -> bool {
        let active_file = self.pipe_log.active_file_id(queue);
        let first_file = self.pipe_log.first_file_id(queue);
        if active_file == first_file {
            return false;
        }

        let total_size = self.pipe_log.total_size(queue);
        let purge_threshold = self.cfg.purge_threshold.0 as usize;
        match queue {
            LogQueue::Append => total_size > purge_threshold,
            LogQueue::Rewrite => total_size * 10 > purge_threshold,
        }
    }

    // Returns (`latest_needs_rewrite`, `latest_needs_force_compact`).
    fn latest_inactive_file_id(&self) -> (FileId, FileId) {
        let queue = LogQueue::Append;

        let first_file = self.pipe_log.first_file_id(queue);
        let active_file = self.pipe_log.active_file_id(queue);
        if active_file == first_file {
            // Can't rewrite or force compact the active file.
            return (Default::default(), Default::default());
        }

        let latest_needs_rewrite = self.pipe_log.file_at(queue, REWRITE_INACTIVE_RATIO);
        let latest_needs_compact = self.pipe_log.file_at(queue, FORCE_COMPACT_RATIO);
        (
            FileId::min(latest_needs_rewrite, active_file.backward(1)),
            FileId::min(latest_needs_compact, active_file.backward(1)),
        )
    }

    pub fn rewrite_queue_needs_squeeze(&self) -> bool {
        // Squeeze the rewrite queue if its garbage ratio reaches 50%.
        let rewrite_operations = self.global_stats.rewrite_operations();
        let compacted_rewrite_operations = self.global_stats.compacted_rewrite_operations();
        compacted_rewrite_operations as f64 / rewrite_operations as f64 > 0.5
    }

    pub fn handle_cleaned_raft(&self, mut log_batch: LogBatch<E, W>) {
        self.rewrite_impl(&mut log_batch, None, true).unwrap();
    }

    fn regions_rewrite_or_force_compact(
        &self,
        latest_rewrite: FileId,
        latest_compact: FileId,
        will_force_compact: &mut Vec<u64>,
    ) {
        assert!(latest_compact <= latest_rewrite);

        let log_batch = self.memtables.clean_regions_before(latest_rewrite);
        self.handle_cleaned_raft(log_batch);

        let memtables = self.memtables.collect(|t| {
            let (rewrite, compact) = t.needs_rewrite_or_compact(
                latest_rewrite,
                latest_compact,
                REWRITE_ENTRY_COUNT_THRESHOLD,
            );
            if compact {
                will_force_compact.push(t.region_id());
            }
            rewrite
        });

        self.rewrite_memtables(
            memtables,
            |t: &MemTable<E, W>, ents_idx| t.fetch_rewrite_entries(latest_rewrite, ents_idx),
            |t: &MemTable<E, W>, kvs| t.fetch_rewrite_kvs(latest_rewrite, kvs),
            REWRITE_ENTRY_COUNT_THRESHOLD,
            Some(latest_rewrite),
        );
    }

    fn squeeze_rewrite_queue(&self) {
        // Switch the active rewrite file.
        self.pipe_log.new_log_file(LogQueue::Rewrite).unwrap();

        let memtables = self
            .memtables
            .collect(|t| t.min_file_id(LogQueue::Rewrite).unwrap_or_default().valid());

        self.rewrite_memtables(
            memtables,
            |t: &MemTable<E, W>, ents_idx| t.fetch_entries_from_rewrite(ents_idx),
            |t: &MemTable<E, W>, kvs| t.fetch_kvs_from_rewrite(kvs),
            0,
            None,
        );
    }

    fn rewrite_memtables<FE, FK>(
        &self,
        memtables: Vec<Arc<RwLock<MemTable<E, W>>>>,
        fe: FE,
        fk: FK,
        expect_count: usize,
        rewrite: Option<FileId>,
    ) where
        FE: Fn(&MemTable<E, W>, &mut Vec<EntryIndex>) -> Result<()> + Copy,
        FK: Fn(&MemTable<E, W>, &mut Vec<(Vec<u8>, Vec<u8>)>) + Copy,
    {
        let mut log_batch = LogBatch::<E, W>::default();
        let mut total_size = 0;
        for memtable in memtables {
            let region_id = memtable.rl().region_id();

            let mut entries = Vec::new();
            fetch_entries(&self.pipe_log, &memtable, &mut entries, expect_count, fe).unwrap();
            entries.iter().for_each(|e| total_size += e.compute_size());
            log_batch.add_entries(region_id, entries);

            let mut kvs = Vec::new();
            fk(&*memtable.rl(), &mut kvs);
            for (k, v) in kvs {
                log_batch.put(region_id, k, v);
            }

            let target_file_size = self.cfg.target_file_size.0 as usize;
            if total_size as usize > cmp::min(REWRITE_BATCH_SIZE, target_file_size) {
                self.rewrite_impl(&mut log_batch, rewrite, false).unwrap();
                total_size = 0;
            }
        }
        self.rewrite_impl(&mut log_batch, rewrite, true).unwrap();
    }

    fn rewrite_impl(
        &self,
        log_batch: &mut LogBatch<E, W>,
        latest_rewrite: Option<FileId>,
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
            self.rewrite_to_memtable(log_batch, file_id, latest_rewrite);
            for listener in &self.listeners {
                listener.post_apply_memtables(LogQueue::Rewrite, file_id);
            }
        }
        Ok(())
    }

    fn rewrite_to_memtable(
        &self,
        log_batch: &mut LogBatch<E, W>,
        file_id: FileId,
        latest_rewrite: Option<FileId>,
    ) {
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
                    memtable.wl().rewrite(entries_index, latest_rewrite);
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
                        memtable.wl().rewrite_key(key, latest_rewrite, file_id);
                    }
                    _ => unreachable!(),
                },
                LogItemContent::Command(Command::Clean) => {
                    debug!("{} append to {:?}.{}, Clean", raft, queue, file_id);
                }
                _ => unreachable!(),
            }
        }
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
