// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::cmp;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use log::{debug, info};
use parking_lot::{Mutex, RwLock};

use crate::config::Config;
use crate::engine::read_entry_bytes_from_file;
use crate::event_listener::EventListener;
use crate::log_batch::{Command, LogBatch, LogItemContent, OpType};
use crate::memtable::{MemTable, MemTableAccessor};
use crate::metrics::*;
use crate::pipe_log::{FileId, LogQueue, PipeLog};
use crate::{GlobalStats, Result};

// Force compact region with oldest 20% logs.
const FORCE_COMPACT_RATIO: f64 = 0.2;
// Only rewrite region with oldest 70% logs.
const REWRITE_RATIO: f64 = 0.7;
// Only rewrite region with stale logs less than this threshold.
const MAX_REWRITE_ENTRIES_PER_REGION: usize = 32;
const MAX_REWRITE_BATCH_BYTES: usize = 1024 * 1024;

#[derive(Clone)]
pub struct PurgeManager<P>
where
    P: PipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor,
    pipe_log: Arc<P>,
    global_stats: Arc<GlobalStats>,
    listeners: Vec<Arc<dyn EventListener>>,

    // Only one thread can run `purge_expired_files` at a time.
    purge_mutex: Arc<Mutex<()>>,
}

impl<P> PurgeManager<P>
where
    P: PipeLog,
{
    pub fn new(
        cfg: Arc<Config>,
        memtables: MemTableAccessor,
        pipe_log: Arc<P>,
        global_stats: Arc<GlobalStats>,
        listeners: Vec<Arc<dyn EventListener>>,
    ) -> PurgeManager<P> {
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
        let mut should_compact = vec![];
        if guard.is_none() {
            return Ok(should_compact);
        }

        if self.needs_rewrite_log_files(LogQueue::Append) {
            let (rewrite_watermark, compact_watermark) = self.append_queue_watermarks();
            should_compact =
                self.rewrite_or_compact_append_queue(rewrite_watermark, compact_watermark)?;
        }

        if self.needs_rewrite_log_files(LogQueue::Rewrite) {
            self.rewrite_rewrite_queue()?;
        }

        let append_queue_barrier = self
            .listeners
            .iter()
            .fold(self.pipe_log.file_span(LogQueue::Append).1, |barrier, l| {
                FileId::min(barrier, l.first_file_not_ready_for_purge(LogQueue::Append))
            });

        // Must rewrite tombstones after acquiring the barrier, in case the log file is rotated
        // shortly after a tombstone is written.
        self.rewrite_append_queue_tombstones()?;

        let (min_file_1, min_file_2) = self.memtables.fold(
            (
                append_queue_barrier,
                self.pipe_log.file_span(LogQueue::Rewrite).1,
            ),
            |(min1, min2), t| {
                (
                    FileId::min(min1, t.min_file_id(LogQueue::Append).unwrap_or_default()),
                    FileId::min(min2, t.min_file_id(LogQueue::Rewrite).unwrap_or_default()),
                )
            },
        );
        debug_assert!(min_file_1.valid() && min_file_2.valid());

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

        Ok(should_compact)
    }

    pub(crate) fn needs_rewrite_log_files(&self, queue: LogQueue) -> bool {
        let (first_file, active_file) = self.pipe_log.file_span(queue);
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
    fn append_queue_watermarks(&self) -> (FileId, FileId) {
        let queue = LogQueue::Append;

        let (first_file, active_file) = self.pipe_log.file_span(queue);
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

    fn rewrite_or_compact_append_queue(
        &self,
        rewrite_watermark: FileId,
        compact_watermark: FileId,
    ) -> Result<Vec<u64>> {
        debug_assert!(compact_watermark <= rewrite_watermark);
        let mut should_compact = Vec::new();

        let memtables = self.memtables.collect(|t| {
            if let Some(f) = t.min_file_id(LogQueue::Append) {
                let sparse = t.entries_count() < MAX_REWRITE_ENTRIES_PER_REGION;
                if f < compact_watermark && !sparse {
                    should_compact.push(t.region_id());
                } else if f < rewrite_watermark {
                    return sparse;
                }
            }
            false
        });

        self.rewrite_memtables(
            memtables,
            MAX_REWRITE_ENTRIES_PER_REGION,
            Some(rewrite_watermark),
        )?;

        Ok(should_compact)
    }

    // Rewrites the entire rewrite queue into new log files.
    fn rewrite_rewrite_queue(&self) -> Result<()> {
        self.pipe_log.new_log_file(LogQueue::Rewrite)?;

        let memtables = self
            .memtables
            .collect(|t| t.min_file_id(LogQueue::Rewrite).is_some());

        self.rewrite_memtables(memtables, 0 /*expect_rewrites_per_memtable*/, None)
    }

    fn rewrite_append_queue_tombstones(&self) -> Result<()> {
        let mut log_batch = self.memtables.take_cleaned_region_logs();
        self.rewrite_impl(
            &mut log_batch,
            None, /*rewrite_watermark*/
            true, /*sync*/
        )
    }

    fn rewrite_memtables(
        &self,
        memtables: Vec<Arc<RwLock<MemTable>>>,
        expect_rewrites_per_memtable: usize,
        rewrite: Option<FileId>,
    ) -> Result<()> {
        let mut log_batch = LogBatch::default();
        let mut total_size = 0;
        for memtable in memtables {
            let mut entry_indexes = Vec::with_capacity(expect_rewrites_per_memtable);
            let mut entries = Vec::with_capacity(expect_rewrites_per_memtable);
            let mut kvs = Vec::new();
            let region_id = {
                let m = memtable.read();
                if let Some(rewrite) = rewrite {
                    m.fetch_entry_indexes_before(rewrite, &mut entry_indexes)?;
                    m.fetch_kvs_before(rewrite, &mut kvs);
                } else {
                    m.fetch_rewritten_entry_indexes(&mut entry_indexes)?;
                    m.fetch_rewritten_kvs(&mut kvs);
                }
                m.region_id()
            };

            for i in &entry_indexes {
                let entry = read_entry_bytes_from_file(self.pipe_log.as_ref(), i)?;
                total_size += entry.len();
                entries.push(entry);
            }
            log_batch.add_raw_entries(region_id, entry_indexes, entries)?;
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
        log_batch: &mut LogBatch,
        rewrite_watermark: Option<FileId>,
        sync: bool,
    ) -> Result<()> {
        let len = log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        if len == 0 {
            if sync {
                self.pipe_log.sync(LogQueue::Rewrite)?;
            }
            return Ok(());
        }

        let (file_id, offset) =
            self.pipe_log
                .append(LogQueue::Rewrite, log_batch.encoded_bytes(), sync)?;
        debug_assert!(file_id.valid());
        log_batch.finish_write(LogQueue::Rewrite, file_id, offset);
        let queue = LogQueue::Rewrite;
        for item in log_batch.drain() {
            let raft = item.raft_group_id;
            let memtable = self.memtables.get_or_insert(raft);
            match item.content {
                LogItemContent::EntryIndexes(entries_to_add) => {
                    let entry_indexes = entries_to_add.0;
                    debug!(
                        "{} append to {:?}.{}, Entries[{:?}:{:?})",
                        raft,
                        queue,
                        file_id,
                        entry_indexes.first().map(|x| x.index),
                        entry_indexes.last().map(|x| x.index + 1),
                    );
                    memtable.write().rewrite(entry_indexes, rewrite_watermark);
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
                        memtable
                            .write()
                            .rewrite_key(key, rewrite_watermark, file_id);
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
        if rewrite_watermark.is_none() {
            BACKGROUND_REWRITE_BYTES.rewrite.inc_by(len as u64);
        } else {
            BACKGROUND_REWRITE_BYTES.append.inc_by(len as u64);
        }
        Ok(())
    }
}

pub struct PurgeHook {
    // Append queue log files that are not yet fully applied to MemTable must not be
    // purged even when not referenced by any MemTable.
    // In order to identify them, maintain a per-file reference counter for all active
    // log files in append queue. No need to track rewrite queue because it is only
    // written by purge thread.
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
            let mut active_log_files = self.active_log_files.write();
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

    fn on_append_log_file(&self, queue: LogQueue, file_id: FileId, _bytes: usize) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[file_id.step_after(&front).unwrap()].1;
            counter.fetch_add(1, Ordering::Release);
        }
    }

    fn post_apply_memtables(&self, queue: LogQueue, file_id: FileId) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[file_id.step_after(&front).unwrap()].1;
            counter.fetch_sub(1, Ordering::Release);
        }
    }

    fn first_file_not_ready_for_purge(&self, queue: LogQueue) -> FileId {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            for (id, counter) in active_log_files.iter() {
                if counter.load(Ordering::Acquire) > 0 {
                    return *id;
                }
            }
        }
        Default::default()
    }

    fn post_purge(&self, queue: LogQueue, file_id: FileId) {
        if queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.write();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            if front <= file_id {
                let mut purged = active_log_files.drain(0..=file_id.step_after(&front).unwrap());
                assert_eq!(purged.next_back().unwrap().0, file_id);
            }
        }
    }
}
