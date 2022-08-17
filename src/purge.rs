// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use log::warn;
use parking_lot::{Mutex, RwLock};

use crate::config::Config;
use crate::engine::read_entry_bytes_from_file;
use crate::event_listener::EventListener;
use crate::log_batch::LogBatch;
use crate::memtable::{MemTableHandle, MemTables};
use crate::metrics::*;
use crate::pipe_log::{
    FileBlockHandle, FileId, FileSeq, FilesView, LogQueue, PipeLog, MAX_WRITE_CHANNELS,
};
use crate::{GlobalStats, Result};

// Force compact region with oldest 20% logs.
const FORCE_COMPACT_RATIO: f64 = 0.2;
// Only rewrite region with oldest 70% logs.
const REWRITE_RATIO: f64 = 0.7;
// Only rewrite region with stale logs less than this threshold.
const MAX_REWRITE_ENTRIES_PER_REGION: usize = 32;
const MAX_REWRITE_BATCH_BYTES: usize = 128 * 1024;
const MAX_COUNT_BEFORE_FORCE_REWRITE: u32 = 9;

pub struct PurgeManager<P>
where
    P: PipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTables,
    pipe_log: Arc<P>,
    global_stats: Arc<GlobalStats>,
    listeners: Vec<Arc<dyn EventListener>>,

    // Only one thread can run `purge_expired_files` at a time.
    //
    // This table records Raft Groups that should be force compacted before. Those that are not
    // compacted in time (after `MAX_EPOCH_BEFORE_FORCE_REWRITE` epochs) will be force rewritten.
    force_rewrite_candidates: Arc<Mutex<HashMap<u64, u32>>>,
}

impl<P> PurgeManager<P>
where
    P: PipeLog,
{
    pub fn new(
        cfg: Arc<Config>,
        memtables: MemTables,
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
            force_rewrite_candidates: Arc::new(Mutex::new(HashMap::default())),
        }
    }

    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let _t = StopWatch::new(&*ENGINE_PURGE_DURATION_HISTOGRAM);
        let guard = self.force_rewrite_candidates.try_lock();
        if guard.is_none() {
            warn!("Unable to purge expired files: locked");
            return Ok(vec![]);
        }
        let mut rewrite_candidate_regions = guard.unwrap();

        let mut should_compact = HashSet::new();
        if self.needs_rewrite_log_files(LogQueue::REWRITE) {
            should_compact.extend(self.rewrite_rewrite_queue()?);
        }

        if self.needs_rewrite_log_files(LogQueue::DEFAULT) {
            if let (Some(compact_watermark), Some(rewrite_watermark)) =
                self.append_queue_watermarks()
            {
                let mut append_queue_barrier =
                    FilesView::simple_view(self.pipe_log.file_span(LogQueue::DEFAULT).1 - 1);
                self.listeners.iter().for_each(|l| {
                    if let Some(view) = l.latest_files_view_allowed_to_purge() {
                        append_queue_barrier.intersect(&view);
                    }
                });
                // Ordering
                // 1. Must rewrite tombstones AFTER acquiring
                //    `append_queue_barrier`, or deletion marks might be lost
                //    after restart.
                // 2. Must rewrite tombstones BEFORE rewrite entries, or
                //    entries from recreated region might be lost after
                //    restart.
                self.rewrite_append_queue_tombstones()?;
                should_compact.extend(self.rewrite_or_compact_append_queue(
                    &compact_watermark,
                    &rewrite_watermark,
                    &mut rewrite_candidate_regions,
                )?);
                // FIXME: scoping the append_queue_barrier will break test
                // `test_pipe_log_listeners`. append_queue_barrier.intersect(&
                // rewrite_watermark);
                self.rescan_memtables_and_purge_append_files(append_queue_barrier)?;
            }
        }
        Ok(should_compact.into_iter().collect())
    }

    /// Rewrite append files with seqno no larger than `watermark`. When it's
    /// None, rewrite the entire queue. Returns the number of purged files.
    #[cfg(test)]
    pub fn must_rewrite_append_queue(
        &self,
        watermark: Option<FilesView>,
        exit_after_step: Option<u64>,
    ) {
        let _lk = self.force_rewrite_candidates.try_lock().unwrap();
        let active = self.pipe_log.fetch_active_file(LogQueue::DEFAULT).id;
        let watermark = watermark.unwrap_or_else(|| FilesView::simple_view(active.seq));
        if watermark.contains(&active) {
            self.pipe_log.rotate(LogQueue::DEFAULT).unwrap();
            assert!(!watermark.contains(&self.pipe_log.fetch_active_file(LogQueue::DEFAULT).id));
        }
        self.rewrite_append_queue_tombstones().unwrap();
        if exit_after_step == Some(1) {
            return;
        }
        self.rewrite_memtables(self.memtables.collect(|_| true), 0, Some(&watermark))
            .unwrap();
        if exit_after_step == Some(2) {
            return;
        }
        self.rescan_memtables_and_purge_append_files(watermark)
            .unwrap();
    }

    #[cfg(test)]
    pub fn must_rewrite_rewrite_queue(&self) {
        let _lk = self.force_rewrite_candidates.try_lock().unwrap();
        self.rewrite_rewrite_queue().unwrap();
    }

    pub(crate) fn needs_rewrite_log_files(&self, queue: LogQueue) -> bool {
        let (first_file, active_file) = self.pipe_log.file_span(queue);
        if active_file == first_file {
            return false;
        }

        let total_size = self.pipe_log.total_size(queue);
        match queue {
            LogQueue::REWRITE => {
                let compacted_rewrites_ratio = self.global_stats.deleted_rewrite_entries() as f64
                    / self.global_stats.rewrite_entries() as f64;
                total_size > self.cfg.purge_rewrite_threshold.unwrap().0 as usize
                    && compacted_rewrites_ratio > self.cfg.purge_rewrite_garbage_ratio
            }
            _ => total_size > self.cfg.purge_threshold.0 as usize,
        }
    }

    // Returns (compact_watermark, rewrite_watermark).
    // Files in (-inf, compact_watermark] should be compacted;
    // Files in (compact_watermark, rewrite_watermark] should be rewritten.
    fn append_queue_watermarks(&self) -> (Option<FilesView>, Option<FilesView>) {
        (
            self.pipe_log.history_files_view(FORCE_COMPACT_RATIO),
            self.pipe_log.history_files_view(REWRITE_RATIO),
        )
    }

    fn rewrite_or_compact_append_queue(
        &self,
        compact_watermark: &FilesView,
        rewrite_watermark: &FilesView,
        rewrite_candidates: &mut HashMap<u64, u32>,
    ) -> Result<Vec<u64>> {
        let _t = StopWatch::new(&*ENGINE_REWRITE_APPEND_DURATION_HISTOGRAM);
        let mut should_compact = Vec::with_capacity(16);

        let mut new_candidates = HashMap::with_capacity(rewrite_candidates.len());
        let memtables = self.memtables.collect(|t| {
            let old = t.has_at_least_some_entries_before(compact_watermark, 1);
            let heavy = t.has_at_least_some_entries_before(
                rewrite_watermark,
                MAX_REWRITE_ENTRIES_PER_REGION,
            );
            // counter is the times that target region triggers force compact.
            let compact_counter = rewrite_candidates.get(&t.region_id()).unwrap_or(&0);
            if old && heavy {
                if *compact_counter < MAX_COUNT_BEFORE_FORCE_REWRITE {
                    should_compact.push(t.region_id());
                    new_candidates.insert(t.region_id(), *compact_counter + 1);
                    return false;
                } else {
                    return true;
                }
            }
            !heavy && t.has_at_least_some_entries_before(rewrite_watermark, 1)
        });

        self.rewrite_memtables(
            memtables,
            MAX_REWRITE_ENTRIES_PER_REGION,
            Some(rewrite_watermark),
        )?;
        *rewrite_candidates = new_candidates;

        Ok(should_compact)
    }

    // Rewrites the entire rewrite queue into new log files.
    fn rewrite_rewrite_queue(&self) -> Result<Vec<u64>> {
        let _t = StopWatch::new(&*ENGINE_REWRITE_REWRITE_DURATION_HISTOGRAM);
        let first_file = self.pipe_log.rotate(LogQueue::REWRITE)?;

        let mut force_compact_regions = vec![];
        let memtables = self.memtables.collect(|t| {
            let rewritten = t.rewritten_entries_count();
            // if the region is force rewritten, we should also trigger compact.
            if rewritten > MAX_REWRITE_ENTRIES_PER_REGION {
                force_compact_regions.push(t.region_id());
            }
            rewritten > 0
        });

        self.rewrite_memtables(memtables, 0 /* expect_rewrites_per_memtable */, None)?;
        self.global_stats.reset_rewrite_counters();
        self.purge_files(first_file)?;
        Ok(force_compact_regions)
    }

    fn rewrite_append_queue_tombstones(&self) -> Result<()> {
        let mut log_batch = self.memtables.take_cleaned_region_logs();
        self.rewrite_impl(
            &mut log_batch,
            None, /* rewrite_watermark */
            true, /* sync */
        )
    }

    fn purge_files(&self, id: FileId) -> Result<()> {
        let purged = self.pipe_log.purge_to(id)?;
        if purged > 0 {
            for listener in &self.listeners {
                listener.post_purge(id);
            }
        }
        Ok(())
    }

    fn rescan_memtables_and_purge_append_files(&self, mut view: FilesView) -> Result<()> {
        self.memtables.for_each(|memtable| {
            memtable.exclude_self_from_view(&mut view);
        });
        for mut id in view.distill() {
            id.seq += 1;
            self.purge_files(id)?;
        }
        Ok(())
    }

    fn rewrite_memtables(
        &self,
        memtables: Vec<MemTableHandle>,
        expect_rewrites_per_memtable: usize,
        rewrite: Option<&FilesView>,
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
                    m.fetch_entries_before(rewrite, &mut entry_indexes)?;
                    m.fetch_kvs_before(rewrite, &mut kvs);
                } else {
                    m.fetch_rewritten_entry_indexes(&mut entry_indexes)?;
                    m.fetch_rewritten_kvs(&mut kvs);
                }
                m.region_id()
            };

            // FIXME: This code makes my brain hurt.
            let mut cursor = 0;
            while cursor < entry_indexes.len() {
                let entry =
                    read_entry_bytes_from_file(self.pipe_log.as_ref(), &entry_indexes[cursor])?;
                total_size += entry.len();
                entries.push(entry);
                if total_size > MAX_REWRITE_BATCH_BYTES {
                    let mut take_entries = Vec::with_capacity(expect_rewrites_per_memtable);
                    std::mem::swap(&mut take_entries, &mut entries);
                    let mut take_entry_indexes = entry_indexes.split_off(cursor + 1);
                    std::mem::swap(&mut take_entry_indexes, &mut entry_indexes);
                    log_batch.add_raw_entries(region_id, take_entry_indexes, take_entries)?;
                    self.rewrite_impl(&mut log_batch, rewrite, false)?;
                    total_size = 0;
                    cursor = 0;
                } else {
                    cursor += 1;
                }
            }
            if !entries.is_empty() {
                log_batch.add_raw_entries(region_id, entry_indexes, entries)?;
            }
            for (k, v) in kvs {
                log_batch.put(region_id, k, v);
            }
        }
        self.rewrite_impl(&mut log_batch, rewrite, true)
    }

    fn rewrite_impl(
        &self,
        log_batch: &mut LogBatch,
        rewrite_watermark: Option<&FilesView>,
        sync: bool,
    ) -> Result<()> {
        let len = log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        if len == 0 {
            return self.pipe_log.maybe_sync(LogQueue::REWRITE, sync);
        }
        let file_context = self.pipe_log.fetch_active_file(LogQueue::REWRITE);
        log_batch.prepare_write(&file_context)?;
        let file_handle = self
            .pipe_log
            .append(LogQueue::REWRITE, log_batch.encoded_bytes())?;
        self.pipe_log.maybe_sync(LogQueue::REWRITE, sync)?;
        log_batch.finish_write(file_handle);
        self.memtables.apply_rewrite_writes(
            log_batch.drain(),
            rewrite_watermark,
            file_handle.id.seq,
        );
        for listener in &self.listeners {
            listener.post_apply_memtables(file_handle.id);
        }
        if rewrite_watermark.is_none() {
            BACKGROUND_REWRITE_BYTES
                .rewrite
                .observe(file_handle.len as f64);
        } else {
            BACKGROUND_REWRITE_BYTES
                .append
                .observe(file_handle.len as f64);
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
    channel_active_files: [RwLock<VecDeque<(FileSeq, AtomicUsize)>>; MAX_WRITE_CHANNELS],
}

impl PurgeHook {
    pub fn new() -> Self {
        PurgeHook {
            channel_active_files: Default::default(),
        }
    }
}

impl EventListener for PurgeHook {
    fn post_new_log_file(&self, file_id: FileId) {
        if file_id.queue == LogQueue::DEFAULT {
            let mut active_log_files =
                self.channel_active_files[file_id.queue.i() as usize].write();
            if let Some(seq) = active_log_files.back().map(|x| x.0) {
                assert_eq!(
                    seq + 1,
                    file_id.seq,
                    "active log files should be contiguous"
                );
            }
            let counter = AtomicUsize::new(0);
            active_log_files.push_back((file_id.seq, counter));
        }
    }

    fn on_append_log_file(&self, handle: FileBlockHandle) {
        if handle.id.queue == LogQueue::DEFAULT {
            let active_log_files = self.channel_active_files[handle.id.queue.i() as usize].read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(handle.id.seq - front) as usize].1;
            counter.fetch_add(1, Ordering::Release);
        }
    }

    fn post_apply_memtables(&self, file_id: FileId) {
        if file_id.queue == LogQueue::DEFAULT {
            let active_log_files = self.channel_active_files[file_id.queue.i() as usize].read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(file_id.seq - front) as usize].1;
            counter.fetch_sub(1, Ordering::Release);
        }
    }

    fn latest_files_view_allowed_to_purge(&self) -> Option<FilesView> {
        let mut view = FilesView::full_view();
        let queue = LogQueue::DEFAULT;
        let active_log_files = self.channel_active_files[queue.i() as usize].read();
        for (id, counter) in active_log_files.iter() {
            if counter.load(Ordering::Acquire) > 0 {
                view.update_to_exclude(&FileId::new(queue, *id));
                break;
            }
        }
        Some(view)
    }

    fn post_purge(&self, file_id: FileId) {
        if file_id.queue == LogQueue::DEFAULT {
            let mut active_log_files =
                self.channel_active_files[file_id.queue.i() as usize].write();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            if front < file_id.seq {
                let mut purged = active_log_files.drain(0..=(file_id.seq - front - 1) as usize);
                debug_assert_eq!(purged.next_back().unwrap().0, file_id.seq - 1);
            }
        }
    }
}
