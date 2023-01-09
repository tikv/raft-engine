// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use fail::fail_point;
use log::{info, warn};
use parking_lot::{Mutex, RwLock};

use crate::codec::{self, NumberEncoder};
use crate::config::Config;
use crate::engine::read_entry_bytes_from_file;
use crate::event_listener::EventListener;
use crate::log_batch::LogBatch;
use crate::memtable::{MemTableHandle, MemTables};
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue, PipeLog, PipeSnapshot};
use crate::{GlobalStats, Result};

// Force compact region with oldest 20% logs.
const FORCE_COMPACT_RATIO: f64 = 0.2;
// Only rewrite region with oldest 70% logs.
const REWRITE_RATIO: f64 = 0.7;
// Only rewrite region with stale logs less than this threshold.
const MAX_REWRITE_ENTRIES_PER_REGION: usize = 32;
const MAX_COUNT_BEFORE_FORCE_REWRITE: u32 = 9;
const REWRITE_MARKER: &str = "REWRITE_MARKER";

fn max_rewrite_batch_bytes() -> usize {
    fail_point!("max_rewrite_batch_bytes", |s| s
        .unwrap()
        .parse::<usize>()
        .unwrap());
    128 * 1024
}

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

    // The rewrite of the entire rewrite queue of a Raft Group should be atomic.
    // We use a special marker to indicate such a rewrite is on-going. Before recovery, rewrite
    // files that are newer than this marker (have bigger seqno) will be removed.
    rewrite_marker_path: PathBuf,
}

fn write_marker(p: &Path, snapshot: PipeSnapshot) -> Result<()> {
    use std::io::Write;
    let mut f = fs::File::create(p)?;
    let mut buf = Vec::new();
    buf.encode_u64(snapshot.file_id.seq)?;
    buf.encode_u64(snapshot.offset as u64)?;
    f.write_all(&buf)?;
    f.sync_all()?;
    f.write_all(&[0u8])?;
    f.sync_all()?;
    if let Some(dir) = p.parent() {
        fs::File::open(dir).and_then(|d| d.sync_all())?;
    }
    Ok(())
}

fn read_marker(p: &Path) -> Result<Option<PipeSnapshot>> {
    match fs::read(p) {
        Err(e) if e.kind() != std::io::ErrorKind::NotFound => Err(e.into()),
        Ok(v) if v.last() == Some(&0) => {
            let buf = &mut &v[..v.len() - 1];
            let seq = codec::decode_u64(buf).unwrap();
            let offset = codec::decode_u64(buf).unwrap() as usize;
            Ok(Some(PipeSnapshot {
                file_id: FileId::new(LogQueue::Rewrite, seq),
                offset,
            }))
        }
        _ => Ok(None),
    }
}

fn remove_marker(p: &Path) -> Result<()> {
    fs::remove_file(p)?;
    if let Some(dir) = p.parent() {
        fs::File::open(dir).and_then(|d| d.sync_all())?;
    }
    Ok(())
}

/// There're two types of partial rewrite:
/// 1. Partial rewrite-rewrite: See `test_partial_rewrite_rewrite`. This type
/// will cause data loss if the partial rewrite entries are merged with some
/// append entries. We handle this case by cleaning up before engine recovery.
/// 2. Partial rewrite-append: See `test_partial_rewrite_append`. This type only
/// causes data loss if there exists partial output from multiple rewrite
/// operations. We handle this case by checking marker before doing rewrite.
pub fn before_recover(cfg: &Config, fs: &impl crate::env::FileSystem) -> Result<()> {
    use crate::file_pipe_log::FileNameExt;
    let mut rewrite_marker_path = PathBuf::from(&cfg.dir);
    rewrite_marker_path.push(REWRITE_MARKER);
    if let Some(snap) = read_marker(&rewrite_marker_path)? {
        let mut f = FileId::new(LogQueue::Rewrite, snap.file_id.seq + 1);
        let mut paths = Vec::new();
        loop {
            let path = f.build_file_path(&cfg.dir);
            if path.exists() {
                paths.push(path);
            } else {
                break;
            }
            f.seq += 1;
        }
        // Avoid leaving holes.
        for f in paths.iter().rev() {
            info!(
                "cleaning up partial rewrite-rewrite output: {}",
                f.display()
            );
            fs.delete(f)?;
        }
    }
    Ok(())
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
        let mut rewrite_marker_path = PathBuf::from(&cfg.dir);
        rewrite_marker_path.push(REWRITE_MARKER);
        PurgeManager {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            listeners,
            force_rewrite_candidates: Arc::new(Mutex::new(HashMap::default())),
            rewrite_marker_path,
        }
    }

    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let _t = StopWatch::new(&*ENGINE_PURGE_DURATION_HISTOGRAM);
        let guard = self.force_rewrite_candidates.try_lock();
        if guard.is_none() {
            warn!("Unable to purge expired files: locked");
            return Ok(vec![]);
        }
        if self.rewrite_marker_path.exists() {
            // Otherwise all writes will be erased next reboot.
            if let Some(snap) = read_marker(&self.rewrite_marker_path)? {
                self.pipe_log.restore(snap)?;
            }
            remove_marker(&self.rewrite_marker_path)?;
        }
        let mut marker_written = false;

        let mut rewrite_candidate_regions = guard.unwrap();
        let mut should_compact = HashSet::new();
        if self.needs_rewrite_log_files(LogQueue::Rewrite) {
            write_marker(
                &self.rewrite_marker_path,
                self.pipe_log.snapshot(LogQueue::Rewrite),
            )?;
            marker_written = true;

            should_compact.extend(self.rewrite_rewrite_queue()?);
            self.rescan_memtables_and_purge_stale_files(
                LogQueue::Rewrite,
                self.pipe_log.file_span(LogQueue::Rewrite).1,
            )?;
        }

        if self.needs_rewrite_log_files(LogQueue::Append) {
            if let (Some(rewrite_watermark), Some(compact_watermark)) =
                self.append_queue_watermarks()
            {
                if !marker_written {
                    write_marker(
                        &self.rewrite_marker_path,
                        self.pipe_log.snapshot(LogQueue::Rewrite),
                    )?;
                    marker_written = true;
                }
                let (first_append, latest_append) = self.pipe_log.file_span(LogQueue::Append);
                let append_queue_barrier =
                    self.listeners.iter().fold(latest_append, |barrier, l| {
                        l.first_file_not_ready_for_purge(LogQueue::Append)
                            .map_or(barrier, |f| std::cmp::min(f, barrier))
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
                    rewrite_watermark,
                    compact_watermark,
                    &mut rewrite_candidate_regions,
                )?);

                if append_queue_barrier == first_append && first_append < latest_append {
                    warn!("Unable to purge expired files: blocked by barrier");
                }
                self.rescan_memtables_and_purge_stale_files(
                    LogQueue::Append,
                    append_queue_barrier,
                )?;
            }
        }

        if marker_written {
            remove_marker(&self.rewrite_marker_path)?;
        }

        Ok(should_compact.into_iter().collect())
    }

    /// Rewrite append files with seqno no larger than `watermark`. When it's
    /// None, rewrite the entire queue. Returns the number of purged files.
    pub fn must_rewrite_append_queue(
        &self,
        watermark: Option<FileSeq>,
        exit_after_step: Option<u64>,
    ) {
        let _lk = self.force_rewrite_candidates.try_lock().unwrap();
        write_marker(
            &self.rewrite_marker_path,
            self.pipe_log.snapshot(LogQueue::Rewrite),
        )
        .unwrap();
        let (_, last) = self.pipe_log.file_span(LogQueue::Append);
        let watermark = watermark.map_or(last, |w| std::cmp::min(w, last));
        if watermark == last {
            self.pipe_log.rotate(LogQueue::Append).unwrap();
        }
        self.rewrite_append_queue_tombstones().unwrap();
        if exit_after_step == Some(1) {
            return;
        }
        self.rewrite_memtables(self.memtables.collect(|_| true), 0, Some(watermark))
            .unwrap();
        if exit_after_step == Some(2) {
            return;
        }
        self.rescan_memtables_and_purge_stale_files(
            LogQueue::Append,
            self.pipe_log.file_span(LogQueue::Append).1,
        )
        .unwrap();
        remove_marker(&self.rewrite_marker_path).unwrap();
    }

    pub fn must_rewrite_rewrite_queue(&self) {
        let _lk = self.force_rewrite_candidates.try_lock().unwrap();
        write_marker(
            &self.rewrite_marker_path,
            self.pipe_log.snapshot(LogQueue::Rewrite),
        )
        .unwrap();
        self.rewrite_rewrite_queue().unwrap();
        self.rescan_memtables_and_purge_stale_files(
            LogQueue::Rewrite,
            self.pipe_log.file_span(LogQueue::Rewrite).1,
        )
        .unwrap();
        remove_marker(&self.rewrite_marker_path).unwrap();
    }

    pub(crate) fn needs_rewrite_log_files(&self, queue: LogQueue) -> bool {
        let (first_file, active_file) = self.pipe_log.file_span(queue);
        if active_file == first_file {
            return false;
        }

        let total_size = self.pipe_log.total_size(queue);
        match queue {
            LogQueue::Append => total_size > self.cfg.purge_threshold.0 as usize,
            LogQueue::Rewrite => {
                let compacted_rewrites_ratio = self.global_stats.deleted_rewrite_entries() as f64
                    / self.global_stats.rewrite_entries() as f64;
                total_size > self.cfg.purge_rewrite_threshold.unwrap().0 as usize
                    && compacted_rewrites_ratio > self.cfg.purge_rewrite_garbage_ratio
            }
        }
    }

    // Returns (rewrite_watermark, compact_watermark).
    // Files older than compact_watermark should be compacted;
    // Files between compact_watermark and rewrite_watermark should be rewritten.
    fn append_queue_watermarks(&self) -> (Option<FileSeq>, Option<FileSeq>) {
        let queue = LogQueue::Append;

        let (first_file, active_file) = self.pipe_log.file_span(queue);
        if active_file == first_file {
            // Can't rewrite or force compact the active file.
            return (None, None);
        }

        let rewrite_watermark = self.pipe_log.file_at(queue, REWRITE_RATIO);
        let compact_watermark = self.pipe_log.file_at(queue, FORCE_COMPACT_RATIO);
        debug_assert!(active_file - 1 > 0);
        (
            Some(std::cmp::min(rewrite_watermark, active_file - 1)),
            Some(std::cmp::min(compact_watermark, active_file - 1)),
        )
    }

    fn rewrite_or_compact_append_queue(
        &self,
        rewrite_watermark: FileSeq,
        compact_watermark: FileSeq,
        rewrite_candidates: &mut HashMap<u64, u32>,
    ) -> Result<Vec<u64>> {
        let _t = StopWatch::new(&*ENGINE_REWRITE_APPEND_DURATION_HISTOGRAM);
        debug_assert!(compact_watermark <= rewrite_watermark);
        let mut should_compact = Vec::with_capacity(16);

        let mut new_candidates = HashMap::with_capacity(rewrite_candidates.len());
        let memtables = self.memtables.collect(|t| {
            let min_append_seq = t.min_file_seq(LogQueue::Append).unwrap_or(u64::MAX);
            let old = min_append_seq < compact_watermark || t.rewrite_count() > 0;
            let has_something_to_rewrite = min_append_seq <= rewrite_watermark;
            let append_heavy = t.has_at_least_some_entries_before(
                FileId::new(LogQueue::Append, rewrite_watermark),
                MAX_REWRITE_ENTRIES_PER_REGION + t.rewrite_count(),
            );
            let full_heavy = t.has_at_least_some_entries_before(
                FileId::new(LogQueue::Append, rewrite_watermark),
                MAX_REWRITE_ENTRIES_PER_REGION,
            );
            // counter is the times that target region triggers force compact.
            let compact_counter = rewrite_candidates.get(&t.region_id()).unwrap_or(&0);
            if old && full_heavy {
                if *compact_counter < MAX_COUNT_BEFORE_FORCE_REWRITE {
                    // repeatedly ask user to compact these heavy regions.
                    should_compact.push(t.region_id());
                    new_candidates.insert(t.region_id(), *compact_counter + 1);
                    return false;
                } else {
                    // user is not responsive, do the rewrite ourselves.
                    should_compact.push(t.region_id());
                    return has_something_to_rewrite;
                }
            }
            !append_heavy && has_something_to_rewrite
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
        self.pipe_log.rotate(LogQueue::Rewrite)?;

        let mut force_compact_regions = vec![];
        let memtables = self.memtables.collect(|t| {
            // if the region is force rewritten, we should also trigger compact.
            if t.rewrite_count() > MAX_REWRITE_ENTRIES_PER_REGION {
                force_compact_regions.push(t.region_id());
            }
            t.min_file_seq(LogQueue::Rewrite).is_some()
        });

        self.rewrite_memtables(memtables, 0 /* expect_rewrites_per_memtable */, None)?;
        self.global_stats.reset_rewrite_counters();
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

    // Exclusive.
    fn rescan_memtables_and_purge_stale_files(&self, queue: LogQueue, seq: FileSeq) -> Result<()> {
        let min_seq = self.memtables.fold(seq, |min, t| {
            t.min_file_seq(queue).map_or(min, |m| std::cmp::min(min, m))
        });

        let purged = self.pipe_log.purge_to(FileId {
            queue,
            seq: min_seq,
        })?;
        if purged > 0 {
            info!("purged {} expired log files for queue {:?}", purged, queue);
            for listener in &self.listeners {
                listener.post_purge(FileId {
                    queue,
                    seq: min_seq - 1,
                });
            }
        }
        Ok(())
    }

    fn rewrite_memtables(
        &self,
        memtables: Vec<MemTableHandle>,
        expect_rewrites_per_memtable: usize,
        rewrite: Option<FileSeq>,
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

            // FIXME: This code makes my brain hurt.
            let mut cursor = 0;
            while cursor < entry_indexes.len() {
                let entry =
                    read_entry_bytes_from_file(self.pipe_log.as_ref(), &entry_indexes[cursor])?;
                total_size += entry.len();
                entries.push(entry);
                if total_size > max_rewrite_batch_bytes() {
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
        rewrite_watermark: Option<FileSeq>,
        sync: bool,
    ) -> Result<()> {
        if log_batch.is_empty() {
            debug_assert!(sync);
            return self.pipe_log.sync(LogQueue::Rewrite);
        }
        log_batch.finish_populate(self.cfg.batch_compression_threshold.0 as usize)?;
        let file_handle = self.pipe_log.append(LogQueue::Rewrite, log_batch)?;
        if sync {
            self.pipe_log.sync(LogQueue::Rewrite)?
        }
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

#[derive(Default)]
pub struct PurgeHook {
    // Append queue log files that are not yet fully applied to MemTable must not be
    // purged even when not referenced by any MemTable.
    // In order to identify them, maintain a per-file reference counter for all active
    // log files in append queue. No need to track rewrite queue because it is only
    // written by purge thread.
    active_log_files: RwLock<VecDeque<(FileSeq, AtomicUsize)>>,
}

impl EventListener for PurgeHook {
    fn post_new_log_file(&self, file_id: FileId) {
        if file_id.queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.write();
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
        if handle.id.queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(handle.id.seq - front) as usize].1;
            counter.fetch_add(1, Ordering::Release);
        }
    }

    fn post_apply_memtables(&self, file_id: FileId) {
        if file_id.queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(file_id.seq - front) as usize].1;
            counter.fetch_sub(1, Ordering::Release);
        }
    }

    fn first_file_not_ready_for_purge(&self, queue: LogQueue) -> Option<FileSeq> {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.read();
            for (id, counter) in active_log_files.iter() {
                if counter.load(Ordering::Acquire) > 0 {
                    return Some(*id);
                }
            }
        }
        None
    }

    fn post_purge(&self, file_id: FileId) {
        if file_id.queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.write();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            if front <= file_id.seq {
                let mut purged = active_log_files.drain(0..=(file_id.seq - front) as usize);
                assert_eq!(purged.next_back().unwrap().0, file_id.seq);
            }
        }
    }
}
