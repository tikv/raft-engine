use std::cmp::{self, Reverse};
use std::collections::BinaryHeap;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use log::{debug, info};
use protobuf::Message;

use crate::config::Config;
use crate::engine::{fetch_entries, MemTableAccessor};
use crate::log_batch::{Command, EntryExt, LogBatch, LogItemContent, OpType};
use crate::memtable::{EntryIndex, MemTable};
use crate::pipe_log::{GenericPipeLog, LogQueue, PipeLogHook};
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
    P: GenericPipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor<E, W>,
    pipe_log: P,
    global_stats: Arc<GlobalStats>,

    // Vector of (file_num, raft_group_id).
    #[allow(clippy::type_complexity)]
    removed_memtables: Arc<Mutex<BinaryHeap<(Reverse<u64>, u64)>>>,

    // Only one thread can run `purge_expired_files` at a time.
    purge_mutex: Arc<Mutex<()>>,
}

impl<E, W, P> PurgeManager<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
    P: GenericPipeLog,
{
    pub fn new(
        cfg: Arc<Config>,
        memtables: MemTableAccessor<E, W>,
        pipe_log: P,
        global_stats: Arc<GlobalStats>,
    ) -> PurgeManager<E, W, P> {
        PurgeManager {
            cfg,
            memtables,
            pipe_log,
            global_stats,
            removed_memtables: Default::default(),
            purge_mutex: Arc::new(Mutex::new(())),
        }
    }

    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        let _purge_mutex = match self.purge_mutex.try_lock() {
            Ok(context) => context,
            _ => return Ok(vec![]),
        };

        if !self.needs_purge_log_files(LogQueue::Append) {
            return Ok(vec![]);
        }

        let (rewrite_limit, compact_limit) = self.latest_inactive_file_num();
        if !self
            .pipe_log
            .hooks()
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
                rewrite_limit + 1,
                self.pipe_log.active_file_num(LogQueue::Rewrite),
            ),
            |(mut min1, mut min2), t| {
                min1 = cmp::min(min1, t.min_file_num(LogQueue::Append).unwrap_or(u64::MAX));
                min2 = cmp::min(min2, t.min_file_num(LogQueue::Rewrite).unwrap_or(u64::MAX));
                (min1, min2)
            },
        );
        assert!(min_file_1 > 0 && min_file_2 > 0);

        for (purge_to, queue) in &[
            (min_file_1, LogQueue::Append),
            (min_file_2, LogQueue::Rewrite),
        ] {
            let purged = self.pipe_log.purge_to(*queue, *purge_to)?;
            info!("purged {} expired log files for queue {:?}", purged, *queue);
            for hook in self.pipe_log.hooks() {
                hook.post_purge(*queue, *purge_to - 1);
            }
        }

        Ok(will_force_compact)
    }

    pub fn needs_purge_log_files(&self, queue: LogQueue) -> bool {
        let active_file_num = self.pipe_log.active_file_num(queue);
        let first_file_num = self.pipe_log.first_file_num(queue);
        if active_file_num == first_file_num {
            return false;
        }

        let total_size = self.pipe_log.total_size(queue);
        let purge_threshold = self.cfg.purge_threshold.0 as usize;
        match queue {
            LogQueue::Append => total_size > purge_threshold,
            LogQueue::Rewrite => total_size * 10 > purge_threshold,
        }
    }

    pub fn remove_memtable(&self, file_num: u64, raft_group_id: u64) {
        let mut tables = self.removed_memtables.lock().unwrap();
        tables.push((Reverse(file_num), raft_group_id));
    }

    // Returns (`latest_needs_rewrite`, `latest_needs_force_compact`).
    fn latest_inactive_file_num(&self) -> (u64, u64) {
        let queue = LogQueue::Append;

        let first_file_num = self.pipe_log.first_file_num(queue);
        let active_file_num = self.pipe_log.active_file_num(queue);
        if active_file_num == first_file_num {
            // Can't rewrite or force compact the active file.
            return (0, 0);
        }

        let total_size = self.pipe_log.total_size(queue) as f64;
        let rewrite_limit = (total_size * (1.0 - REWRITE_INACTIVE_RATIO)) as usize;
        let compact_limit = (total_size * (1.0 - FORCE_COMPACT_RATIO)) as usize;
        let mut latest_needs_rewrite = self.pipe_log.latest_file_before(queue, rewrite_limit);
        let mut latest_needs_compact = self.pipe_log.latest_file_before(queue, compact_limit);
        latest_needs_rewrite = cmp::min(latest_needs_rewrite, active_file_num - 1);
        latest_needs_compact = cmp::min(latest_needs_compact, active_file_num - 1);
        (latest_needs_rewrite, latest_needs_compact)
    }

    pub fn rewrite_queue_needs_squeeze(&self) -> bool {
        // Squeeze the rewrite queue if its garbage ratio reaches 50%.
        let rewrite_operations = self.global_stats.rewrite_operations();
        let compacted_rewrite_operations = self.global_stats.compacted_rewrite_operations();
        compacted_rewrite_operations as f64 / rewrite_operations as f64 > 0.5
    }

    fn regions_rewrite_or_force_compact(
        &self,
        latest_rewrite: u64,
        latest_compact: u64,
        will_force_compact: &mut Vec<u64>,
    ) {
        assert!(latest_compact <= latest_rewrite);

        let mut log_batch = LogBatch::<E, W>::default();
        let mut removed_memtables = self.removed_memtables.lock().unwrap();
        while let Some(item) = removed_memtables.pop() {
            let (file_num, raft_id) = ((item.0).0, item.1);
            if file_num > latest_rewrite {
                removed_memtables.push(item);
                break;
            }
            log_batch.clean_region(raft_id);
        }
        drop(removed_memtables);
        self.rewrite_impl(&mut log_batch, Some(latest_rewrite), true)
            .unwrap();

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
            |t: &MemTable<E, W>, ents, ents_idx| {
                t.fetch_rewrite_entries(latest_rewrite, ents, ents_idx)
            },
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
            .collect(|t| t.min_file_num(LogQueue::Rewrite).unwrap_or_default() > 0);

        self.rewrite_memtables(
            memtables,
            |t: &MemTable<E, W>, ents, ents_idx| t.fetch_entries_from_rewrite(ents, ents_idx),
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
        rewrite: Option<u64>,
    ) where
        FE: Fn(&MemTable<E, W>, &mut Vec<E>, &mut Vec<EntryIndex>) -> Result<()> + Copy,
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
        latest_rewrite: Option<u64>,
        sync: bool,
    ) -> Result<()> {
        if log_batch.is_empty() {
            if sync {
                self.pipe_log.sync(LogQueue::Rewrite)?;
            }
            return Ok(());
        }

        let mut file_num = 0;
        self.pipe_log.rewrite(log_batch, sync, &mut file_num)?;
        if file_num > 0 {
            self.rewrite_to_memtable(log_batch, file_num, latest_rewrite);
            for hook in self.pipe_log.hooks() {
                hook.post_apply_memtables(LogQueue::Rewrite, file_num);
            }
        }
        Ok(())
    }

    fn rewrite_to_memtable(
        &self,
        log_batch: &mut LogBatch<E, W>,
        file_num: u64,
        latest_rewrite: Option<u64>,
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
                        file_num,
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
                            file_num,
                            hex::encode(&key),
                        );
                        memtable.wl().rewrite_key(key, latest_rewrite, file_num);
                    }
                    _ => unreachable!(),
                },
                LogItemContent::Command(Command::Clean) => {
                    debug!("{} append to {:?}.{}, Clean", raft, queue, file_num);
                    // Nothing need to do.
                }
                _ => unreachable!(),
            }
        }
    }
}

#[derive(Default)]
pub struct PurgeHook {
    // There could be a gap between a write batch is written into a file and applied to memtables.
    // If a log-rewrite happens at the time, entries and kvs in the batch can be omited. So after
    // the log file gets purged, those entries and kvs will be lost.
    //
    // This field is used to forbid the bad case. Log files `active_log_files` can't be purged.
    // It's only used for the append queue. The rewrite queue doesn't need this.
    active_log_files: RwLock<VecDeque<(u64, AtomicUsize)>>,
}

impl PipeLogHook for PurgeHook {
    fn post_new_log_file(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.wl();
            if let Some(num) = active_log_files.back().map(|x| x.0) {
                assert_eq!(num + 1, file_num, "active log files should be contiguous");
            }
            let counter = AtomicUsize::new(0);
            active_log_files.push_back((file_num, counter));
        }
    }

    fn on_append_log_file(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(file_num - front) as usize].1;
            counter.fetch_add(1, Ordering::Release);
        }
    }

    fn post_apply_memtables(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            let counter = &active_log_files[(file_num - front) as usize].1;
            counter.fetch_sub(1, Ordering::Release);
        }
    }

    fn ready_for_purge(&self, queue: LogQueue, file_num: u64) -> bool {
        if queue == LogQueue::Append {
            let active_log_files = self.active_log_files.rl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            for (i, n) in (front..=file_num).enumerate() {
                assert_eq!(active_log_files[i].0, n);
                let counter = &active_log_files[i].1;
                if counter.load(Ordering::Acquire) > 0 {
                    return false;
                }
            }
        }
        true
    }

    fn post_purge(&self, queue: LogQueue, file_num: u64) {
        if queue == LogQueue::Append {
            let mut active_log_files = self.active_log_files.wl();
            assert!(!active_log_files.is_empty());
            let front = active_log_files[0].0;
            for x in front..=file_num {
                let (y, counter) = active_log_files.pop_front().unwrap();
                assert_eq!(x, y);
                assert_eq!(counter.load(Ordering::Acquire), 0);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::Engine;
    use crate::pipe_log::PipeLog;
    use raft::eraftpb::Entry;

    type RaftLogEngine = Engine<Entry, Entry, PipeLog>;

    #[test]
    fn test_remove_memtable() {
        let dir = tempfile::Builder::new()
            .prefix("test_remove_memtable")
            .tempdir()
            .unwrap();

        let mut cfg = Config::default();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        let engine = RaftLogEngine::new(cfg.clone());

        engine.purge_manager().remove_memtable(3, 10);
        engine.purge_manager().remove_memtable(3, 9);
        engine.purge_manager().remove_memtable(3, 11);
        engine.purge_manager().remove_memtable(2, 9);
        engine.purge_manager().remove_memtable(4, 4);
        engine.purge_manager().remove_memtable(4, 3);

        let mut tables = engine.purge_manager().removed_memtables.lock().unwrap();
        for (file_num, raft_id) in vec![(2, 9), (3, 11), (3, 10), (3, 9), (4, 4), (4, 3)] {
            let item = tables.pop().unwrap();
            assert_eq!((item.0).0, file_num);
            assert_eq!(item.1, raft_id);
        }
    }
}
