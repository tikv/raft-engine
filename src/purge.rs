use std::cmp::{self, Reverse};
use std::collections::BinaryHeap;
use std::sync::{Arc, Mutex};

use log::info;
use protobuf::Message;

use crate::config::Config;
use crate::engine::{fetch_entries, MemTableAccessor};
use crate::log_batch::{Command, EntryExt, LogBatch, LogItemContent, OpType};
use crate::pipe_log::{GenericPipeLog, LogQueue};
use crate::util::HandyRwLock;
use crate::Result;

// If a region has some very old raft logs less than this threshold,
// rewrite them to clean stale log files ASAP.
const REWRITE_ENTRY_COUNT_THRESHOLD: usize = 32;
const REWRITE_INACTIVE_RATIO: f64 = 0.7;
const FORCE_COMPACT_RATIO: f64 = 0.2;

#[derive(Clone)]
pub struct PurgeManager<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E>,
    P: GenericPipeLog,
{
    cfg: Arc<Config>,
    memtables: MemTableAccessor<E, W>,
    pipe_log: P,
    // Vector of (file_num, raft_group_id).
    removed_memtables: Arc<Mutex<BinaryHeap<(Reverse<u64>, u64)>>>,
}

impl<E, W, P> PurgeManager<E, W, P>
where
    E: Message + Clone,
    W: EntryExt<E>,
    P: GenericPipeLog,
{
    pub fn new(
        cfg: Arc<Config>,
        memtables: MemTableAccessor<E, W>,
        pipe_log: P,
    ) -> PurgeManager<E, W, P> {
        PurgeManager {
            cfg,
            memtables,
            pipe_log,
            removed_memtables: Default::default(),
        }
    }

    pub fn purge_expired_files(&self) -> Result<Vec<u64>> {
        if !self.needs_purge_log_files() {
            return Ok(vec![]);
        }

        let (rewrite_limit, compact_limit) = self.latest_inactive_file_num();
        let mut will_force_compact = Vec::new();
        self.regions_rewrite_or_force_compact(
            rewrite_limit,
            compact_limit,
            &mut will_force_compact,
        );

        let min_file_num = self.memtables.fold(u64::MAX, |min, t| {
            cmp::min(min, t.min_file_num(LogQueue::Append).unwrap_or(u64::MAX))
        });
        let purged = self.pipe_log.purge_to(LogQueue::Append, min_file_num)?;
        info!("purged {} expired log files", purged);

        Ok(will_force_compact)
    }

    pub fn needs_purge_log_files(&self) -> bool {
        let total_size = self.pipe_log.total_size(LogQueue::Append);
        let purge_threshold = self.cfg.purge_threshold.0 as usize;
        total_size > purge_threshold
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
        latest_needs_rewrite = cmp::min(latest_needs_rewrite, active_file_num);
        latest_needs_compact = cmp::min(latest_needs_compact, active_file_num);
        (latest_needs_rewrite, latest_needs_compact)
    }

    // FIXME: We need to ensure that all operations before `latest_rewrite` (included) are written
    // into memtables.
    fn regions_rewrite_or_force_compact(
        &self,
        latest_rewrite: u64,
        latest_compact: u64,
        will_force_compact: &mut Vec<u64>,
    ) {
        assert!(latest_compact <= latest_rewrite);
        let mut log_batch = LogBatch::<E, W>::new();

        while let Some(item) = self.removed_memtables.lock().unwrap().pop() {
            let (file_num, raft_id) = ((item.0).0, item.1);
            if file_num > latest_rewrite {
                self.removed_memtables.lock().unwrap().push(item);
                break;
            }
            log_batch.clean_region(raft_id);
        }

        let memtables = self.memtables.collect(|t| {
            let min_file_num = t.min_file_num(LogQueue::Append).unwrap_or(u64::MAX);
            let count = t.entries_count();
            if min_file_num <= latest_compact && count > REWRITE_ENTRY_COUNT_THRESHOLD {
                will_force_compact.push(t.region_id());
                return false;
            }
            min_file_num <= latest_rewrite && count <= REWRITE_ENTRY_COUNT_THRESHOLD
        });

        for memtable in memtables {
            let region_id = memtable.rl().region_id();

            let entries_count = memtable.rl().entries_count();
            let mut entries = Vec::with_capacity(entries_count);
            fetch_entries(
                &self.pipe_log,
                &memtable,
                &mut entries,
                entries_count,
                |t, ents, ents_idx| t.fetch_rewrite_entries(latest_rewrite, ents, ents_idx),
            )
            .unwrap();
            log_batch.add_entries(region_id, entries);

            let mut kvs = Vec::new();
            memtable.rl().fetch_rewrite_kvs(latest_rewrite, &mut kvs);
            for (k, v) in kvs {
                log_batch.put(region_id, k, v);
            }
        }
        self.rewrite_impl(&mut log_batch, latest_rewrite).unwrap();
    }

    fn rewrite_impl(&self, log_batch: &mut LogBatch<E, W>, latest_rewrite: u64) -> Result<()> {
        let mut file_num = 0;
        self.pipe_log.rewrite(&log_batch, true, &mut file_num)?;
        if file_num > 0 {
            self.rewrite_to_memtable(log_batch, file_num, latest_rewrite);
        }
        Ok(())
    }

    fn rewrite_to_memtable(
        &self,
        log_batch: &mut LogBatch<E, W>,
        file_num: u64,
        latest_rewrite: u64,
    ) {
        for item in log_batch.items.drain(..) {
            let memtable = self.memtables.get_or_insert(item.raft_group_id);
            match item.content {
                LogItemContent::Entries(entries_to_add) => {
                    let entries_index = entries_to_add.entries_index.into_inner();
                    memtable.wl().rewrite(entries_index, latest_rewrite);
                }
                LogItemContent::Kv(kv) => match kv.op_type {
                    OpType::Put => memtable.wl().rewrite_key(kv.key, latest_rewrite, file_num),
                    _ => unreachable!(),
                },
                LogItemContent::Command(Command::Clean) => {
                    // Nothing need to do.
                }
                _ => unreachable!(),
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
