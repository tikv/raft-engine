// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::{
    collections::{HashSet, VecDeque},
    sync::Arc,
};

use log::{debug, warn};
use rayon::prelude::*;

use crate::{
    engine::MemTableAccessor, file_system::Readable, log_batch::LogItemBatch, memtable::MemTable,
    reader::LogItemBatchFileReader, util::HashMap, FileId, GlobalStats, LogQueue, RecoveryMode,
    Result,
};

struct RecoverContext {
    file_id: FileId,
    file_size: usize,
    file_reader: Option<Box<dyn Readable>>,
}

impl std::fmt::Debug for RecoverContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RecoverContext")
            .field("file_id", &self.file_id)
            .field("file_size", &self.file_size)
            .finish()
    }
}

#[derive(Default)]
pub struct ParallelRecoverContext {
    pub removed_memtables: HashSet<u64>,
    pub compacted_memtables: HashMap<u64, u64>,
    pub removed_keys: HashSet<(u64, Vec<u8>)>,
}

/// ParallelRecoverContext only masks previous items, not current or future items.
// TODO(MrCroxx): unit tests. A: del(a) B: put(a,1) => A&B: del(a) rignt?
impl ParallelRecoverContext {
    pub fn merge(&mut self, right: &mut Self) {
        self.removed_memtables
            .extend(right.removed_memtables.drain());
        self.compacted_memtables
            .extend(right.compacted_memtables.drain());
        self.removed_keys.extend(right.removed_keys.drain());
    }
}

pub struct RecoverManager<F>
where
    F: Fn(&MemTableAccessor, LogQueue, FileId, LogItemBatch) + Sync,
{
    recovery_mode: RecoveryMode,
    concurrency: usize,
    read_block_size: usize,
    global_stats: Arc<GlobalStats>,
    append: Vec<RecoverContext>,
    rewrite: Vec<RecoverContext>,

    replay: F,
}

impl<F> RecoverManager<F>
where
    F: Fn(&MemTableAccessor, LogQueue, FileId, LogItemBatch) + Sync,
{
    pub fn new(
        recovery_mode: RecoveryMode,
        concurrency: usize,
        read_block_size: usize,
        global_stats: Arc<GlobalStats>,
        replay: F,
    ) -> Self {
        Self {
            recovery_mode,
            concurrency,
            read_block_size,
            global_stats,
            append: vec![],
            rewrite: vec![],
            replay,
        }
    }

    pub fn append(
        &mut self,
        queue: LogQueue,
        file_id: FileId,
        file_size: usize,
        file_reader: Box<dyn Readable>,
    ) {
        match queue {
            LogQueue::Append => &mut self.append,
            LogQueue::Rewrite => &mut self.rewrite,
        }
        .push(RecoverContext {
            file_id,
            file_size,
            file_reader: Some(file_reader),
        });
    }

    pub fn recover(&mut self) -> Result<MemTableAccessor> {
        struct RecoverQueueTask<'a, F>
        where
            F: Fn(&MemTableAccessor, LogQueue, FileId, LogItemBatch) + Sync,
        {
            recovery_mode: RecoveryMode,
            queue: LogQueue,
            concurrency: usize,
            read_block_size: usize,
            recover_contexts: Vec<RecoverContext>,
            global_stats: Arc<GlobalStats>,
            replay: &'a F,
        }

        let (append_concurrency, rewrite_concurrency) = self.assign_concurrency();

        let mut tasks = [
            RecoverQueueTask {
                recovery_mode: self.recovery_mode,
                queue: LogQueue::Append,
                concurrency: append_concurrency,
                read_block_size: self.read_block_size,
                recover_contexts: std::mem::take(&mut self.append),
                global_stats: self.global_stats.clone(),
                replay: &self.replay,
            },
            RecoverQueueTask {
                recovery_mode: self.recovery_mode,
                queue: LogQueue::Rewrite,
                concurrency: rewrite_concurrency,
                read_block_size: self.read_block_size,
                recover_contexts: std::mem::take(&mut self.rewrite),
                global_stats: self.global_stats.clone(),
                replay: &self.replay,
            },
        ];

        let mut memtable_accessors: VecDeque<Result<MemTableAccessor>> = tasks
            .par_iter_mut()
            .map(|task| {
                Self::recover_queue(
                    task.recovery_mode,
                    task.concurrency,
                    task.read_block_size,
                    task.queue,
                    std::mem::take(&mut task.recover_contexts),
                    task.global_stats.clone(),
                    &task.replay,
                )
            })
            .collect();

        let memtables_append = memtable_accessors.pop_front().unwrap()?;
        let mut memtables_rewrite = memtable_accessors.pop_front().unwrap()?;

        let ids = memtables_append.cleaned_region_ids();
        for (raft_group_id, memtable_rewrite) in memtables_rewrite.drain() {
            if let Some(memtable_append) = memtables_append.get(raft_group_id) {
                memtable_append
                    .write()
                    .merge_lower_prio(&mut memtable_rewrite.write());
            } else if !ids.contains(&raft_group_id) {
                memtables_append.insert(raft_group_id, memtable_rewrite);
            }
        }
        Ok(memtables_append)
    }

    fn assign_concurrency(&self) -> (usize, usize) {
        if self.append.is_empty() && self.rewrite.is_empty() {
            return (0, 0);
        }
        if self.append.is_empty() {
            return (0, self.concurrency);
        }
        if self.rewrite.is_empty() {
            return (self.concurrency, 0);
        }
        let append_concurrency = std::cmp::max(
            self.concurrency - 1,
            std::cmp::min(
                1,
                self.append.len() / (self.append.len() + self.rewrite.len()),
            ),
        );
        let rewrite_concurrency = self.concurrency - append_concurrency;
        (append_concurrency, rewrite_concurrency)
    }

    fn recover_queue(
        recovery_mode: RecoveryMode,
        concurrency: usize,
        read_block_size: usize,
        queue: LogQueue,
        mut recover_contexts: Vec<RecoverContext>,
        global_stats: Arc<GlobalStats>,
        replay: &F,
    ) -> Result<MemTableAccessor> {
        debug!(
            "Recover queue: {:?}, total:{}, concurrency: {}.",
            queue,
            recover_contexts.len(),
            concurrency
        );
        if concurrency == 0 {
            debug!("Recover queue:{:?} finish (nothing to recover).", queue);
            return Ok(Self::new_memtables_and_parallel_recover_context(global_stats).0);
        }
        let files_per_thread = std::cmp::max(1, recover_contexts.len() / concurrency);
        let chunks = recover_contexts.par_chunks_mut(files_per_thread);
        let chunk_chount = chunks.len();
        let (memtables, _) = chunks
            .enumerate()
            .map(|(index, chunk)| {
                debug!("recover {} files in one thread: {:?}", chunk.len(), chunk);
                Self::recover_queue_part(
                    read_block_size,
                    queue,
                    chunk,
                    global_stats.clone(),
                    recovery_mode == RecoveryMode::TolerateCorruptedTailRecords
                        && index == chunk_chount - 1,
                    replay,
                )
            })
            .try_reduce(
                || Self::new_memtables_and_parallel_recover_context(global_stats.clone()),
                |left, right| {
                    debug!("Reduce.");
                    Ok(Self::merge(queue, left.0, right.0, left.1, right.1))
                },
            )?;
        debug!("Recover queue:{:?} finish.", queue);
        Ok(memtables)
    }

    fn recover_queue_part(
        read_block_size: usize,
        queue: LogQueue,
        recover_contexts: &mut [RecoverContext],
        global_stats: Arc<GlobalStats>,
        tolerate_tail_failure: bool,
        replay: &F,
    ) -> Result<(MemTableAccessor, ParallelRecoverContext)> {
        debug!("Recover files: {:?}.", recover_contexts);
        let mut reader = LogItemBatchFileReader::new(read_block_size);
        let memtables = MemTableAccessor::new(Arc::new(move |id: u64| {
            MemTable::new(id, global_stats.clone())
        }));
        let mut parallel_recover_context = ParallelRecoverContext::default();
        let file_count = recover_contexts.len();
        for (i, recover_context) in recover_contexts.iter_mut().enumerate() {
            reader
                .open(
                    recover_context.file_reader.take().unwrap(),
                    recover_context.file_size,
                )
                .unwrap();
            loop {
                match reader.next() {
                    Ok(Some(mut item_batch)) => {
                        item_batch.set_position(queue, recover_context.file_id, None);
                        let mut context = item_batch.pick_parallel_recover_context();
                        parallel_recover_context.merge(&mut context);
                        replay(&memtables, queue, recover_context.file_id, item_batch);
                    }
                    Ok(None) => break,
                    Err(e) if tolerate_tail_failure && i == file_count - 1 => {
                        warn!("The tail of raft log is corrupted but ignored: {}", e);
                        break;
                    }
                    Err(e) => return Err(e),
                }
            }
        }
        debug!("Recover files: {:?} finish.", recover_contexts);
        Ok((memtables, parallel_recover_context))
    }

    fn new_memtables_and_parallel_recover_context(
        global_stats: Arc<GlobalStats>,
    ) -> (MemTableAccessor, ParallelRecoverContext) {
        let memtables = MemTableAccessor::new(Arc::new(move |id: u64| {
            MemTable::new(id, global_stats.clone())
        }));
        let parallel_recover_context = ParallelRecoverContext::default();
        (memtables, parallel_recover_context)
    }

    fn merge(
        queue: LogQueue,
        mut ml: MemTableAccessor,
        mut mr: MemTableAccessor,
        mut ctxl: ParallelRecoverContext,
        mut ctxr: ParallelRecoverContext,
    ) -> (MemTableAccessor, ParallelRecoverContext) {
        ml.mask(&ctxr, queue);
        ml.merge(&mut mr);
        ctxl.merge(&mut ctxr);
        (ml, ctxl)
    }
}
