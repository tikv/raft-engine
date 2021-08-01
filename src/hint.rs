use core::panic;
use std::vec;

use log::warn;
use protobuf::Message;

use crate::codec::NumberEncoder;
use crate::pipe_log::FileId;
use crate::util::{HashMap, Runnable, Scheduler};
use crate::{log_batch::LogItemContent, memtable::EntryIndex, EntryExt, LogBatch};

pub struct HintManager<E, W>
where
    E: Message + Clone + 'static,
    W: EntryExt<E> + Clone + 'static,
{
    scheduler: Scheduler<HintTask<E, W>>,
}

impl<E, W> HintManager<E, W>
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
{
    pub fn new(scheduler: Scheduler<HintTask<E, W>>) -> Self {
        Self { scheduler }
    }

    pub fn submit_log_batch(&self, file_id: FileId, batch: LogBatch<E, W>) {
        self.submit(HintTask::Append(file_id, batch));
    }

    pub fn submit_flush(&self, file_id: FileId) {
        self.submit(HintTask::Flush(file_id));
    }

    fn submit(&self, task: HintTask<E, W>) {
        if let Err(e) = self.scheduler.schedule(task) {
            panic!("hint manager task submitting error: {}", e);
        }
    }
}

pub struct Hint {
    entries_index: Vec<(u64, Vec<EntryIndex>)>,
}

impl Hint {
    pub fn new() -> Self {
        Self {
            entries_index: vec![],
        }
    }

    /// append all entry indexes in a log batch
    pub fn append<E: Message, W: EntryExt<E>>(&mut self, batch: &LogBatch<E, W>) {
        self.entries_index
            .extend(batch.items.iter().filter_map(|item| match &item.content {
                LogItemContent::Command(_) | LogItemContent::Kv(_) => None,
                LogItemContent::Entries(entries) => {
                    Some((item.raft_group_id, entries.entries_index.to_owned()))
                }
            }));
    }

    /// encode Hint to bytes
    pub fn to_bytes(&self) -> Vec<u8> {
        // layout = { [ raft_group_id | EntryIndex ] }
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        self.entries_index
            .iter()
            .for_each(|(raft_group_id, entries_index)| {
                buf.encode_u64(*raft_group_id).unwrap();
                buf.encode_var_u64(entries_index.len() as u64).unwrap();
                entries_index.iter().for_each(|entry_index| {
                    entry_index.encode_to_bytes(&mut buf);
                });
            });

        buf
    }
}

#[derive(Clone, Debug)]
pub enum HintTask<E: Message + 'static, W: EntryExt<E> + 'static> {
    Append(FileId, LogBatch<E, W>),
    Flush(FileId),
}

pub struct Runner {
    hints: HashMap<FileId, Hint>,
}

impl Runner {
    pub fn new() -> Self {
        Self {
            hints: HashMap::default(),
        }
    }

    fn append_log_batch<E: Message, W: EntryExt<E>>(
        &mut self,
        file_id: FileId,
        batch: LogBatch<E, W>,
    ) {
        self.hints
            .entry(file_id)
            .or_insert(Hint::new())
            .append(&batch);
    }

    fn flush(&mut self, file_id: FileId) {
        if let Some(hint) = self.hints.remove(&file_id) {
            let buf = hint.to_bytes();
            // TODO(MrCroxx): write file
            // todo!()
        }
    }
}

impl<E, W> Runnable<HintTask<E, W>> for Runner
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
{
    fn run(&mut self, task: HintTask<E, W>) -> bool {
        match task {
            HintTask::Append(file_id, batch) => self.append_log_batch(file_id, batch),
            HintTask::Flush(file_id) => self.flush(file_id),
        }
        true
    }
    fn on_tick(&mut self) {
        // todo!()
    }
}
