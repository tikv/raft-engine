use core::panic;
use std::fmt::Debug;
use std::os::unix::io::RawFd;
use std::path::PathBuf;
use std::vec;

use log::warn;
use nix::fcntl;
use nix::fcntl::OFlag;
use nix::sys::stat::Mode;
use nix::NixPath;
use protobuf::Message;

use crate::codec::{self, NumberEncoder};
use crate::event_listener::EventListener;
use crate::log_batch::{Command, KeyValue, SliceReader};

use crate::pipe_log::{FileId, LogQueue};
use crate::util::{
    file_size, parse_nix_error, pread_exact, pwrite_exact, HashMap, Runnable, Scheduler,
};
use crate::Result;
use crate::{log_batch::LogItemContent, memtable::EntryIndex, EntryExt, LogBatch};

const HINT_SUFFIX: &str = ".hint";

pub struct HintManager<E, W>
where
    E: Message + Clone + 'static,
    W: EntryExt<E> + Clone + 'static,
{
    dir: String,
    scheduler: Scheduler<HintTask<E, W>>,
}

impl<E, W> HintManager<E, W>
where
    E: Message + Clone + 'static,
    W: EntryExt<E> + Clone + 'static,
{
    pub fn open(dir: String, scheduler: Scheduler<HintTask<E, W>>) -> Self {
        Self { dir, scheduler }
    }

    pub fn submit_log_batch(&self, queue: LogQueue, file_id: FileId, batch: LogBatch<E, W>) {
        self.submit(HintTask::Append {
            queue,
            file_id,
            batch,
        });
    }

    pub fn submit_flush(&self, dir: &str, queue: LogQueue, file_id: FileId) {
        self.submit(HintTask::Flush {
            queue,
            dir: dir.to_owned(),
            file_id,
        });
    }

    fn submit(&self, task: HintTask<E, W>) {
        if let Err(e) = self.scheduler.schedule(task) {
            panic!("hint manager task submitting error: {}", e);
        }
    }

    pub fn read_hint_file(&self, queue: LogQueue, file_id: FileId) -> Result<LogBatch<E, W>> {
        let mut path = PathBuf::from(&self.dir);
        path.push(generate_filename(queue, file_id));
        let fd = open_hint_file_r(&path)?;
        let size = file_size(fd)?;
        // TODO(MrCroxx): read header & version
        let buf = pread_exact(fd, 0, size)?;
        Ok(Self::read_bytes(queue, file_id, &mut &buf[..])?)
    }

    fn read_bytes(
        queue: LogQueue,
        file_id: FileId,
        buf: &mut SliceReader<'_>,
    ) -> Result<LogBatch<E, W>> {
        if buf.is_empty() {
            return Ok(LogBatch::default());
        }
        let mut batch: LogBatch<E, W> = LogBatch::default();
        while !buf.is_empty() {
            match HintItem::from_bytes(buf, queue, file_id)? {
                HintItem::EntriesIndex(raft_group_id, entries_index) => {
                    batch.add_raw_entries_index(raft_group_id, entries_index)
                }
                HintItem::Command(raft_group_id, command) => {
                    batch.add_raw_command(raft_group_id, command)
                }
                HintItem::KV(raft_group_id, kv) => batch.add_raw_kv(raft_group_id, kv),
            }
        }
        Ok(batch)
    }
}

impl<E, W> EventListener for HintManager<E, W>
where
    E: Message + Clone + 'static,
    W: EntryExt<E> + Clone + 'static,
{
    fn post_log_file_frozen(&self, queue: LogQueue, file_id: FileId) {
        self.submit_flush(&self.dir, queue, file_id)
    }
}

enum HintItem {
    EntriesIndex(u64, Vec<EntryIndex>),
    KV(u64, KeyValue),
    Command(u64, Command),
}

impl HintItem {
    pub fn encode_to(&self, buf: &mut Vec<u8>) -> Result<()> {
        // format = { item type | raft_group_id | <content> }
        match self {
            HintItem::EntriesIndex(raft_group_id, entries_index) => {
                buf.encode_var_u64(1)?;
                buf.encode_u64(*raft_group_id)?;
                buf.encode_var_u64(entries_index.len() as u64)?;
                entries_index.iter().for_each(|entry_index| {
                    entry_index.encode_to_bytes(buf);
                });
            }
            HintItem::KV(raft_group_id, kv) => {
                buf.encode_var_u64(2)?;
                buf.encode_u64(*raft_group_id)?;
                kv.encode_to(buf)?;
            }
            HintItem::Command(raft_group_id, command) => {
                buf.encode_var_u64(3)?;
                buf.encode_u64(*raft_group_id)?;
                command.encode_to(buf);
            }
        }
        Ok(())
    }

    pub fn from_bytes(buf: &mut SliceReader<'_>, queue: LogQueue, file_id: FileId) -> Result<Self> {
        let item = codec::decode_var_u64(buf)?;
        let raft_group_id = codec::decode_u64(buf)?;
        match item {
            1 => {
                let entry_index_count = codec::decode_var_u64(buf)?;
                let mut entries_index = Vec::with_capacity(entry_index_count as usize);
                for _ in 0..entry_index_count {
                    entries_index.push(EntryIndex::from_bytes(buf, file_id, queue)?);
                }
                Ok(HintItem::EntriesIndex(raft_group_id, entries_index))
            }
            2 => {
                let kv = KeyValue::from_bytes(buf)?;
                Ok(HintItem::KV(raft_group_id, kv))
            }
            3 => {
                let command = Command::from_bytes(buf)?;
                Ok(HintItem::Command(raft_group_id, command))
            }
            _ => Err(box_err!("unrecognized hint item".to_owned())),
        }
    }
}

pub struct Hint {
    items: Vec<HintItem>,
}

impl Hint {
    pub fn new() -> Self {
        Self { items: vec![] }
    }

    /// append all entry indexes in a log batch
    pub fn append<E: Message, W: EntryExt<E>>(&mut self, batch: &LogBatch<E, W>) {
        self.items
            .extend(batch.items.iter().map(|item| match &item.content {
                LogItemContent::Entries(entries) => {
                    HintItem::EntriesIndex(item.raft_group_id, entries.entries_index.to_owned())
                }
                LogItemContent::Command(command) => {
                    HintItem::Command(item.raft_group_id, command.to_owned())
                }
                LogItemContent::Kv(kv) => HintItem::KV(item.raft_group_id, kv.to_owned()),
            }));
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        // layout = { [item] }
        let mut buf: Vec<u8> = Vec::with_capacity(1024);
        for item in self.items.iter() {
            if let Err(e) = item.encode_to(&mut buf) {
                return Err(e);
            }
        }
        Ok(buf)
    }
}

#[derive(Clone, Debug)]
pub enum HintTask<E, W>
where
    E: Message + 'static,
    W: EntryExt<E> + 'static,
{
    Append {
        queue: LogQueue,
        file_id: FileId,
        batch: LogBatch<E, W>,
    },
    Flush {
        dir: String,
        queue: LogQueue,
        file_id: FileId,
    },
}

pub struct Runner {
    hints: HashMap<(LogQueue, FileId), Hint>,
}

impl Runner {
    pub fn new() -> Self {
        Self {
            hints: HashMap::default(),
        }
    }

    fn append_log_batch<E: Message, W: EntryExt<E>>(
        &mut self,
        queue: LogQueue,
        file_id: FileId,
        batch: LogBatch<E, W>,
    ) {
        self.hints
            .entry((queue, file_id))
            .or_insert(Hint::new())
            .append(&batch);
    }

    fn flush(&mut self, dir: String, queue: LogQueue, file_id: FileId) -> Result<()> {
        match self.hints.remove(&(queue, file_id)) {
            Some(hint) => {
                let mut path = PathBuf::from(&dir);
                path.push(generate_filename(queue, file_id));
                let buf = hint.to_bytes()?;
                let fd = open_hint_file_w(&path)?;
                // TODO(MrCroxx): write header & version
                pwrite_exact(fd, 0, &buf)?;
            }
            None => {
                warn!("hint {:?}:{:?} buffer not found", queue, file_id);
            }
        }
        Ok(())
    }
}

impl<E, W> Runnable<HintTask<E, W>> for Runner
where
    E: Message + Clone,
    W: EntryExt<E> + Clone,
{
    fn run(&mut self, task: HintTask<E, W>) -> bool {
        match task {
            HintTask::Append {
                queue,
                file_id,
                batch,
            } => self.append_log_batch(queue, file_id, batch),
            HintTask::Flush {
                dir,
                queue,
                file_id,
            } => {
                if let Err(e) = self.flush(dir, queue, file_id) {
                    warn!("flush hint file error: {}", e);
                }
            }
        }
        true
    }
    fn on_tick(&mut self) {
        // todo!()
    }
}

fn open_hint_file_w<P: ?Sized + NixPath>(path: &P) -> Result<RawFd> {
    let flags = OFlag::O_RDWR | OFlag::O_CREAT;
    let mode = Mode::S_IRUSR | Mode::S_IWUSR;
    fcntl::open(path, flags, mode).map_err(|e| parse_nix_error(e, "open_hint_file_w"))
}

fn open_hint_file_r<P: ?Sized + NixPath>(path: &P) -> Result<RawFd> {
    let flags = OFlag::O_RDONLY;
    let mode = Mode::S_IRWXU;
    fcntl::open(path, flags, mode).map_err(|e| parse_nix_error(e, "open_hint_file_r"))
}

fn generate_filename(queue: LogQueue, file_id: FileId) -> String {
    match queue {
        LogQueue::Append => format!("{:016}{}", file_id, HINT_SUFFIX),
        LogQueue::Rewrite => format!("{:08}{}", file_id, HINT_SUFFIX),
    }
}
