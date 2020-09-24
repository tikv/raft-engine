use std::sync::{Arc, RwLock};
use std::sync::atomic::{Ordering, AtomicUsize};
use std::sync::mpsc::{Receiver, RecvError, TryRecvError};
use futures::channel::oneshot::Sender;

use crate::cache_evict::CacheSubmitor;
use crate::log_batch::{LogBatch, EntryExt, LogItem, LogItemContent};
use crate::engine::MemTableAccessor;
use crate::pipe_log::{LogQueue, GenericPipeLog};
use crate::errors::Result;
use protobuf::Message;
use log::{info, warn};

pub struct WriteTask {
    pub content: Vec<u8>,
    pub group_infos: Vec<(u64, u64)>,
    pub entries_size: usize,
    pub sync: bool,
    pub sender: Sender<(u64, u64, Option<Arc<AtomicUsize>>)>,
}

pub enum LogMsg {
    Write(WriteTask),
    Stop
}

pub struct WalRunner<E, W, P>
    where
        E: Message + Clone,
        W: EntryExt<E>,
        P: GenericPipeLog,
{
    cache_submitter: CacheSubmitor,
    pipe_log: P,
    receiver: Receiver<LogMsg>,
}

impl<E, W, P> WalRunner<E, W, P>
    where
        E: Message + Clone,
        W: EntryExt<E>,
        P: GenericPipeLog,
{
    pub fn new(
        cache_submitter: CacheSubmitor,
        pipe_log: P,
        receiver: Receiver<LogMsg>,
    ) -> WalRunner<E, W, P> {
        WalRunner {
            pipe_log,
            cache_submitter,
            receiver,
        }
    }
}


impl<E, W, P>  WalRunner<E, W, P>
    where
        E: Message + Clone,
        W: EntryExt<E>,
        P: GenericPipeLog,
{
    pub fn run(&mut self) -> Result<()>{
        let mut write_ret = vec![];
        let mut sync = false;
        let mut entries_size = 0;
        let mut run = false;
        while run {
            let mut task = match self.receiver.recv().unwrap() {
                LogMsg::Write(task) => task,
                LogMsg::Stop => return Ok(()),
            };
            sync = task.sync;
            let (file_num, fd) = self.pipe_log.switch_log_file(LogQueue::Append).unwrap()
            let offset = self.pipe_log.append(LogQueue::Append, &task.content).unwrap();
            write_ret.push((offset, task.sender));
            entries_size = task.entries_size;
            let tracker= self.cache_submitter.get_cache_tracker(file_num);
            self.cache_submitter.fill_cache(task.entries_size, &mut task.group_infos);
            while let Ok(msg) = self.receiver.try_recv() {
                let mut task = match msg {
                    LogMsg::Write(task) => task,
                    LogMsg::Stop => {
                        run = false;
                        break;
                    },
                };
                if task.sync {
                    sync = true;
                }
                self.cache_submitter.fill_cache(task.entries_size, &mut task.group_infos);
                let offset = self.pipe_log.append(LogQueue::Append, &task.content).unwrap();
                write_ret.push((offset, task.sender));
            }
            if sync {
                if let Err(e) = fd.sync() {
                    warn!("write wal failed because of: {} ", e);
                    write_ret.clear();
                }
                sync = false;
            }
            for (offset, sender) in write_ret.drain(..) {
                let _ = sender.send((file_num, offset, tracker.clone()));
            }
        }
        Ok(())
    }
}
