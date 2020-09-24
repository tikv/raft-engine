use futures::channel::oneshot::Sender;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::Receiver;
use std::sync::Arc;

use crate::cache_evict::CacheSubmitor;
use crate::errors::Result;
use crate::pipe_log::{GenericPipeLog, LogQueue};
use log::warn;

pub struct WriteTask {
    pub content: Vec<u8>,
    pub group_infos: Vec<(u64, u64)>,
    pub entries_size: usize,
    pub sync: bool,
    pub sender: Sender<(u64, u64, Option<Arc<AtomicUsize>>)>,
}

pub enum LogMsg {
    Write(WriteTask),
    Stop,
}

pub struct WalRunner<P: GenericPipeLog> {
    cache_submitter: CacheSubmitor,
    pipe_log: P,
    receiver: Receiver<LogMsg>,
}

impl<P: GenericPipeLog> WalRunner<P> {
    pub fn new(
        cache_submitter: CacheSubmitor,
        pipe_log: P,
        receiver: Receiver<LogMsg>,
    ) -> WalRunner<P> {
        WalRunner {
            pipe_log,
            cache_submitter,
            receiver,
        }
    }
}

impl<P> WalRunner<P>
where
    P: GenericPipeLog,
{
    pub fn run(&mut self) -> Result<()> {
        let mut write_ret = vec![];
        while let Ok(LogMsg::Write(mut task)) = self.receiver.recv() {
            let mut sync = task.sync;
            let (file_num, fd) = self.pipe_log.switch_log_file(LogQueue::Append).unwrap();
            let offset = self
                .pipe_log
                .append(LogQueue::Append, &task.content)
                .unwrap();
            write_ret.push((offset, task.sender));
            let tracker = self.cache_submitter.get_cache_tracker(file_num);
            self.cache_submitter
                .fill_cache(task.entries_size, &mut task.group_infos);
            while let Ok(msg) = self.receiver.try_recv() {
                let mut task = match msg {
                    LogMsg::Write(task) => task,
                    LogMsg::Stop => {
                        return Ok(());
                    }
                };
                if task.sync {
                    sync = true;
                }
                self.cache_submitter
                    .fill_cache(task.entries_size, &mut task.group_infos);
                let offset = self
                    .pipe_log
                    .append(LogQueue::Append, &task.content)
                    .unwrap();
                write_ret.push((offset, task.sender));
            }
            if sync {
                if let Err(e) = fd.sync() {
                    warn!("write wal failed because of: {} ", e);
                    write_ret.clear();
                }
            }
            for (offset, sender) in write_ret.drain(..) {
                let _ = sender.send((file_num, offset, tracker.clone()));
            }
        }
        Ok(())
    }
}
