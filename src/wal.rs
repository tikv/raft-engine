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
        const MAX_WRITE_BUFFER: usize = 2 * 1024 * 1024; // 2MB
        let mut write_buffer = Vec::with_capacity(MAX_WRITE_BUFFER);
        while let Ok(LogMsg::Write(task)) = self.receiver.recv() {
            let mut sync = task.sync;
            let mut entries_size = task.entries_size;
            let (file_num, fd) = self.pipe_log.switch_log_file(LogQueue::Append).unwrap();
            write_ret.push((0, task.sender));

            while let Ok(msg) = self.receiver.try_recv() {
                if write_buffer.is_empty() {
                    write_buffer.extend_from_slice(&task.content);
                }
                let task = match msg {
                    LogMsg::Write(task) => task,
                    LogMsg::Stop => {
                        return Ok(());
                    }
                };
                if task.sync && !sync {
                    sync = true;
                }
                entries_size += task.entries_size;
                write_ret.push((write_buffer.len() as u64, task.sender));
                write_buffer.extend_from_slice(&task.content);
                if write_buffer.len() >= MAX_WRITE_BUFFER {
                    break;
                }
            }
            let base_offset = if write_buffer.is_empty() {
                self.pipe_log
                    .append(LogQueue::Append, &task.content)
                    .unwrap()
            } else {
                self.pipe_log
                    .append(LogQueue::Append, &write_buffer)
                    .unwrap()
            };
            if sync {
                if let Err(e) = fd.sync() {
                    warn!("write wal failed because of: {} ", e);
                    write_ret.clear();
                }
            }
            let tracker = self
                .cache_submitter
                .get_cache_tracker(file_num, base_offset);
            self.cache_submitter.fill_chunk(entries_size);
            for (offset, sender) in write_ret.drain(..) {
                let _ = sender.send((file_num, base_offset + offset, tracker.clone()));
            }
            write_buffer.clear();
        }
        Ok(())
    }
}
