use futures::channel::oneshot::Sender;
use std::sync::mpsc::Receiver;

use crate::cache_evict::{CacheSubmitor, CacheTracker};
use crate::errors::Result;
use crate::pipe_log::{GenericPipeLog, LogQueue};
use crate::util::Statistic;
use log::warn;
use std::time::Instant;

pub struct WriteTask {
    pub content: Vec<u8>,
    pub entries_size: usize,
    pub sync: bool,
    pub sender: Sender<(u64, u64, Option<CacheTracker>)>,
}

pub enum LogMsg {
    Write(WriteTask),
    Metric(Sender<Statistic>),
    Stop,
}

pub struct WalRunner<P: GenericPipeLog> {
    cache_submitter: CacheSubmitor,
    pipe_log: P,
    receiver: Receiver<LogMsg>,
    statistic: Statistic,
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
            statistic: Statistic::default(),
        }
    }
}

impl<P> WalRunner<P>
where
    P: GenericPipeLog,
{
    pub fn run(&mut self) -> Result<()> {
        let mut write_ret = vec![];
        const MAX_WRITE_BUFFER: usize = 1 * 1024 * 1024; // 2MB
        let mut write_buffer = Vec::with_capacity(MAX_WRITE_BUFFER);
        while let Ok(msg) = self.receiver.recv() {
            let task = match msg {
                LogMsg::Write(task) => task,
                LogMsg::Stop => {
                    return Ok(());
                }
                LogMsg::Metric(cb) => {
                    self.report(cb);
                    continue;
                }
            };
            let now = Instant::now();
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
                    LogMsg::Metric(cb) => {
                        self.report(cb);
                        continue;
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
            let before_sync_cost = now.elapsed().as_micros();
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
            let wal_cost = now.elapsed().as_micros();
            self.statistic.add_wal(wal_cost as usize);
            self.statistic
                .add_sync((wal_cost - before_sync_cost) as usize);
            self.statistic.freq += 1;
            for (offset, sender) in write_ret.drain(..) {
                let _ = sender.send((file_num, base_offset + offset, tracker.clone()));
            }
            write_buffer.clear();
        }
        Ok(())
    }

    pub fn report(&mut self, cb: Sender<Statistic>) {
        self.statistic.divide();
        let _ = cb.send(self.statistic.clone());
        self.statistic.clear();
    }
}
