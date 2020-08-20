use std::collections::VecDeque;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, RwLock};
use std::thread::{self, Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;

use crate::log_batch::{LogBatch, LogItemContent};
use crate::memtable::MemTable;
use crate::pipe_log::PipeLog;
use crate::util::HandyRwLock;

const CACHE_CHUNK_SIZE: usize = 4 * 1024 * 1024;

pub struct CacheAgent {
    file_num: u64,
    offset: u64,
    chunk_size: usize,
    size_tracker: Arc<AtomicUsize>,
    task_sender: Sender<CacheTask>,
}

impl CacheAgent {
    fn reset(&mut self, file_num: u64, offset: u64) {
        self.file_num = file_num;
        self.offset = offset;
        self.size_tracker = Arc::new(AtomicUsize::new(0));
    }

    pub fn get_cache_tracker(&mut self, file_num: u64, offset: u64, size: usize) -> CacheTracker {
        if self.file_num == 0 {
            self.file_num = file_num;
            self.offset = offset;
        }

        if self.chunk_size >= CACHE_CHUNK_SIZE {
            let task = CacheTask::NewChunk(CacheChunk {
                file_num: self.file_num,
                base_offset: self.offset,
                end_offset: offset,
                size_tracker: self.size_tracker.clone(),
            });
            self.task_sender.send(task).unwrap();
            self.reset(file_num, offset);
        }
        self.chunk_size += size;
        self.size_tracker.fetch_add(size, Ordering::SeqCst);
        CacheTracker {
            chunk_size: self.size_tracker.clone(),
            sub_on_drop: size,
        }
    }
}

pub struct EntryCache {
    task_receiver: Option<Receiver<CacheTask>>,
    handle: Option<JoinHandle<()>>,
}

impl EntryCache {
    pub fn start(&mut self, name: String, pipe_log: PipeLog) {
        let task_receiver = match self.task_receiver.take() {
            Some(receiver) => receiver,
            None => {
                assert!(self.handle.is_some());
                return;
            }
        };

        let handle = ThreadBuilder::new()
            .name(name)
            .spawn(move || {
                let mut inner = EntryCacheInner::new(pipe_log);
                loop {
                    match task_receiver.recv_timeout(Duration::from_secs(10)) {
                        Ok(CacheTask::Stop) | Err(RecvTimeoutError::Disconnected) => return,
                        Ok(task) => inner.handle_task(task),
                        Err(RecvTimeoutError::Timeout) => inner.retain_valid_cache(),
                    }
                }
            })
            .unwrap();
        self.handle = Some(handle);
    }
}

struct EntryCacheInner {
    valid_cache_chunks: VecDeque<CacheChunk>,
    pipe_log: PipeLog,
}

impl EntryCacheInner {
    fn new(pipe_log: PipeLog) -> EntryCacheInner {
        EntryCacheInner {
            valid_cache_chunks: Default::default(),
            pipe_log,
        }
    }

    fn retain_valid_cache(&mut self) {
        self.valid_cache_chunks
            .retain(|chunk| chunk.size_tracker.load(Ordering::Relaxed) > 0);
    }

    fn handle_task(&mut self, task: CacheTask) {
        match task {
            CacheTask::NewChunk(chunk) => self.valid_cache_chunks.push_back(chunk),
            CacheTask::HighWaterAcquire(size) => self.evict_oldest_cache(size),
            _ => unreachable!(),
        }
    }

    fn get_memtable(&self, raft_group_id: u64) -> Option<Arc<RwLock<MemTable>>> {
        // TODO: access memtables.
        None
    }

    fn evict_oldest_cache(&mut self, target_size: usize) {
        let mut gc_size = 0;
        while let Some(chunk) = self.valid_cache_chunks.pop_front() {
            if chunk.size_tracker.load(Ordering::Relaxed) == 0 {
                continue;
            }
            let CacheChunk {
                file_num,
                base_offset,
                end_offset,
                ..
            } = chunk;
            let len = end_offset - base_offset;
            let chunk_content = self.pipe_log.fread(file_num, base_offset, len).unwrap();

            let mut reader: &[u8] = chunk_content.as_ref();
            let mut reader_len = reader.len();
            let mut offset = base_offset;
            while let Some(log_batch) = LogBatch::from_bytes(&mut reader, file_num, offset).unwrap()
            {
                for item in log_batch.items {
                    if let LogItemContent::Entries(entries) = item.content {
                        let gc_cache_to = match entries.entries.last() {
                            Some(entry) => entry.index + 1,
                            None => continue,
                        };
                        if let Some(memtable) = self.get_memtable(item.raft_group_id) {
                            let (_, size) = memtable.wl().compact_cache_to(gc_cache_to);
                            gc_size += size;
                        }
                    }
                }
            }
            offset += (reader.len() - reader_len) as u64;
            reader_len = reader.len();
            if gc_size >= target_size {
                break;
            }
        }
    }
}

pub enum CacheTask {
    NewChunk(CacheChunk),
    HighWaterAcquire(usize),
    Stop,
}

pub struct CacheChunk {
    file_num: u64,
    base_offset: u64,
    end_offset: u64,
    size_tracker: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct CacheTracker {
    chunk_size: Arc<AtomicUsize>,
    sub_on_drop: usize,
}

impl Drop for CacheTracker {
    fn drop(&mut self) {
        self.chunk_size
            .fetch_sub(self.sub_on_drop, Ordering::SeqCst);
    }
}
