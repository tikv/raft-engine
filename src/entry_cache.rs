use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::thread::{self, JoinHandle, Builder as ThreadBuilder};
use std::sync::mpsc::{Sender, Receiver, RecvTimeoutError};
use std::collections::VecDeque;

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
            let task = CacheTask::NewChunk(CacheChunk{
                file_num: self.file_num,
                base_offset: self.offset,
                end_offset: offset,
                size_tracker: self.size_tracker.clone();
            });
            self.task_sender.send(task).unwrap();
            self.reset(file_num, offset);
        }
        self.chunk_size += size;
        self.size_tracker.fetch_add(size);
        CacheTracker {
            chunk_size: self.size_tracker.clone(),
            sub_on_drop: size
        }
    }
}

pub struct EntryCache {
    task_receiver: Option<Receiver<CacheTask>>,
    handle: Option<JoinHandle>,
}

impl EntryCache {
    pub fn start(&mut self, name: String) {
        let task_receiver = self.task_receiver.take();
        let handle = ThreadBuilder::new()
            .name(Some(name))
            .spawn(move || {
                let mut inner = EntryCacheInner::new();
                loop {
                    match task_receiver.recv_timeout(Duration::from_secs(10)) {
                        Ok(CacheTask::Stop) | Err(RecvTimeoutError::Disconnected) => return,
                        Ok(task) => inner.handle_task(task),
                        Err(RecvTimeoutError::Timeout) => inner.retain_valid_cache(),
                    }
                }
            }).unwrap();
        self.handle = Some(handle);
    }
}

struct EntryCacheInner {
    valid_cache_chunks: VecDeque<CacheChunk>,
    pipe_log: PipeLog,
    memtables: Arc<RwLock<MemTables>>,
}

impl EntryCacheInner {
    fn handle_task(&mut self, task: CacheTask) {
        match task {
            CacheTask::NewChunk(chunk) => self.valid_cache_chunks.push_back(chunk),
            CacheTask::HighWaterAcquire(size) => self.evict_oldest_cache(size),
            _ => unreachable!(),
        }
    }

    fn evict_oldest_cache(&mut self, target_size: usize) {
        while let Some(chunk) = self.valid_cache_chunks.pop_front() {
            let chunk_size = chunk.size_tracker.load(Ordering::Relaxed);
            if chunk_size == 0 {
                continue;
            }
            let CacheChunk {file_num, base_offset, end_offset} = chunk;
            let len = end_offset - base_offset;
            let chunk_content = self.pipe_log.fread(file_num, base_offset, len).unwrap();
            let mut reader = &chunk_content;
            while let Some(log_batch) = LogBatch::from_bytes(&mut reader).unwrap() {
                for item in log_batch.items {
                    if let LogItemContent::Entries(entries) = item.content {
                        let index = match entries.entries.last() {
                            Some(entry) => entry.index,
                            None => continue,
                        };
                    }
                }
            }
        }
    }
}

pub enum CacheTask {
    NewChunk(CacheChunk),
    HighWaterAcquire(usize),
    Stop,
}

struct CacheChunk {
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
        self.chunk_size.fetch_sub(self.sub_on_drop);
    }
}
