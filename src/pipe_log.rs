// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::cmp::Ordering;

use crate::Result;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum LogQueue {
    Append = 0,
    Rewrite = 1,
}

pub type FileSeq = u64;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FileId {
    pub queue: LogQueue,
    pub seq: FileSeq,
}

impl FileId {
    pub fn new(queue: LogQueue, seq: FileSeq) -> Self {
        Self { queue, seq }
    }

    #[cfg(test)]
    pub fn dummy(queue: LogQueue) -> Self {
        Self { queue, seq: 0 }
    }
}

// Order by freshness.
impl std::cmp::Ord for FileId {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self.queue, other.queue) {
            (LogQueue::Append, LogQueue::Rewrite) => Ordering::Greater,
            (LogQueue::Rewrite, LogQueue::Append) => Ordering::Less,
            _ => self.seq.cmp(&other.seq),
        }
    }
}

impl std::cmp::PartialOrd for FileId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FileBlockHandle {
    pub id: FileId,
    pub offset: u64,
    pub len: usize,
}

impl FileBlockHandle {
    pub fn new(id: FileId, offset: u64, len: usize) -> Self {
        Self { id, offset, len }
    }

    #[cfg(test)]
    pub fn dummy(queue: LogQueue) -> Self {
        Self {
            id: FileId::dummy(queue),
            offset: 0,
            len: 0,
        }
    }
}

pub trait PipeLog: Sized {
    /// Read some bytes from the given position.
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>>;

    /// Write a batch into the append queue.
    ///
    /// # Panics
    ///
    /// Panics if cannot rollback to a consistent state when error.
    fn append(&self, queue: LogQueue, bytes: &[u8]) -> Result<FileBlockHandle>;

    /// Sync and rotate the given queue if needed.
    ///
    /// # Panics
    ///
    /// Panics if sync goes wrong.
    fn maybe_sync(&self, queue: LogQueue, sync: bool) -> Result<()>;

    fn file_span(&self, queue: LogQueue) -> (FileSeq, FileSeq);

    /// Returns the oldest id containing given file size percentile.
    fn file_at(&self, queue: LogQueue, mut position: f64) -> FileSeq {
        if position > 1.0 {
            position = 1.0;
        } else if position < 0.0 {
            position = 0.0;
        }
        let (first, active) = self.file_span(queue);
        let count = active - first + 1;
        first + (count as f64 * position) as u64
    }

    /// Total size of the given queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    fn rotate(&self, queue: LogQueue) -> Result<()>;

    fn purge_to(&self, file_id: FileId) -> Result<usize>;
}
