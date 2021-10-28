// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::Result;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum LogQueue {
    Append,
    Rewrite,
}

pub type FileSeq = u64;

#[derive(Debug, Copy, Clone, PartialEq)]
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
    type WriteContext;

    /// Read some bytes from the given position.
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>>;

    fn pre_write(&self, queue: LogQueue) -> Self::WriteContext;

    fn post_write(&self, queue: LogQueue, ctx: Self::WriteContext, force_sync: bool) -> Result<()>;

    /// Write a batch into the append queue.
    fn append(
        &self,
        queue: LogQueue,
        ctx: &mut Self::WriteContext,
        bytes: &[u8],
    ) -> Result<FileBlockHandle>;

    /// Sync the given queue.
    fn sync(&self, queue: LogQueue) -> Result<()>;

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

    fn new_log_file(&self, queue: LogQueue) -> Result<()>;

    fn purge_to(&self, file_id: FileId) -> Result<usize>;
}
