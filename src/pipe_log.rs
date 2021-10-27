// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::Result;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum LogQueue {
    Append,
    Rewrite,
}

#[derive(PartialOrd, PartialEq, Eq, Copy, Clone, Default, Debug, Hash)]
pub struct FileId(u64);

impl std::fmt::Display for FileId {
    #[inline]
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<u64> for FileId {
    fn from(num: u64) -> FileId {
        FileId(num)
    }
}

impl FileId {
    pub fn as_u64(&self) -> u64 {
        self.0
    }

    /// Default constructor must return an invalid instance.
    pub fn valid(&self) -> bool {
        self.0 > 0
    }

    /// Returns an invalid ID when applied on an invalid file ID.
    pub fn forward(&self, step: usize) -> Self {
        if self.0 == 0 {
            Default::default()
        } else {
            FileId(self.0 + step as u64)
        }
    }

    /// Returns an invalid ID when out of bound or applied on an invalid file ID.
    pub fn backward(&self, step: usize) -> Self {
        if self.0 <= step as u64 {
            Default::default()
        } else {
            FileId(self.0 - step as u64)
        }
    }

    /// Returns step distance from another older ID.
    pub fn step_after(&self, rhs: &Self) -> Option<usize> {
        if self.0 == 0 || rhs.0 == 0 || self.0 < rhs.0 {
            None
        } else {
            Some((self.0 - rhs.0) as usize)
        }
    }

    pub fn min(lhs: FileId, rhs: FileId) -> FileId {
        if !lhs.valid() {
            rhs
        } else if !rhs.valid() {
            lhs
        } else {
            std::cmp::min(lhs.0, rhs.0).into()
        }
    }

    pub fn max(lhs: FileId, rhs: FileId) -> FileId {
        if !lhs.valid() {
            rhs
        } else if !rhs.valid() {
            lhs
        } else {
            std::cmp::max(lhs.0, rhs.0).into()
        }
    }
}

pub trait PipeLog: Sized {
    /// Read some bytes from the given position.
    fn read_bytes(
        &self,
        queue: LogQueue,
        file_id: FileId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>>;

    /// Write a batch into the append queue.
    fn append(&self, queue: LogQueue, bytes: &[u8], sync: bool) -> Result<(FileId, u64)>;

    /// Sync the given queue.
    fn sync(&self, queue: LogQueue) -> Result<()>;

    fn file_span(&self, queue: LogQueue) -> (FileId, FileId);

    /// Returns the oldest id containing given file size percentile.
    fn file_at(&self, queue: LogQueue, mut position: f64) -> FileId {
        if position > 1.0 {
            position = 1.0;
        } else if position < 0.0 {
            position = 0.0;
        }
        let (first, active) = self.file_span(queue);
        let count = active.step_after(&first).unwrap() + 1;
        let file_id = first.forward((count as f64 * position) as usize);
        file_id
    }

    /// Total size of the given queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    fn new_log_file(&self, queue: LogQueue) -> Result<()>;

    fn purge_to(&self, queue: LogQueue, file_id: FileId) -> Result<usize>;
}
