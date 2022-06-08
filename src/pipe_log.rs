// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! A generic log storage.

use std::cmp::Ordering;

use crate::file_pipe_log::Version;
use crate::Result;

/// The type of log queue.
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum LogQueue {
    Append = 0,
    Rewrite = 1,
}

/// Sequence number for log file. It is unique within a log queue.
pub type FileSeq = u64;

/// A unique identifier for a log file.
#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub struct FileId {
    pub queue: LogQueue,
    pub seq: FileSeq,
}

impl FileId {
    /// Creates a [`FileId`] from a [`LogQueue`] and a [`FileSeq`].
    pub fn new(queue: LogQueue, seq: FileSeq) -> Self {
        Self { queue, seq }
    }

    /// Creates a new [`FileId`] representing a non-existing file.
    #[cfg(test)]
    pub fn dummy(queue: LogQueue) -> Self {
        Self { queue, seq: 0 }
    }
}

/// Order by freshness.
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

/// A logical pointer to a chunk of log file data.
#[derive(Debug, Copy, Clone, PartialEq)]
pub struct FileBlockHandle {
    pub id: FileId,
    pub offset: u64,
    pub len: usize,
}

impl FileBlockHandle {
    /// Creates a new [`FileBlockHandle`] that points to nothing.
    #[cfg(test)]
    pub fn dummy(queue: LogQueue) -> Self {
        Self {
            id: FileId::dummy(queue),
            offset: 0,
            len: 0,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq)]
enum Content {
    Checksum(u32),
}

#[derive(Debug, Clone)]
pub struct Signature {
    pub id: FileId,
    _cont: Option<Content>,
}

impl Signature {
    pub fn new(file_id: FileId) -> Self {
        let mut ret = Self {
            id: file_id,
            _cont: None,
        };
        ret.encode_as_u32(); // default
        ret
    }

    #[cfg(test)]
    pub fn dummy(queue: LogQueue) -> Self {
        Self {
            id: FileId::dummy(queue),
            _cont: None,
        }
    }

    /// Encode the signature and output it as `u32`.
    pub fn encode_as_u32(&mut self) -> u32 {
        if self._cont.is_none() {
            let high = (self.id.seq as u64 >> 32) as u32;
            let low = self.id.seq as u32;
            let flag = self.id.queue as usize;
            self._cont = Some(Content::Checksum(high ^ low ^ (flag as u32)));
        }
        match self._cont.unwrap() {
            Content::Checksum(v) => v,
        }
    }
}

/// A `PipeLog` serves reads and writes over multiple queues of log files.
pub trait PipeLog: Sized {
    /// Reads some bytes from the specified position.
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>>;

    /// Appends some bytes to the specified log queue. Returns file position of
    /// the written bytes.
    fn append(&self, queue: LogQueue, bytes: &[u8]) -> Result<FileBlockHandle>;

    /// Hints it to synchronize buffered writes. The synchronization is
    /// mandotory when `sync` is true.
    ///
    /// This operation might incurs a great latency overhead. It's advised to
    /// call it once every batch of writes.
    fn maybe_sync(&self, queue: LogQueue, sync: bool) -> Result<()>;

    /// Returns the smallest and largest file sequence number of the specified
    /// log queue.
    fn file_span(&self, queue: LogQueue) -> (FileSeq, FileSeq);

    /// Returns the oldest file ID that is newer than `position`% of all files.
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

    /// Returns total size of the specified log queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    /// Rotates a new log file for the specified log queue.
    ///
    /// Implementation should be atomic under error conditions but not
    /// necessarily panic-safe.
    fn rotate(&self, queue: LogQueue) -> Result<()>;

    /// Deletes all log files up to the specified file ID. The scope is limited
    /// to the log queue of `file_id`.
    ///
    /// Returns the number of deleted files.
    fn purge_to(&self, file_id: FileId) -> Result<usize>;

    /// Returns the `Version` corresponding to the given `[FileId]`
    fn fetch_file_version(&self, file_id: FileId) -> Result<Version>;

    /// Returns the active `[FileId]` and related `[Version]` of the specific
    /// log queue.
    ///
    /// Returns the `[Version]` and `[FileId]` of the related file.
    fn fetch_active_file(&self, queue: LogQueue) -> (Version, FileId);
}
