// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! A generic log storage.
use fail::fail_point;
use num_derive::{FromPrimitive, ToPrimitive};
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use strum::EnumIter;

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

/// Version of log file format.
#[derive(
    Clone, Copy, Debug, Eq, PartialEq, FromPrimitive, ToPrimitive, Serialize, Deserialize, EnumIter,
)]
#[repr(u64)]
pub enum Version {
    V1 = 1,
    V2 = 2,
}

impl Version {
    pub fn has_log_signing(&self) -> bool {
        fail_point!("pipe_log::version::skip_check", |_| { true });
        match self {
            Version::V1 => false,
            Version::V2 => true,
        }
    }
}

impl Default for Version {
    fn default() -> Self {
        Version::V1
    }
}

#[derive(Debug, Clone)]
pub struct LogFileContext {
    pub id: FileId,
    pub version: Version,
}

impl LogFileContext {
    pub fn new(file_id: FileId, version: Version) -> Self {
        Self {
            id: file_id,
            version,
        }
    }

    /// Return the `signature` in `u32` format.
    ///
    /// Here, the count of files will be always limited to less than
    /// `UINT32_MAX`. So, we just use the low 32 bit as the `signature`
    /// by default.
    pub fn get_signature(&self) -> u32 {
        self.id.seq as u32
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

    /// Returns the smallest and largest file sequence number, still in use,
    /// of the specified log queue.
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

    /// Returns the `[Version]` corresponding to the given `[FileId]`
    fn fetch_format_version(&self, file_id: FileId) -> Result<Version>;

    /// Returns `[LogFileContext]` of the active file in the specific
    /// log queue.
    fn fetch_active_file(&self, queue: LogQueue) -> LogFileContext;
}
