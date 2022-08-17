// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! A generic log storage.

use std::fmt::{self, Display};

use fail::fail_point;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::ToPrimitive;
use serde_repr::{Deserialize_repr, Serialize_repr};
use strum::EnumIter;

use crate::Result;

/// Making it possible to use fixed-size array to store channel info.
pub const MAX_WRITE_CHANNELS: usize = 8;

/// The type of log queue.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub struct LogQueue(u8);

impl LogQueue {
    pub const REWRITE: LogQueue = LogQueue(0);
    pub const DEFAULT: LogQueue = LogQueue(1);

    #[inline]
    pub fn i(&self) -> u8 {
        debug_assert!(self.0 as usize <= MAX_WRITE_CHANNELS);
        self.0
    }
}

/// Sequence number for log file. It is unique within a log queue.
pub type FileSeq = u64;

/// A unique identifier for a log file.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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

/// A view of files in Append queue. Files in Rewrite queue are not included.
/// Internally it contains a list of latest file IDs of all write channels.
/// `None` means the channel doesn't exist when the view is created.
#[derive(Debug)]
pub struct FilesView([Option<FileSeq>; MAX_WRITE_CHANNELS]);

impl FilesView {
    /// Returns whether the file `id` is visible to the files view.
    #[inline]
    pub fn contains(&self, id: &FileId) -> bool {
        if id.queue != LogQueue::REWRITE {
            if let Some(seq) = self.0[id.queue.i() as usize - 1] {
                return id.seq <= seq;
            }
        }
        false
    }

    /// A simple view that contains default channel files with seqno no larger
    /// than `seq`.
    #[inline]
    pub fn simple_view(seq: u64) -> Self {
        let mut view = Self::empty_view();
        view.0[LogQueue::DEFAULT.i() as usize - 1] = Some(seq);
        view
    }

    #[inline]
    pub fn empty_view() -> Self {
        FilesView(Default::default())
    }

    /// Often used in combo with `empty_view` to calculate the upper bound of
    /// some files.
    #[inline]
    pub fn update_to_contain(&mut self, id: &FileId) {
        if !self.contains(id) {
            debug_assert!(id.queue != LogQueue::REWRITE);
            self.0[id.queue.i() as usize - 1] = Some(id.seq);
        }
    }

    #[inline]
    pub fn full_view() -> Self {
        let mut view = Self::empty_view();
        for i in view.0.iter_mut() {
            *i = Some(u64::MAX);
        }
        view
    }

    /// Often used in combo with `full_view` to calculate the lower bound of
    /// some files.
    #[inline]
    pub fn update_to_exclude(&mut self, id: &FileId) {
        // Cannot exclude rewrite.
        assert!(id.queue != LogQueue::REWRITE);
        if self.contains(id) {
            self.0[id.queue.i() as usize - 1] = if id.seq == 0 { None } else { Some(id.seq - 1) };
        }
    }

    #[inline]
    pub fn intersect(&mut self, rhs: &FilesView) {
        for i in 0..MAX_WRITE_CHANNELS {
            if self.contains(&FileId::new(LogQueue(i as u8 + 1), rhs.0[i].unwrap_or(0))) {
                self.0[i] = rhs.0[i];
            }
        }
    }

    #[inline]
    pub fn distill(&self) -> Vec<FileId> {
        self.0
            .iter()
            .enumerate()
            .filter_map(|(ch, seq)| seq.map(|seq| FileId::new(LogQueue(ch as u8 + 1), seq)))
            .collect()
    }
}

/// A logical pointer to a chunk of log file data.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
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
#[repr(u64)]
#[derive(
    Clone,
    Copy,
    Debug,
    Eq,
    PartialEq,
    FromPrimitive,
    ToPrimitive,
    Serialize_repr,
    Deserialize_repr,
    EnumIter,
)]
pub enum Version {
    V1 = 1,
    V2 = 2,
}

impl Version {
    pub fn has_log_signing(&self) -> bool {
        fail_point!("pipe_log::version::force_enable_log_signing", |_| { true });
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

impl Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_u64().unwrap())
    }
}

#[derive(Debug, Clone)]
pub struct LogFileContext {
    pub id: FileId,
    pub version: Version,
}

impl LogFileContext {
    pub fn new(id: FileId, version: Version) -> Self {
        Self { id, version }
    }

    /// Returns the `signature` in `u32` format.
    pub fn get_signature(&self) -> Option<u32> {
        if self.version.has_log_signing() {
            // Here, the count of files will be always limited to less than
            // `u32::MAX`. So, we just use the low 32 bit as the `signature`
            // by default.
            Some(self.id.seq as u32)
        } else {
            None
        }
    }
}

/// A `PipeLog` serves reads and writes over multiple queues of log files.
pub trait PipeLog: Sized {
    /// Reads some bytes from the specified position.
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>>;

    /// Appends some bytes to the specified log queue. Returns file position of
    /// the written bytes.
    ///
    /// The result of `fetch_active_file` will not be affected by this method.
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

    /// Returns an approximate view of the pipe when the total size of
    /// Append queues is only `portion`% of current size. The view is
    /// approximate because being visible in it is only necessary (but not
    /// sufficient) to fit the requirement. The view must only contain immutable
    /// files. Returns `None` if there is no file in Append queue matching
    /// the requirement.
    fn history_files_view(&self, portion: f64) -> Option<FilesView>;

    fn latest_files_view(&self) -> FilesView {
        FilesView::simple_view(self.file_span(LogQueue::DEFAULT).1)
    }

    /// Returns total size of the specified log queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    /// Rotates a new log file for the specified log queue. Returns the newly
    /// created file ID.
    ///
    /// Implementation should be atomic under error conditions but not
    /// necessarily panic-safe.
    fn rotate(&self, queue: LogQueue) -> Result<FileId>;

    /// Deletes all log files up to the specified file ID. The scope is limited
    /// to the log queue of `file_id`.
    ///
    /// Returns the number of deleted files.
    fn purge_to(&self, file_id: FileId) -> Result<usize>;

    /// Returns [`LogFileContext`] of the active file in the specific
    /// log queue.
    fn fetch_active_file(&self, queue: LogQueue) -> LogFileContext;
}
