use protobuf::Message;

use crate::config::RecoveryMode;
use crate::log_batch::{EntryExt, LogBatch};
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

impl std::iter::Step for FileId {
    fn steps_between(start: &Self, end: &Self) -> Option<usize> {
        if start.0 == 0 || end.0 == 0 || start.0 > end.0 {
            None
        } else {
            Some((end.0 - start.0) as usize)
        }
    }
    fn forward_checked(start: Self, count: usize) -> Option<Self> {
        if start.0 == 0 {
            None
        } else {
            Some(FileId(start.0 + count as u64))
        }
    }
    fn backward_checked(start: Self, count: usize) -> Option<Self> {
        if start.0 <= count as u64 {
            None
        } else {
            Some(FileId(start.0 - count as u64))
        }
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
        std::iter::Step::forward_checked(*self, step).unwrap_or_default()
    }
    /// Returns an invalid ID when out of bound or applied on an invalid file ID.
    pub fn backward(&self, step: usize) -> Self {
        std::iter::Step::backward_checked(*self, step).unwrap_or_default()
    }
    /// Returns step distance from another older ID.
    pub fn distance_from(&self, rhs: &Self) -> Option<usize> {
        std::iter::Step::steps_between(rhs, self)
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

pub trait PipeLog: Sized + Clone + Send {
    /// Close the pipe log.
    fn close(&self) -> Result<()>;

    fn file_len(&self, queue: LogQueue, file_id: FileId) -> Result<u64>;

    /// Total size of the append queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    /// Read some bytes from the given position.
    fn read_bytes(
        &self,
        queue: LogQueue,
        file_id: FileId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>>;

    /// Read a file into bytes.
    fn read_file_bytes(&self, queue: LogQueue, file_id: FileId) -> Result<Vec<u8>>;

    fn read_file<E: Message, W: EntryExt<E>>(
        &self,
        queue: LogQueue,
        file_id: FileId,
        mode: RecoveryMode,
        batches: &mut Vec<LogBatch<E, W>>,
    ) -> Result<()>;

    /// Write a batch into the append queue.
    fn append<E: Message, W: EntryExt<E>>(
        &self,
        queue: LogQueue,
        batch: &mut LogBatch<E, W>,
        sync: bool,
    ) -> Result<(FileId, usize)>;

    /// Sync the given queue.
    fn sync(&self, queue: LogQueue) -> Result<()>;

    /// Active file id in the given queue.
    fn active_file_id(&self, queue: LogQueue) -> FileId;

    /// First file id in the given queue.
    fn first_file_id(&self, queue: LogQueue) -> FileId;

    /// Returns the oldest id containing given file size percentile.
    fn file_at(&self, queue: LogQueue, position: f64) -> FileId;

    fn new_log_file(&self, queue: LogQueue) -> Result<()>;

    /// Truncate the active log file of `queue`.
    fn truncate_active_log(&self, queue: LogQueue, offset: Option<usize>) -> Result<()>;

    /// Purge the append queue to the given file id.
    fn purge_to(&self, queue: LogQueue, file_id: FileId) -> Result<usize>;
}
