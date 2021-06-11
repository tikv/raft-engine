use protobuf::Message;

use crate::config::RecoveryMode;
use crate::log_batch::{EntryExt, LogBatch};
use crate::Result;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum LogQueue {
    Append,
    Rewrite,
}

/// Timely ordered file identifier.
pub trait GenericFileId:
    Ord
    + Eq
    + Copy
    + Clone
    + Send
    + Sync
    + Default
    + std::fmt::Display
    + std::fmt::Debug
    + std::hash::Hash
{
    /// Default constructor must return an invalid instance.
    fn valid(&self) -> bool;
    /// Returns an invalid ID when applied on an invalid file ID.
    fn forward(&self, step: u64) -> Self;
    /// Returns an invalid ID when out of bound or applied on an invalid file ID.
    fn backward(&self, step: u64) -> Self;
    /// Returns step distance from another older ID.
    fn distance_from(&self, rhs: &Self) -> Option<u64>;
}

pub fn min_id<I: GenericFileId>(lhs: I, rhs: I) -> I {
    if !lhs.valid() {
        rhs
    } else if !rhs.valid() {
        lhs
    } else {
        std::cmp::min(lhs, rhs)
    }
}

#[allow(dead_code)]
pub fn max_id<I: GenericFileId>(lhs: I, rhs: I) -> I {
    if !lhs.valid() {
        rhs
    } else if !rhs.valid() {
        lhs
    } else {
        std::cmp::max(lhs, rhs)
    }
}

impl GenericFileId for u64 {
    fn valid(&self) -> bool {
        *self > 0
    }

    fn forward(&self, step: u64) -> Self {
        if *self == 0 {
            0
        } else {
            *self + step
        }
    }

    fn backward(&self, step: u64) -> Self {
        if *self <= step {
            0
        } else {
            *self - step
        }
    }

    fn distance_from(&self, rhs: &Self) -> Option<u64> {
        if *self == 0 || *rhs == 0 || *self < *rhs {
            None
        } else {
            Some(*self - *rhs)
        }
    }
}

pub trait PipeLog: Sized + Clone + Send {
    type FileId: GenericFileId;

    /// Close the pipe log.
    fn close(&self) -> Result<()>;

    fn file_len(&self, queue: LogQueue, file_id: Self::FileId) -> Result<u64>;

    /// Total size of the append queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    /// Read some bytes from the given position.
    fn read_bytes(
        &self,
        queue: LogQueue,
        file_id: Self::FileId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>>;

    /// Read a file into bytes.
    fn read_file_bytes(&self, queue: LogQueue, file_id: Self::FileId) -> Result<Vec<u8>>;

    fn read_file<E: Message, W: EntryExt<E>>(
        &self,
        queue: LogQueue,
        file_id: u64,
        mode: RecoveryMode,
        batches: &mut Vec<LogBatch<E, W, Self::FileId>>,
    ) -> Result<()>;

    /// Write a batch into the append queue.
    fn append<E: Message, W: EntryExt<E>>(
        &self,
        queue: LogQueue,
        batch: &mut LogBatch<E, W, Self::FileId>,
        sync: bool,
    ) -> Result<(Self::FileId, usize)>;

    /// Sync the given queue.
    fn sync(&self, queue: LogQueue) -> Result<()>;

    /// Active file id in the given queue.
    fn active_file_id(&self, queue: LogQueue) -> Self::FileId;

    /// First file id in the given queue.
    fn first_file_id(&self, queue: LogQueue) -> Self::FileId;

    /// Returns the oldest id containing given file size percentile.
    fn file_at(&self, queue: LogQueue, position: f64) -> Self::FileId;

    fn new_log_file(&self, queue: LogQueue) -> Result<()>;

    /// Truncate the active log file of `queue`.
    fn truncate_active_log(&self, queue: LogQueue, offset: Option<usize>) -> Result<()>;

    /// Purge the append queue to the given file id.
    fn purge_to(&self, queue: LogQueue, file_id: Self::FileId) -> Result<usize>;
}
