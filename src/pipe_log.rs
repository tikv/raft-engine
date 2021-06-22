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

pub trait PipeLog: Sized + Clone + Send {
    /// Close the pipe log.
    fn close(&self) -> Result<()>;

    fn file_size(&self, queue: LogQueue, file_id: FileId) -> Result<u64>;

    /// Total size of the given queue.
    fn total_size(&self, queue: LogQueue) -> usize;

    /// Read some bytes from the given position.
    fn read_bytes(
        &self,
        queue: LogQueue,
        file_id: FileId,
        offset: u64,
        len: u64,
    ) -> Result<Vec<u8>>;

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

    /// Purge the append queue to the given file id.
    fn purge_to(&self, queue: LogQueue, file_id: FileId) -> Result<usize>;
}
