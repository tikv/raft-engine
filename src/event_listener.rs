// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue};

pub trait EventListener: Sync + Send {
    /// Called *after* a new log file is created.
    fn post_new_log_file(&self, _file_id: FileId) {}

    /// Called *before* a [`LogBatch`] has been written into a log file.
    fn on_append_log_file(&self, _handle: FileBlockHandle) {}

    /// Called *after* a [`LogBatch`] has been applied to the [`MemTable`].
    fn post_apply_memtables(&self, _file_id: FileId) {}

    /// Returns the oldest file sequence number that are not ready to be purged.
    fn first_file_not_ready_for_purge(&self, _queue: LogQueue) -> Option<FileSeq> {
        None
    }

    /// Called *after* a log file is purged.
    fn post_purge(&self, _file_id: FileId) {}
}
