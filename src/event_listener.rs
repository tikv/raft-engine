// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::pipe_log::{FileBlockHandle, FileId, FilesView};

/// `EventListener` contains a set of callback functions that will be notified
/// on specific events inside Raft Engine.
///
/// # Threading
///
/// Different callbacks are called under different threading contexts.
/// [`on_append_log_file`], for example, will be called under a global lock of
/// one specific queue.
///
/// [`on_append_log_file`]: EventListener::on_append_log_file
pub trait EventListener: Sync + Send {
    /// Called *after* a new log file is created.
    fn post_new_log_file(&self, _file_id: FileId) {}

    /// Called *before* a [`LogBatch`] has been written into a log file.
    ///
    /// [`LogBatch`]: crate::log_batch::LogBatch
    fn on_append_log_file(&self, _handle: FileBlockHandle) {}

    /// Called *after* a [`LogBatch`] has been applied to the [`MemTable`].
    ///
    /// [`LogBatch`]: crate::log_batch::LogBatch
    /// [`MemTable`]: crate::memtable::MemTable
    fn post_apply_memtables(&self, _file_id: FileId) {}

    /// Returns a view of Append log files that can be purged.
    fn latest_files_view_allowed_to_purge(&self) -> Option<FilesView> {
        None
    }

    /// Called *after* `PipeLog::purge_to`.
    fn post_purge(&self, _file_id: FileId) {}
}
