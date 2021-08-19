// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::pipe_log::{FileId, LogQueue};

pub trait EventListener: Sync + Send {
    /// Called *after* a new log file is created.
    fn post_new_log_file(&self, _queue: LogQueue, _file_id: FileId) {}

    /// Called *before* a log batch has been appended into a log file.
    fn on_append_log_file(&self, _queue: LogQueue, _file_id: FileId, _bytes: usize) {}

    /// Called *after* a log batch has been applied to memtables.
    fn post_apply_memtables(&self, _queue: LogQueue, _file_id: FileId) {}

    /// Test whether a log file can be purged or not.
    fn first_file_not_ready_for_purge(&self, _queue: LogQueue) -> FileId {
        FileId::from(u64::MAX)
    }

    /// Called *after* a log file get purged.
    fn post_purge(&self, _queue: LogQueue, _file_id: FileId) {}
}
