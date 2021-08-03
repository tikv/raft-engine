use crate::pipe_log::{FileId, LogQueue};

pub trait EventListener: Sync + Send {
    /// Called *after* a new log file is created.
    fn post_new_log_file(&self, _queue: LogQueue, _file_id: FileId) {}

    /// Called *after* a log file is frozen.
    fn post_log_file_frozen(&self, _queue: LogQueue, _file_id: FileId) {}

    /// Called *before* a log batch has been appended into a log file.
    fn on_append_log_file(&self, _queue: LogQueue, _file_id: FileId) {}

    /// Called *after* a log batch has been applied to memtables.
    fn post_apply_memtables(&self, _queue: LogQueue, _file_id: FileId) {}

    /// Test whether a log file can be purged or not.
    fn ready_for_purge(&self, _queue: LogQueue, _file_id: FileId) -> bool {
        true
    }

    /// Called *after* a log file get purged.
    fn post_purge(&self, _queue: LogQueue, _file_id: FileId) {}

    /// Called *after* a log file is actually removed.
    fn post_removed(&self, _queue: LogQueue, _file_id: FileId) {}
}
