use crate::pipe_log::{FileId, LogQueue};

pub trait EventListener: Sync + Send {
    /// Called *after* a new log file is created.
    fn post_new_log_file(&self, queue: LogQueue, file_id: FileId);

    /// Called *before* a log batch has been appended into a log file.
    fn on_append_log_file(&self, queue: LogQueue, file_id: FileId);

    /// Called *after* a log batch has been applied to memtables.
    fn post_apply_memtables(&self, queue: LogQueue, file_id: FileId);

    /// Test whether a log file can be purged or not.
    fn ready_for_purge(&self, queue: LogQueue, file_id: FileId) -> bool;

    /// Called *after* a log file get purged.
    fn post_purge(&self, queue: LogQueue, file_id: FileId);
}
