use std::sync::atomic::{AtomicUsize};
use std::sync::Arc;
use std::collections::VecDeque;

struct PurgeManagerInner {
    progress: u64,

    // Count and size for all log files.
    index_trackers: VecDeque<Arc<(AtomicUsize, AtomicUsize)>>,
}

pub struct PurgeManager {
    thread_name: String,
    progress: Arc<Mutex<PurgeManagerState>>,
}

impl PurgeManager {

}
