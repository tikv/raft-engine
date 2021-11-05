// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::panic::{self, AssertUnwindSafe};

use raft::eraftpb::Entry;
use raft_engine::MessageExt;

#[derive(Clone)]
pub struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

/// Catch panic while suppressing default panic hook.
pub fn catch_unwind_silent<F, R>(f: F) -> std::thread::Result<R>
where
    F: FnOnce() -> R,
{
    let prev_hook = panic::take_hook();
    panic::set_hook(Box::new(|_| {}));
    let result = panic::catch_unwind(AssertUnwindSafe(f));
    panic::set_hook(prev_hook);
    result
}
