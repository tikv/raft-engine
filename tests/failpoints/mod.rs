// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

#![cfg_attr(feature = "swap", feature(allocator_api))]

mod util;

mod test_engine;
mod test_io_error;

use raft_engine::*;
use util::*;

#[test]
fn test_log_batch_full() {
    let _f = FailGuard::new("log_batch::1kb_entries_size_per_batch", "return");
    let mut batch_1 = LogBatch::default();
    let mut batch_2 = LogBatch::default();
    let data = vec![b'x'; 800];
    let entries = generate_entries(1, 2, Some(&data));
    batch_1.add_entries::<MessageExtTyped>(1, &entries).unwrap();
    batch_2.add_entries::<MessageExtTyped>(2, &entries).unwrap();

    let mut batch_1_clone = batch_1.clone();
    let mut batch_2_clone = batch_2.clone();
    assert!(matches!(
        batch_1_clone.merge(&mut batch_2_clone),
        Err(Error::Full)
    ));
    assert_eq!(batch_1, batch_1_clone);
    assert_eq!(batch_2, batch_2_clone);

    let mut batch_1_clone = batch_1.clone();
    assert!(matches!(
        batch_1_clone.add_entries::<MessageExtTyped>(3, &entries),
        Err(Error::Full)
    ));
    assert_eq!(batch_1, batch_1_clone);
}
