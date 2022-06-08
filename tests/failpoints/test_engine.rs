// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::sync::Arc;

use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;
use raft_engine::env::{FileSystem, ObfuscatedFileSystem};
use raft_engine::internals::*;
use raft_engine::*;

use crate::util::*;

fn append<FS: FileSystem>(
    engine: &Engine<FS>,
    rid: u64,
    start_index: u64,
    end_index: u64,
    data: Option<&[u8]>,
) {
    let entries = generate_entries(start_index, end_index, data);
    if !entries.is_empty() {
        let mut batch = LogBatch::default();
        batch.add_entries::<MessageExtTyped>(rid, &entries).unwrap();
        batch
            .put_message(
                rid,
                b"last_index".to_vec(),
                &RaftLocalState {
                    last_index: entries[entries.len() - 1].index,
                    ..Default::default()
                },
            )
            .unwrap();
        engine.write(&mut batch, true).unwrap();
    }
}

#[test]
fn test_pipe_log_listeners() {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Default)]
    struct QueueHook {
        files: AtomicUsize,
        appends: AtomicUsize,
        applys: AtomicUsize,
        purged: AtomicU64,
    }

    impl QueueHook {
        fn files(&self) -> usize {
            self.files.load(Ordering::Acquire)
        }
        fn appends(&self) -> usize {
            self.appends.load(Ordering::Acquire)
        }
        fn applys(&self) -> usize {
            self.applys.load(Ordering::Acquire)
        }
        fn purged(&self) -> u64 {
            self.purged.load(Ordering::Acquire)
        }
    }

    struct Hook(HashMap<LogQueue, QueueHook>);
    impl Default for Hook {
        fn default() -> Hook {
            let mut hash = HashMap::default();
            hash.insert(LogQueue::Append, QueueHook::default());
            hash.insert(LogQueue::Rewrite, QueueHook::default());
            Hook(hash)
        }
    }

    impl EventListener for Hook {
        fn post_new_log_file(&self, id: FileId) {
            self.0[&id.queue].files.fetch_add(1, Ordering::Release);
        }

        fn on_append_log_file(&self, handle: FileBlockHandle) {
            self.0[&handle.id.queue]
                .appends
                .fetch_add(1, Ordering::Release);
        }

        fn post_apply_memtables(&self, id: FileId) {
            self.0[&id.queue].applys.fetch_add(1, Ordering::Release);
        }

        fn post_purge(&self, id: FileId) {
            self.0[&id.queue].purged.store(id.seq, Ordering::Release);
        }
    }

    let dir = tempfile::Builder::new()
        .prefix("test_pipe_log_listeners")
        .tempdir()
        .unwrap();

    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(128),
        purge_threshold: ReadableSize::kb(512),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };

    let hook = Arc::new(Hook::default());
    let engine = Arc::new(Engine::open_with_listeners(cfg.clone(), vec![hook.clone()]).unwrap());
    assert_eq!(hook.0[&LogQueue::Append].files(), 1);
    assert_eq!(hook.0[&LogQueue::Rewrite].files(), 1);

    let data = vec![b'x'; 64 * 1024];

    // Append 10 logs for region 1, 10 logs for region 2.
    for i in 1..=20 {
        let region_id = (i as u64 - 1) % 2 + 1;
        append(
            &engine,
            region_id,
            (i as u64 + 1) / 2,
            (i as u64 + 1) / 2 + 1,
            Some(&data),
        );
        assert_eq!(hook.0[&LogQueue::Append].appends(), i);
        assert_eq!(hook.0[&LogQueue::Append].applys(), i);
    }
    assert_eq!(hook.0[&LogQueue::Append].files(), 11);

    engine.purge_expired_files().unwrap();
    assert_eq!(hook.0[&LogQueue::Append].purged(), 8);

    let rewrite_files = hook.0[&LogQueue::Rewrite].files();

    // Append 5 logs for region 1, 5 logs for region 2.
    for i in 21..=30 {
        let region_id = (i as u64 - 1) % 2 + 1;
        append(
            &engine,
            region_id,
            (i as u64 + 1) / 2,
            (i as u64 + 1) / 2 + 1,
            Some(&data),
        );
        assert_eq!(hook.0[&LogQueue::Append].appends(), i);
        assert_eq!(hook.0[&LogQueue::Append].applys(), i);
    }
    // Compact so that almost all content of rewrite queue will become garbage.
    engine.compact_to(1, 14);
    engine.compact_to(2, 14);
    assert_eq!(hook.0[&LogQueue::Append].appends(), 32);
    assert_eq!(hook.0[&LogQueue::Append].applys(), 32);

    engine.purge_expired_files().unwrap();
    assert_eq!(hook.0[&LogQueue::Append].purged(), 13);
    assert_eq!(hook.0[&LogQueue::Rewrite].purged(), rewrite_files as u64);

    // Write region 3 without applying.
    let apply_memtable_region_3_fp = "memtable_accessor::apply_append_writes::region_3";
    fail::cfg(apply_memtable_region_3_fp, "pause").unwrap();
    let engine_clone = engine.clone();
    let data_clone = data.clone();
    let th = std::thread::spawn(move || {
        append(&engine_clone, 3, 1, 2, Some(&data_clone));
    });

    // Sleep a while to wait the log batch `Append(3, [1])` to get written.
    std::thread::sleep(Duration::from_millis(200));
    assert_eq!(hook.0[&LogQueue::Append].appends(), 33);
    let file_not_applied = engine.file_span(LogQueue::Append).1;
    assert_eq!(hook.0[&LogQueue::Append].applys(), 32);

    for i in 31..=40 {
        let region_id = (i as u64 - 1) % 2 + 1;
        append(
            &engine,
            region_id,
            (i as u64 + 1) / 2,
            (i as u64 + 1) / 2 + 1,
            Some(&data),
        );
        assert_eq!(hook.0[&LogQueue::Append].appends(), i + 3);
        assert_eq!(hook.0[&LogQueue::Append].applys(), i + 2);
    }

    // Can't purge because region 3 is not yet applied.
    engine.purge_expired_files().unwrap();
    let first = engine.file_span(LogQueue::Append).0;
    assert_eq!(file_not_applied, first);

    // Resume write on region 3.
    fail::remove(apply_memtable_region_3_fp);
    th.join().unwrap();

    std::thread::sleep(Duration::from_millis(200));
    engine.purge_expired_files().unwrap();
    let new_first = engine.file_span(LogQueue::Append).0;
    assert_ne!(file_not_applied, new_first);

    // Drop and then recover.
    drop(engine);

    let hook = Arc::new(Hook::default());
    let engine = Engine::open_with_listeners(cfg, vec![hook.clone()]).unwrap();
    assert_eq!(
        hook.0[&LogQueue::Append].files() as u64,
        engine.file_span(LogQueue::Append).1 - engine.file_span(LogQueue::Append).0 + 1
    );
    assert_eq!(
        hook.0[&LogQueue::Rewrite].files() as u64,
        engine.file_span(LogQueue::Rewrite).1 - engine.file_span(LogQueue::Rewrite).0 + 1
    );
}

#[test]
fn test_concurrent_write_empty_log_batch() {
    let dir = tempfile::Builder::new()
        .prefix("test_concurrent_write_empty_log_batch")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Arc::new(Engine::open(cfg.clone()).unwrap());
    let mut ctx = ConcurrentWriteContext::new(engine.clone());

    let some_entries = vec![
        Entry::new(),
        Entry {
            index: 1,
            ..Default::default()
        },
    ];

    ctx.write(LogBatch::default());
    let mut log_batch = LogBatch::default();
    log_batch
        .add_entries::<MessageExtTyped>(1, &some_entries)
        .unwrap();
    ctx.write(log_batch);
    ctx.join();

    let mut log_batch = LogBatch::default();
    log_batch
        .add_entries::<MessageExtTyped>(2, &some_entries)
        .unwrap();
    ctx.write(log_batch);
    ctx.write(LogBatch::default());
    ctx.join();
    drop(ctx);
    drop(engine);

    let engine = Engine::open(cfg).unwrap();
    let mut entries = Vec::new();
    engine
        .fetch_entries_to::<MessageExtTyped>(
            1,    /* region */
            0,    /* begin */
            2,    /* end */
            None, /* max_size */
            &mut entries,
        )
        .unwrap();
    assert_eq!(entries, some_entries);
    entries.clear();
    engine
        .fetch_entries_to::<MessageExtTyped>(
            2,    /* region */
            0,    /* begin */
            2,    /* end */
            None, /* max_size */
            &mut entries,
        )
        .unwrap();
    assert_eq!(entries, some_entries);
}

#[test]
fn test_consistency_tools() {
    let dir = tempfile::Builder::new()
        .prefix("test_consistency_tools")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize(128),
        ..Default::default()
    };
    let engine = Arc::new(Engine::open(cfg.clone()).unwrap());
    let data = vec![b'x'; 128];
    for index in 1..=100 {
        for rid in 1..=10 {
            let _f = if index == rid * rid {
                Some(FailGuard::new("log_batch::corrupted_items", "return"))
            } else {
                None
            };
            append(&engine, rid, index, index + 1, Some(&data));
        }
    }
    drop(engine);
    assert!(Engine::open(cfg.clone()).is_err());

    let ids = Engine::consistency_check(dir.path()).unwrap();
    for (id, index) in ids.iter() {
        assert_eq!(id * id, index + 1);
    }

    // Panic instead of err because `consistency_check` also removes corruptions.
    assert!(catch_unwind_silent(|| Engine::open(cfg.clone())).is_err());
}

#[cfg(feature = "scripting")]
#[test]
fn test_repair_tool() {
    let dir = tempfile::Builder::new()
        .prefix("test_repair_tool")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize(128),
        ..Default::default()
    };
    let engine = Arc::new(Engine::open(cfg.clone()).unwrap());
    let data = vec![b'x'; 128];
    for index in 1..=100 {
        for rid in 1..=10 {
            let _f = if index == rid * rid {
                Some(FailGuard::new("log_batch::corrupted_items", "return"))
            } else {
                None
            };
            append(&engine, rid, index, index + 1, Some(&data));
        }
    }
    drop(engine);

    assert!(Engine::open(cfg.clone()).is_err());
    let script = "".to_owned();
    assert!(Engine::unsafe_repair(dir.path(), None, script).is_err());
    let script = "
        fn filter_append(id, first, count, rewrite_count, queue, ifirst, ilast) {
            if first + count < ifirst {
                return 2; // discard existing
            }
            0 // default
        }
    "
    .to_owned();
    Engine::unsafe_repair(dir.path(), None, script).unwrap();
    Engine::open(cfg).unwrap();
}

#[test]
fn test_incomplete_purge() {
    let dir = tempfile::Builder::new()
        .prefix("test_incomplete_purge")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize(1),
        purge_threshold: ReadableSize(1),
        ..Default::default()
    };
    let rid = 1;
    let data = vec![b'7'; 1024];

    let engine = Engine::open(cfg.clone()).unwrap();

    {
        let _f = FailGuard::new("file_pipe_log::remove_file_failure", "return");
        append(&engine, rid, 0, 20, Some(&data));
        let append_first = engine.file_span(LogQueue::Append).0;
        engine.compact_to(rid, 18);
        engine.purge_expired_files().unwrap();
        assert!(engine.file_span(LogQueue::Append).0 > append_first);
    }

    // Create a hole.
    append(&engine, rid, 20, 40, Some(&data));
    let append_first = engine.file_span(LogQueue::Append).0;
    engine.compact_to(rid, 38);
    engine.purge_expired_files().unwrap();
    assert!(engine.file_span(LogQueue::Append).0 > append_first);

    append(&engine, rid, 40, 60, Some(&data));
    let append_first = engine.file_span(LogQueue::Append).0;
    drop(engine);

    let engine = Engine::open(cfg).unwrap();
    assert_eq!(engine.file_span(LogQueue::Append).0, append_first);
    assert_eq!(engine.first_index(rid).unwrap(), 38);
    assert_eq!(engine.last_index(rid).unwrap(), 59);
}

#[test]
fn test_tail_corruption() {
    let data = vec![b'x'; 16];
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let rid = 1;
    // Header is correct, record is corrupted.
    {
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption_1")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            format_version: 2,
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        let _f = FailGuard::new("log_batch::corrupted_items", "return");
        append(&engine, rid, 1, 5, Some(&data));
        drop(engine);
        let engine = Engine::open_with_file_system(cfg, fs.clone()).unwrap();
        assert_eq!(engine.first_index(rid), None);
    }
    // Header is corrupted.
    {
        let _f = FailGuard::new("log_file_header::corrupted", "return");
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption_2")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        drop(engine);
        Engine::open_with_file_system(cfg, fs.clone()).unwrap();
    }
    // Header is corrupted, followed by some records.
    {
        let _f = FailGuard::new("log_file_header::corrupted", "return");
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption_3")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        append(&engine, rid, 1, 5, Some(&data));
        drop(engine);
        assert!(Engine::open_with_file_system(cfg, fs).is_err());
    }
}
