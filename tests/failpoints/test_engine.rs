// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Barrier};
use std::time::Duration;

use fail::FailGuard;
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
    assert_eq!(hook.0[&LogQueue::Append].files(), 10);

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
    assert_eq!(hook.0[&LogQueue::Append].purged(), 14);
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
        let _f = FailGuard::new("default_fs::delete_skipped", "return");
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
            format_version: Version::V2,
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
        Engine::open_with_file_system(cfg, fs.clone()).unwrap();
    }
    // Version::V1 in header owns abnormal DataLayout.
    {
        let _f = FailGuard::new("log_file_header::too_large", "return");
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption_4")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize(1),
            purge_threshold: ReadableSize(1),
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        drop(engine);
        // Version::V1 will be parsed successfully as the data_layout when the related
        // `version == V1` will be ignored.
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        append(&engine, rid, 1, 5, Some(&data));
        drop(engine);
        Engine::open_with_file_system(cfg, fs.clone()).unwrap();
    }
    // DataLayout in header is corrupted for Version::V2
    {
        let _f = FailGuard::new("log_file_header::too_small", "return");
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption_5")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            format_version: Version::V2,
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        drop(engine);
        Engine::open_with_file_system(cfg, fs.clone()).unwrap();
    }
    // DataLayout in header is abnormal for Version::V2
    {
        let _f = FailGuard::new("log_file_header::too_large", "return");
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption_6")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            format_version: Version::V2,
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        drop(engine);
        Engine::open_with_file_system(cfg, fs.clone()).unwrap();
    }
    // DataLayout in header is corrupted for Version::V2, followed with records
    {
        let _f = FailGuard::new("log_file_header::too_small", "return");
        let dir = tempfile::Builder::new()
            .prefix("test_tail_corruption_7")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            target_file_size: ReadableSize(1),
            purge_threshold: ReadableSize(1),
            format_version: Version::V2,
            ..Default::default()
        };
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        drop(engine);
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        append(&engine, rid, 1, 2, Some(&data));
        append(&engine, rid, 2, 3, Some(&data));
        drop(engine);
        assert!(Engine::open_with_file_system(cfg, fs).is_err());
    }
}

#[test]
fn test_concurrent_write_perf_context() {
    let dir = tempfile::Builder::new()
        .prefix("test_concurrent_write_perf_context")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };

    let some_entries = vec![
        Entry::new(),
        Entry {
            index: 1,
            ..Default::default()
        },
    ];

    let engine = Arc::new(Engine::open(cfg).unwrap());
    let barrier = Arc::new(Barrier::new(4));

    let ths: Vec<_> = (1..=3)
        .map(|i| {
            let engine = engine.clone();
            let barrier = barrier.clone();
            let some_entries = some_entries.clone();
            std::thread::spawn(move || {
                barrier.wait();
                let mut log_batch = LogBatch::default();
                log_batch
                    .add_entries::<MessageExtTyped>(i, &some_entries)
                    .unwrap();
                let old_perf_context = get_perf_context();
                engine.write(&mut log_batch, true).unwrap();
                let new_perf_context = get_perf_context();
                (old_perf_context, new_perf_context)
            })
        })
        .collect();

    fail::cfg_callback("write_barrier::leader_exit", move || {
        barrier.wait();
        // Sleep a while until new writers enter the next write group.
        std::thread::sleep(Duration::from_millis(100));
        fail::remove("write_barrier::leader_exit");
    })
    .unwrap();

    let mut log_batch = LogBatch::default();
    log_batch
        .add_entries::<MessageExtTyped>(4, &some_entries)
        .unwrap();
    engine.write(&mut log_batch, true).unwrap();

    for th in ths {
        let (old, new) = th.join().unwrap();
        assert_ne!(old.log_populating_duration, new.log_populating_duration);
        assert_ne!(old.write_wait_duration, new.write_wait_duration);
        assert_ne!(old.log_write_duration, new.log_write_duration);
        assert_ne!(old.apply_duration, new.apply_duration);
    }
}

// FIXME: this test no longer works because recovery cannot reliably detect
// overwrite anomaly.
// See https://github.com/tikv/raft-engine/issues/250
#[test]
#[should_panic]
fn test_recycle_with_stale_logbatch_at_tail() {
    let dir = tempfile::Builder::new()
        .prefix("test_recycle_with_stale_log_batch_at_tail")
        .tempdir()
        .unwrap();
    let data = vec![b'x'; 1024];
    let rid = 1;
    let cfg_err = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(2),
        purge_threshold: ReadableSize::kb(4),
        enable_log_recycle: true,
        format_version: Version::V1,
        ..Default::default()
    };
    // Force open Engine with `enable_log_recycle == true` and
    // `format_version == Version::V1`.
    let engine = {
        let _f = FailGuard::new("pipe_log::version::force_enable_log_signing", "return");
        Engine::open(cfg_err.clone()).unwrap()
    };
    // Do not truncate the active_file when exit
    let _f = FailGuard::new("file_pipe_log::log_file_writer::skip_truncate", "return");
    assert_eq!(cfg_err.format_version, Version::V1);
    append(&engine, rid, 1, 2, Some(&data)); // file_seq: 1
    append(&engine, rid, 2, 3, Some(&data));
    append(&engine, rid, 3, 4, Some(&data)); // file_seq: 2
    append(&engine, rid, 4, 5, Some(&data));
    append(&engine, rid, 5, 6, Some(&data)); // file_seq: 3
    let append_first = engine.file_span(LogQueue::Append).0;
    engine.compact_to(rid, 3);
    engine.purge_expired_files().unwrap();
    assert!(engine.file_span(LogQueue::Append).0 > append_first);
    // append, written into seq: 3
    append(&engine, rid, 4, 5, Some(&data));
    // recycle, written into seq: 1
    append(&engine, rid, 5, 6, Some(&data));
    drop(engine);
    // Recover the engine with invalid Version::default().
    // Causing the final log file is a recycled file, containing rewritten
    // LogBatchs and end with stale LogBatchs, `Engine::open(...)` should
    // `panic` when recovering the relate `Memtable`.
    assert!(catch_unwind_silent(|| {
        let cfg_v2 = Config {
            format_version: Version::V2,
            ..cfg_err
        };
        Engine::open(cfg_v2)
    })
    .is_err());
}

#[test]
fn test_build_engine_with_multi_datalayout() {
    let dir = tempfile::Builder::new()
        .prefix("test_build_engine_with_multi_datalayout")
        .tempdir()
        .unwrap();
    let data = vec![b'x'; 12827];
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(2),
        purge_threshold: ReadableSize::kb(4),
        recovery_mode: RecoveryMode::AbsoluteConsistency,
        ..Default::default()
    };
    // Defaultly, File with DataLayout::NoAlignment.
    let engine = Engine::open(cfg.clone()).unwrap();
    for rid in 1..=3 {
        append(&engine, rid, 1, 11, Some(&data));
    }
    drop(engine);
    // File with DataLayout::Alignment
    let _f = FailGuard::new("file_pipe_log::open::force_set_alignment", "return");
    let cfg_v2 = Config {
        format_version: Version::V2,
        ..cfg
    };
    let engine = Engine::open(cfg_v2.clone()).unwrap();
    for rid in 1..=3 {
        append(&engine, rid, 11, 20, Some(&data));
    }
    drop(engine);
    Engine::open(cfg_v2).unwrap();
}

#[test]
fn test_build_engine_with_datalayout_abnormal() {
    let dir = tempfile::Builder::new()
        .prefix("test_build_engine_with_datalayout_abnormal")
        .tempdir()
        .unwrap();
    let data = vec![b'x'; 1024];
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(2),
        purge_threshold: ReadableSize::kb(4),
        recovery_mode: RecoveryMode::AbsoluteConsistency,
        format_version: Version::V2,
        ..Default::default()
    };
    let _f = FailGuard::new("file_pipe_log::open::force_set_alignment", "return");
    let engine = Engine::open(cfg.clone()).unwrap();
    // Content durable with DataLayout::Alignment.
    append(&engine, 1, 1, 11, Some(&data));
    append(&engine, 2, 1, 11, Some(&data));
    {
        // Set failpoint to dump content with invalid paddings into log file.
        let _f1 = FailGuard::new("file_pipe_log::append::corrupted_padding", "return");
        append(&engine, 3, 1, 11, Some(&data));
        drop(engine);
        assert!(Engine::open(cfg.clone()).is_err());
    }
    {
        // Reopen the Engine with TolerateXXX mode.
        let mut cfg_v2 = cfg.clone();
        cfg_v2.recovery_mode = RecoveryMode::TolerateTailCorruption;
        let engine = Engine::open(cfg_v2).unwrap();
        for rid in 4..=8 {
            append(&engine, rid, 1, 11, Some(&data));
        }
        drop(engine);
        Engine::open(cfg).unwrap();
    }
}

// issue-228
#[test]
fn test_partial_rewrite_rewrite() {
    let dir = tempfile::Builder::new()
        .prefix("test_partial_rewrite_rewrite")
        .tempdir()
        .unwrap();
    let _f = FailGuard::new("max_rewrite_batch_bytes", "return(1)");
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        recovery_threads: 1,
        ..Default::default()
    };
    let engine = Engine::open(cfg.clone()).unwrap();
    let data = vec![b'x'; 128];

    for rid in 1..=3 {
        append(&engine, rid, 1, 5, Some(&data));
        append(&engine, rid, 5, 11, Some(&data));
    }

    let old_active_file = engine.file_span(LogQueue::Append).1;
    engine.purge_manager().must_rewrite_append_queue(None, None);
    assert_eq!(engine.file_span(LogQueue::Append).0, old_active_file + 1);

    for rid in 1..=3 {
        append(&engine, rid, 11, 16, Some(&data));
    }

    {
        let _f = FailGuard::new("log_fd::write::err", "10*off->return->off");
        assert!(
            catch_unwind_silent(|| engine.purge_manager().must_rewrite_rewrite_queue()).is_err()
        );
    }

    drop(engine);
    let engine = Engine::open(cfg).unwrap();
    for rid in 1..=3 {
        assert_eq!(engine.first_index(rid).unwrap(), 1);
        assert_eq!(engine.last_index(rid).unwrap(), 15);
    }
}

#[test]
fn test_partial_rewrite_rewrite_online() {
    let dir = tempfile::Builder::new()
        .prefix("test_partial_rewrite_rewrite_online")
        .tempdir()
        .unwrap();
    let _f = FailGuard::new("max_rewrite_batch_bytes", "return(1)");
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg.clone()).unwrap();
    let data = vec![b'x'; 128];

    for rid in 1..=3 {
        append(&engine, rid, 1, 5, Some(&data));
        append(&engine, rid, 5, 11, Some(&data));
    }

    let old_active_file = engine.file_span(LogQueue::Append).1;
    engine.purge_manager().must_rewrite_append_queue(None, None);
    assert_eq!(engine.file_span(LogQueue::Append).0, old_active_file + 1);

    {
        let _f = FailGuard::new("log_fd::write::err", "10*off->return->off");
        assert!(
            catch_unwind_silent(|| engine.purge_manager().must_rewrite_rewrite_queue()).is_err()
        );
    }

    for rid in 1..=3 {
        append(&engine, rid, 11, 16, Some(&data));
    }
    let old_active_file = engine.file_span(LogQueue::Append).1;
    engine.purge_manager().must_rewrite_append_queue(None, None);
    assert_eq!(engine.file_span(LogQueue::Append).0, old_active_file + 1);

    drop(engine);
    let engine = Engine::open(cfg).unwrap();
    for rid in 1..=3 {
        assert_eq!(engine.first_index(rid).unwrap(), 1);
        assert_eq!(engine.last_index(rid).unwrap(), 15);
    }
}

fn test_split_rewrite_batch_imp(regions: u64, region_size: u64, split_size: u64, file_size: u64) {
    let dir = tempfile::Builder::new()
        .prefix("test_split_rewrite_batch")
        .tempdir()
        .unwrap();
    let _f1 = FailGuard::new("max_rewrite_batch_bytes", &format!("return({split_size})"));
    let _f2 = FailGuard::new("force_use_atomic_group", "return");

    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize(file_size),
        batch_compression_threshold: ReadableSize(0),
        ..Default::default()
    };
    let engine = Engine::open(cfg.clone()).unwrap();
    let data = vec![b'x'; region_size as usize / 10];

    for rid in 1..=regions {
        append(&engine, rid, 1, 5, Some(&data));
        append(&engine, rid, 5, 11, Some(&data));
    }

    let old_active_file = engine.file_span(LogQueue::Append).1;
    engine.purge_manager().must_rewrite_append_queue(None, None);
    assert_eq!(engine.file_span(LogQueue::Append).0, old_active_file + 1);

    drop(engine);
    let engine = Engine::open(cfg.clone()).unwrap();
    for rid in 1..=regions {
        assert_eq!(engine.first_index(rid).unwrap(), 1);
        assert_eq!(engine.last_index(rid).unwrap(), 10);
    }

    for rid in 1..=regions {
        append(&engine, rid, 11, 16, Some(&data));
    }
    let old_active_file = engine.file_span(LogQueue::Append).1;
    engine.purge_manager().must_rewrite_append_queue(None, None);
    assert_eq!(engine.file_span(LogQueue::Append).0, old_active_file + 1);
    drop(engine);

    for i in 1..=10 {
        let engine = Engine::open(cfg.clone()).unwrap();
        let count = AtomicU64::new(0);
        fail::cfg_callback("atomic_group::begin", move || {
            if count.fetch_add(1, Ordering::Relaxed) + 1 == i {
                fail::cfg("log_fd::write::err", "return").unwrap();
            }
        })
        .unwrap();
        let r = catch_unwind_silent(|| engine.purge_manager().must_rewrite_rewrite_queue());
        fail::remove("atomic_group::begin");
        fail::remove("log_fd::write::err");
        if r.is_ok() {
            break;
        }
    }
    for i in 1..=10 {
        let engine = Engine::open(cfg.clone()).unwrap();
        for rid in 1..=regions {
            assert_eq!(engine.first_index(rid).unwrap(), 1);
            assert_eq!(engine.last_index(rid).unwrap(), 15);
        }
        let count = AtomicU64::new(0);
        fail::cfg_callback("atomic_group::add", move || {
            if count.fetch_add(1, Ordering::Relaxed) + 1 == i {
                fail::cfg("log_fd::write::err", "return").unwrap();
            }
        })
        .unwrap();
        let r = catch_unwind_silent(|| engine.purge_manager().must_rewrite_rewrite_queue());
        fail::remove("atomic_group::begin");
        fail::remove("log_fd::write::err");
        if r.is_ok() {
            break;
        }
    }
    let engine = Engine::open(cfg).unwrap();
    for rid in 1..=regions {
        assert_eq!(engine.first_index(rid).unwrap(), 1);
        assert_eq!(engine.last_index(rid).unwrap(), 15);
    }
}

#[test]
fn test_split_rewrite_batch() {
    test_split_rewrite_batch_imp(10, 40960, 1, 1);
    test_split_rewrite_batch_imp(10, 40960, 1, 40960 * 2);
    test_split_rewrite_batch_imp(25, 4096, 6000, 40960 * 2);
}

#[test]
fn test_split_rewrite_batch_with_only_kvs() {
    let dir = tempfile::Builder::new()
        .prefix("test_split_rewrite_batch_with_only_kvs")
        .tempdir()
        .unwrap();
    let _f = FailGuard::new("max_rewrite_batch_bytes", "return(1)");
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let engine = Engine::open(cfg.clone()).unwrap();
    let mut log_batch = LogBatch::default();
    let key = vec![b'x'; 2];
    let value = vec![b'y'; 8];

    let mut rid = 1;
    {
        log_batch.put(rid, key.clone(), Vec::new()).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        log_batch.put(rid, key.clone(), value.clone()).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        engine.purge_manager().must_rewrite_rewrite_queue();

        rid += 1;
        log_batch.put(rid, key.clone(), value.clone()).unwrap();
        rid += 1;
        log_batch.put(rid, key.clone(), value.clone()).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        engine.purge_manager().must_rewrite_rewrite_queue();
    }
    {
        let _f = FailGuard::new("force_use_atomic_group", "return");
        log_batch.put(rid, key.clone(), Vec::new()).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        log_batch.put(rid, key.clone(), value.clone()).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        engine.purge_manager().must_rewrite_rewrite_queue();

        rid += 1;
        log_batch.put(rid, key.clone(), value.clone()).unwrap();
        rid += 1;
        log_batch.put(rid, key.clone(), value.clone()).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        engine.purge_manager().must_rewrite_rewrite_queue();
    }

    drop(engine);
    let engine = Engine::open(cfg).unwrap();
    for i in 1..=rid {
        assert_eq!(engine.get(i, &key).unwrap(), value);
    }
}

#[test]
fn test_build_engine_with_recycling_and_multi_dirs() {
    let dir = tempfile::Builder::new()
        .prefix("test_build_engine_with_multi_dirs_main")
        .tempdir()
        .unwrap();
    let spill_dir = tempfile::Builder::new()
        .prefix("test_build_engine_with_multi_dirs_spill")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        spill_dir: Some(spill_dir.path().to_str().unwrap().to_owned()),
        target_file_size: ReadableSize::kb(1),
        purge_threshold: ReadableSize::kb(20),
        enable_log_recycle: true,
        prefill_for_recycle: true,
        ..Default::default()
    };
    let data = vec![b'x'; 1024];
    {
        // Prerequisite - case 1: all disks are full, Engine can be opened normally.
        {
            // Multi directories.
            let _f = FailGuard::new("file_pipe_log::force_no_spare_space", "return");
            Engine::open(cfg.clone()).unwrap();
            // Single diretory - spill-dir is None.
            let cfg_single_dir = Config {
                spill_dir: None,
                ..cfg.clone()
            };
            Engine::open(cfg_single_dir).unwrap();
        }
        // Prerequisite - case 2: all disks are full after writing, and the current
        // engine should be available for `read`.
        {
            let cfg_no_prefill = Config {
                prefill_for_recycle: false,
                ..cfg.clone()
            };
            let engine = Engine::open(cfg_no_prefill.clone()).unwrap();
            engine
                .write(&mut generate_batch(101, 11, 21, Some(&data)), true)
                .unwrap();
            drop(engine);
            let _f1 = FailGuard::new("file_pipe_log::force_no_spare_space", "return");
            let _f2 = FailGuard::new("log_fd::write::no_space_err", "return");
            let engine = Engine::open(cfg_no_prefill).unwrap();
            assert_eq!(
                10,
                engine
                    .fetch_entries_to::<MessageExtTyped>(101, 11, 21, None, &mut vec![])
                    .unwrap()
            );
        }
        // Prerequisite - case 3: prefill several recycled logs but no space for
        // remains, making prefilling progress exit in advance.
        {
            let _f1 = FailGuard::new(
                "file_pipe_log::force_choose_dir",
                "10*return(0)->5*return(1)",
            );
            let _f2 = FailGuard::new("file_pipe_log::force_no_spare_space", "return");
            let _f3 = FailGuard::new("log_fd::write::no_space_err", "return");
            let _ = Engine::open(cfg.clone()).unwrap();
        }
        // Clean-up the env for later testing.
        let cfg_err = Config {
            enable_log_recycle: false,
            prefill_for_recycle: false,
            ..cfg.clone()
        };
        let _ = Engine::open(cfg_err).unwrap();
    }
    {
        // Case 1: prefill recycled logs into multi-dirs (when preparing recycled logs,
        // this circumstance also equals to `main dir is full, but spill-dir
        // is free`.)
        let engine = {
            let _f = FailGuard::new("file_pipe_log::force_choose_dir", "10*return(0)->return(1)");
            Engine::open(cfg.clone()).unwrap()
        };
        for rid in 1..10 {
            append(&engine, rid, 1, 5, Some(&data));
        }
        let append_first = engine.file_span(LogQueue::Append).0;
        for rid in 1..10 {
            engine.compact_to(rid, 3);
        }
        // Purge do not exceed purge_threshold, and first active file_seq won't change.
        engine.purge_expired_files().unwrap();
        assert_eq!(engine.file_span(LogQueue::Append).0, append_first);
        for rid in 1..20 {
            append(&engine, rid, 3, 5, Some(&data));
            engine.compact_to(rid, 4);
        }
        // Purge obsolete logs.
        engine.purge_expired_files().unwrap();
        assert!(engine.file_span(LogQueue::Append).0 > append_first);
    }
    {
        // Case 2: prefill is on but no spare space for new log files.
        let _f = FailGuard::new("file_pipe_log::force_no_spare_space", "return");
        let engine = Engine::open(cfg.clone()).unwrap();
        let append_end = engine.file_span(LogQueue::Append).1;
        // As there still exists several recycled logs for incoming writes, so the
        // following writes will success.
        for rid in 1..10 {
            append(&engine, rid, 5, 7, Some(&data));
        }
        assert!(engine.file_span(LogQueue::Append).1 > append_end);
    }
    {
        // Case 3: no prefill and no spare space for new log files.
        let cfg_no_prefill = Config {
            enable_log_recycle: true,
            prefill_for_recycle: false,
            ..cfg
        };
        let _f1 = FailGuard::new("file_pipe_log::force_no_spare_space", "return");
        let engine = Engine::open(cfg_no_prefill).unwrap();
        let _f2 = FailGuard::new("log_fd::write::no_space_err", "return");
        let (append_first, append_end) = engine.file_span(LogQueue::Append);
        // Cannot append new data into engine as no spare space.
        for rid in 1..20 {
            assert!(catch_unwind_silent(|| append(&engine, rid, 8, 9, Some(&data))).is_err());
        }
        assert_eq!(
            engine.file_span(LogQueue::Append),
            (append_first, append_end)
        );
    }
}
