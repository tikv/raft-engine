// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::sync::Arc;

use fail::FailGuard;
use raft::eraftpb::Entry;
use raft_engine::env::ObfuscatedFileSystem;
use raft_engine::internals::*;
use raft_engine::*;

use crate::util::*;

#[test]
fn test_file_open_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_file_open_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());

    {
        let _f = FailGuard::new("default_fs::create::err", "return");
        assert!(Engine::open_with_file_system(cfg.clone(), fs.clone()).is_err());
    }
    {
        let _f = FailGuard::new("default_fs::open::err", "return");
        let _ = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        assert!(Engine::open_with_file_system(cfg, fs).is_err());
    }
}

#[test]
fn test_file_read_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_file_read_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let entry = vec![b'x'; 1024];

    let engine = Engine::open_with_file_system(cfg, fs).unwrap();
    // Writing an empty message.
    engine
        .write(&mut generate_batch(1, 0, 1, None), true)
        .unwrap();
    engine
        .write(&mut generate_batch(2, 1, 10, Some(&entry)), true)
        .unwrap();
    let mut kv_batch = LogBatch::default();
    let entry_value = Entry {
        index: 111,
        data: entry.to_vec().into(),
        ..Default::default()
    };
    kv_batch
        .put_message(1, b"k".to_vec(), &entry_value)
        .unwrap();
    engine.write(&mut kv_batch, true).unwrap();

    let mut entries = Vec::new();
    let _f = FailGuard::new("log_file::read::err", "return");
    engine
        .fetch_entries_to::<MessageExtTyped>(1, 0, 1, None, &mut entries)
        .unwrap();
    engine.get_message::<Entry>(1, b"k".as_ref()).unwrap();
    engine
        .fetch_entries_to::<MessageExtTyped>(2, 1, 10, None, &mut entries)
        .unwrap_err();
}

#[test]
fn test_file_write_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_file_write_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(1024),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let entry = vec![b'x'; 1024];

    let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
    engine
        .write(&mut generate_batch(1, 1, 2, Some(&entry)), false)
        .unwrap();
    {
        let _f = FailGuard::new("log_file::write::err", "return");
        engine
            .write(&mut generate_batch(1, 2, 3, Some(&entry)), false)
            .unwrap_err();
    }
    {
        let _f = FailGuard::new("log_fd::sync::err", "return");
        engine
            .write(&mut generate_batch(1, 2, 3, Some(&entry)), false)
            .unwrap();
        assert!(catch_unwind_silent(|| {
            let _ = engine.write(&mut generate_batch(1, 3, 4, Some(&entry)), true);
        })
        .is_err());
    }

    // Internal states are consistent after panics. But outstanding writes are not
    // reverted.
    engine
        .write(&mut generate_batch(2, 1, 2, Some(&entry)), true)
        .unwrap();
    drop(engine);
    let engine = Engine::open_with_file_system(cfg, fs).unwrap();
    assert_eq!(engine.first_index(1).unwrap(), 1);
    assert_eq!(engine.last_index(1).unwrap(), 3);
    assert_eq!(engine.first_index(2).unwrap(), 1);
    assert_eq!(engine.last_index(2).unwrap(), 1);
}

#[test]
fn test_file_rotate_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_file_rotate_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(4),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let entry = vec![b'x'; 1024];

    let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
    engine
        .write(&mut generate_batch(1, 1, 2, Some(&entry)), false)
        .unwrap();
    engine
        .write(&mut generate_batch(1, 2, 3, Some(&entry)), false)
        .unwrap();
    engine
        .write(&mut generate_batch(1, 3, 4, Some(&entry)), false)
        .unwrap();
    engine
        .write(&mut generate_batch(1, 4, 5, Some(&entry)), false)
        .unwrap();
    assert_eq!(engine.file_span(LogQueue::Append).1, 1);
    // The next write will be followed by a rotate.
    {
        // Fail to sync old log file.
        let _f = FailGuard::new("log_fd::sync::err", "return");
        assert!(catch_unwind_silent(|| {
            let _ = engine.write(&mut generate_batch(1, 4, 5, Some(&entry)), false);
        })
        .is_err());
        assert_eq!(engine.file_span(LogQueue::Append).1, 1);
    }
    {
        // Fail to create new log file.
        let _f = FailGuard::new("default_fs::create::err", "return");
        assert!(engine
            .write(&mut generate_batch(1, 4, 5, Some(&entry)), false)
            .is_err());
        assert_eq!(engine.file_span(LogQueue::Append).1, 1);
    }
    {
        // Fail to write header of new log file.
        let _f = FailGuard::new("log_file::write::err", "1*off->return");
        assert!(engine
            .write(&mut generate_batch(1, 4, 5, Some(&entry)), false)
            .is_err());
        assert_eq!(engine.file_span(LogQueue::Append).1, 1);
    }
    {
        // Fail to sync new log file. The old log file is already sync-ed at this point.
        let _f = FailGuard::new("log_fd::sync::err", "return");
        assert!(catch_unwind_silent(|| {
            let _ = engine.write(&mut generate_batch(1, 4, 5, Some(&entry)), false);
        })
        .is_err());
        assert_eq!(engine.file_span(LogQueue::Append).1, 1);
    }

    // We can continue writing after the incidents.
    engine
        .write(&mut generate_batch(2, 1, 2, Some(&entry)), true)
        .unwrap();
    drop(engine);
    let engine = Engine::open_with_file_system(cfg, fs).unwrap();
    assert_eq!(engine.first_index(1).unwrap(), 1);
    assert_eq!(engine.last_index(1).unwrap(), 4);
    assert_eq!(engine.first_index(2).unwrap(), 1);
    assert_eq!(engine.last_index(2).unwrap(), 1);
}

#[test]
fn test_concurrent_write_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_concurrent_write_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(1024),
        ..Default::default()
    };
    let entry = vec![b'x'; 1024];

    // Don't use ObfuscatedFileSystem. It will split IO.
    let engine = Arc::new(Engine::open(cfg.clone()).unwrap());
    let mut ctx = ConcurrentWriteContext::new(engine.clone());

    // The second of three writes will fail.
    fail::cfg("log_file::write::err", "1*off->1*return->off").unwrap();
    let entry_clone = entry.clone();
    ctx.write_ext(move |e| {
        e.write(&mut generate_batch(1, 1, 11, Some(&entry_clone)), false)
            .unwrap();
    });
    let entry_clone = entry.clone();
    ctx.write_ext(move |e| {
        e.write(&mut generate_batch(2, 1, 11, Some(&entry_clone)), false)
            .unwrap_err();
    });
    let entry_clone = entry.clone();
    ctx.write_ext(move |e| {
        e.write(&mut generate_batch(3, 1, 11, Some(&entry_clone)), false)
            .unwrap();
    });
    ctx.join();

    assert_eq!(
        10,
        engine
            .fetch_entries_to::<MessageExtTyped>(1, 1, 11, None, &mut vec![])
            .unwrap()
    );
    assert_eq!(
        0,
        engine
            .fetch_entries_to::<MessageExtTyped>(2, 1, 11, None, &mut vec![])
            .unwrap()
    );
    assert_eq!(
        10,
        engine
            .fetch_entries_to::<MessageExtTyped>(3, 1, 11, None, &mut vec![])
            .unwrap()
    );

    {
        let _f1 = FailGuard::new("log_file::write::err", "return");
        let _f2 = FailGuard::new("log_file::truncate::err", "return");
        let entry_clone = entry.clone();
        ctx.write_ext(move |e| {
            catch_unwind_silent(|| {
                e.write(&mut generate_batch(1, 11, 21, Some(&entry_clone)), false)
            })
            .unwrap_err();
        });
        // We don't test followers, their panics are hard to catch.
        ctx.join();
    }

    // Internal states are consistent after panics.
    engine
        .write(&mut generate_batch(1, 11, 21, Some(&entry)), true)
        .unwrap();
    drop(ctx);
    drop(engine);

    let engine = Engine::open(cfg).unwrap();
    assert_eq!(
        20,
        engine
            .fetch_entries_to::<MessageExtTyped>(1, 1, 21, None, &mut vec![])
            .unwrap()
    );
}

#[test]
fn test_non_atomic_write_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_non_atomic_write_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(1024),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let entry = vec![b'x'; 1024];
    let rid = 1;

    {
        // Write partially succeeds. We can reopen.
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        let _f1 = FailGuard::new("log_file::write::err", "return");
        engine
            .write(&mut generate_batch(rid, 0, 1, Some(&entry)), true)
            .unwrap_err();
    }
    {
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        assert_eq!(engine.first_index(rid), None);
    }
    {
        // Write partially succeeds. We can overwrite.
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        let _f1 = FailGuard::new("log_file::write::err", "1*off->1*return->off");
        engine
            .write(&mut generate_batch(rid, 0, 1, Some(&entry)), true)
            .unwrap_err();
        engine
            .write(&mut generate_batch(rid, 5, 6, Some(&entry)), true)
            .unwrap();
        assert_eq!(engine.first_index(rid).unwrap(), 5);
    }
    {
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        assert_eq!(engine.first_index(rid).unwrap(), 5);
    }
    {
        // Write partially succeeds and can't be reverted. We panic.
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        let _f1 = FailGuard::new("log_file::write::err", "return");
        let _f2 = FailGuard::new("log_file::seek::err", "return");
        assert!(catch_unwind_silent(|| {
            engine
                .write(&mut generate_batch(rid, 6, 7, Some(&entry)), true)
                .unwrap_err();
        })
        .is_err());
    }
    {
        let engine = Engine::open_with_file_system(cfg, fs).unwrap();
        assert_eq!(engine.last_index(rid), Some(5));
    }
}

#[cfg(feature = "scripting")]
#[test]
fn test_error_during_repair() {
    let dir = tempfile::Builder::new()
        .prefix("test_error_during_repair")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize(1),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let entry = vec![b'x'; 1024];

    let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
    for rid in 1..=10 {
        engine
            .write(&mut generate_batch(rid, 1, 11, Some(&entry)), true)
            .unwrap();
    }
    drop(engine);

    let script = "
        fn filter_append(id, first, count, rewrite_count, queue, ifirst, ilast) {
            1 // discard incoming
        }
    "
    .to_owned();
    {
        let _f = FailGuard::new("log_file::write::err", "return");
        assert!(
            Engine::unsafe_repair_with_file_system(dir.path(), None, script, fs.clone()).is_err()
        );
    }
    let engine = Engine::open_with_file_system(cfg, fs).unwrap();
    for rid in 1..=10 {
        assert_eq!(
            10,
            engine
                .fetch_entries_to::<MessageExtTyped>(rid, 1, 11, None, &mut vec![])
                .unwrap()
        );
    }
}

#[cfg(all(feature = "swap", feature = "internals"))]
#[test]
fn test_swappy_page_create_error() {
    use raft_engine::internals::SwappyAllocator;
    let dir = tempfile::Builder::new()
        .prefix("test_swappy_page_create_error")
        .tempdir()
        .unwrap();

    let allocator = SwappyAllocator::new(dir.path(), 0);

    let mut vec: Vec<u8, _> = Vec::new_in(allocator.clone());
    {
        let _f = FailGuard::new("swappy::page::new_failure", "return");
        vec.resize(128, 0);
        assert_eq!(allocator.memory_usage(), 128);
    }
    vec.resize(1024, 0);
    assert_eq!(allocator.memory_usage(), 0);
}

#[test]
fn test_file_allocate_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_file_allocate_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::mb(100),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let entry = vec![b'x'; 1024];
    {
        let _f = FailGuard::new("log_file::allocate::err", "return");
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        engine
            .write(&mut generate_batch(1, 1, 5, Some(&entry)), true)
            .unwrap();
    }
    let engine = Engine::open_with_file_system(cfg, fs).unwrap();
    assert_eq!(engine.first_index(1).unwrap(), 1);
    assert_eq!(engine.last_index(1).unwrap(), 4);
}

#[test]
fn test_start_with_recycled_file_allocate_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_start_with_recycled_file_allocate_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize::kb(1),
        purge_threshold: ReadableSize::kb(10), // capacity is 12
        enable_log_recycle: true,
        prefill_for_recycle: true,
        ..Default::default()
    };
    let entry = vec![b'x'; 1024];
    // Mock that the engine starts with the circumstance where
    // the pref-reserved file with seqno[5] failed to be generated.
    {
        let _f = FailGuard::new("log_file::write::zero", "4*off->1*return->off");
        Engine::open(cfg.clone()).unwrap();
    }
    // Extra recycled files have been supplemented.
    let engine = Engine::open(cfg).unwrap();
    engine
        .write(&mut generate_batch(1, 1, 5, Some(&entry)), true)
        .unwrap();
    let (start, end) = engine.file_span(LogQueue::Append);
    assert_eq!(start, end);
    // Append several entries to make Engine reuse the recycled logs.
    for r in 2..6 {
        engine
            .write(&mut generate_batch(r, 1, 5, Some(&entry)), true)
            .unwrap();
    }
    let (reused_start, reused_end) = engine.file_span(LogQueue::Append);
    assert_eq!((reused_start, reused_end), (1, 5));
    assert!(reused_end > end);
    assert_eq!(engine.first_index(1).unwrap(), 1);
    assert_eq!(engine.last_index(1).unwrap(), 4);
    assert_eq!(engine.last_index(5).unwrap(), 4);
    let mut entries = Vec::new();
    engine
        .fetch_entries_to::<MessageExtTyped>(5, 1, 5, None, &mut entries)
        .unwrap();
    // Continously append entries to reach the purge_threshold.
    for r in 6..=15 {
        engine
            .write(&mut generate_batch(r, 1, 5, Some(&entry)), true)
            .unwrap();
    }
    assert_eq!(engine.file_span(LogQueue::Append).0, reused_start);
    assert!(engine.file_span(LogQueue::Append).1 > reused_end);
    let (start, _) = engine.file_span(LogQueue::Append);
    // Purge and check.
    engine.purge_expired_files().unwrap();
    assert!(engine.file_span(LogQueue::Append).0 > start);
}

#[test]
fn test_no_space_write_error() {
    let mut cfg_list = [
        Config {
            target_file_size: ReadableSize::kb(2),
            format_version: Version::V1,
            enable_log_recycle: false,
            ..Default::default()
        },
        Config {
            target_file_size: ReadableSize::kb(2),
            format_version: Version::V2,
            enable_log_recycle: true,
            ..Default::default()
        },
    ];
    let entry = vec![b'x'; 1024];
    for cfg in cfg_list.iter_mut() {
        let dir = tempfile::Builder::new()
            .prefix("test_no_space_write_error_main")
            .tempdir()
            .unwrap();
        let spill_dir = tempfile::Builder::new()
            .prefix("test_no_space_write_error_spill")
            .tempdir()
            .unwrap();
        cfg.dir = dir.path().to_str().unwrap().to_owned();
        cfg.spill_dir = Some(spill_dir.path().to_str().unwrap().to_owned());
        {
            // Case 1: `Write` is abnormal for no space left, Engine should fail at
            // `rotate`.
            let cfg_err = Config {
                target_file_size: ReadableSize(1),
                ..cfg.clone()
            };
            let engine = Engine::open(cfg_err).unwrap();
            let _f = FailGuard::new("log_fd::write::no_space_err", "return");
            assert!(engine
                .write(&mut generate_batch(2, 11, 21, Some(&entry)), true)
                .is_err());
            assert_eq!(
                0,
                engine
                    .fetch_entries_to::<MessageExtTyped>(2, 11, 21, None, &mut vec![])
                    .unwrap()
            );
        }
        {
            let engine = Engine::open(cfg.clone()).unwrap();
            // Case 2: disk goes from `full(nospace err)` -> `spare for writing`.
            let _f1 = FailGuard::new("log_fd::write::no_space_err", "2*return->off");
            let _f2 = FailGuard::new("file_pipe_log::force_choose_dir", "return");
            // The first write should fail, because all dirs run out of space for writing.
            assert!(engine
                .write(&mut generate_batch(2, 11, 21, Some(&entry)), true)
                .is_err());
            assert_eq!(
                0,
                engine
                    .fetch_entries_to::<MessageExtTyped>(2, 11, 21, None, &mut vec![])
                    .unwrap()
            );
            // The second write should success, as there exists free space for later writing
            // after cleaning up.
            engine
                .write(&mut generate_batch(3, 11, 21, Some(&entry)), true)
                .unwrap();
            assert_eq!(
                10,
                engine
                    .fetch_entries_to::<MessageExtTyped>(3, 11, 21, None, &mut vec![])
                    .unwrap()
            );
        }
        {
            // Case 3: disk status -- `main dir is full (has nospace err)` -> `spill-dir
            // is spare (has enough space)`.
            let engine = Engine::open(cfg.clone()).unwrap();
            let _f1 = FailGuard::new("log_fd::write::no_space_err", "1*return->off");
            let _f2 = FailGuard::new("file_pipe_log::force_choose_dir", "return(1)");
            engine
                .write(&mut generate_batch(5, 11, 21, Some(&entry)), true)
                .unwrap();
            engine
                .write(&mut generate_batch(6, 11, 21, Some(&entry)), true)
                .unwrap();
            assert_eq!(
                10,
                engine
                    .fetch_entries_to::<MessageExtTyped>(5, 11, 21, None, &mut vec![])
                    .unwrap()
            );
            assert_eq!(
                10,
                engine
                    .fetch_entries_to::<MessageExtTyped>(6, 11, 21, None, &mut vec![])
                    .unwrap()
            );
        }
        {
            // Case 4: disk status -- `main dir has free space for rotating new files
            // but no space for dumping LogBatch`, disk goes into endless `spare(nospace
            // err)`, engine do panic for multi-retrying.
            let engine = Engine::open(cfg.clone()).unwrap();
            let _f = FailGuard::new(
                "log_fd::write::no_space_err",
                "1*return->1*off->1*return->1*off",
            );
            assert!(engine
                .write(&mut generate_batch(7, 11, 21, Some(&entry)), true)
                .is_err());
            assert_eq!(
                0,
                engine
                    .fetch_entries_to::<MessageExtTyped>(7, 11, 21, None, &mut vec![])
                    .unwrap()
            );
        }
    }
}
