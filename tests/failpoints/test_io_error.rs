// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::sync::Arc;

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
        let _f = FailGuard::new("log_fd::create::err", "return");
        assert!(Engine::open_with_file_system(cfg.clone(), fs.clone()).is_err());
    }

    {
        let _f = FailGuard::new("log_fd::open::err", "return");
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
    let _f = FailGuard::new("log_fd::read::err", "return");
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
        bytes_per_sync: ReadableSize::kb(1024),
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
        let _f = FailGuard::new("log_fd::write::err", "return");
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
        bytes_per_sync: ReadableSize::kb(1024),
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
        let _f = FailGuard::new("log_fd::create::err", "return");
        assert!(catch_unwind_silent(|| {
            let _ = engine.write(&mut generate_batch(1, 4, 5, Some(&entry)), false);
        })
        .is_err());
        assert_eq!(engine.file_span(LogQueue::Append).1, 1);
    }
    {
        // Fail to write header of new log file.
        let _f = FailGuard::new("log_fd::write::err", "1*off->return");
        assert!(catch_unwind_silent(|| {
            let _ = engine.write(&mut generate_batch(1, 4, 5, Some(&entry)), false);
        })
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
        bytes_per_sync: ReadableSize::kb(1024),
        target_file_size: ReadableSize::kb(1024),
        ..Default::default()
    };
    let entry = vec![b'x'; 1024];

    // Don't use ObfuscatedFileSystem. It will split IO.
    let engine = Arc::new(Engine::open(cfg.clone()).unwrap());
    let mut ctx = ConcurrentWriteContext::new(engine.clone());

    // The second of three writes will fail.
    fail::cfg("log_fd::write::err", "1*off->1*return->off").unwrap();
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
        let _f1 = FailGuard::new("log_fd::write::err", "return");
        let _f2 = FailGuard::new("log_fd::truncate::err", "return");
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
fn test_no_space_write_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_no_space_write_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        target_file_size: ReadableSize(2048),
        ..Default::default()
    };
    let entry = vec![b'x'; 1024];
    {
        // If disk is full, a new Engine cannot be opened.
        let _f = FailGuard::new("file_pipe_log::force_no_free_space", "return");
        assert!(Engine::open(cfg.clone()).is_err());
    }
    {
        // If disk is full after writing, the old engine should be available
        // for `read`.
        let engine = Engine::open(cfg.clone()).unwrap();
        engine
            .write(&mut generate_batch(1, 11, 21, Some(&entry)), true)
            .unwrap();
        drop(engine);
        let _f = FailGuard::new("file_pipe_log::force_no_free_space", "return");
        let engine = Engine::open(cfg.clone()).unwrap();
        assert_eq!(
            10,
            engine
                .fetch_entries_to::<MessageExtTyped>(1, 11, 21, None, &mut vec![])
                .unwrap()
        );
    }
    {
        // `Write` is abnormal for no space left, Engine should panic at `rotate`.
        let _f = FailGuard::new("log_fd::write::no_space_err", "return");
        let cfg_err = Config {
            target_file_size: ReadableSize(1),
            ..cfg.clone()
        };
        let engine = Engine::open(cfg_err).unwrap();
        assert!(catch_unwind_silent(|| {
            engine
                .write(&mut generate_batch(2, 11, 21, Some(&entry)), true)
                .unwrap_err();
        })
        .is_err());
    }
    {
        // Disk goes from `spare(nospace err)` -> `full` -> `spare`.
        let _f1 = FailGuard::new("file_pipe_log::force_no_free_space", "1*off->1*return->off");
        let _f2 = FailGuard::new("log_fd::write::no_space_err", "1*return->off");
        let engine = Engine::open(cfg.clone()).unwrap();
        assert!(catch_unwind_silent(|| {
            engine
                .write(&mut generate_batch(2, 11, 21, Some(&entry)), true)
                .unwrap_err();
        })
        .is_err());
        engine
            .write(&mut generate_batch(3, 11, 21, Some(&entry)), true)
            .unwrap();
        assert_eq!(
            0,
            engine
                .fetch_entries_to::<MessageExtTyped>(2, 11, 21, None, &mut vec![])
                .unwrap()
        );
        assert_eq!(
            10,
            engine
                .fetch_entries_to::<MessageExtTyped>(3, 11, 21, None, &mut vec![])
                .unwrap()
        );
    }
    {
        // Disk is `full` -> `spare`, the first `write` operation should failed.
        let _f1 = FailGuard::new("file_pipe_log::force_no_free_space", "1*return->off");
        let _f2 = FailGuard::new("log_fd::write::no_space_err", "1*return->off");
        let cfg_err = Config {
            target_file_size: ReadableSize(1),
            ..cfg.clone()
        };
        let engine = Engine::open(cfg_err).unwrap();
        engine
            .write(&mut generate_batch(4, 11, 21, Some(&entry)), true)
            .unwrap_err();
        engine
            .write(&mut generate_batch(4, 11, 21, Some(&entry)), true)
            .unwrap();
        assert_eq!(
            0,
            engine
                .fetch_entries_to::<MessageExtTyped>(2, 11, 21, None, &mut vec![])
                .unwrap()
        );
        assert_eq!(
            10,
            engine
                .fetch_entries_to::<MessageExtTyped>(4, 11, 21, None, &mut vec![])
                .unwrap()
        );
    }
    {
        // Disk goes from `spare(nospace err)` -> `spare(another dir has enough space)`.
        let _f = FailGuard::new("log_fd::write::no_space_err", "1*return->off");
        let engine = Engine::open(cfg.clone()).unwrap();
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
        // Disk goes into endless `spare(nospace err)`, engine do panic for multi-
        // retrying.
        let _f = FailGuard::new(
            "log_fd::write::no_space_err",
            "1*return->1*off->1*return->1*off",
        );
        let engine = Engine::open(cfg).unwrap();
        assert!(catch_unwind_silent(|| {
            engine
                .write(&mut generate_batch(7, 11, 21, Some(&entry)), true)
                .unwrap_err();
        })
        .is_err());
        assert_eq!(
            0,
            engine
                .fetch_entries_to::<MessageExtTyped>(7, 11, 21, None, &mut vec![])
                .unwrap()
        );
    }
}

#[test]
fn test_non_atomic_write_error() {
    let dir = tempfile::Builder::new()
        .prefix("test_non_atomic_write_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        bytes_per_sync: ReadableSize::kb(1024),
        target_file_size: ReadableSize::kb(1024),
        ..Default::default()
    };
    let fs = Arc::new(ObfuscatedFileSystem::default());
    let entry = vec![b'x'; 1024];
    let rid = 1;

    {
        // Write partially succeeds. We can reopen.
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        let _f1 = FailGuard::new("log_fd::write::err", "return");
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
        let _f1 = FailGuard::new("log_fd::write::err", "1*off->1*return->off");
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
        let _f1 = FailGuard::new("log_fd::write::err", "return");
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
        let _f = FailGuard::new("log_fd::write::err", "return");
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
        let _f = FailGuard::new("log_fd::allocate::err", "return");
        let engine = Engine::open_with_file_system(cfg.clone(), fs.clone()).unwrap();
        engine
            .write(&mut generate_batch(1, 1, 5, Some(&entry)), true)
            .unwrap();
    }
    let engine = Engine::open_with_file_system(cfg, fs).unwrap();
    assert_eq!(engine.first_index(1).unwrap(), 1);
    assert_eq!(engine.last_index(1).unwrap(), 4);
}
