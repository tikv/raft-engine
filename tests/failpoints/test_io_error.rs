use crate::util::catch_unwind_silent;
use raft::eraftpb::Entry;
use raft_engine::{Config, Engine as RaftLogEngine, LogBatch, MessageExt, ReadableSize, Result};
use std::sync::atomic::AtomicU64;
use std::sync::Arc;

#[derive(Clone)]
pub struct M;

impl MessageExt for M {
    type Entry = Entry;

    fn index(e: &Entry) -> u64 {
        e.index
    }
}

fn generate_batch(
    region: u64,
    begin_index: u64,
    end_index: u64,
    data: Option<Vec<u8>>,
) -> LogBatch {
    let mut batch = LogBatch::default();
    let mut v = vec![Entry::new(); (end_index - begin_index) as usize];
    let mut index = begin_index;
    for e in v.iter_mut() {
        e.set_index(index);
        if let Some(ref data) = data {
            e.set_data(data.clone().into())
        }
        index += 1;
    }
    batch.add_entries::<M>(region, &v).unwrap();
    batch
}

fn write_tmp_engine(
    bytes_per_sync: ReadableSize,
    target_file_size: ReadableSize,
    entry_size: usize,
    entry_count: u64,
    batch_count: u64,
) -> Result<usize> {
    let dir = tempfile::Builder::new()
        .prefix("test_io_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        bytes_per_sync,
        target_file_size,
        batch_compression_threshold: ReadableSize(0),
        ..Default::default()
    };
    let engine = RaftLogEngine::open(cfg).unwrap();

    let entry_data = vec![b'x'; entry_size];
    let mut index = 1;
    let mut written = 0;
    for _ in 0..batch_count {
        let mut log_batch =
            generate_batch(1, index, index + entry_count + 1, Some(entry_data.clone()));
        index += entry_count;
        written += engine.write(&mut log_batch, false)?;
    }
    Ok(written)
}

struct ConcurrentWriteContext {
    engine: Arc<RaftLogEngine>,
    ths: Vec<std::thread::JoinHandle<()>>,
    ticket: u64,
    timer: Arc<AtomicU64>,
}

impl ConcurrentWriteContext {
    fn new(engine: Arc<RaftLogEngine>) -> Self {
        Self {
            engine,
            ths: Vec::new(),
            ticket: 0,
            timer: Arc::new(AtomicU64::new(0)),
        }
    }

    fn leader_write<F: FnOnce(Result<usize>) + Send + Sync + 'static>(
        &mut self,
        mut log_batch: LogBatch,
        sync: bool,
        cb: Option<F>,
        panic: bool,
    ) {
        if self.ths.is_empty() {
            fail::cfg("write_barrier::leader_exit", "pause").unwrap();
            let engine_clone = self.engine.clone();
            let ticket = self.ticket;
            self.ticket += 1;
            let timer = self.timer.clone();
            self.ths.push(
                std::thread::Builder::new()
                    .spawn(move || {
                        while ticket != timer.load(std::sync::atomic::Ordering::Acquire) {}
                        engine_clone.write(&mut LogBatch::default(), false).unwrap();
                        timer.store(ticket + 1, std::sync::atomic::Ordering::Release);
                    })
                    .unwrap(),
            );
        }
        let engine_clone = self.engine.clone();
        let ticket = self.ticket;
        self.ticket += 1;
        let timer = self.timer.clone();
        self.ths.push(
            std::thread::Builder::new()
                .spawn(move || {
                    while ticket != timer.load(std::sync::atomic::Ordering::Acquire) {}
                    if panic {
                        assert!(catch_unwind_silent(|| {
                            engine_clone.write(&mut log_batch, sync).unwrap();
                        })
                        .is_err());
                        return;
                    }
                    let r = engine_clone.write(&mut log_batch, sync);
                    timer.store(ticket + 1, std::sync::atomic::Ordering::Release);
                    if let Some(f) = cb {
                        f(r)
                    }
                })
                .unwrap(),
        );
    }

    fn follower_write<F: FnOnce(Result<usize>) + Send + Sync + 'static>(
        &mut self,
        mut log_batch: LogBatch,
        sync: bool,
        cb: Option<F>,
        panic: bool,
    ) {
        assert!(self.ticket >= 2);
        let engine_clone = self.engine.clone();
        let ticket = self.ticket;
        self.ticket += 1;
        let timer = self.timer.clone();
        self.ths.push(
            std::thread::Builder::new()
                .spawn(move || {
                    while ticket != timer.load(std::sync::atomic::Ordering::Acquire) {}
                    if panic {
                        assert!(catch_unwind_silent(|| {
                            engine_clone.write(&mut log_batch, sync).unwrap();
                        })
                        .is_err());
                        return;
                    }
                    let r = engine_clone.write(&mut log_batch, sync);
                    if let Some(f) = cb {
                        f(r)
                    }
                    timer.store(ticket + 1, std::sync::atomic::Ordering::Release);
                })
                .unwrap(),
        );
    }

    fn join(&mut self) {
        fail::remove("write_barrier::leader_exit");
        for t in self.ths.drain(..) {
            t.join().unwrap();
        }
    }
}

#[test]
fn test_open_error() {
    fail::cfg("log_fd::open::err", "return").unwrap();
    assert!(catch_unwind_silent(|| {
        let dir = tempfile::Builder::new()
            .prefix("test_open_error")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            bytes_per_sync: ReadableSize::kb(1),
            target_file_size: ReadableSize::kb(4),
            batch_compression_threshold: ReadableSize(0),
            ..Default::default()
        };
        let engine = RaftLogEngine::open(cfg.clone()).unwrap();
        drop(engine);
        let _ = RaftLogEngine::open(cfg).unwrap();
    })
    .is_err());
    fail::cfg("log_fd::open::err", "off").unwrap();
}

#[test]
fn test_rotate_error() {
    // panic when truncate
    fail::cfg("active_file::truncate::force", "return").unwrap();
    fail::cfg("log_fd::truncate::err", "return").unwrap();
    assert!(catch_unwind_silent(|| {
        write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
    })
    .is_err());

    // panic when sync
    fail::cfg("active_file::truncate::force", "off").unwrap();
    fail::cfg("log_fd::truncate::err", "off").unwrap();
    fail::cfg("log_fd::sync::err", "return").unwrap();
    assert!(catch_unwind_silent(|| {
        write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
    })
    .is_err());

    // panic when create file
    fail::cfg("log_fd::sync::err", "off").unwrap();
    fail::cfg("log_fd::create::err", "return").unwrap();
    assert!(catch_unwind_silent(|| {
        write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
    })
    .is_err());

    // panic when write header
    fail::cfg("log_fd::create::err", "off").unwrap();
    fail::cfg("log_fd::write::err", "return").unwrap();
    assert!(catch_unwind_silent(|| {
        write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
    })
    .is_err());
    fail::cfg("log_fd::write::err", "off").unwrap();
}

#[test]
fn test_concurrent_write_error() {
    // write-1 success; write-2 fail, truncate; write-3 success
    let timer = AtomicU64::new(0);
    fail::cfg_callback("engine::write::pre", move || {
        match timer.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            2 => fail::cfg("log_fd::write::err", "return").unwrap(),
            3 => fail::cfg("log_fd::write::err", "off").unwrap(),
            _ => {}
        }
    })
    .unwrap();

    // truncate and sync when write error
    let dir = tempfile::Builder::new()
        .prefix("test_concurrent_write_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        bytes_per_sync: ReadableSize::kb(1024),
        target_file_size: ReadableSize::kb(1024),
        batch_compression_threshold: ReadableSize(0),
        ..Default::default()
    };

    let engine = Arc::new(RaftLogEngine::open(cfg).unwrap());
    let mut ctx = ConcurrentWriteContext::new(engine.clone());

    let content = vec![b'x'; 1024];

    ctx.leader_write(
        generate_batch(1, 1, 11, Some(content.clone())),
        false,
        Some(|r: Result<usize>| {
            assert!(r.is_ok());
        }),
        false,
    );
    ctx.follower_write(
        generate_batch(2, 1, 11, Some(content.clone())),
        false,
        Some(|r: Result<usize>| {
            assert!(r.is_err());
        }),
        false,
    );
    ctx.follower_write(
        generate_batch(3, 1, 11, Some(content)),
        false,
        Some(|r: Result<usize>| {
            assert!(r.is_ok());
        }),
        false,
    );
    ctx.join();
    assert_eq!(
        10,
        engine
            .fetch_entries_to::<M>(1, 1, 11, None, &mut vec![])
            .unwrap()
    );
    assert_eq!(
        0,
        engine
            .fetch_entries_to::<M>(2, 1, 11, None, &mut vec![])
            .unwrap()
    );
    assert_eq!(
        10,
        engine
            .fetch_entries_to::<M>(3, 1, 11, None, &mut vec![])
            .unwrap()
    );
    fail::cfg("engine::write::pre", "off").unwrap();
}

#[test]
fn test_concurrent_write_truncate_error() {
    // truncate and sync when write error
    // write-1 success; write-2 fail, truncate, panic; write-3 (x)
    let timer = AtomicU64::new(0);
    fail::cfg_callback("engine::write::pre", move || {
        match timer.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
            2 => {
                fail::cfg("log_fd::write::err", "return").unwrap();
                fail::cfg("log_fd::truncate::err", "return").unwrap()
            }
            3 => {
                fail::cfg("log_fd::write::err", "off").unwrap();
                fail::cfg("log_fd::truncate::err", "off").unwrap()
            }
            _ => {}
        }
    })
    .unwrap();

    let dir = tempfile::Builder::new()
        .prefix("test_concurrent_write_truncate_error")
        .tempdir()
        .unwrap();
    let cfg = Config {
        dir: dir.path().to_str().unwrap().to_owned(),
        bytes_per_sync: ReadableSize::kb(1024),
        target_file_size: ReadableSize::kb(1024),
        batch_compression_threshold: ReadableSize(0),
        ..Default::default()
    };

    let engine = Arc::new(RaftLogEngine::open(cfg).unwrap());
    let mut ctx = ConcurrentWriteContext::new(engine);

    let content = vec![b'x'; 1024];

    ctx.leader_write(
        generate_batch(1, 1, 11, Some(content.clone())),
        false,
        None::<fn(_)>,
        false,
    );
    ctx.follower_write(
        generate_batch(2, 1, 11, Some(content)),
        false,
        Some(|_| unreachable!()),
        true,
    );
    ctx.join();
}
