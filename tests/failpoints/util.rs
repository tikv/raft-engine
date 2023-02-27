// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::panic::{self, AssertUnwindSafe};
use std::sync::{mpsc, Arc};

use raft::eraftpb::Entry;
use raft_engine::env::FileSystem;
use raft_engine::{Engine, LogBatch, MessageExt};

#[derive(Clone)]
pub struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

pub fn generate_entries(begin_index: u64, end_index: u64, data: Option<&[u8]>) -> Vec<Entry> {
    let mut v = vec![Entry::new(); (end_index - begin_index) as usize];
    let mut index = begin_index;
    for e in v.iter_mut() {
        e.set_index(index);
        if let Some(data) = data {
            e.set_data(data.to_vec().into())
        }
        index += 1;
    }
    v
}

pub fn generate_batch(
    region: u64,
    begin_index: u64,
    end_index: u64,
    data: Option<&[u8]>,
) -> LogBatch {
    let mut batch = LogBatch::default();
    batch
        .add_entries::<MessageExtTyped>(region, &generate_entries(begin_index, end_index, data))
        .unwrap();
    batch
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

pub struct ConcurrentWriteContext<FS: 'static + FileSystem> {
    engine: Arc<Engine<FS>>,
    ths: Vec<std::thread::JoinHandle<()>>,
}

impl<FS: 'static + FileSystem> ConcurrentWriteContext<FS> {
    pub fn new(engine: Arc<Engine<FS>>) -> Self {
        Self {
            engine,
            ths: Vec::new(),
        }
    }

    pub fn write(&mut self, mut log_batch: LogBatch) {
        self.write_ext(move |e| {
            e.write(&mut log_batch, true).unwrap();
        });
    }

    pub fn write_ext<F>(&mut self, f: F)
    where
        F: FnOnce(&Engine<FS>) + Send + Sync + 'static,
    {
        let (ready_tx, ready_rx) = mpsc::channel();
        if self.ths.is_empty() {
            fail::cfg("write_barrier::leader_exit", "pause").unwrap();
            let engine_clone = self.engine.clone();
            let ready_tx_clone = ready_tx.clone();
            self.ths.push(
                std::thread::Builder::new()
                    .spawn(move || {
                        ready_tx_clone.send(()).unwrap();
                        // No-op.
                        engine_clone.write(&mut LogBatch::default(), false).unwrap();
                    })
                    .unwrap(),
            );
            std::thread::sleep(std::time::Duration::from_millis(100));
            ready_rx.recv().unwrap();
        } else {
            // Follower.
            assert!(self.ths.len() >= 2);
        }
        let engine_clone = self.engine.clone();
        self.ths.push(
            std::thread::Builder::new()
                .spawn(move || {
                    ready_tx.send(()).unwrap();
                    f(&engine_clone);
                })
                .unwrap(),
        );
        std::thread::sleep(std::time::Duration::from_millis(100));
        ready_rx.recv().unwrap();
    }

    pub fn join(&mut self) {
        fail::remove("write_barrier::leader_exit");
        for t in self.ths.drain(..) {
            t.join().unwrap();
        }
    }
}
