// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use kvproto::raft_serverpb::RaftLocalState;
use raft::eraftpb::Entry;
use raft_engine::{Config, Engine, LogBatch, MessageExt, ReadableSize};
use rand::thread_rng;
use rand_distr::{Distribution, Normal};

#[derive(Clone)]
pub struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

// How to run the example:
// $ RUST_LOG=debug cargo run --release --example append-compact-purge
fn main() {
    env_logger::init();

    let config = Config {
        dir: "append-compact-purge-data".to_owned(),
        purge_threshold: ReadableSize::gb(2),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    let engine = Engine::open(config).expect("Open raft engine");

    let compact_offset = 32; // In src/purge.rs, it's the limit for rewrite.

    let mut rand_regions = Normal::new(128.0, 96.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x| x as u64);
    let mut rand_compacts = Normal::new(compact_offset as f64, 16.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x| x as u64);

    let mut batch = LogBatch::with_capacity(256);
    let mut entry = Entry::new();
    entry.set_data(vec![b'x'; 1024 * 32].into());
    let init_state = RaftLocalState {
        last_index: 0,
        ..Default::default()
    };
    loop {
        for _ in 0..1024 {
            let region = rand_regions.next().unwrap();
            let mut state = engine
                .get_message::<RaftLocalState>(region, b"last_index")
                .unwrap()
                .unwrap_or_else(|| init_state.clone());
            state.last_index += 1; // manually update the state
            let mut e = entry.clone();
            e.index = state.last_index + 1;
            batch.add_entries::<MessageExtTyped>(region, &[e]).unwrap();
            batch
                .put_message(region, b"last_index".to_vec(), &state)
                .unwrap();
            engine.write(&mut batch, false).unwrap();

            if state.last_index % compact_offset == 0 {
                let rand_compact_offset = rand_compacts.next().unwrap();
                if state.last_index > rand_compact_offset {
                    let compact_to = state.last_index - rand_compact_offset;
                    engine.compact_to(region, compact_to);
                    println!("[EXAMPLE] compact {} to {}", region, compact_to);
                }
            }
        }
        for region in engine.purge_expired_files().unwrap() {
            let state = engine
                .get_message::<RaftLocalState>(region, b"last_index")
                .unwrap()
                .unwrap();
            engine.compact_to(region, state.last_index - 7);
            println!(
                "[EXAMPLE] force compact {} to {}",
                region,
                state.last_index - 7
            );
        }
    }
}
