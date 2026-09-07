// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use raft_engine::{Config, Engine, JsonCodec, LogBatch, MessageExt, ReadableSize};
use rand::thread_rng;
use rand_distr::{Distribution, Normal};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct JsonEntry {
    index: u64,
    term: u64,
    payload: String,
}

#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
struct RegionState {
    last_index: u64,
    last_round: u64,
}

struct JsonEntryExt;

impl MessageExt<JsonCodec> for JsonEntryExt {
    type Entry = JsonEntry;

    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

const DATA_DIR: &str = "append_with_json_codec";
const ROUNDS: u64 = 256;
const WRITES_PER_ROUND: u64 = 512;
const COMPACT_OFFSET: u64 = 32;

fn main() {
    env_logger::init();

    let config = Config {
        dir: DATA_DIR.to_owned(),
        // Small thresholds so that a short run actually rotates files, purges
        // and exercises the rewrite path.
        target_file_size: ReadableSize::kb(128),
        purge_threshold: ReadableSize::mb(1),
        batch_compression_threshold: ReadableSize::kb(0),
        ..Default::default()
    };
    let engine = Engine::open(config).expect("Open raft engine");
    let recovered = engine.raft_groups();
    if recovered.is_empty() {
        println!("[EXAMPLE] starting from an empty {DATA_DIR}/");
    } else {
        println!(
            "[EXAMPLE] recovered {} raft groups written by a previous run",
            recovered.len()
        );
    }

    let mut rand_regions = Normal::new(8.0, 4.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x: f64| (x as u64) % 16);
    let mut rand_compacts = Normal::new(COMPACT_OFFSET as f64, 16.0)
        .unwrap()
        .sample_iter(thread_rng())
        .map(|x: f64| x as u64);

    let mut batch = LogBatch::with_capacity(256);
    let payload = "x".repeat(1024);

    for round in 1..=ROUNDS {
        for _ in 0..WRITES_PER_ROUND {
            let region = rand_regions.next().unwrap();
            let mut state = engine
                .get_value::<RegionState, JsonCodec>(region, b"state")
                .unwrap()
                .unwrap_or_default();

            state.last_index += 1; // manually update the state
            state.last_round = round;

            let entry = JsonEntry {
                index: state.last_index,
                term: round,
                payload: payload.clone(),
            };
            batch
                .add_entries_with::<JsonEntryExt, JsonCodec>(region, &[entry])
                .unwrap();
            batch
                .put_value::<JsonCodec, _>(region, b"state".to_vec(), &state)
                .unwrap();
            engine.write(&mut batch, false).unwrap();

            if state.last_index % COMPACT_OFFSET == 0 {
                let rand_compact_offset = rand_compacts.next().unwrap();
                if state.last_index > rand_compact_offset {
                    let compact_to = state.last_index - rand_compact_offset;
                    engine.compact_to(region, compact_to);
                }
            }
        }

        for region in engine.purge_expired_files().unwrap() {
            let state = engine
                .get_value::<RegionState, JsonCodec>(region, b"state")
                .unwrap()
                .unwrap();
            let compact_to = state.last_index.saturating_sub(7);
            engine.compact_to(region, compact_to);
            println!("[EXAMPLE] round {round}: force compact {region} to {compact_to}");
        }
    }
    engine.sync().unwrap();

    // Read everything back and check it survived the whole cycle.
    let mut regions = engine.raft_groups();
    regions.sort_unstable();
    for &region in &regions {
        let (first, last) = match (engine.first_index(region), engine.last_index(region)) {
            (Some(f), Some(l)) => (f, l),
            _ => continue,
        };
        let mut entries = Vec::new();
        engine
            .fetch_entries_to_with::<JsonEntryExt, JsonCodec>(
                region,
                first,
                last + 1,
                None,
                &mut entries,
            )
            .unwrap();
        assert_eq!(entries.len() as u64, last - first + 1);
        for (offset, e) in entries.iter().enumerate() {
            assert_eq!(e.index, first + offset as u64);
            assert_eq!(e.payload, payload);
        }
        let state = engine
            .get_value::<RegionState, JsonCodec>(region, b"state")
            .unwrap()
            .unwrap();
        assert_eq!(state.last_index, last);
        println!("[EXAMPLE] region {region}: entries [{first}, {last}] verified");
    }
    println!("[EXAMPLE] done, data left in {DATA_DIR}/");
}
