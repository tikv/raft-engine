use std::convert::TryInto;

use raft::eraftpb::Entry;
use raft_engine::{Config, EntryExt, LogBatch, RaftLogEngine, ReadableSize};
use rand::thread_rng;
use rand_distr::{Distribution, Normal};

#[derive(Clone)]
struct EntryExtImpl;
impl EntryExt<Entry> for EntryExtImpl {
    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

// How to run the example:
// $ RUST_LOG=debug cargo run --release --example append-compact-purge
fn main() {
    env_logger::init();

    let mut config = Config::default();
    config.dir = "append-compact-purge-data".to_owned();
    config.purge_threshold = ReadableSize::gb(2);
    config.batch_compression_threshold = ReadableSize::kb(0);
    let engine = RaftLogEngine::<Entry, EntryExtImpl>::new(config);

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
    loop {
        for _ in 0..1024 {
            let region = rand_regions.next().unwrap();
            let index = match engine.get(region, b"last_index").unwrap() {
                Some(value) => u64::from_le_bytes(value.try_into().unwrap()) + 1,
                None => 1,
            };

            let mut e = entry.clone();
            e.index = index;
            batch.add_entries(region, vec![e; 1]);
            batch.put(region, b"last_index".to_vec(), index.to_le_bytes().to_vec());
            engine.write(&mut batch, false).unwrap();

            if index % compact_offset == 0 {
                let rand_compact_offset = rand_compacts.next().unwrap();
                if index > rand_compact_offset {
                    let compact_to = index - rand_compact_offset;
                    engine.compact_to(region, compact_to);
                    println!("[EXAMPLE] compact {} to {}", region, compact_to);
                }
            }
        }
        for region in engine.purge_expired_files().unwrap() {
            let index = match engine.get(region, b"last_index").unwrap() {
                Some(value) => u64::from_le_bytes(value.try_into().unwrap()) + 1,
                None => unreachable!(),
            };
            engine.compact_to(region, index - 8);
            println!("[EXAMPLE] force compact {} to {}", region, index - 8);
        }
    }
}
