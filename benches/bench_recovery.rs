use byte_unit::Byte;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use raft::eraftpb::Entry;
use raft_engine::ReadableSize;
use raft_engine::{Config as EngineConfig, LogBatch, MessageExt, RaftLogEngine, Result};
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use tempfile::TempDir;

extern crate libc;

type Engine = RaftLogEngine<MessageExtTyped>;

#[derive(Clone)]
struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

struct Config {
    total_size: ReadableSize,
    region_count: u64,
    batch_size: ReadableSize,
    item_size: ReadableSize,
    entry_size: ReadableSize,
    batch_compression_threshold: ReadableSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            total_size: ReadableSize::gb(1),
            region_count: 100,
            batch_size: ReadableSize::mb(1),
            item_size: ReadableSize::kb(1),
            entry_size: ReadableSize(256),
            batch_compression_threshold: ReadableSize(0),
        }
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} [region-count: {}][batch-size: {}][item-size: {}][entry-size: {}][batch-compression-threshold: {}]",
        size_to_readable_string(self.total_size.0 as u128),
        self.region_count,
        size_to_readable_string(self.batch_size.0 as u128),
        size_to_readable_string(self.item_size.0 as u128),
        size_to_readable_string(self.entry_size.0 as u128),
        size_to_readable_string(self.batch_compression_threshold.0 as u128)
    )
    }
}

fn generate(cfg: &Config) -> Result<TempDir> {
    let dir = tempfile::Builder::new().prefix("bench").tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_owned();
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let mut ecfg = EngineConfig::new();
    ecfg.dir = path.clone();
    ecfg.batch_compression_threshold = cfg.batch_compression_threshold;

    let engine = Engine::open(ecfg.clone()).unwrap();

    let mut indexes: HashMap<u64, u64> = (1..cfg.region_count + 1).map(|rid| (rid, 0)).collect();
    while dir_size(&path) < cfg.total_size.0 as u128 {
        let mut batch = LogBatch::default();
        while batch.approximate_size() < cfg.batch_size.0 as usize {
            let region_id = rng.gen_range(1, cfg.region_count + 1);
            let mut item_size = 0;
            let mut entries = vec![];
            while item_size < cfg.item_size.0 {
                entries.push(Entry {
                    data: (&mut rng)
                        .sample_iter(rand::distributions::Standard)
                        .take(cfg.entry_size.0 as usize)
                        .collect::<Vec<u8>>(),
                    ..Default::default()
                });
                item_size += cfg.entry_size.0;
            }
            let mut index = *indexes.get(&region_id).unwrap();
            index = entries.iter_mut().fold(index, |index, e| {
                e.index = index + 1;
                index + 1
            });
            *indexes.get_mut(&region_id).unwrap() = index;
            batch.add_entries(region_id, entries);
        }
        engine.write(&mut batch, false).unwrap();
    }
    drop(engine);
    Ok(dir)
}

fn dir_size(path: &str) -> u128 {
    let mut size: u128 = 0;
    for entry in std::fs::read_dir(PathBuf::from(path)).unwrap() {
        let entry = entry.unwrap();
        size += std::fs::metadata(entry.path()).unwrap().len() as u128;
    }
    size
}

fn size_to_readable_string(size: u128) -> String {
    Byte::from_bytes(size)
        .get_appropriate_unit(true)
        .to_string()
}

// Benchmarks

fn bench_recovery(c: &mut Criterion) {
    // prepare input
    let cfgs = vec![
        (
            "default".to_owned(),
            Config {
                ..Default::default()
            },
        ),
        (
            "compressed".to_owned(),
            Config {
                batch_compression_threshold: ReadableSize::kb(8),
                ..Default::default()
            },
        ),
        (
            "small-batch(1KB)".to_owned(),
            Config {
                region_count: 100,
                batch_size: ReadableSize::kb(1),
                item_size: ReadableSize(256),
                entry_size: ReadableSize(32),
                ..Default::default()
            },
        ),
    ];

    for (i, (name, cfg)) in cfgs.iter().enumerate() {
        println!("config-{}: [{}] {}", i, name, cfg);
    }

    for (i, (name, cfg)) in cfgs.iter().enumerate() {
        let dir = generate(&cfg).unwrap();
        let path = dir.path().to_str().unwrap().to_owned();
        fail::cfg("fadvise-dontneed", "return").unwrap();
        let mut ecfg = EngineConfig::default();
        ecfg.dir = path.clone();
        ecfg.batch_compression_threshold = cfg.batch_compression_threshold;

        c.bench_with_input(
            BenchmarkId::new(
                "Engine::open",
                format!(
                    "size:{} config-{}: {}",
                    size_to_readable_string(dir_size(&path)),
                    i,
                    name
                ),
            ),
            &ecfg,
            |b, cfg| {
                b.iter(|| {
                    Engine::open(cfg.to_owned()).unwrap();
                })
            },
        );
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_recovery
}
criterion_main!(benches);
