// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use criterion::{criterion_group, BenchmarkId, Criterion};
use raft::eraftpb::Entry;
use raft_engine::ReadableSize;
use raft_engine::{Config as EngineConfig, Engine, LogBatch, MessageExt, Result};
use rand::{Rng, SeedableRng};
use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use tempfile::TempDir;

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
            self.total_size,
            self.region_count,
            self.batch_size,
            self.item_size,
            self.entry_size,
            self.batch_compression_threshold
        )
    }
}

fn generate(cfg: &Config) -> Result<TempDir> {
    let dir = tempfile::Builder::new().prefix("bench").tempdir().unwrap();
    let path = dir.path().to_str().unwrap().to_owned();
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);

    let ecfg = EngineConfig {
        dir: path.clone(),
        batch_compression_threshold: cfg.batch_compression_threshold,
        ..Default::default()
    };

    let engine = Engine::open(ecfg).unwrap();

    let mut indexes: HashMap<u64, u64> = (1..cfg.region_count + 1).map(|rid| (rid, 0)).collect();
    while dir_size(&path).0 < cfg.total_size.0 {
        let mut batch = LogBatch::default();
        while batch.approximate_size() < cfg.batch_size.0 as usize {
            let region_id = rng.gen_range(1..cfg.region_count + 1);
            let mut item_size = 0;
            let mut entries = vec![];
            while item_size < cfg.item_size.0 {
                entries.push(Entry {
                    data: (&mut rng)
                        .sample_iter(rand::distributions::Standard)
                        .take(cfg.entry_size.0 as usize)
                        .collect::<Vec<u8>>()
                        .into(),
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
            batch
                .add_entries::<MessageExtTyped>(region_id, &entries)
                .unwrap();
        }
        engine.write(&mut batch, false).unwrap();
    }
    engine.sync().unwrap();
    drop(engine);
    Ok(dir)
}

fn dir_size(path: &str) -> ReadableSize {
    ReadableSize(
        std::fs::read_dir(PathBuf::from(path))
            .unwrap()
            .map(|entry| std::fs::metadata(entry.unwrap().path()).unwrap().len() as u64)
            .sum(),
    )
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
        (
            "10GB".to_owned(),
            Config {
                total_size: ReadableSize::gb(10),
                region_count: 1000,
                ..Default::default()
            },
        ),
    ];

    for (i, (name, cfg)) in cfgs.iter().enumerate() {
        println!("config-{}: [{}] {}", i, name, cfg);
    }

    for (i, (name, cfg)) in cfgs.iter().enumerate() {
        let dir = generate(cfg).unwrap();
        let path = dir.path().to_str().unwrap().to_owned();
        fail::cfg("log_fd::open::fadvise_dontneed", "return").unwrap();
        let ecfg = EngineConfig {
            dir: path.clone(),
            batch_compression_threshold: cfg.batch_compression_threshold,
            ..Default::default()
        };
        c.bench_with_input(
            BenchmarkId::new(
                "Engine::open",
                format!("size:{} config-{}: {}", dir_size(&path), i, name),
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
