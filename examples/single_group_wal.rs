// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use raft::eraftpb::Entry;
use raft_engine::{Config, Engine, LogBatch, MessageExt};

#[derive(Clone)]
pub struct EntryType;

impl MessageExt for EntryType {
    type Entry = Entry;
    fn index(e: &Self::Entry) -> u64 {
        e.index
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = "/tmp/raft-wal";
    let _ = std::fs::remove_dir_all(dir);

    let region_id = 1u64;

    // Open engine
    let cfg = Config { dir: dir.to_owned(), ..Default::default() };
    let engine = Engine::open(cfg)?;

    // Write 5 entries
    let mut entries = Vec::new();
    for i in 1..=5 {
        let mut entry = Entry::new();
        entry.index = i;
        entry.set_data(format!("data {}", i).into_bytes().into());
        entries.push(entry);
    }

    let mut batch = LogBatch::default();
    batch.add_entries::<EntryType>(region_id, &entries)?;
    engine.write(&mut batch, true)?;

    println!("Wrote 5 entries");

    // Read them back
    let mut fetched = Vec::new();
    engine.fetch_entries_to::<EntryType>(region_id, 1, 6, None, &mut fetched)?;

    for e in &fetched {
        println!("Read index {}: {}", e.index, String::from_utf8_lossy(e.get_data()));
    }

    // Write metadata
    let mut batch = LogBatch::default();
    batch.put(region_id, b"committed".to_vec(), b"5".to_vec())?;
    batch.put(region_id, b"applied".to_vec(), b"5".to_vec())?;
    engine.write(&mut batch, false)?;

    println!("\nWrote metadata");

    Ok(())
}
