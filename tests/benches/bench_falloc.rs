// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use criterion::{BenchmarkId, Criterion};

use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::ToPrimitive;
use raft_engine::env::{DefaultFileSystem, FileSystem, WriteExt};
use raft_engine::internals::{FileId, FileNameExt, LogQueue};
use raft_engine::{ReadableSize, Result};
use std::fmt;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq, FromPrimitive, ToPrimitive)]
#[repr(u64)]
enum AllocateMode {
    Default = 1,
    WithHole = 2,
}

struct Config {
    dir: String,
    mode: AllocateMode,
    file_size: ReadableSize,
    data_block_size: ReadableSize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            dir: String::from("./"),
            mode: AllocateMode::Default,
            file_size: ReadableSize::mb(1),
            data_block_size: ReadableSize::kb(1),
        }
    }
}

impl fmt::Display for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "[BenchFallocate - dir: {}, mode: {}, file_size: {}, data_block_size: {}]",
            self.dir,
            self.mode.to_u64().unwrap(),
            self.file_size,
            self.data_block_size
        )
    }
}

fn gen_file_with_filling_data<F: FileSystem>(
    file_system: &F,
    dir: &String,
    data: &[u8],
    data_len: usize,
    mode: AllocateMode,
    file_size: usize,
) -> Result<()> {
    let mut loop_count = 10;
    let bytes_per_sync = ReadableSize::kb(64);
    while loop_count > 0 {
        let file_id = FileId {
            queue: LogQueue::Append,
            seq: loop_count,
        };
        let fd = Arc::new(file_system.create(&file_id.build_file_path(dir))?);
        let mut writer = file_system.new_writer(fd)?;
        match mode {
            AllocateMode::Default =>
                /* writer.allocate(0, file_size)? */
                {}
            AllocateMode::WithHole => {
                // writer.allocate_with_hole(0, file_size)?;
                writer.allocate(0, file_size)?;
                writer.seek(SeekFrom::Start(0))?;
            }
        };
        if data_len > 0 {
            let mut write_count = (file_size / data_len) as i64;
            let sync_frequency = (bytes_per_sync.0 as usize / data_len) as i64;
            let mut idx = 0_i64;
            while write_count > 0 {
                writer.write_all(data)?;
                if write_count - idx >= 0 && idx < sync_frequency {
                    idx += 1;
                    continue;
                }
                writer.sync()?;
                write_count -= idx;
            }
        }
        writer.sync()?;
        loop_count -= 1;
    }
    Ok(())
}

fn bench_by_config(cfg: &Config) {
    let dir = tempfile::Builder::new()
        .prefix(format!("bench_falloc_{}", cfg.dir).as_str())
        .tempdir()
        .unwrap();
    let path = dir.path().to_str().unwrap().to_owned();
    //
    let file_system = Arc::new(DefaultFileSystem);
    let entry_data = vec![b'x'; cfg.data_block_size.0 as usize];
    gen_file_with_filling_data(
        file_system.as_ref(),
        &path,
        &entry_data[..],
        cfg.data_block_size.0 as usize,
        cfg.mode,
        cfg.file_size.0 as usize,
    )
    .unwrap();
}

// Benchmarks

pub fn bench_fallocate(c: &mut Criterion) {
    let cfg = vec![
        (
            "AllocateMode::Default".to_owned(),
            Config {
                dir: "128_default".to_owned(),
                mode: AllocateMode::Default,
                file_size: ReadableSize::mb(128),
                data_block_size: ReadableSize::kb(1),
            },
        ),
        (
            "AllocateMode::WithHole".to_owned(),
            Config {
                dir: "128_default".to_owned(),
                mode: AllocateMode::WithHole,
                file_size: ReadableSize::mb(128),
                data_block_size: ReadableSize::kb(1),
            },
        ),
    ];
    for (i, (name, ecfg)) in cfg.iter().enumerate() {
        c.bench_with_input(
            BenchmarkId::new(
                "Fallocate with zeros",
                format!("dir_name:{} config-{}: {}", &ecfg.dir, i, name),
            ),
            &ecfg,
            |b, cfg| b.iter(|| bench_by_config(cfg)),
        );
    }
}
