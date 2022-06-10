// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use criterion::{BenchmarkId, Criterion};

use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::ToPrimitive;
use raft_engine::env::{DefaultFileSystem, FileSystem, WriteExt};
use raft_engine::internals::{FileId, FileNameExt, LogQueue};
use raft_engine::{ReadableSize, Result};
use rand::{Rng, SeedableRng};
use std::fmt;
use std::io::{Seek, SeekFrom, Write};
use std::sync::Arc;

const BYTES_PER_SYNC: usize = ReadableSize::kb(64).0 as usize;

#[derive(Clone, Copy, Debug, Eq, PartialEq, FromPrimitive, ToPrimitive)]
#[repr(u64)]
enum AllocateMode {
    Default = 1,
    FillingWithoutHole = 2,
    FillingWithHole = 3,
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

/*
fn gen_file_with_filling_data<F: FileSystem>(
    file_system: &F,
    dir: &String,
    data: &[u8],
    data_len: usize,
    mode: AllocateMode,
    file_size: usize,
) -> Result<()> {
    let mut loop_count = 10;
    let black_hole: Vec<u8> = vec![b'\0' as u8; BYTES_PER_SYNC];
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
            AllocateMode::FillingWithoutHole => {
                writer.allocate(0, file_size)?;
                writer.sync()?;
                writer.seek(SeekFrom::Start(0))?;
            }
            AllocateMode::FillingWithHole => {
                // writer.allocate_with_hole(0, file_size)?;
                // let black_hole: Vec<u8> = vec![b'\0' as u8; file_size];
                writer.allocate(0, file_size - BYTES_PER_SYNC)?;
                writer.write(&black_hole[..])?; // extra
                writer.sync()?;
                writer.seek(SeekFrom::Start(0))?;
            }
        };
        if data_len > 0 {
            let mut write_count = (file_size / data_len) as i64;
            let sync_frequency = (BYTES_PER_SYNC / data_len) as i64;
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
*/

fn prepare_with_config(
    cfg: &Config,
) -> Result<(<DefaultFileSystem as FileSystem>::Writer, Vec<u8>)> {
    let file_size = cfg.file_size.0 as usize;
    let file_system = Arc::new(DefaultFileSystem);
    let entry_data = vec![b'x'; cfg.data_block_size.0 as usize];
    let black_hole: Vec<u8> = vec![b'\0' as u8; file_size];
    //
    let dir = tempfile::Builder::new()
        .prefix(format!("bench_falloc_{}", cfg.dir).as_str())
        .tempdir()
        .unwrap();
    let path = dir.path().to_str().unwrap().to_owned();
    //
    let mut rng = rand::rngs::StdRng::seed_from_u64(0);
    let file_id = FileId {
        queue: LogQueue::Append,
        seq: rng.gen_range(1..100000),
    };
    let fd = Arc::new(file_system.create(&file_id.build_file_path(path))?);
    let mut writer = file_system.new_writer(fd)?;
    match cfg.mode {
        AllocateMode::Default =>
            /* writer.allocate(0, file_size)? */
            {}
        AllocateMode::FillingWithoutHole => {
            writer.allocate(0, file_size)?;
            writer.sync()?;
            writer.seek(SeekFrom::Start(0))?;
        }
        AllocateMode::FillingWithHole => {
            // writer.allocate_with_hole(0, file_size)?;
            // let black_hole: Vec<u8> = vec![b'\0' as u8; file_size];
            writer.allocate(0, file_size)?;
            writer.write(&black_hole[..])?; // extra
            writer.sync()?;
            writer.seek(SeekFrom::Start(0))?;
        }
    };
    Ok((writer, entry_data))
}

fn bench_with_config<F: FileSystem>(
    mut writer: F::Writer,
    file_size: usize,
    bytes_per_sync: usize,
    data: &[u8],
) -> Result<()> {
    let data_len = data.len();
    if data_len > 0 {
        let mut write_count = (file_size / data_len) as i64;
        let sync_frequency = (bytes_per_sync / data_len) as i64;
        let mut idx = 0_i64;
        while write_count > idx {
            if idx < sync_frequency {
                idx += 1;
                writer.write_all(data)?;
                continue;
            }
            writer.sync()?;
            write_count -= idx;
            idx = 0;
        }
    }
    writer.sync()?;
    Ok(())
}

// Benchmarks

pub fn bench_fallocate(c: &mut Criterion) {
    struct Param {
        writer: <DefaultFileSystem as FileSystem>::Writer,
        file_size: usize,
        data: Vec<u8>,
    }
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
            "AllocateMode::FillingWithoutHole -- without filling zeros".to_owned(),
            Config {
                dir: "128_default".to_owned(),
                mode: AllocateMode::FillingWithoutHole,
                file_size: ReadableSize::mb(128),
                data_block_size: ReadableSize::kb(1),
            },
        ),
        (
            "AllocateMode::FillingWithHole".to_owned(),
            Config {
                dir: "128_default".to_owned(),
                mode: AllocateMode::FillingWithHole,
                file_size: ReadableSize::mb(128),
                data_block_size: ReadableSize::kb(1),
            },
        ),
    ];
    for (i, (name, ecfg)) in cfg.iter().enumerate() {
        let (writer, data) = prepare_with_config(ecfg).unwrap();
        let file_size = ecfg.file_size.0 as usize;
        let params = Param {
            writer,
            file_size,
            data,
        };
        c.bench_with_input(
            BenchmarkId::new(
                "Fallocate with zeros",
                format!("dir_name:{} config-{}: {}", &ecfg.dir, i, name),
            ),
            &params,
            |b, params| {
                b.iter(|| {
                    bench_with_config::<DefaultFileSystem>(
                        params.writer.clone(),
                        params.file_size,
                        BYTES_PER_SYNC,
                        &params.data[..],
                    )
                })
            },
        );
    }
}
