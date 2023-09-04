# Raft Engine

[![Rust](https://github.com/tikv/raft-engine/workflows/Rust/badge.svg?branch=master)](https://github.com/tikv/raft-engine/actions/workflows/rust.yml)
[![codecov](https://codecov.io/gh/tikv/raft-engine/branch/master/graph/badge.svg)](https://app.codecov.io/gh/tikv/raft-engine)
[![Docs](https://docs.rs/raft-engine/badge.svg)](https://docs.rs/raft-engine)
[![crates.io](https://img.shields.io/crates/v/raft-engine.svg)](https://crates.io/crates/raft-engine)

Raft Engine is a persistent embedded storage engine with a log-structured design similar to [bitcask](https://github.com/basho/bitcask). It is built for [TiKV](https://github.com/tikv/tikv) to store [Multi-Raft](https://raft.github.io/) logs.

## Features

- APIs for storing and retrieving [protobuf](https://crates.io/crates/protobuf) log entries with consecutive indexes
- Key-value storage for individual Raft Groups
- Minimum write amplification
- Collaborative garbage collection
- Supports [lz4](http://www.lz4.org/) compression over log entries
- Supports file system extension

## Design

Raft Engine consists of two basic constructs: memtable and log file.

In memory, each Raft Group holds its own memtable, containing all the key value pairs and the file locations of all log entries. On storage, user writes are sequentially written to the active log file, which is periodically rotated below a configurable threshold. Different Raft Groups share the same log stream.

### Write

Similar to [RocksDB](https://github.com/facebook/rocksdb), Raft Engine provides atomic writes. Users can stash the changes into a log batch before submitting.

The writing of one log batch can be broken down into three steps:

1. Optionally compress the log entries
2. Write to log file
3. Apply to memtable

At step 2, to group concurrent requests, each writing thread must enter a queue. The first in line automatically becomes the queue leader, responsible for writing the entire group to the log file.

Both synchronous and non-sync writes are supported. When one write in a batch is marked synchronous, the batch leader will call [`fdatasync()`](https://linux.die.net/man/2/fdatasync) after writing. This way, buffered data is guaranteed to be flushed out onto the storage.

After its data is written, each writing thread will proceed to apply the changes to memtable on their own.

### Garbage Collection

After changes are applied to the local state machine, the corresponding log entries can be compacted from Raft Engine, logically. Because multiple Raft Groups share the same log stream, these truncated logs will punch holes in the log files. During garbage collection, Raft Engine scans for these holes and compacts log files to free up storage space. Only at this point, the unneeded log entries are deleted physically.

Raft Engine carries out garbage collection in a collaborative manner.

First, its timing is controlled by the user. Raft Engine consolidates and removes its log files only when the user voluntarily calls the `purge_expired_files()` routine. For reference, [TiKV](https://github.com/tikv/tikv) calls it every 10 seconds by default.

Second, it sends useful feedback to the user. Each time the GC routine is called, Raft Engine will examine itself and return a list of Raft Groups that hold particularly old log entries. Those log entries block the GC progress and should be compacted by the user.

## Using this crate

Put this in your Cargo.toml:

```rust
[dependencies]
raft-engine = "0.4.0"
```

Available Cargo features:

- `scripting`: Compiles with [Rhai](https://github.com/rhaiscript/rhai). This enables script debugging utilities including `unsafe_repair`.
- `nightly`: Enables nightly-only features including `test`.
- `internals`: Re-exports key components internal to Raft Engine. Enabled when building for docs.rs.
- `failpoints`: Enables fail point testing powered by [tikv/fail-rs](https://github.com/tikv/fail-rs).
- `swap`: Use `SwappyAllocator` to limit the memory usage of Raft Engine. The memory budget can be configured with "memory-limit". Depending on the `nightly` feature.

See some basic use cases under the [examples](https://github.com/tikv/raft-engine/tree/master/examples) directory.

## Contributing

Contributions are always welcome! Here are a few tips for making a PR:

- All commits must be signed off (with `git commit -s`) to pass the [DCO check](https://probot.github.io/apps/dco/).
- Tests are automatically run against the changes, some of them can be run locally:

```bash
# run tests with nightly features
make
# run tests on stable toolchain
make WITH_STABLE_TOOLCHAIN=force
# filter a specific test case
make test EXTRA_CARGO_ARGS=<testname>
```

- For changes that might induce performance effects, please quote the targeted benchmark results in the PR description. In addition to micro-benchmarks, there is a standalone [stress test tool](https://github.com/tikv/raft-engine/tree/master/stress) which you can use to demonstrate the system performance.

```
cargo +nightly bench --all-features <bench-case-name>
cargo run --release --package stress -- --help
```

## License

Copyright (c) 2017-present, PingCAP, Inc. Released under the Apache 2.0 license. See [LICENSE](https://github.com/tikv/raft-engine/blob/master/LICENSE) for details.
