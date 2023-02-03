# Raft Engine Change Log

## [Unreleased]

### Behavior Changes

* Disable log recycling by default.
* `LogBatch::put` returns a `Result<()>` instead of `()`. It errs when the key is reserved for internal use.

### Bug Fixes

* Fix data loss caused by aborted rewrite operation. Downgrading to an earlier version without the fix may produce phantom Raft Groups or keys, i.e. never written but appear in queries.

### New Features

* Support preparing prefilled logs to enable log recycling when start-up.

## [0.3.0] - 2022-09-14

### Bug Fixes

* Unconditionally tolerate `fallocate` failures as a fix to its portability issue. Errors other than `EOPNOTSUPP` will still emit a warning.
* Avoid leaving fractured write after failure by reseeking the file writer. Panic if the reseek fails as well.
* Fix a parallel recovery panic bug.
* Fix panic when an empty batch is written to engine and then reused.

### New Features

* Add `PerfContext` which records detailed time breakdown of the write process to thread-local storage.
* Support recycling obsolete log files to reduce the cost of `fallocate`-ing new ones.

### Public API Changes

* Add `is_empty` to `Engine` API.
* Add metadata deletion capability to `FileSystem` trait. Users can implement `exists_metadata` and `delete_metadata` to clean up obsolete metadata from older versions of Raft Engine.
* Add `Engine::scan_messages` and `Engine::scan_raw_messages` for iterating over written key-values. 
* Add `Engine::get` for getting raw value.
* Move `sync` from `env::WriteExt` to `env::Handle`.
* Deprecate `bytes_per_sync`.

### Behavior Changes

* Change format version to 2 from 1 by default.
* Enable log recycling by default.

## [0.2.0] - 2022-05-25

### Bug Fixes

* Fix a false negative case of `LogBatch::is_empty()` #212
* Fix fsync ordering when rotating log file #219

### New Features

* Support limiting the memory usage of Raft Engine under new feature `swap` #211 
* Add a new Prometheus counter `raft_engine_memory_usage` to track memory usage #207

### Improvements

* Reduce memory usage by 25% #206

### Public API Changes

* Introduce a new error type `Full` #206
* `LogBatch::merge` returns a `Result<()>` instead of `()` #206
