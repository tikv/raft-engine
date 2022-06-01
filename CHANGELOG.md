# Raft Engine Change Log

## [Unreleased]

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
