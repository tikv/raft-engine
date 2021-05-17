# raft-engine

![Rust](https://github.com/tikv/raft-engine/workflows/Rust/badge.svg?branch=master)
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Ftikv%2Fraft-engine.svg?type=shield)](https://app.fossa.com/projects/git%2Bgithub.com%2Ftikv%2Fraft-engine?ref=badge_shield)

A WAL-is-data engine that used to store multi-raft log

# background

Currently, we use an individual RocksDB instance to store raft entries in TiKV, there are some problems to use RocksDB to store raft entries. First entries are appended to WAL and then flushed into SST, which are duplicated operation in this situation. On the other hand, writing amplification hurts performance when compaction. Writing is also needed when dropping applied entries. Disk read is also needed when compaction.

RaftEngine is aimed at replacing the RocksDB instance to store raft entries. Here I will explain how RaftEngine works and what we benefit.

## thread safe

First of all, all operations on RaftEngine are thread safe.

## WAL is data

## pipe_log

RaftEngine contains two major components, `pipe_log` and `memtable`.
`pipe_log` is consists of a series of files, which only supports appending write and all regions share the same pipe_log. `pipe_log` is where data persist.

## memtable

Each region has an individual `memtable`. A `memtable` is consists of three members: `entries`, `kvs`, and `file_index`.

- `entries`: a `VecDeque`, which stores active entries or the position in `pipe_log` of these active entries for this region. Operation on `VecDeque` is fast.
- `kvs`: a HashMap, stores key-value pairs for this region.
- `file_index`: another `VecDeque`, store which files contain entries for this region, it is used to GC expired files.

## LogBatch

`LogBatch` is similar to rocksdb’s WriteBatch, giving write an atomic guarantee. It supports adding entries, putting key-value pairs, deleting keys, and the command to clean a region.

## compact entries

`compact_to(region_id, to_indx)` only drops entries in memtable, and update memtable’s `file_index`. This operation is only in memory and is very fast.

## recovery

When the engine restarts we read files in `pipe_log` one by one, and apply each LogBatch to `memtable` for each region. After that, we get the first index info from kv engine for each region and use this first index to compact `memtable`.
Currently, we have two RocksDB instances, and each one has its own WAL. When kv engine’s WAL lost some data after the machine's crash down, the applied index which persisted in kv engine maybe fallback, but raftdb’s WAL says these entries have been dropped, which will cause panic.
RaftEngine rarely meets such a situation because RaftEngine will keep the latest several files.
As I have tested, recovering from 1GB `pipe_log` data takes 4.6 seconds, which is acceptable.
After using `crc32c`, recover from 1.37GB log files takes 2.3 seconds.

## write process

Add entries or key-value pairs into LogBatch, LogBatch will be encoded to a bytes slice, and append to the currently active file. The previous writing will return the file number which serves the writing, and then apply this LogBatch to associated `memtable`s.

## GC expired files

We use `memtable`’s `file_index` to get which file is the oldest file that contains active entries for each region, and files before the oldest file of all regions who can be dropped.
RaftEngine has a total-size-limit parameter, which is default 1GB. And when the total size of files reaches this value, we will check whose entries left behind so long.
If a region doesn’t have new entries for a long time, and the number of entries doesn't reach `gc_raftlog_threshold`, we need to rewrite these entries to the currently active file, and the old files can be dropped as soon as possible. Since the amount of entries is small, rewriting is trivial.
If a region’s follower lefts behind a lot, causing entries in this region can’t be compacted. We hint `raftstore` to compact this region’s entries, since we wait for the slow follower so long, “total-size-limt’s” time.

## Test result in TiKV

Reduce above 60% raft associated IO.
Reduce disk load and disk IO utilization by 50% at lease.
Increase data importing speed around 10%-15%.


## License
[![FOSSA Status](https://app.fossa.com/api/projects/git%2Bgithub.com%2Ftikv%2Fraft-engine.svg?type=large)](https://app.fossa.com/projects/git%2Bgithub.com%2Ftikv%2Fraft-engine?ref=badge_large)