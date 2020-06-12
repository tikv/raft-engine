# raft-engine
A WAL-is-data engine that used to store multi-raft log

# background
Currently we use an individual RocksDB instance to store raft entries in TiKV, there are some problems to use RocksDB to store raft entries. First append to WAL, and then flush to sst, they are duplicated in this situation. On the other hand there is writing amplification when compaction. Writing is also needed when dropping applied entries. Disk read is also needed when compaction.

RaftEngine is aim to instead of the RocksDB instance to store raft entries. Here I will give an explanation about how RaftEngine works and what we benefits.

## thread safe
First of all, all operations on RaftEngine are thread safe.

## WAL is data

## pipe_log
RaftEngine contains two major component, `pipe_log` and `memtables`.
`pipe_log` is consist of a series of files, only support appending write,  all regions share the same pipe_log. `pipe_log` is where data persist.

## memtable
Each region has an individual `memtable`. A `memtable` is consist of three members, `entries`, `kvs` and `file_index`.
entries: a `VecDeque`,  which stores active entries or the position in `pipe_log` of these active entries for this region. Operation on `VecDeque` is fast.
kvs: a HashMap, stores key value pairs for this region.
file_index: another `VecDeque`, store which files contains entries for this region, it is used to GC expired files.

## LogBatch
`LogBatch` is similar to rocksdb’s WriteBatch, giving write atomic guarantee. It support add entries, put key value pairs, delete key and add a command to clean a region.

## compact entries
compact_to(region_id, to_indx) only dropped enries in memtable, and update memtable’s file_index. this operation is only in memory and is very fast. 

## recovery
When restart we read files in `pipe_log` one by one, and apply each LogBatch to `memtable` for each region. After that we get first index info from kv engine for each region, and use this first index to compact `memtable`. 
Currently we have two RocksDB instance, and each one has its own WAL, when kv engine’s WAL lost some data after machine's crash down, the applied index which persisted in kv engine maybe fallback, but raftdb’s WAL says these entries has been dropped, this will cause panic.
RaftEngine rarely meet such situation, because RaftEngine will keep latest several files. 
As I have tested, recover from 1GB `pipe_log` data takes 4.6 seconds, it is acceptable.
After using `crc32c`, recover from 1.37GB log files takes 2.3 seconds.

## write process
Add entries or key value pairs into LogBatch, LogBatch will be encoded to a bytes slice, and append to current active file.The previous writing will return the file number which serve the writing, and then apply this LogBatch to associated `memtables`.

## GC expired files
We use `memtable`’s file_index to get which file is the oldest file that contains active entries for each region, and files before the oldest file of all regions can be dropped.
RaftEngine has a total-size-limit parameter, it is default 1GB, when the total size of files reach this value, we will check whose entries left behind so long.
If a region don’t has new entries for a long time, and the amount of entries does’t reach gc_raftlog_threshold, we need rewrite these entries to current active file, and the old files can be dropped as soon as possible. Since the amount entries is small, so rewriting is triavial.
If a region’s one follower left behind a lot, causing entries in this region can’t be compacted. We hint `raftstore` to compact this region’s entries, since we wait for the slow follower so long, “total-size-limt’s” time.

## Test result in TiKV
Reduce above 60% raft associated IO.
Reduce disk load and disk IO utilization by 50% at lease.
Increase data importing speed around 10%-15%.
