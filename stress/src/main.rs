// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

extern crate hdrhistogram;

use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    thread::{sleep, Builder as ThreadBuilder, JoinHandle},
    time::{Duration, Instant},
};

use clap::{crate_authors, crate_version, Parser};
use const_format::formatcp;
use hdrhistogram::Histogram;
use num_traits::FromPrimitive;
use parking_lot_core::SpinWait;
use raft::eraftpb::Entry;
use raft_engine::{
    internals::{EventListener, FileBlockHandle},
    Command, Config, Engine, LogBatch, MessageExt, ReadableSize, Version,
};
use rand::{thread_rng, Rng, RngCore};

type WriteBatch = LogBatch;

#[derive(Clone)]
struct MessageExtTyped;

impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

const DEFAULT_TIME: Duration = Duration::from_secs(60);
const DEFAULT_REGIONS: u64 = 5;
const DEFAULT_PURGE_INTERVAL: Duration = Duration::from_millis(10 * 1000);
const DEFAULT_COMPACT_COUNT: u64 = 0;
const DEFAULT_FORCE_COMPACT_FACTOR: f64 = 0.5;
const DEFAULT_WRITE_THREADS: u64 = 1;
const DEFAULT_WRITE_OPS_PER_THREAD: u64 = 0;
const DEFAULT_READ_THREADS: u64 = 0;
const DEFAULT_READ_OPS_PER_THREAD: u64 = 0;
// Default 50KB log batch size.
const DEFAULT_ENTRY_SIZE: usize = 1024;
const DEFAULT_WRITE_ENTRY_COUNT: u64 = 10;
const DEFAULT_WRITE_REGION_COUNT: u64 = 5;
const DEFAULT_WRITE_SYNC: bool = false;

#[derive(Debug, Clone, Parser)]
#[clap(
    name = "Engine Stress (stress)",
    about = "A stress test tool for Raft Engine",
    author = crate_authors!(),
    version = crate_version!(),
    dont_collapse_args_in_usage = true,
)]
struct ControlOpt {
    /// Path of raft-engine storage directory
    #[clap(
        long = "path",
        takes_value = true,
        help = "Set the data path for Raft Engine"
    )]
    path: String,

    #[clap(
        long = "time",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_TIME.as_secs()),
        value_name = "time[s]",
        help = "Set the stress test time"
    )]
    time: u64,

    #[clap(
        long = "regions",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_REGIONS),
        help = "Set the region count"
    )]
    regions: u64,

    #[clap(
        long = "purge-interval",
        value_name = "interval[s]",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_PURGE_INTERVAL.as_secs()),
        help = "Set the interval to purge obsolete log files"
    )]
    purge_interval: u64,

    #[clap(
        long = "compact-count",
        value_name = "n",
        takes_value = true,
        required = false,
        help = "Compact log entries exceeding this threshold"
    )]
    compact_count: Option<u64>,

    #[clap(
        long = "force-compact-factor",
        value_name = "factor",
        takes_value = true,
        default_value = "0.5",
        validator = |s| {
            let factor = s.parse::<f64>().unwrap();
            if factor >= 1.0 {
                Err(String::from("Factor must be smaller than 1.0"))
            } else if factor <= 0.0 {
                Err(String::from("Factor must be positive"))
            } else {
                Ok(())
            }
        },
        help = "Factor to shrink raft log during force compact"
    )]
    force_compact_factor: f64,

    #[clap(
        long = "write-threads",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_WRITE_THREADS),
        help = "Set the thread count for writing logs"
    )]
    write_threads: u64,

    #[clap(
        long = "write-ops-per-thread",
        value_name = "ops",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_WRITE_OPS_PER_THREAD),
        help = "Set the per-thread OPS for read entry requests"
    )]
    write_ops_per_thread: u64,

    #[clap(
        long = "read-threads",
        value_name = "threads",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_READ_THREADS),
        help = "Set the thread count for reading logs"
    )]
    read_threads: u64,

    #[clap(
        long = "read-ops-per-thread",
        value_name = "ops",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_READ_OPS_PER_THREAD),
        help = "Set the per-thread OPS for read entry requests"
    )]
    read_ops_per_thread: u64,

    #[clap(
        long = "entry-size",
        value_name = "size",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_ENTRY_SIZE),
        help = "Set the average size of log entry"
    )]
    entry_size: usize,

    #[clap(
        long = "write-entry-count",
        value_name = "count",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_WRITE_ENTRY_COUNT),
        help = "Set the average number of written entries of a region in a log batch"
    )]
    write_entry_count: u64,

    #[clap(
        long = "write-region-count",
        value_name = "count",
        takes_value = true,
        default_value = formatcp!("{}", DEFAULT_WRITE_REGION_COUNT),
        help = "Set the average number of written regions in a log batch"
    )]
    write_region_count: u64,

    #[clap(long = "write-without-sync", help = "Do not sync after write")]
    write_without_sync: bool,

    #[clap(long = "reuse-data", help = "Reuse existing data in specified path")]
    reuse_data: bool,

    #[clap(
        long = "target-file-size",
        value_name = "size",
        takes_value = true,
        default_value = "128MB",
        help = "Target log file size for Raft Engine"
    )]
    target_file_size: String,

    #[clap(
        long = "purge-threshold",
        value_name = "size",
        takes_value = true,
        default_value = "10GB",
        help = "Purge if log files are greater than this threshold"
    )]
    purge_threshold: String,

    #[clap(
        long = "purge-rewrite-threshold",
        value_name = "size",
        takes_value = true,
        default_value = "1GB",
        help = "Purge if rewrite log files are greater than this threshold"
    )]
    purge_rewrite_threshold: String,

    #[clap(
        long = "purge-rewrite-garbage-ratio",
        value_name = "ratio",
        takes_value = true,
        default_value = "0.6",
        help = "Purge if rewrite log files garbage ratio is greater than this threshold"
    )]
    purge_rewrite_garbage_ratio: f64,

    #[clap(
        long = "compression-threshold",
        value_name = "size",
        takes_value = true,
        default_value = "8KB",
        help = "Compress log batch bigger than this threshold"
    )]
    batch_compression_threshold: String,

    #[clap(
        long = "format-version",
        takes_value = true,
        default_value = "1",
        help = "Format version of log files"
    )]
    format_version: u64,

    #[clap(
        long = "enable-log-recycle",
        help = "Recycle purged and stale logs for incoming writing"
    )]
    enable_log_recycle: bool,
}

#[derive(Debug, Clone)]
struct TestArgs {
    time: Duration,
    regions: u64,
    purge_interval: Duration,
    compact_count: u64,
    force_compact_factor: f64,
    write_threads: u64,
    write_ops_per_thread: u64,
    read_threads: u64,
    read_ops_per_thread: u64,
    entry_size: usize,
    write_entry_count: u64,
    write_region_count: u64,
    write_without_sync: bool,
}

impl Default for TestArgs {
    fn default() -> Self {
        TestArgs {
            time: DEFAULT_TIME,
            regions: DEFAULT_REGIONS,
            purge_interval: DEFAULT_PURGE_INTERVAL,
            compact_count: DEFAULT_COMPACT_COUNT,
            force_compact_factor: DEFAULT_FORCE_COMPACT_FACTOR,
            write_threads: DEFAULT_WRITE_THREADS,
            write_ops_per_thread: DEFAULT_WRITE_OPS_PER_THREAD,
            read_threads: DEFAULT_READ_THREADS,
            read_ops_per_thread: DEFAULT_READ_OPS_PER_THREAD,
            entry_size: DEFAULT_ENTRY_SIZE,
            write_entry_count: DEFAULT_WRITE_ENTRY_COUNT,
            write_region_count: DEFAULT_WRITE_REGION_COUNT,
            write_without_sync: DEFAULT_WRITE_SYNC,
        }
    }
}

impl TestArgs {
    fn validate(&self) -> Result<(), String> {
        if self.regions < self.write_threads {
            return Err("Write thread count must be smaller than region count.".to_owned());
        }
        if self.regions < self.read_threads {
            return Err("Read thread count must be smaller than region count.".to_owned());
        }
        if self.write_region_count > self.regions / self.write_threads {
            return Err(
                "Write region count must be smaller than region-count / write-threads.".to_owned(),
            );
        }
        Ok(())
    }
}

struct ThreadSummary {
    hist: Histogram<u64>,
    first: Option<Instant>,
    last: Option<Instant>,
}

impl ThreadSummary {
    fn new() -> Self {
        ThreadSummary {
            hist: Histogram::new(3 /* significant figures */).unwrap(),
            first: None,
            last: None,
        }
    }

    fn record(&mut self, start: Instant, end: Instant) {
        self.hist
            .record(end.duration_since(start).as_micros() as u64)
            .unwrap();
        if self.first.is_none() {
            self.first = Some(start);
        } else {
            self.last = Some(start);
        }
    }

    fn qps(&self) -> f64 {
        if let (Some(first), Some(last)) = (self.first, self.last) {
            (self.hist.len() as f64 - 1.0) / last.duration_since(first).as_secs_f64()
        } else {
            0.0
        }
    }
}

struct Summary {
    hist: Option<Histogram<u64>>,
    thread_qps: Vec<f64>,
}

impl Summary {
    fn new() -> Self {
        Summary {
            hist: None,
            thread_qps: Vec::new(),
        }
    }

    fn add(&mut self, s: ThreadSummary) {
        self.thread_qps.push(s.qps());
        if let Some(hist) = &mut self.hist {
            *hist += s.hist;
        } else {
            self.hist = Some(s.hist);
        }
    }

    fn print(&self, name: &str) {
        if !self.thread_qps.is_empty() {
            println!("[{name}]");
            println!(
                "Throughput(QPS) = {:.02}",
                self.thread_qps.iter().fold(0.0, |sum, qps| { sum + qps })
            );
            let hist = self.hist.as_ref().unwrap();
            println!(
                "Latency(μs) min = {}, avg = {:.02}, p50 = {}, p90 = {}, p95 = {}, p99 = {}, p99.9 = {}, max = {}",
                hist.min(),
                hist.mean(),
                hist.value_at_quantile(0.5),
                hist.value_at_quantile(0.9),
                hist.value_at_quantile(0.95),
                hist.value_at_quantile(0.99),
                hist.value_at_quantile(0.999),
                hist.max()
            );
            let fairness = if self.thread_qps.len() > 2 {
                let median = statistical::median(&self.thread_qps);
                let stddev = statistical::standard_deviation(&self.thread_qps, None);
                stddev / median
            } else {
                let first = *self.thread_qps.first().unwrap();
                let last = *self.thread_qps.last().unwrap();
                f64::abs(first - last) / (first + last)
            };
            println!("Fairness = {:.01}%", 100.0 - fairness * 100.0);
        }
    }
}

fn spawn_write(
    engine: Arc<Engine>,
    args: TestArgs,
    index: u64,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<ThreadSummary> {
    ThreadBuilder::new()
        .name(format!("stress-write-thread-{index}"))
        .spawn(move || {
            let mut summary = ThreadSummary::new();
            let mut log_batch = WriteBatch::with_capacity(4 * 1024);
            let mut entry_batch = Vec::with_capacity(args.write_entry_count as usize);
            let min_interval = if args.write_ops_per_thread > 0 {
                Some(Duration::from_secs_f64(
                    1.0 / args.write_ops_per_thread as f64,
                ))
            } else {
                None
            };
            for _ in 0..args.write_entry_count {
                let mut data = vec![0; args.entry_size];
                thread_rng().fill_bytes(&mut data);
                entry_batch.push(Entry {
                    data: data.into(),
                    ..Default::default()
                });
            }
            while !shutdown.load(Ordering::Relaxed) {
                // TODO(tabokie): scattering regions in one batch
                let mut rid = thread_rng().gen_range(0..(args.regions / args.write_threads))
                    * args.write_threads
                    + index;
                for _ in 0..args.write_region_count {
                    let first = engine.first_index(rid).unwrap_or(0);
                    let last = engine.last_index(rid).unwrap_or(0);
                    let entries = prepare_entries(&entry_batch, last + 1);
                    log_batch
                        .add_entries::<MessageExtTyped>(rid, &entries)
                        .unwrap();
                    if args.compact_count > 0 && last - first + 1 > args.compact_count {
                        log_batch.add_command(
                            rid,
                            Command::Compact {
                                index: last - args.compact_count + 1,
                            },
                        );
                    }
                    rid += args.write_threads;
                }
                let mut start = Instant::now();
                if let (Some(i), Some(last)) = (min_interval, summary.last) {
                    // TODO(tabokie): compensate for slow requests
                    wait_til(&mut start, last + i);
                }
                if let Err(e) = engine.write(&mut log_batch, !args.write_without_sync) {
                    println!("write error {e:?} in thread {index}");
                }
                let end = Instant::now();
                summary.record(start, end);
            }
            summary
        })
        .unwrap()
}

fn spawn_read(
    engine: Arc<Engine>,
    args: TestArgs,
    index: u64,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<ThreadSummary> {
    ThreadBuilder::new()
        .name(format!("stress-read-thread-{index}"))
        .spawn(move || {
            let mut summary = ThreadSummary::new();
            let min_interval = if args.read_ops_per_thread > 0 {
                Some(Duration::from_secs_f64(
                    1.0 / args.read_ops_per_thread as f64,
                ))
            } else {
                None
            };
            while !shutdown.load(Ordering::Relaxed) {
                let rid = thread_rng().gen_range(0..(args.regions / args.read_threads))
                    * args.read_threads
                    + index;
                let mut start = Instant::now();
                if let (Some(i), Some(last)) = (min_interval, summary.last) {
                    wait_til(&mut start, last + i);
                }
                // Read newest entry to avoid conflicting with compact
                if let Some(last) = engine.last_index(rid) {
                    if let Err(e) = engine.get_entry::<MessageExtTyped>(rid, last) {
                        println!("read error {e:?} in thread {index}");
                    }
                    let end = Instant::now();
                    summary.record(start, end);
                }
            }
            summary
        })
        .unwrap()
}

fn spawn_purge(
    engine: Arc<Engine>,
    args: TestArgs,
    index: u64,
    shutdown: Arc<AtomicBool>,
) -> JoinHandle<()> {
    ThreadBuilder::new()
        .name(format!("stress-purge-thread-{index}"))
        .spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                sleep(args.purge_interval);
                match engine.purge_expired_files() {
                    Ok(regions) => {
                        for region in regions.into_iter() {
                            let first = engine.first_index(region).unwrap_or(0);
                            let last = engine.last_index(region).unwrap_or(0);
                            let compact_to = last
                                - ((last - first + 1) as f64 * args.force_compact_factor) as u64
                                + 1;
                            engine.compact_to(region, compact_to);
                        }
                    }
                    Err(e) => println!("purge error {e:?} in thread {index}"),
                }
            }
        })
        .unwrap()
}

fn prepare_entries(entries: &[Entry], mut start_index: u64) -> Vec<Entry> {
    entries
        .iter()
        .cloned()
        .map(|mut e| {
            e.set_index(start_index);
            start_index += 1;
            e
        })
        .collect()
}

fn wait_til(now: &mut Instant, t: Instant) {
    // Spin for at most 0.01ms
    const MAX_SPIN_MICROS: u64 = 10;
    if t > *now {
        let mut spin = SpinWait::new();
        let wait_duration = t - *now;
        if wait_duration.as_micros() > MAX_SPIN_MICROS.into() {
            sleep(wait_duration - Duration::from_micros(MAX_SPIN_MICROS));
        }
        loop {
            *now = Instant::now();
            if *now >= t {
                break;
            } else {
                spin.spin_no_yield();
            }
        }
    }
}

struct WrittenBytesHook(AtomicUsize);

impl WrittenBytesHook {
    pub fn new() -> Self {
        Self(AtomicUsize::new(0))
    }

    pub fn print(&self, time: u64) {
        println!(
            "Write Bandwidth = {}/s",
            ReadableSize(self.0.load(Ordering::Relaxed) as u64 / time)
        );
    }
}

impl EventListener for WrittenBytesHook {
    fn on_append_log_file(&self, handle: FileBlockHandle) {
        self.0.fetch_add(handle.len, Ordering::Relaxed);
    }
}

fn main() {
    let mut config = Config::default();
    let mut args: TestArgs = TestArgs::default();

    let opts: ControlOpt = ControlOpt::parse();

    // Raft Engine configurations
    config.dir = opts.path;
    config.target_file_size = ReadableSize::from_str(&opts.target_file_size).unwrap();
    config.purge_threshold = ReadableSize::from_str(&opts.purge_threshold).unwrap();
    config.purge_rewrite_threshold =
        Some(ReadableSize::from_str(&opts.purge_rewrite_threshold).unwrap());
    config.purge_rewrite_garbage_ratio = opts.purge_rewrite_garbage_ratio;
    config.batch_compression_threshold =
        ReadableSize::from_str(&opts.batch_compression_threshold).unwrap();
    config.enable_log_recycle = opts.enable_log_recycle;
    config.format_version = Version::from_u64(opts.format_version).unwrap();
    args.time = Duration::from_secs(opts.time);
    args.regions = opts.regions;
    args.purge_interval = Duration::from_secs(opts.purge_interval);

    if let Some(count) = opts.compact_count {
        args.compact_count = count;
    }

    args.force_compact_factor = opts.force_compact_factor;
    args.write_threads = opts.write_threads;
    args.write_ops_per_thread = opts.write_ops_per_thread;
    args.read_threads = opts.read_threads;
    args.read_ops_per_thread = opts.read_ops_per_thread;
    args.entry_size = opts.entry_size;
    args.write_entry_count = opts.write_entry_count;
    args.write_region_count = opts.write_region_count;
    args.write_without_sync = opts.write_without_sync;
    if !opts.reuse_data {
        // clean up existing log files
        let _ = std::fs::remove_dir_all(&config.dir);
    }

    args.validate().unwrap();
    config.sanitize().unwrap();

    let wb = Arc::new(WrittenBytesHook::new());

    let engine = Arc::new(Engine::open_with_listeners(config, vec![wb.clone()]).unwrap());
    let mut write_threads = Vec::new();
    let mut read_threads = Vec::new();
    let mut misc_threads = Vec::new();
    let shutdown = Arc::new(AtomicBool::new(false));
    if args.purge_interval.as_millis() > 0 {
        misc_threads.push(spawn_purge(
            engine.clone(),
            args.clone(),
            0,
            shutdown.clone(),
        ));
    }
    if args.read_threads > 0 {
        for i in 0..args.read_threads {
            read_threads.push(spawn_read(
                engine.clone(),
                args.clone(),
                i,
                shutdown.clone(),
            ));
        }
    }
    if args.write_threads > 0 {
        for i in 0..args.write_threads {
            write_threads.push(spawn_write(
                engine.clone(),
                args.clone(),
                i,
                shutdown.clone(),
            ));
        }
    }
    sleep(args.time);
    shutdown.store(true, Ordering::Relaxed);
    write_threads
        .into_iter()
        .fold(Summary::new(), |mut s, t| {
            s.add(t.join().unwrap());
            s
        })
        .print("write");
    read_threads
        .into_iter()
        .fold(Summary::new(), |mut s, t| {
            s.add(t.join().unwrap());
            s
        })
        .print("read");
    misc_threads.into_iter().for_each(|t| t.join().unwrap());
    wb.print(args.time.as_secs());
}
