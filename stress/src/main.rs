// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

extern crate hdrhistogram;

use std::str::FromStr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread::{sleep, Builder as ThreadBuilder, JoinHandle};
use std::time::{Duration, Instant};

use clap::{App, Arg};
use const_format::formatcp;
use hdrhistogram::Histogram;
use parking_lot_core::SpinWait;
use raft::eraftpb::Entry;
use raft_engine::{
    Command, Config, EventListener, FileId, LogBatch, LogQueue, MessageExt, RaftLogEngine,
    ReadableSize,
};
use rand::{thread_rng, Rng};

type Engine = RaftLogEngine<MessageExtTyped>;
type WriteBatch = LogBatch<MessageExtTyped>;

#[derive(Clone)]
struct MessageExtTyped;
impl MessageExt for MessageExtTyped {
    type Entry = Entry;

    fn index(entry: &Entry) -> u64 {
        entry.index
    }
}

const DEFAULT_TIME: Duration = Duration::from_secs(60);
const DEFAULT_REGIONS: u64 = 1;
const DEFAULT_PURGE_INTERVAL: Duration = Duration::from_millis(10 * 1000);
const DEFAULT_COMPACT_TTL: Duration = Duration::from_millis(0);
const DEFAULT_COMPACT_COUNT: u64 = 0;
const DEFAULT_FORCE_COMPACT_FACTOR_STR: &str = "0.5";
const DEFAULT_WRITE_THREADS: u64 = 1;
const DEFAULT_WRITE_OPS_PER_THREAD: u64 = 0;
const DEFAULT_READ_THREADS: u64 = 0;
const DEFAULT_READ_OPS_PER_THREAD: u64 = 0;
// Default 50KB log batch size.
const DEFAULT_ENTRY_SIZE: usize = 1024;
const DEFAULT_WRITE_ENTRY_COUNT: u64 = 10;
const DEFAULT_WRITE_REGION_COUNT: u64 = 5;
const DEFAULT_WRITE_SYNC: bool = true;

#[derive(Debug, Clone)]
struct TestArgs {
    time: Duration,
    regions: u64,
    purge_interval: Duration,
    compact_ttl: Duration,
    compact_count: u64,
    force_compact_factor: f32,
    write_threads: u64,
    write_ops_per_thread: u64,
    read_threads: u64,
    read_ops_per_thread: u64,
    entry_size: usize,
    write_entry_count: u64,
    write_region_count: u64,
    write_sync: bool,
}

impl Default for TestArgs {
    fn default() -> Self {
        TestArgs {
            time: DEFAULT_TIME,
            regions: DEFAULT_REGIONS,
            purge_interval: DEFAULT_PURGE_INTERVAL,
            compact_ttl: DEFAULT_COMPACT_TTL,
            compact_count: DEFAULT_COMPACT_COUNT,
            force_compact_factor: DEFAULT_FORCE_COMPACT_FACTOR_STR.parse::<f32>().unwrap(),
            write_threads: DEFAULT_WRITE_THREADS,
            write_ops_per_thread: DEFAULT_WRITE_OPS_PER_THREAD,
            read_threads: DEFAULT_READ_THREADS,
            read_ops_per_thread: DEFAULT_READ_OPS_PER_THREAD,
            entry_size: DEFAULT_ENTRY_SIZE,
            write_entry_count: DEFAULT_WRITE_ENTRY_COUNT,
            write_region_count: DEFAULT_WRITE_REGION_COUNT,
            write_sync: DEFAULT_WRITE_SYNC,
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
            hist: Histogram::new(3 /*significant figures*/).unwrap(),
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
            println!("[{}]", name);
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
                let first = *self.thread_qps.first().unwrap() as f64;
                let last = *self.thread_qps.last().unwrap() as f64;
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
        .name(format!("stress-write-thread-{}", index))
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
                entry_batch.push(Entry {
                    data: vec![0u8; args.entry_size].into(),
                    ..Default::default()
                });
            }
            while !shutdown.load(Ordering::Relaxed) {
                // TODO(tabokie): scattering regions in one batch
                let mut rid = thread_rng().gen_range(0, args.regions / args.write_threads)
                    * args.write_threads
                    + index;
                for _ in 0..args.write_region_count {
                    let first = engine.first_index(rid).unwrap_or(0);
                    let last = engine.last_index(rid).unwrap_or(0);
                    let entries = prepare_entries(&entry_batch, last + 1);
                    log_batch.add_entries(rid, entries);
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
                if let Err(e) = engine.write(&mut log_batch, args.write_sync) {
                    println!("write error {:?} in thread {}", e, index);
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
        .name(format!("stress-read-thread-{}", index))
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
                let rid = thread_rng().gen_range(0, args.regions / args.read_threads)
                    * args.read_threads
                    + index;
                let mut start = Instant::now();
                if let (Some(i), Some(last)) = (min_interval, summary.last) {
                    wait_til(&mut start, last + i);
                }
                // Read newest entry to avoid conflicting with compact
                if let Some(last) = engine.last_index(rid) {
                    if let Err(e) = engine.get_entry(rid, last) {
                        println!("read error {:?} in thread {}", e, index);
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
        .name(format!("stress-purge-thread-{}", index))
        .spawn(move || {
            while !shutdown.load(Ordering::Relaxed) {
                sleep(args.purge_interval);
                match engine.purge_expired_files() {
                    Ok(regions) => {
                        for region in regions.into_iter() {
                            let first = engine.first_index(region).unwrap_or(0);
                            let last = engine.last_index(region).unwrap_or(0);
                            engine.compact_to(
                                region,
                                last - ((last - first + 1) as f32 * args.force_compact_factor)
                                    as u64
                                    + 1,
                            );
                        }
                    }
                    Err(e) => println!("purge error {:?} in thread {}", e, index),
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
    fn on_append_log_file(&self, _queue: LogQueue, _file_id: FileId, bytes: usize) {
        self.0.fetch_add(bytes, Ordering::Relaxed);
    }
}

fn main() {
    let mut args = TestArgs::default();
    let mut config = Config::default();
    let matches = App::new("Engine Stress (stress)")
        .about("A stress test tool for Raft Engine")
        .arg(
            Arg::with_name("path")
                .long("path")
                .required(true)
                .help("Set the data path for Raft Engine")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("time")
                .long("time")
                .value_name("time[s]")
                .default_value(formatcp!("{}", DEFAULT_TIME.as_secs()))
                .help("Set the stress test time")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("regions")
                .long("regions")
                .default_value(formatcp!("{}", DEFAULT_REGIONS))
                .help("Set the region count")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("purge_interval")
                .long("purge-interval")
                .value_name("interval[ms]")
                .default_value(formatcp!("{}", DEFAULT_PURGE_INTERVAL.as_millis()))
                .help("Set the interval to purge obsolete log files")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("compact_ttl")
                .conflicts_with("compact_count")
                .long("compact-ttl")
                .value_name("ttl[ms]")
                .default_value(formatcp!("{}", DEFAULT_COMPACT_TTL.as_millis()))
                .help("Compact log entries older than TTL")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("compact_count")
                .conflicts_with("compact_ttl")
                .long("compact-count")
                .value_name("n")
                .default_value(formatcp!("{}", DEFAULT_COMPACT_COUNT))
                .help("Compact log entries exceeding this threshold")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("force_compact_factor")
                .long("force-compact-factor")
                .value_name("factor")
                .default_value("0.5")
                .validator(|s| {
                    let factor = s.parse::<f32>().unwrap();
                    if factor >= 1.0 {
                        Err(String::from("Factor must be smaller than 1.0"))
                    } else if factor <= 0.0 {
                        Err(String::from("Factor must be positive"))
                    } else {
                        Ok(())
                    }
                })
                .help("Factor to shrink raft log during force compact")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("write_threads")
                .long("write-threads")
                .default_value(formatcp!("{}", DEFAULT_WRITE_THREADS))
                .help("Set the thread count for writing logs")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("write_ops_per_thread")
                .long("write-ops-per-thread")
                .value_name("ops")
                .default_value(formatcp!("{}", DEFAULT_WRITE_OPS_PER_THREAD))
                .help("Set the per-thread OPS for writing logs")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("read_threads")
                .long("read-threads")
                .value_name("threads")
                .default_value(formatcp!("{}", DEFAULT_READ_THREADS))
                .help("Set the thread count for reading logs")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("read_ops_per_thread")
                .long("read-ops-per-thread")
                .value_name("ops")
                .default_value(formatcp!("{}", DEFAULT_READ_OPS_PER_THREAD))
                .help("Set the per-thread OPS for read entry requests")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("entry_size")
                .long("entry-size")
                .value_name("size")
                .default_value(formatcp!("{}", DEFAULT_ENTRY_SIZE))
                .help("Set the average size of log entry")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("write_entry_count")
                .long("write-entry-count")
                .value_name("count")
                .default_value(formatcp!("{}", DEFAULT_WRITE_ENTRY_COUNT))
                .help("Set the average number of written entries of a region in a log batch")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("write_region_count")
                .long("write-region-count")
                .value_name("count")
                .default_value(formatcp!("{}", DEFAULT_WRITE_REGION_COUNT))
                .help("Set the average number of written regions in a log batch")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("write_sync")
                .long("write-sync")
                .value_name("sync")
                .default_value(formatcp!("{}", DEFAULT_WRITE_SYNC))
                .help("Whether to sync write raft logs")
                .takes_value(true),
        )
        // Raft Engine configurations
        .arg(
            Arg::with_name("target_file_size")
                .long("target-file-size")
                .value_name("size")
                .default_value("128MB")
                .help("Target log file size for Raft Engine")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("purge_threshold")
                .long("purge-threshold")
                .value_name("size")
                .default_value("10GB")
                .help("Purge if log files are greater than this threshold")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("batch_compression_threshold")
                .long("compression-threshold")
                .value_name("size")
                .default_value("8KB")
                .help("Compress log batch bigger than this threshold")
                .takes_value(true),
        )
        .get_matches();
    if let Some(s) = matches.value_of("time") {
        args.time = Duration::from_secs(s.parse::<u64>().unwrap());
    }
    if let Some(s) = matches.value_of("regions") {
        args.regions = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("purge_interval") {
        args.purge_interval = Duration::from_millis(s.parse::<u64>().unwrap());
    }
    if let Some(s) = matches.value_of("compact_ttl") {
        args.compact_ttl = Duration::from_millis(s.parse::<u64>().unwrap());
        if args.compact_ttl.as_millis() > 0 {
            println!("Not supported");
            std::process::exit(1);
        }
    }
    if let Some(s) = matches.value_of("compact_count") {
        args.compact_count = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("force_compact_factor") {
        args.force_compact_factor = s.parse::<f32>().unwrap();
    }
    if let Some(s) = matches.value_of("write_threads") {
        args.write_threads = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("write_ops_per_thread") {
        args.write_ops_per_thread = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("read_threads") {
        args.read_threads = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("read_ops_per_thread") {
        args.read_ops_per_thread = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("entry_size") {
        args.entry_size = s.parse::<usize>().unwrap();
    }
    if let Some(s) = matches.value_of("write_entry_count") {
        args.write_entry_count = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("write_region_count") {
        args.write_region_count = s.parse::<u64>().unwrap();
    }
    if let Some(s) = matches.value_of("write_sync") {
        args.write_sync = s.parse::<bool>().unwrap();
    }
    // Raft Engine configurations
    if let Some(s) = matches.value_of("path") {
        config.dir = s.to_string();
    }
    if let Some(s) = matches.value_of("target_file_size") {
        config.target_file_size = ReadableSize::from_str(s).unwrap();
    }
    if let Some(s) = matches.value_of("purge_threshold") {
        config.purge_threshold = ReadableSize::from_str(s).unwrap();
    }
    if let Some(s) = matches.value_of("batch_compression_threshold") {
        config.batch_compression_threshold = ReadableSize::from_str(s).unwrap();
    }
    args.validate().unwrap();

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
