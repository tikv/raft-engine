// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use std::time::Instant;

use prometheus::*;
use prometheus_static_metric::*;

use crate::util::InstantExt;

pub struct StopWatch<'a> {
    histogram: &'a Histogram,
    start: Instant,
}

impl<'a> StopWatch<'a> {
    #[inline]
    pub fn new(histogram: &'a Histogram) -> Self {
        Self {
            histogram,
            start: Instant::now(),
        }
    }

    #[inline]
    pub fn new_with(histogram: &'a Histogram, start: Instant) -> Self {
        Self { histogram, start }
    }
}

impl<'a> Drop for StopWatch<'a> {
    fn drop(&mut self) {
        self.histogram
            .observe(self.start.saturating_elapsed().as_secs_f64());
    }
}

make_static_metric! {
    pub label_enum LogQueueKind {
        rewrite,
        append,
    }

    pub struct LogQueueHistogramVec: Histogram {
        "type" => LogQueueKind,
    }

    pub struct LogQueueCounterVec: IntCounter {
        "type" => LogQueueKind,
    }

    pub struct LogQueueGaugeVec: IntGauge {
        "type" => LogQueueKind,
    }
}

lazy_static! {
    // Write path.
    pub static ref ENGINE_WRITE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_duration_seconds",
        "Bucketed histogram of Raft Engine write duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_WRITE_PREPROCESS_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_preprocess_duration_seconds",
        "Bucketed histogram of Raft Engine write preprocess duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_WRITE_LEADER_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_leader_duration_seconds",
        "Bucketed histogram of Raft Engine write leader duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_WRITE_APPLY_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_apply_duration_seconds",
        "Bucketed histogram of Raft Engine write apply duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_WRITE_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_size",
        "Bucketed histogram of Raft Engine write size",
        exponential_buckets(256.0, 1.8, 22).unwrap()
    )
    .unwrap();
    pub static ref LOG_ALLOCATE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_allocate_log_duration_seconds",
        "Bucketed histogram of Raft Engine allocate log duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref LOG_SYNC_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_sync_log_duration_seconds",
        "Bucketed histogram of Raft Engine sync log duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref LOG_ROTATE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_rotate_log_duration_seconds",
        "Bucketed histogram of Raft Engine rotate log duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    // Read path.
    pub static ref ENGINE_READ_ENTRY_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_read_entry_duration_seconds",
        "Bucketed histogram of Raft Engine read entry duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_READ_ENTRY_COUNT_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_read_entry_count",
        "Bucketed histogram of Raft Engine read entry count",
        exponential_buckets(1.0, 1.8, 22).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_READ_MESSAGE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_read_message_duration_seconds",
        "Bucketed histogram of Raft Engine read message duration",
        exponential_buckets(0.00005, 1.8, 26).unwrap()
    )
    .unwrap();
    // Misc.
    pub static ref ENGINE_PURGE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_purge_duration_seconds",
        "Bucketed histogram of Raft Engine purge expired files duration",
        exponential_buckets(0.001, 1.8, 22).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_REWRITE_APPEND_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_rewrite_append_duration_seconds",
        "Bucketed histogram of Raft Engine rewrite append queue duration",
        exponential_buckets(0.001, 1.8, 22).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_REWRITE_REWRITE_DURATION_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_rewrite_rewrite_duration_seconds",
        "Bucketed histogram of Raft Engine rewrite rewrite queue duration",
        exponential_buckets(0.001, 1.8, 22).unwrap()
    )
    .unwrap();
    pub static ref BACKGROUND_REWRITE_BYTES: LogQueueHistogramVec = register_static_histogram_vec!(
        LogQueueHistogramVec,
        "raft_engine_background_rewrite_bytes",
        "Bucketed histogram of bytes written during background rewrite",
        &["type"],
        exponential_buckets(256.0, 1.8, 22).unwrap()
    )
    .unwrap();
    pub static ref LOG_FILE_COUNT: LogQueueGaugeVec = register_static_int_gauge_vec!(
        LogQueueGaugeVec,
        "raft_engine_log_file_count",
        "Amount of log files in Raft engine",
        &["type"]
    )
    .unwrap();
    pub static ref LOG_ENTRY_COUNT: LogQueueGaugeVec = register_static_int_gauge_vec!(
        LogQueueGaugeVec,
        "raft_engine_log_entry_count",
        "Number of log entries in Raft engine",
        &["type"]
    )
    .unwrap();
    pub static ref MEMORY_USAGE: IntGauge = register_int_gauge!(
        "raft_engine_memory_usage",
        "Memory in bytes used by Raft engine",
    )
    .unwrap();
}
