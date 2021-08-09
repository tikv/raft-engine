// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::*;
use prometheus_static_metric::*;

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
    pub static ref ENGINE_WRITE_TIME_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_duration_seconds",
        "Bucketed histogram of Raft Engine write duration",
        exponential_buckets(0.0005, 2.0, 24).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_WRITE_SIZE_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_size",
        "Bucketed histogram of Raft Engine write size",
        exponential_buckets(256.0, 2.0, 20).unwrap()
    )
    .unwrap();
    pub static ref ENGINE_READ_ENTRY_TIME_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_read_entry_duration_seconds",
        "Bucketed histogram of Raft Engine read entry duration",
        exponential_buckets(0.0005, 2.0, 24).unwrap()
    )
    .unwrap();
    pub static ref LOG_APPEND_TIME_HISTOGRAM_VEC: LogQueueHistogramVec =
        register_static_histogram_vec!(
            LogQueueHistogramVec,
            "raft_engine_append_log_duration_seconds",
            "Bucketed histogram of Raft Engine append log duration",
            &["type"],
            exponential_buckets(0.0005, 2.0, 24).unwrap()
        )
        .unwrap();
    pub static ref LOG_SYNC_TIME_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_sync_log_duration_seconds",
        "Bucketed histogram of Raft Engine sync log duration",
        exponential_buckets(0.0005, 2.0, 24).unwrap()
    )
    .unwrap();
    pub static ref BACKGROUND_REWRITE_BYTES: LogQueueCounterVec = register_static_int_counter_vec!(
        LogQueueCounterVec,
        "raft_engine_background_rewrite_bytes",
        "Total bytes written during background rewrite",
        &["type"]
    )
    .unwrap();
    pub static ref LOG_FILE_COUNT: LogQueueGaugeVec = register_static_int_gauge_vec!(
        LogQueueGaugeVec,
        "raft_engine_log_file_count",
        "Amount of log files in Raft engine",
        &["type"]
    )
    .unwrap();
}
