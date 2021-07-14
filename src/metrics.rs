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
}

lazy_static! {
    pub static ref ENGINE_WRITE_TIME_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_write_duration_seconds",
        "Bucketed histogram of Raft Engine write duration",
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
    pub static ref LOG_APPEND_SIZE_HISTOGRAM_VEC: LogQueueHistogramVec =
        register_static_histogram_vec!(
            LogQueueHistogramVec,
            "raft_engine_append_log_bytes_per_write",
            "Bucketed histogram of Raft Engine append log size",
            &["type"],
            exponential_buckets(256.0, 2.0, 20).unwrap()
        )
        .unwrap();
    pub static ref LOG_SYNC_TIME_HISTOGRAM: Histogram = register_histogram!(
        "raft_engine_sync_log_duration_seconds",
        "Bucketed histogram of Raft Engine sync log duration",
        exponential_buckets(0.0005, 2.0, 24).unwrap()
    )
    .unwrap();
    pub static ref LOG_READ_SIZE_HISTOGRAM_VEC: LogQueueHistogramVec =
        register_static_histogram_vec!(
            LogQueueHistogramVec,
            "raft_engine_read_log_bytes_per_read",
            "Bucketed histogram of Raft Engine read log size",
            &["type"],
            exponential_buckets(256.0, 2.0, 20).unwrap()
        )
        .unwrap();
}
