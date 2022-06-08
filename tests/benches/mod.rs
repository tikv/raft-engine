// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

extern crate libc;

use criterion::{criterion_group, criterion_main, Criterion};

mod bench_falloc;
mod bench_recovery;
mod bench_recycle;

// criterion_main!(bench_recovery::benches);
criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = /* bench_recovery::bench_recovery, bench_recycle::bench_recycle, */bench_falloc::bench_fallocate
}
criterion_main!(benches);
