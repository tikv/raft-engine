// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

extern crate libc;

use criterion::criterion_main;

mod bench_recovery;

criterion_main!(bench_recovery::benches);
