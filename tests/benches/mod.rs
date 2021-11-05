// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

#![feature(test)]

extern crate libc;
extern crate test;

use criterion::criterion_main;

mod bench_recovery;

criterion_main!(bench_recovery::benches);
