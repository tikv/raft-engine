// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use raft_engine_ctl::ControlOpt;
use clap::Parser;

fn main() {
    let opts: ControlOpt = ControlOpt::parse();

    if let Err(e) = opts.validate_and_execute() {
        println!("{:?}", e);
    }
}
