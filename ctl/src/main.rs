// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use clap::Parser;
use raft_engine_ctl::ControlOpt;

fn main() {
    env_logger::init();
    let opts: ControlOpt = ControlOpt::parse();

    if let Err(e) = opts.validate_and_execute() {
        println!("{:?}", e);
    }
}
