// Copyright (c) 2017-present, PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

//! # Raft Engine Control

use std::path::Path;

use clap::{crate_authors, crate_version, AppSettings, Parser};
use raft_engine::internals::LogQueue;
use raft_engine::{Engine, Error, Result as EngineResult};

#[derive(Debug, clap::Parser)]
#[clap(
    name = "ctl",
    author = crate_authors!(),
    version = crate_version!(),
    setting = AppSettings::DontCollapseArgsInUsage,
)]
pub struct ControlOpt {
    // sub command type
    #[clap(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Debug, Parser)]
enum Cmd {
    /// Dump log entries in data file(s).
    Dump {
        /// Path of Raft Engine directory or specific log file.
        #[clap(short, long)]
        path: String,

        #[clap(short, long, use_delimiter = true)]
        raft_groups: Vec<u64>,
    },

    /// Check data files for logical errors.
    Check {
        /// Path of Raft Engine directory.
        #[clap(short, long)]
        path: String,
    },

    /// Run Rhai script to repair data files.
    Repair {
        /// Path of Raft Engine directory.
        #[clap(short, long)]
        path: String,

        #[clap(
            short,
            long,
            possible_values = &["append", "rewrite", "all"]
        )]
        queue: String,

        /// Path of Rhai script file.
        #[clap(short, long)]
        script: String,
    },

    /// Try running `purge_expired_files` on existing data directory.
    TryPurge {
        /// Path of Raft Engine directory.
        #[clap(short, long)]
        path: String,
    },
}

fn convert_queue(queue: &str) -> Option<LogQueue> {
    match queue {
        "append" => Some(LogQueue::Append),
        "rewrite" => Some(LogQueue::Rewrite),
        "all" => None,
        _ => unreachable!(),
    }
}

impl ControlOpt {
    pub fn validate_and_execute(mut self) -> EngineResult<()> {
        if self.cmd.is_none() {
            return Err(Error::InvalidArgument("subcommand is needed".to_owned()));
        }

        match self.cmd.take().unwrap() {
            Cmd::Dump { path, raft_groups } => {
                let it = Engine::dump(Path::new(&path))?;
                for item in it {
                    if let Ok(v) = item {
                        if raft_groups.is_empty() || raft_groups.contains(&v.raft_group_id) {
                            println!("{:?}", v)
                        }
                    } else {
                        // output error message
                        println!("{:?}", item)
                    }
                }
            }
            Cmd::Repair {
                path,
                queue,
                script,
            } => {
                let script = std::fs::read_to_string(script)?;
                Engine::unsafe_repair(Path::new(&path), convert_queue(&queue), script)?;
            }
            Cmd::Check { path } => {
                let r = Engine::consistency_check(Path::new(&path))?;
                if r.is_empty() {
                    println!("All data is Ok")
                } else {
                    println!("Corrupted info are as follows:\nraft_group_id, last_intact_index\n");
                    r.iter().for_each(|(x, y)| println!("{:?}, {:?}", x, y))
                }
            }
            Cmd::TryPurge { path } => {
                let e = Engine::open(raft_engine::Config {
                    dir: path,
                    ..Default::default()
                })?;
                println!(
                    "purge_expired_files() returns {:?}",
                    e.purge_expired_files()?
                );
            }
        }
        Ok(())
    }
}

pub fn run_command(mut args: Vec<String>) {
    args.insert(0, "ctl".to_owned());
    let opts = ControlOpt::parse_from(args);
    if let Err(e) = opts.validate_and_execute() {
        println!("{:?}", e);
    }
}
