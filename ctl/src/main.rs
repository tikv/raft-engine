// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::path::Path;

use structopt::StructOpt;

use raft_engine::{Engine, Error, LogQueue, Result};

#[derive(Debug, StructOpt)]
#[structopt[name="basic"]]
struct ControlOpt {
    // sub command type
    #[structopt[subcommand]]
    cmd: Option<Cmd>,
}

#[derive(Debug, StructOpt)]
pub enum Cmd {
    /// dump out all operations in log files
    Dump {
        #[structopt(
            short,
            long = "path",
            help = "Path of log file directory or specific log file"
        )]
        path: String,

        /// raft_group ids(optional), format: raft_groups_id1,raft_group_id2....
        #[structopt(short, long, use_delimiter = true)]
        raft_groups: Vec<u64>,
    },

    /// check log files for logical errors
    Check {
        /// Path of directory
        #[structopt(short, long)]
        path: String,
    },

    /// check log files for logical errors
    Truncate {
        /// Path of raft-engine storage directory
        #[structopt(short, long)]
        path: String,

        /// check mode
        #[structopt(short, long, possible_values = &["front", "back", "all"])]
        mode: String,

        /// queue name
        #[structopt(short, long, possible_values = &["append", "rewrite", "all"])]
        queue: String,

        /// raft_group ids(optional), format: raft_groups_id1,raft_group_id2....
        #[structopt(short, long, use_delimiter = true)]
        raft_groups: Vec<u64>,
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
    pub fn validate_and_execute(&self) -> Result<()> {
        if self.cmd.is_none() {
            return Err(Error::InvalidArgument("subcommand is needed".to_owned()));
        }

        let cmd = self.cmd.as_ref().unwrap();
        match cmd {
            Cmd::Dump { path, raft_groups } => self.dump(path, raft_groups),
            Cmd::Truncate {
                path,
                mode,
                queue,
                raft_groups,
            } => self.truncate(path, mode, queue, raft_groups),
            Cmd::Check { path } => self.check(path),
        }
    }

    fn help() {
        Self::clap().print_help().ok();
    }

    fn dump(&self, path: &str, raft_groups: &[u64]) -> Result<()> {
        let r = Engine::dump(Path::new(path), &raft_groups.to_vec())?;

        if r.is_empty() {
            println!("No data");
        } else {
            println!("Raft entrys are as follows:\n");
            r.iter().for_each(|entry| println!("{:?}", entry));
        }

        Ok(())
    }

    fn truncate(&self, path: &str, mode: &str, queue: &str, raft_groups: &[u64]) -> Result<()> {
        Engine::unsafe_truncate(Path::new(path), mode, convert_queue(queue), raft_groups)
    }

    fn check(&self, path: &str) -> Result<()> {
        let r = Engine::consistency_check(Path::new(path))?;

        if r.is_empty() {
            println!("All data is Ok")
        } else {
            println!("Corrupted info are as follows:\nraft_group_id, last_intact_index\n");
            r.iter().for_each(|(x, y)| println!("{:?}, {:?}", x, y))
        }

        Ok(())
    }
}

fn main() {
    let opt = ControlOpt::from_args();

    if let Err(e) = opt.validate_and_execute() {
        println!("{:?}", e);
        ControlOpt::help();
    }
}
