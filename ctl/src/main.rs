// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::path::PathBuf;

use structopt::StructOpt;

use raft_engine::Error;
use raft_engine::Result;

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
        /// Path of a specific raft-engine file
        #[structopt(
            short = "file",
            long,
            required_unless = "path",
            conflicts_with = "path"
        )]
        file: Option<String>,

        /// Path of raft-engine storage directory
        #[structopt(
            short,
            long = "path",
            required_unless = "file",
            conflicts_with = "file"
        )]
        path: Option<String>,

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

    /// repair log entry holes by fill in empty message
    Autofill {
        /// Path of raft-engine storage directory
        #[structopt(short, long)]
        path: String,

        /// queue name
        #[structopt(short, long, possible_values = &["append", "rewrite", "all"])]
        queue: String,

        /// raft_group ids(optional), format: raft_groups_id1,raft_group_id2....
        #[structopt(short, long, use_delimiter = true)]
        raft_groups: Vec<u64>,
    },
}

impl ControlOpt {
    pub fn validate_and_execute(&self) -> Result<()> {
        if self.cmd.is_none() {
            return Err(Error::InvalidArgument("subcommand is needed".to_owned()));
        }

        let cmd = self.cmd.as_ref().unwrap();
        match cmd {
            Cmd::Autofill {
                path,
                queue,
                raft_groups,
            } => self.auto_fill(path, queue, raft_groups),
            Cmd::Dump {
                file,
                path,
                raft_groups,
            } => {
                let p = if file.is_some() {
                    file.as_ref().unwrap()
                } else {
                    path.as_ref().unwrap()
                };
                self.dump(p, raft_groups)
            }
            Cmd::Truncate {
                path,
                mode,
                queue,
                raft_groups,
            } => self.truncate(path, mode, queue, raft_groups),
            Cmd::Check { path } => self.check(path),
        }
    }

    fn auto_fill(&self, path: &str, queue: &str, raft_groups: &[u64]) -> Result<()> {
        let p = PathBuf::from(path);
        raft_engine::Engine::auto_fill(p.as_path(), queue, &raft_groups.to_vec())
    }

    fn help() {
        Self::clap().print_help().ok();
    }

    fn dump(&self, path: &str, raft_groups: &[u64]) -> Result<()> {
        let p = PathBuf::from(path);
        let r = raft_engine::Engine::dump(p.as_path(), &raft_groups.to_vec())?;

        if r.is_empty() {
            println!("No data");
        } else {
            println!("Raft entrys are as follows:\n");
            r.iter().for_each(|entry| println!("{:?}", entry));
        }

        Ok(())
    }

    fn truncate(&self, path: &str, mode: &str, queue: &str, raft_groups: &[u64]) -> Result<()> {
        let p = PathBuf::from(path);
        raft_engine::Engine::truncate(p.as_path(), mode, queue, raft_groups)
    }

    fn check(&self, path: &str) -> Result<()> {
        let p = PathBuf::from(path);
        let r = raft_engine::Engine::consistency_check(p.as_path())?;

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
