use raft_engine::Error;
use raft_engine::Result;
use std::path::PathBuf;
use structopt::StructOpt;

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
        #[structopt(short, long)]
        file: Option<String>,

        /// Path of raft-engine storage directory
        #[structopt(short, long, required_unless = "file")]
        path: Option<String>,

        /// raft_group ids(optional), format: raft_groups_id1,raft_group_id2....
        #[structopt(short, long)]
        raft_groups: Option<String>,
    },

    /// check log files for logical errors
    Check {
        /// Path of directory
        #[structopt(short, long)]
        path: Option<String>,
    },

    /// check log files for logical errors
    Truncate {
        /// Path of raft-engine storage directory
        #[structopt(short, long)]
        path: Option<String>,

        /// check mode
        #[structopt(short, long)]
        mode: Option<String>,

        /// queue name
        #[structopt(short, long)]
        queue: Option<String>,

        /// raft_group ids(optional), format: raft_groups_id1,raft_group_id2....
        #[structopt(short, long)]
        raft_groups: Option<String>,
    },

    /// repair log entry holes by fill in empty message
    Autofill {
        /// Path of raft-engine storage directory
        #[structopt(short, long)]
        path: Option<String>,

        /// queue name
        #[structopt(short, long)]
        queue: Option<String>,

        /// raft_group ids(optional), format: raft_groups_id1,raft_group_id2....
        #[structopt(short, long)]
        raft_groups: Option<Vec<u32>>,
    },
}

impl ControlOpt {
    pub fn validate_and_execute(&self) -> Result<()> {
        if self.cmd.is_none() {
            return Err(Error::InvalidArgument("subcommand is needed".to_owned()));
        }

        let cmd = self.cmd.as_ref().unwrap();
        match cmd {
            Cmd::Autofill { .. } => self.auto_fill(),
            Cmd::Dump { .. } => self.dump(),
            Cmd::Truncate { .. } => self.truncate(),
            Cmd::Check { path } => self.check(path),
        }
    }

    fn auto_fill(&self) -> Result<()> {
        todo!("unplement!")
    }

    fn help() {
        Self::clap().print_help().ok();
    }

    fn dump(&self) -> Result<()> {
        todo!("unplement!")
    }

    fn truncate(&self) -> Result<()> {
        todo!("unplement!")
    }

    fn check(&self, dir: &Option<String>) -> Result<()> {
        if dir.is_none() {
            return Err(Error::InvalidArgument(
                "path of raft-engine should be specify".to_owned(),
            ));
        }
        let p = PathBuf::from(dir.as_ref().unwrap());
        let r = raft_engine::Engine::consistency_check(p.as_path())?;

        if r.is_empty() {
            println!("All data is Ok")
        } else {
            println!(
                "Corrupted info are as follows:\nraft_group_id, last_intact_index\n");
            r.iter().for_each(|(x, y)| {println!("{:?}, {:?}", x, y)})
        }

        Ok(())
    }
}

fn main() {
    let opt = ControlOpt::from_args();
    let result = opt.validate_and_execute();

    if let Err(e) = result {
        println!("{:?}", e);
        ControlOpt::help();
    }
}
