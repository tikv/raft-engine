use crate::file_pipe_log::ReplayMachine;
use crate::log_batch::{LogItem, LogItemBatch};
use crate::{FileId, LogQueue};

use std::collections::HashMap;

pub struct LogDumer {
    // raft_groups_id to LogItem vec
    pub(crate) logs: HashMap<u64, Vec<LogItem>>,
}

impl Default for LogDumer {
    fn default() -> Self {
        todo!()
    }
}

impl ReplayMachine for LogDumer {
    fn replay(&mut self, item_batch: LogItemBatch, _: FileId) -> crate::Result<()> {
        for item in item_batch.iter() {
            if let Some(value) = self.logs.get_mut(&item.raft_group_id) {
                value.push(item.clone());
            } else {
                self.logs.insert(item.raft_group_id, vec![item.clone()]);
            }
        }

        Ok(())
    }

    #[allow(unused_variables)]
    fn merge(&mut self, rhs: Self, queue: LogQueue) -> crate::Result<()> {
        todo!()
    }
}
