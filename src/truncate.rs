// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::errors::Error;
use crate::errors::Error::InvalidArgument;
use crate::file_pipe_log::{build_file_reader, build_file_writer, LogFileReader, LogFileWriter};
use crate::file_pipe_log::{LogFd, ReplayMachine};
use crate::log_batch::LogItemBatch;
use crate::log_batch::LogItemContent::{Command, EntryIndexes, Kv};
use crate::memtable::EntryIndex;
use crate::truncate::TruncateMode::{All, Back, Front};
use crate::{log_batch, FileBuilder, FileId, LogBatch, LogItemContent, LogQueue};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

pub struct TruncateMachine {
    items: Vec<LogItemBatch>,
    truncate_info: Option<TruncateQueueParameter>,
    has_hole: bool,
    raft_id_to_index: HashMap<u64, u64>,
}

#[derive(Clone, Copy)]
pub enum TruncateMode {
    Front = 0,
    Back = 1,
    All = 2,
}

impl FromStr for TruncateMode {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "front" => Ok(Front),
            "back" => Ok(Back),
            "all" => Ok(All),
            _ => Err(InvalidArgument(format!(
                "unsupported truncate mode {}, only support 'front', 'back', 'all'",
                s
            ))),
        }
    }
}

#[derive(Clone)]
pub struct TruncateQueueParameter {
    pub queue: Option<LogQueue>,
    pub truncate_mode: TruncateMode,
    pub raft_groups_ids: Vec<u64>,
}

impl Default for TruncateMachine {
    fn default() -> Self {
        Self {
            items: vec![],
            truncate_info: None,
            has_hole: false,
            raft_id_to_index: Default::default(),
        }
    }
}

impl ReplayMachine for TruncateMachine {
    fn replay(&mut self, item_batch: LogItemBatch, _file_id: FileId) -> crate::Result<()> {
        for it in item_batch.iter() {
            let raft_group_id = it.raft_group_id;
            if let LogItemContent::EntryIndexes(entry) = &it.content {
                let first_index = entry.0.first().unwrap().index;
                let last_index = entry.0.last().unwrap().index;

                if let Some(v) = self.raft_id_to_index.get(&raft_group_id) {
                    let ids = &self.truncate_info.as_ref().unwrap().raft_groups_ids;
                    let in_raft_groups = ids.is_empty() || ids.contains(&raft_group_id);
                    if v + 1 != first_index && in_raft_groups {
                        self.has_hole = true;
                        break;
                    }
                }

                self.raft_id_to_index.insert(raft_group_id, last_index);
            }
        }

        self.items.push(item_batch);
        Ok(())
    }

    fn merge(&mut self, _rhs: Self, _queue: LogQueue) -> crate::Result<()> {
        Ok(())
    }

    fn truncate<B: FileBuilder>(
        &mut self,
        path: &Path,
        builder: Arc<B>,
    ) -> crate::Result<()> {
        if !self.has_hole {
            return Ok(());
        }

        let origin_name = path.to_str().unwrap();
        let truncate_file_name = origin_name.to_owned() + "_truncated";

        let buf = PathBuf::from(truncate_file_name);
        let fd = Arc::new(LogFd::create(buf.as_path())?);
        let mut log_writer = build_file_writer(builder.as_ref(), buf.as_path(), fd, true)?;

        let mut log_reader =
            build_file_reader(builder.as_ref(), path, Arc::new(LogFd::open(path)?)).unwrap();

        //write to file
        self.flush_to_new_file::<B>(&mut log_writer, &mut log_reader)?;

        //rename to original file name
        fs::rename(buf.as_path(), path)?;

        self.has_hole = false;
        Ok(())
    }

    fn init(&mut self, _params: Option<TruncateQueueParameter>) {
        self.truncate_info = _params;
    }
}

impl TruncateMachine {
    fn flush_to_new_file<B: FileBuilder>(
        &mut self,
        log_writer: &mut LogFileWriter<B>,
        log_reader: &mut LogFileReader<B>,
    ) -> crate::Result<()> {
        let mut batch = LogBatch::with_capacity(self.items.len());
        let mut region_id_to_index = HashMap::<u64, Vec<(Vec<EntryIndex>, Vec<Vec<u8>>)>>::new();

        for log_item_batch in self.items.iter() {
            for item in log_item_batch.iter() {
                let item_type = &item.content;
                let raft_id = item.raft_group_id;

                match item_type {
                    EntryIndexes(entry_indexes) => {
                        let mut cursor = 0;
                        let mut entrys = Vec::new();
                        while cursor < entry_indexes.0.len() {
                            let entry = log_reader
                                .read(entry_indexes.0[cursor].entries.unwrap())
                                .unwrap();
                            entrys.push(entry);
                            cursor += 1;
                        }
                        if let Some(v) = region_id_to_index.get_mut(&raft_id) {
                            v.push((entry_indexes.0.clone(), entrys));
                        } else {
                            let vec = vec![(entry_indexes.0.clone(), entrys)];
                            region_id_to_index.insert(raft_id, vec);
                        }
                    }
                    Command(cmd) => batch.add_command(raft_id, cmd.clone()),
                    Kv(kv) => match kv.op_type {
                        log_batch::OpType::Put => {
                            batch.put(raft_id, kv.key.clone(), kv.value.as_ref().unwrap().to_vec())
                        }
                        log_batch::OpType::Del => batch.delete(raft_id, kv.key.clone()),
                    },
                }
            }
        }


        let truncate_parms = self.truncate_info.as_mut().unwrap();
        let raft_group_ids = &truncate_parms.raft_groups_ids;
        let truncate_mode = truncate_parms.truncate_mode;

        for (k, v) in region_id_to_index.iter() {
            if raft_group_ids.is_empty() || raft_group_ids.contains(k) {
                match truncate_mode {
                    TruncateMode::Front => {
                        let mut last_index: u64 = 0;
                        for (index, entrys) in v {
                            let first_entry_index_in_batch = index.get(0).unwrap().index;
                            let last_entry_index_in_batch = index.last().unwrap().index;
                            if last_index != 0 && last_index + 1 != first_entry_index_in_batch {
                                break;
                            }
                            batch.add_raw_entries(*k, index.clone(), entrys.clone())?;
                            last_index = last_entry_index_in_batch;
                        }
                    }

                    TruncateMode::Back => {
                        let mut last_index: u64 = 0;
                        let mut need_keep = Vec::new();
                        for (index, entrys) in v {
                            let first_entry_index_in_batch = index.get(0).unwrap().index;
                            let last_entry_index_in_batch = index.last().unwrap().index;

                            if last_index != 0 && last_index + 1 != first_entry_index_in_batch {
                                need_keep.clear();
                            }
                            need_keep.push((index.clone(), entrys.clone()));
                            last_index = last_entry_index_in_batch;
                        }

                        for (index, entrys) in need_keep {
                            batch.add_raw_entries(*k, index.clone(), entrys.clone())?;
                        }
                    }

                    TruncateMode::All => {
                        //judge whether there is hole in the raft group
                        let mut last_index: u64 = 0;
                        let mut has_hole = false;
                        for (index, _) in v {
                            let first_entry_index_in_batch = index.get(0).unwrap().index;
                            let last_entry_index_in_batch = index.last().unwrap().index;
                            if last_index != 0 && last_index + 1 != first_entry_index_in_batch {
                                has_hole = true;
                                break;
                            }
                            last_index = last_entry_index_in_batch;
                        }

                        if !has_hole {
                            for (index, entrys) in v {
                                batch.add_raw_entries(*k, index.clone(), entrys.clone())?;
                            }
                        }
                    }
                }
            } else {
                // do not need to truncate, just write to file as origin
                for (index, entry) in v {
                    batch.add_raw_entries(*k, index.clone(), entry.clone())?;
                }
            }
        }

        batch.finish_populate(1)?;
        log_writer.write(batch.encoded_bytes(), 0)?;
        log_writer.close()?;

        Ok(())
    }
}
