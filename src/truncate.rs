// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::errors::Error;
use crate::errors::Error::InvalidArgument;
use crate::file_pipe_log::{build_file_reader, build_file_writer};
use crate::file_pipe_log::{LogFd, ReplayMachine};
use crate::log_batch::LogItemBatch;
use crate::log_batch::LogItemContent::{Command, EntryIndexes, Kv};
use crate::truncate::TruncateMode::{All, Back, Front};
use crate::{log_batch, FileBuilder, FileId, LogBatch, LogItemContent, LogQueue};
use hashbrown::HashMap;
use std::fs;
use std::fs::File;
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

#[derive(Default)]
pub struct TruncateMachine {
    items: Vec<LogItemBatch>,
    all_items: HashMap<String, Vec<LogItemBatch>>,
    all_item_postion_info: HashMap<String, HashMap<u64, (u64, u64)>>,
}

#[derive(Clone, Copy)]
pub enum TruncateMode {
    Front = 0,
    Back = 1,
    All = 2,
}

impl Default for TruncateMode {
    fn default() -> Self {
        TruncateMode::All
    }
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
    pub lock_file: Arc<File>,
}

impl ReplayMachine for TruncateMachine {
    fn replay(
        &mut self,
        item_batch: LogItemBatch,
        _file_id: FileId,
        path: &Path,
    ) -> crate::Result<()> {
        let file_name = path.to_str().unwrap().to_owned();
        self.get_item_position_info(&item_batch, &file_name);

        self.all_items
            .entry(file_name)
            .and_modify(|v| v.push(item_batch.clone()))
            .or_insert(vec![item_batch.clone()]);
        Ok(())
    }

    fn merge(&mut self, _rhs: Self, _queue: LogQueue) -> crate::Result<()> {
        for (k, v) in _rhs.all_items {
            self.all_items.insert(k, v);
        }
        for (k, v) in _rhs.all_item_postion_info {
            self.all_item_postion_info.insert(k, v);
        }

        Ok(())
    }
}

impl TruncateMachine {
    pub fn truncate<B: FileBuilder>(
        &mut self,
        builder: Arc<B>,
        truncate_info: &TruncateQueueParameter,
    ) -> crate::Result<()> {
        //first we should convert map to vec and sort
        let mut all_item_vec = Vec::from_iter(self.all_items.iter());
        all_item_vec.sort_by(|(a, _), (b, _)| a.cmp(b));

        let truncate_mode = truncate_info.truncate_mode;
        let need_truncate_raft_group_id = &truncate_info.raft_groups_ids;

        // raft_group_id to last valid index
        let mut last_valid_offset = HashMap::new();
        // raft_group_id to the last index
        let mut group_last_index = HashMap::new();
        // raft_group to last continuous section
        let mut group_last_valid_start_index = HashMap::<u64, (u64, u64)>::new();
        self.fill_raft_group_postion_info(
            &all_item_vec,
            &mut last_valid_offset,
            &mut group_last_index,
            &mut group_last_valid_start_index,
        );

        for (file_name, items) in &all_item_vec {
            let need_truncate = self.need_truncate(
                truncate_mode,
                need_truncate_raft_group_id,
                &mut last_valid_offset,
                &mut group_last_index,
                &mut group_last_valid_start_index,
                file_name,
            );

            let index_info = (
                &mut last_valid_offset,
                &mut group_last_index,
                &mut group_last_valid_start_index,
            );
            if need_truncate {
                self.truncate_to_new_file(
                    Path::new(file_name).as_ref(),
                    builder.clone(),
                    items,
                    index_info,
                    truncate_info,
                )?;
            }
        }
        Ok(())
    }
}

type OffsetInfo<'a> = (
    &'a mut HashMap<u64, u64>,
    &'a mut HashMap<u64, u64>,
    &'a mut HashMap<u64, (u64, u64)>,
);

impl TruncateMachine {
    fn truncate_to_new_file<B: FileBuilder>(
        &self,
        path: &Path,
        builder: Arc<B>,
        items: &[LogItemBatch],
        index_info: OffsetInfo,
        truncate_parms: &TruncateQueueParameter,
    ) -> crate::Result<()> {
        let origin_name = path.to_str().unwrap();
        let truncate_file_name = origin_name.to_owned() + "_truncated";

        let last_valid_offset = index_info.0;
        let group_last_index = index_info.1;
        let group_last_valid_start_index = index_info.2;

        let buf = PathBuf::from(truncate_file_name);
        let fd = Arc::new(LogFd::create(buf.as_path())?);
        let mut log_writer = build_file_writer(builder.as_ref(), buf.as_path(), fd, true)?;
        let mut log_reader =
            build_file_reader(builder.as_ref(), path, Arc::new(LogFd::open(path)?)).unwrap();

        let mut batch = LogBatch::with_capacity(self.items.len());

        let raft_group_ids = &truncate_parms.raft_groups_ids;
        let truncate_mode = truncate_parms.truncate_mode;

        for log_item_batch in items {
            for item in log_item_batch.iter() {
                let item_type = &item.content;
                let raft_id = item.raft_group_id;

                match item_type {
                    EntryIndexes(entry_indexes) => {
                        let raft_group_last_valid_offset =
                            *last_valid_offset.get(&raft_id).unwrap();
                        let mut entrys = Vec::new();
                        let mut indexs = Vec::new();
                        match truncate_mode {
                            All => {
                                let need_skip = *group_last_index.get(&raft_id).unwrap()
                                    != raft_group_last_valid_offset
                                    && (raft_group_ids.is_empty()
                                        || raft_group_ids.contains(&raft_id));
                                if need_skip {
                                    continue;
                                }

                                for entry_index in &entry_indexes.0 {
                                    let entry =
                                        log_reader.read(entry_index.entries.unwrap()).unwrap();
                                    entrys.push(entry);
                                    indexs.push(*entry_index);
                                }
                            }

                            Front => {
                                for entry_index in &entry_indexes.0 {
                                    let idx = entry_index.index;
                                    if (raft_group_ids.is_empty()
                                        || raft_group_ids.contains(&raft_id))
                                        && idx > raft_group_last_valid_offset
                                    {
                                        continue;
                                    }

                                    let entry =
                                        log_reader.read(entry_index.entries.unwrap()).unwrap();
                                    entrys.push(entry);
                                    indexs.push(*entry_index);
                                }
                            }
                            Back => {
                                let first_idx_to_keep =
                                    group_last_valid_start_index.get(&raft_id).unwrap().0;
                                for entry_index in &entry_indexes.0 {
                                    let idx = entry_index.index;
                                    if (raft_group_ids.is_empty()
                                        || raft_group_ids.contains(&raft_id))
                                        && idx < first_idx_to_keep
                                    {
                                        continue;
                                    }

                                    let entry =
                                        log_reader.read(entry_index.entries.unwrap()).unwrap();
                                    entrys.push(entry);
                                    indexs.push(*entry_index);
                                }
                            }
                        }
                        if !indexs.is_empty() {
                            batch.add_raw_entries(raft_id, indexs, entrys)?;
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

        batch.finish_populate(1)?;
        log_writer.write(batch.encoded_bytes(), 0)?;
        log_writer.close()?;
        fs::rename(buf.as_path(), path)?;

        Ok(())
    }

    fn get_item_position_info(&mut self, item_batch: &LogItemBatch, file_name: &str) {
        for it in item_batch.iter() {
            let raft_group_id = it.raft_group_id;
            if let LogItemContent::EntryIndexes(entry) = &it.content {
                let first_index = entry.0.first().unwrap().index;
                let last_index = entry.0.last().unwrap().index;

                let mut new_map = HashMap::new();
                new_map.insert(raft_group_id, (first_index, last_index));
                self.all_item_postion_info
                    .entry(file_name.to_string())
                    .and_modify(|v| {
                        v.entry(raft_group_id).and_modify(|y| {
                            if last_index > y.1 {
                                *y = (y.0, last_index)
                            }
                        });
                    })
                    .or_insert(new_map);
            }
        }
    }

    fn fill_raft_group_postion_info(
        &self,
        all_item_vec: &[(&String, &Vec<LogItemBatch>)],
        last_valid_offset: &mut HashMap<u64, u64>,
        group_last_index: &mut HashMap<u64, u64>,
        group_last_valid_start_index: &mut HashMap<u64, (u64, u64)>,
    ) {
        for (file_name, _) in all_item_vec {
            let idx_info = self
                .all_item_postion_info
                .get(&(*file_name).clone())
                .unwrap();
            for (k, v) in idx_info {
                group_last_index
                    .entry(*k)
                    .and_modify(|x| {
                        if *x < v.1 {
                            *x = v.1
                        }
                    })
                    .or_insert(v.1);
                group_last_valid_start_index
                    .entry(*k)
                    .and_modify(|val| {
                        if val.1 + 1 == v.0 {
                            *val = (val.0, v.1);
                        }

                        if val.1 + 1 < v.0 {
                            *val = *v;
                        }
                    })
                    .or_insert(*v);

                last_valid_offset
                    .entry(*k)
                    .and_modify(|x| {
                        if *x + 1 == v.0 {
                            *x = v.1
                        }
                    })
                    .or_insert(v.1);
            }
        }
    }

    fn need_truncate(
        &self,
        truncate_mode: TruncateMode,
        need_truncate_raft_group_id: &[u64],
        last_valid_offset: &mut HashMap<u64, u64>,
        group_last_index: &mut HashMap<u64, u64>,
        group_last_valid_start_index: &mut HashMap<u64, (u64, u64)>,
        file_name: &&String,
    ) -> bool {
        let position_info = self
            .all_item_postion_info
            .get(&(*file_name).clone())
            .unwrap();

        for (k, v) in position_info {
            let in_truncate_raft_groups =
                need_truncate_raft_group_id.is_empty() || need_truncate_raft_group_id.contains(k);
            if in_truncate_raft_groups {
                match truncate_mode {
                    TruncateMode::Front => {
                        if let Some(last_valid_index) = last_valid_offset.get(k) {
                            if last_valid_index < &v.1 {
                                return true;
                            }
                        }
                    }
                    TruncateMode::Back => {
                        let last_sequence = group_last_valid_start_index.get(k).unwrap();
                        if !(v.0 >= last_sequence.0 && v.1 <= last_sequence.1) {
                            return true;
                        }
                    }

                    TruncateMode::All => {
                        let offset_in_all_file = group_last_index.get(k).unwrap();
                        let valid_offset_in_all_file = last_valid_offset.get(k).unwrap();
                        if offset_in_all_file != valid_offset_in_all_file {
                            return true;
                        }
                    }
                }
            }
        }
        false
    }
}
