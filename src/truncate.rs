// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::file_pipe_log::{build_file_reader, build_file_writer, LogFileReader, LogFileWriter};
use crate::file_pipe_log::{LogFd, ReplayMachine};
use crate::log_batch::LogItemBatch;
use crate::log_batch::LogItemContent::{Command, EntryIndexes, Kv};
use crate::{log_batch, FileBuilder, FileId, LogBatch, LogQueue};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct Truncater {
    items: Vec<LogItemBatch>,
}

impl Default for Truncater {
    fn default() -> Self {
        Self { items: vec![] }
    }
}

impl ReplayMachine for Truncater {
    fn replay(&mut self, item_batch: LogItemBatch, _file_id: FileId) -> crate::Result<()> {
        self.items.push(item_batch);
        Ok(())
    }

    fn merge(&mut self, _rhs: Self, _queue: LogQueue) -> crate::Result<()> {
        Ok(())
    }

    fn end<B: FileBuilder>(&mut self, path: &Path, builder: Arc<B>) -> crate::Result<()> {
        let origin_name = path.to_str().unwrap();
        let truncate_file_name = origin_name.to_owned() + "_truncated";

        let buf = PathBuf::from(truncate_file_name);
        let fd = Arc::new(LogFd::create(path)?);
        let mut log_writer = build_file_writer(builder.as_ref(), buf.as_path(), fd, true)?;

        let mut log_reader =
            build_file_reader(builder.as_ref(), path, Arc::new(LogFd::open(path)?)).unwrap();

        //write to file
        self.flush_to_new_file::<B>(&mut log_writer, &mut log_reader)?;

        let tmp_path = origin_name.to_owned() + "_tmp";
        //frist rename old file to tmp and then rename truncate file to new file
        fs::rename(path, Path::new(&tmp_path))?;
        fs::rename(buf.as_path(), path)?;
        Ok(())
    }
}

impl Truncater {
    fn flush_to_new_file<B: FileBuilder>(
        &mut self,
        log_writer: &mut LogFileWriter<B>,
        log_reader: &mut LogFileReader<B>,
    ) -> crate::Result<()> {
        let mut batch = LogBatch::default();
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

                        batch.add_raw_entries(raft_id, entry_indexes.0.clone(), entrys)?;
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
        log_writer.sync()?;

        Ok(())
    }
}
