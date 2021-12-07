// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

mod format;
mod log_file;
mod pipe;
mod pipe_builder;
mod reader;

pub use pipe::DualPipes as FilePipeLog;
pub use pipe_builder::{DualPipesBuilder as FilePipeLogBuilder, ReplayMachine};

/// Public utilities used only for debugging purposes.
pub mod debug {
    use std::collections::VecDeque;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use super::format::FileNameExt;
    use super::log_file::{LogFd, LogFileReader, LogFileWriter};
    use super::reader::LogItemBatchFileReader;
    use crate::file_builder::FileBuilder;
    use crate::log_batch::LogItem;
    use crate::pipe_log::FileId;
    use crate::{Error, Result};

    #[allow(dead_code)]
    pub fn build_file_writer<B: FileBuilder>(
        builder: &B,
        path: &Path,
        create: bool,
    ) -> Result<LogFileWriter<B>> {
        let fd = if create {
            LogFd::create(path)?
        } else {
            LogFd::open(path)?
        };
        let fd = Arc::new(fd);
        super::log_file::build_file_writer(builder, path, fd, create)
    }

    #[allow(dead_code)]
    pub fn build_file_reader<B: FileBuilder>(builder: &B, path: &Path) -> Result<LogFileReader<B>> {
        let fd = Arc::new(LogFd::open(path)?);
        super::log_file::build_file_reader(builder, path, fd)
    }

    #[allow(dead_code)]
    pub struct LogItemReader<B: FileBuilder> {
        builder: Arc<B>,
        files: VecDeque<(FileId, PathBuf)>,
        batch_reader: LogItemBatchFileReader<B>,
        items: VecDeque<LogItem>,
    }

    impl<B: FileBuilder> LogItemReader<B> {
        #[allow(dead_code)]
        pub fn new_file_reader(builder: Arc<B>, file: &Path) -> Result<Self> {
            if !file.is_file() {
                return Err(Error::InvalidArgument(format!(
                    "Not a file: {}",
                    file.display()
                )));
            }
            let file_name = file.file_name().unwrap().to_str().unwrap();
            let file_id = FileId::parse_file_name(file_name);
            if file_id.is_none() {
                return Err(Error::InvalidArgument(format!(
                    "Invalid log file name: {}",
                    file_name
                )));
            }
            Ok(Self {
                builder,
                files: vec![(file_id.unwrap(), file.into())].into(),
                batch_reader: LogItemBatchFileReader::new(0),
                items: VecDeque::new(),
            })
        }

        #[allow(dead_code)]
        pub fn new_directory_reader(builder: Arc<B>, dir: &Path) -> Result<Self> {
            if !dir.is_dir() {
                return Err(Error::InvalidArgument(format!(
                    "Not a directory: {}",
                    dir.display()
                )));
            }
            let mut files: Vec<_> = std::fs::read_dir(dir)?
                .filter_map(|e| {
                    if let Ok(e) = e {
                        let p = e.path();
                        if p.is_file() {
                            if let Some(file_id) =
                                FileId::parse_file_name(p.file_name().unwrap().to_str().unwrap())
                            {
                                return Some((file_id, p));
                            }
                        }
                    }
                    None
                })
                .collect();
            files.sort_by_key(|pair| pair.0);
            Ok(Self {
                builder,
                files: files.into(),
                batch_reader: LogItemBatchFileReader::new(0),
                items: VecDeque::new(),
            })
        }

        #[allow(dead_code)]
        pub fn next(&mut self) -> Option<Result<LogItem>> {
            if self.items.is_empty() {
                let next_batch = self.batch_reader.next();
                match next_batch {
                    Ok(Some(b)) => {
                        self.items.extend(b.into_items());
                    }
                    Ok(None) => {
                        if let Err(e) = self.find_next_readable_file() {
                            self.batch_reader.reset();
                            return Some(Err(e));
                        }
                    }
                    Err(e) => {
                        self.batch_reader.reset();
                        return Some(Err(e));
                    }
                }
            }
            self.items.pop_front().map(Ok)
        }

        fn find_next_readable_file(&mut self) -> Result<()> {
            while let Some((file_id, path)) = self.files.pop_front() {
                self.batch_reader
                    .open(file_id, build_file_reader(self.builder.as_ref(), &path)?)?;
                if let Some(b) = self.batch_reader.next()? {
                    self.items.extend(b.into_items());
                    break;
                }
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::file_builder::DefaultFileBuilder;
        use crate::log_batch::{Command, LogBatch};
        use crate::pipe_log::{FileBlockHandle, LogQueue};
        use crate::test_util::generate_entries;
        use raft::eraftpb::Entry;

        #[test]
        fn test_dump_file() {}

        #[test]
        fn test_debug_file_basic() {
            let dir = tempfile::Builder::new()
                .prefix("test_debug_file_basic")
                .tempdir()
                .unwrap();
            let mut file_id = FileId {
                queue: LogQueue::Rewrite,
                seq: 7,
            };
            let builder = Arc::new(DefaultFileBuilder);
            let entry_data = vec![b'x'; 1024];

            let mut batches = vec![vec![LogBatch::default()]];
            let mut batch = LogBatch::default();
            batch
                .add_entries::<Entry>(7, &generate_entries(1, 11, Some(&entry_data)))
                .unwrap();
            batch.add_command(7, Command::Clean);
            batch.put(7, b"key".to_vec(), b"value".to_vec());
            batch.delete(7, b"key2".to_vec());
            batches.push(vec![batch.clone()]);
            let mut batch2 = LogBatch::default();
            batch2.put(8, b"key3".to_vec(), b"value".to_vec());
            batch2
                .add_entries::<Entry>(8, &generate_entries(5, 15, Some(&entry_data)))
                .unwrap();
            batches.push(vec![batch, batch2]);

            for bs in batches.iter_mut() {
                let file_path = file_id.build_file_path(dir.path());
                // Write a file.
                let mut writer =
                    build_file_writer(builder.as_ref(), &file_path, true /*create*/).unwrap();
                for batch in bs.iter_mut() {
                    let offset = writer.offset() as u64;
                    let len = batch.finish_populate(1 /*compression_threshold*/).unwrap();
                    writer
                        .write(batch.encoded_bytes(), 0 /*target_file_hint*/)
                        .unwrap();
                    batch.finish_write(FileBlockHandle {
                        id: file_id,
                        offset,
                        len,
                    });
                }
                writer.close().unwrap();
                // Read and verify.
                let mut reader =
                    LogItemReader::new_file_reader(builder.clone(), &file_path).unwrap();
                for batch in bs {
                    for item in batch.clone().drain() {
                        assert_eq!(item, reader.next().unwrap().unwrap());
                    }
                }
                assert!(reader.next().is_none());
                file_id.seq += 1;
            }
            // Read directory and verify.
            let mut reader = LogItemReader::new_directory_reader(builder, dir.path()).unwrap();
            for bs in batches.iter() {
                for batch in bs {
                    for item in batch.clone().drain() {
                        assert_eq!(item, reader.next().unwrap().unwrap());
                    }
                }
            }
            assert!(reader.next().is_none())
        }

        #[test]
        fn test_debug_file_error() {
            let dir = tempfile::Builder::new()
                .prefix("test_debug_file_error")
                .tempdir()
                .unwrap();
            let builder = Arc::new(DefaultFileBuilder);
            // An unrelated sub-directory.
            let unrelated_dir = dir.path().join(Path::new("random_dir"));
            std::fs::create_dir(&unrelated_dir).unwrap();
            // An unrelated file.
            let unrelated_file_path = dir.path().join(Path::new("random_file"));
            let _unrelated_file = std::fs::File::create(&unrelated_file_path).unwrap();
            // A corrupted log file.
            let corrupted_file_path = FileId::dummy(LogQueue::Append).build_file_path(dir.path());
            let _corrupted_file = std::fs::File::create(&corrupted_file_path).unwrap();
            // An empty log file.
            let empty_file_path = FileId::dummy(LogQueue::Rewrite).build_file_path(dir.path());
            let mut writer =
                build_file_writer(builder.as_ref(), &empty_file_path, true /*create*/).unwrap();
            writer.close().unwrap();

            assert!(LogItemReader::new_file_reader(builder.clone(), dir.path()).is_err());
            assert!(LogItemReader::new_file_reader(builder.clone(), &unrelated_file_path).is_err());
            assert!(
                LogItemReader::new_directory_reader(builder.clone(), &empty_file_path).is_err()
            );

            let mut reader = LogItemReader::new_directory_reader(builder, dir.path()).unwrap();
            assert!(reader.next().unwrap().is_err());
            assert!(reader.next().is_none());
        }
    }
}
