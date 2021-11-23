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

    pub struct LogItemReader<B: FileBuilder> {
        builder: Arc<B>,
        files: Vec<(FileId, PathBuf)>,
        batch_reader: LogItemBatchFileReader<B>,
        items: Vec<LogItem>,
    }

    impl<B: FileBuilder> LogItemReader<B> {
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
                files: vec![(file_id.unwrap(), file.into())],
                batch_reader: LogItemBatchFileReader::new(0),
                items: Vec::new(),
            })
        }

        pub fn new_directory_reader(builder: Arc<B>, dir: &Path) -> Result<Self> {
            if !dir.is_dir() {
                return Err(Error::InvalidArgument(format!(
                    "Not a directory: {}",
                    dir.display()
                )));
            }
            let files = std::fs::read_dir(dir)?
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
            Ok(Self {
                builder,
                files,
                batch_reader: LogItemBatchFileReader::new(0),
                items: Vec::new(),
            })
        }

        pub fn next(&mut self) -> Option<Result<LogItem>> {
            if self.items.is_empty() {
                let next_batch = self.batch_reader.next();
                match next_batch {
                    Ok(Some(b)) => {
                        self.items.append(&mut b.into_items());
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
            self.items.pop().map(Ok)
        }

        fn find_next_readable_file(&mut self) -> Result<()> {
            while let Some((file_id, path)) = self.files.pop() {
                self.batch_reader
                    .open(file_id, build_file_reader(self.builder.as_ref(), &path)?)?;
                if let Some(b) = self.batch_reader.next()? {
                    self.items.append(&mut b.into_items());
                    break;
                }
            }
            Ok(())
        }
    }
}
