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
    use std::path::Path;
    use std::sync::Arc;

    use super::log_file::{LogFd, LogFileReader, LogFileWriter};
    use crate::file_builder::FileBuilder;
    use crate::Result;

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
}
