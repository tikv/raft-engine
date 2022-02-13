// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! A [`PipeLog`] implementation that stores data in filesystem.
//!
//! [`PipeLog`]: crate::pipe_log::PipeLog

mod format;
mod log_file;
mod pipe;
mod pipe_builder;
mod reader;

pub use format::FileNameExt;
pub use log_file::DefaultFileSystem;
pub use pipe::DualPipes as FilePipeLog;
pub use pipe_builder::{
    DefaultMachineFactory, DualPipesBuilder as FilePipeLogBuilder, ReplayMachine,
};

pub mod debug {
    //! A set of public utilities used for interacting with log files.

    use std::collections::VecDeque;
    use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use crate::file_pipe_log::log_file::LogFd;
    use crate::file_system::{FileSystem, LowExt, ReadExt, WriteExt};
    use crate::log_batch::LogItem;
    use crate::pipe_log::FileId;
    use crate::{Error, Result};

    use super::format::FileNameExt;
    use super::log_file::{LogFileReader, LogFileWriter};
    use super::reader::LogItemBatchFileReader;

    pub struct ObfuscatedFile {
        inner: Arc<LogFd>,
        offset: usize,
    }

    impl LowExt for ObfuscatedFile {
        fn file_size(&self) -> IoResult<usize> {
            self.inner.file_size()
        }
    }

    impl ReadExt for ObfuscatedFile {}

    impl WriteExt for ObfuscatedFile {
        fn truncate(&self, offset: usize) -> IoResult<()> {
            self.inner.truncate(offset)
        }

        fn sync(&self) -> IoResult<()> {
            self.inner.sync()
        }

        fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
            self.inner.allocate(offset, size)
        }
    }

    impl Write for ObfuscatedFile {
        fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
            let mut new_buf = buf.to_owned();
            for c in &mut new_buf {
                *c = c.wrapping_add(1);
            }
            let len = self.inner.write(self.offset, &new_buf)?;
            self.offset += len;
            Ok(len)
        }

        fn flush(&mut self) -> IoResult<()> {
            Ok(())
        }
    }

    impl Read for ObfuscatedFile {
        fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
            let len = self.inner.read(self.offset, buf)?;
            for c in buf {
                *c = c.wrapping_sub(1);
            }
            self.offset += len;
            Ok(len)
        }
    }

    impl Seek for ObfuscatedFile {
        fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
            match pos {
                SeekFrom::Start(offset) => self.offset = offset as usize,
                SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as usize,
                SeekFrom::End(i) => self.offset = (self.inner.file_size()? as i64 + i) as usize,
            }
            Ok(self.offset as u64)
        }
    }

    pub struct ObfuscatedFileSystem;

    impl FileSystem for ObfuscatedFileSystem {
        type Handle = LogFd;
        type Reader = ObfuscatedFile;
        type Writer = ObfuscatedFile;

        fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<LogFd> {
            LogFd::create(path.as_ref())
        }

        fn open<P: AsRef<Path>>(&self, path: P) -> IoResult<LogFd> {
            LogFd::open(path.as_ref())
        }

        fn new_reader(&self, inner: Arc<LogFd>) -> IoResult<Self::Reader> {
            Ok(ObfuscatedFile { inner, offset: 0 })
        }

        fn new_writer(&self, inner: Arc<LogFd>) -> IoResult<Self::Writer> {
            Ok(ObfuscatedFile { inner, offset: 0 })
        }
    }

    /// Opens a log file for write. When `create` is true, the specified file
    /// will be created first if not exists.
    #[allow(dead_code)]
    pub fn build_file_writer<F: FileSystem>(
        file_system: &F,
        path: &Path,
        create: bool,
    ) -> Result<LogFileWriter<F>> {
        let fd = if create {
            file_system.create(path)?
        } else {
            file_system.open(path)?
        };
        let fd = Arc::new(fd);
        super::log_file::build_file_writer(file_system, path, fd, create)
    }

    /// Opens a log file for read.
    pub fn build_file_reader<F: FileSystem>(
        file_system: &F,
        path: &Path,
    ) -> Result<LogFileReader<F>> {
        let fd = Arc::new(file_system.open(path)?);
        super::log_file::build_file_reader(file_system, path, fd)
    }

    /// An iterator over the log items in log files.
    pub struct LogItemReader<F: FileSystem> {
        system: Arc<F>,
        files: VecDeque<(FileId, PathBuf)>,
        batch_reader: LogItemBatchFileReader<F>,
        items: VecDeque<LogItem>,
    }

    impl<F: FileSystem> Iterator for LogItemReader<F> {
        type Item = Result<LogItem>;

        fn next(&mut self) -> Option<Self::Item> {
            self.next()
        }
    }

    impl<F: FileSystem> LogItemReader<F> {
        /// Creates a new log item reader over one specified log file.
        pub fn new_file_reader(system: Arc<F>, file: &Path) -> Result<Self> {
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
                system,
                files: vec![(file_id.unwrap(), file.into())].into(),
                batch_reader: LogItemBatchFileReader::new(0),
                items: VecDeque::new(),
            })
        }

        /// Creates a new log item reader over all log files under the
        /// specified directory.
        pub fn new_directory_reader(system: Arc<F>, dir: &Path) -> Result<Self> {
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
                system,
                files: files.into(),
                batch_reader: LogItemBatchFileReader::new(0),
                items: VecDeque::new(),
            })
        }

        fn next(&mut self) -> Option<Result<LogItem>> {
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
                    .open(file_id, build_file_reader(self.system.as_ref(), &path)?)?;
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
        use crate::file_pipe_log::DefaultFileSystem;
        use crate::log_batch::{Command, LogBatch};
        use crate::pipe_log::{FileBlockHandle, LogQueue};
        use crate::test_util::generate_entries;
        use raft::eraftpb::Entry;

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
            let file_system = Arc::new(DefaultFileSystem);
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
                    build_file_writer(file_system.as_ref(), &file_path, true /* create */).unwrap();
                for batch in bs.iter_mut() {
                    let offset = writer.offset() as u64;
                    let len = batch
                        .finish_populate(1 /* compression_threshold */)
                        .unwrap();
                    writer
                        .write(batch.encoded_bytes(), 0 /* target_file_hint */)
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
                    LogItemReader::new_file_reader(file_system.clone(), &file_path).unwrap();
                for batch in bs {
                    for item in batch.clone().drain() {
                        assert_eq!(item, reader.next().unwrap().unwrap());
                    }
                }
                assert!(reader.next().is_none());
                file_id.seq += 1;
            }
            // Read directory and verify.
            let mut reader = LogItemReader::new_directory_reader(file_system, dir.path()).unwrap();
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
            let file_system = Arc::new(DefaultFileSystem);
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
            let mut writer = build_file_writer(
                file_system.as_ref(),
                &empty_file_path,
                true, /* create */
            )
            .unwrap();
            writer.close().unwrap();

            assert!(LogItemReader::new_file_reader(file_system.clone(), dir.path()).is_err());
            assert!(
                LogItemReader::new_file_reader(file_system.clone(), &unrelated_file_path).is_err()
            );
            assert!(
                LogItemReader::new_directory_reader(file_system.clone(), &empty_file_path).is_err()
            );

            let mut reader = LogItemReader::new_directory_reader(file_system, dir.path()).unwrap();
            assert!(reader.next().unwrap().is_err());
            assert!(reader.next().is_none());
        }
    }
}
