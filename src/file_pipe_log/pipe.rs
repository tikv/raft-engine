// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::utils::CachePadded;
use fail::fail_point;
use log::error;
use num_traits::FromPrimitive;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::config::Config;
use crate::env::FileSystem;
use crate::event_listener::EventListener;
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue, PipeLog};
use crate::{perf_context, Error, Result};

use super::format::{FileNameExt, LogFileContext, Version};
use super::log_file::{build_file_reader, build_file_writer, FileHandler, LogFileWriter};

struct FileCollection<F: FileSystem> {
    /// Sequence number of the first file.
    first_seq: FileSeq,
    /// Sequence number of the first file that is in use.
    first_seq_in_use: FileSeq,
    fds: VecDeque<FileHandler<F>>,
    /// `0` => no capbility for recycling stale files
    /// `_` => finite volume for recycling stale files
    capacity: usize,
}

#[cfg(test)]
impl<F: FileSystem> Default for FileCollection<F> {
    fn default() -> Self {
        Self {
            first_seq: 0,
            first_seq_in_use: 0,
            fds: VecDeque::new(),
            capacity: 0,
        }
    }
}

impl<F: FileSystem> FileCollection<F> {
    /// Recycle the first obsolete(stale) file and renewed with new FileId.
    ///
    /// Attention please, the recycled file would be automatically `renamed` in
    /// this func.
    pub fn recycle_one_file(&mut self, file_system: &F, dir_path: &str, dst_fd: FileId) -> bool {
        if self.capacity == 0 || self.first_seq >= self.first_seq_in_use {
            return false;
        }
        let mut ret = false;
        let first_file_id = FileId {
            queue: dst_fd.queue,
            seq: self.first_seq,
        };
        if self.fds.pop_front().is_some() {
            let src_path = first_file_id.build_file_path(dir_path); // src filepath
            let dst_path = dst_fd.build_file_path(dir_path); // dst filepath
            match file_system.rename(&src_path, &dst_path) {
                Ok(_) => {
                    // Update the first_seq
                    self.first_seq = first_file_id.seq + 1;
                    if self.first_seq >= self.first_seq_in_use {
                        self.first_seq_in_use = self.first_seq;
                    }
                    ret = true;
                }
                Err(e) => {
                    ret = false;
                    error!("error while trying to recycle one expired file: {}", e);
                }
            }
        }
        // Only if the `rename` made sense, we could return success.
        ret
    }
}

struct ActiveFile<F: FileSystem> {
    seq: FileSeq,
    writer: LogFileWriter<F>,
}

/// A file-based log storage that arranges files as one single queue.
pub(super) struct SinglePipe<F: FileSystem> {
    queue: LogQueue,
    dir: String,
    format_version: Version,
    target_file_size: usize,
    bytes_per_sync: usize,
    file_system: Arc<F>,
    listeners: Vec<Arc<dyn EventListener>>,

    /// All log files.
    files: CachePadded<RwLock<FileCollection<F>>>,
    /// The log file opened for write.
    active_file: CachePadded<Mutex<ActiveFile<F>>>,
}

impl<F: FileSystem> Drop for SinglePipe<F> {
    fn drop(&mut self) {
        let mut active_file = self.active_file.lock();
        if let Err(e) = active_file.writer.close() {
            error!("error while closing single pipe: {}", e);
        }
    }
}

impl<F: FileSystem> SinglePipe<F> {
    /// Opens a new [`SinglePipe`].
    pub fn open(
        cfg: &Config,
        file_system: Arc<F>,
        listeners: Vec<Arc<dyn EventListener>>,
        queue: LogQueue,
        mut first_seq: FileSeq,
        mut fds: VecDeque<FileHandler<F>>,
        capacity: usize,
    ) -> Result<Self> {
        let create_file = first_seq == 0;
        let active_seq = if create_file {
            first_seq = 1;
            let file_id = FileId {
                queue,
                seq: first_seq,
            };
            let fd = Arc::new(file_system.create(&file_id.build_file_path(&cfg.dir))?);
            fds.push_back(FileHandler {
                handle: fd,
                context: LogFileContext::new(
                    file_id,
                    Version::from_u64(cfg.format_version).unwrap(),
                ),
            });
            first_seq
        } else {
            first_seq + fds.len() as u64 - 1
        };

        for seq in first_seq..=active_seq {
            for listener in &listeners {
                listener.post_new_log_file(FileId { queue, seq });
            }
        }
        let active_fd = fds.back().unwrap();
        let active_file = ActiveFile {
            seq: active_seq,
            // Here, we should keep the LogFileFormat conincident with the original one, written by
            // the previous `Pipe`.
            writer: build_file_writer(
                file_system.as_ref(),
                active_fd.handle.clone(),
                active_fd.context.version,
            )?,
        };

        let total_files = fds.len();
        let pipe = Self {
            queue,
            dir: cfg.dir.clone(),
            format_version: Version::from_u64(cfg.format_version).unwrap(),
            target_file_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            file_system,
            listeners,

            files: CachePadded::new(RwLock::new(FileCollection {
                first_seq,
                first_seq_in_use: first_seq,
                fds,
                capacity,
            })),
            active_file: CachePadded::new(Mutex::new(active_file)),
        };
        pipe.flush_metrics(total_files);
        Ok(pipe)
    }

    /// Synchronizes all metadatas associated with the working directory to the
    /// filesystem.
    fn sync_dir(&self) -> Result<()> {
        let path = PathBuf::from(&self.dir);
        std::fs::File::open(path).and_then(|d| d.sync_all())?;
        Ok(())
    }

    /// Returns a shared [`LogFd`] for the specified file sequence number.
    fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<F::Handle>> {
        let files = self.files.read();
        if file_seq < files.first_seq || (file_seq >= files.first_seq + files.fds.len() as u64) {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq) as usize]
            .handle
            .clone())
    }

    /// Returns a shared [`Version`] for the specified file sequence number.
    fn get_format_version(&self, file_seq: FileSeq) -> Result<Version> {
        let files = self.files.read();
        if file_seq < files.first_seq || (file_seq >= files.first_seq + files.fds.len() as u64) {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq) as usize]
            .context
            .version)
    }

    /// Creates a new file for write, and rotates the active log file.
    ///
    /// This operation is atomic in face of errors.
    fn rotate_imp(&self, active_file: &mut MutexGuard<ActiveFile<F>>) -> Result<()> {
        let _t = StopWatch::new((
            &*LOG_ROTATE_DURATION_HISTOGRAM,
            perf_context!(log_rotate_duration),
        ));
        let seq = active_file.seq + 1;
        debug_assert!(seq > 1);

        active_file.writer.close()?;

        let file_id = FileId {
            queue: self.queue,
            seq,
        };
        let path = file_id.build_file_path(&self.dir);
        let fd = {
            let mut files = self.files.write();
            if files.recycle_one_file(&self.file_system, &self.dir, file_id) {
                // Open the recycled file(file is already renamed)
                Arc::new(self.file_system.open(&path)?)
            } else {
                // A new file is introduced.
                Arc::new(self.file_system.create(&path)?)
            }
        };
        let mut new_file = ActiveFile {
            seq,
            writer: build_file_writer(self.file_system.as_ref(), fd.clone(), self.format_version)?,
        };
        // The file might generated from a recycled stale-file, we should reset the file
        // header of it manually.
        new_file.writer.reset()?;
        // File header must be persisted. This way we can recover gracefully if power
        // loss before a new entry is written.
        new_file.writer.sync()?;
        self.sync_dir()?;
        let active_file_format_version = new_file.writer.header.version();
        **active_file = new_file;

        let len = {
            let mut files = self.files.write();
            debug_assert!(files.first_seq + files.fds.len() as u64 == seq);
            files.fds.push_back(FileHandler {
                handle: fd,
                context: LogFileContext::new(
                    FileId {
                        seq,
                        queue: self.queue,
                    },
                    active_file_format_version,
                ),
            });
            for listener in &self.listeners {
                listener.post_new_log_file(FileId {
                    queue: self.queue,
                    seq,
                });
            }
            files.fds.len()
        };
        self.flush_metrics(len);
        Ok(())
    }

    /// Synchronizes current states to related metrics.
    fn flush_metrics(&self, len: usize) {
        match self.queue {
            LogQueue::Append => LOG_FILE_COUNT.append.set(len as i64),
            LogQueue::Rewrite => LOG_FILE_COUNT.rewrite.set(len as i64),
        }
    }
}

impl<F: FileSystem> SinglePipe<F> {
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let fd = self.get_fd(handle.id.seq)?;
        // As the header of each log file already parsed in the processing of loading
        // log files, we just need to build the `LogFileReader`.
        let mut reader = build_file_reader(
            self.file_system.as_ref(),
            fd,
            Some(self.get_format_version(handle.id.seq)?),
        )?;
        reader.read(handle)
    }

    fn append(&self, bytes: &[u8]) -> Result<FileBlockHandle> {
        fail_point!("file_pipe_log::append");
        let mut active_file = self.active_file.lock();
        let seq = active_file.seq;
        let writer = &mut active_file.writer;

        let start_offset = writer.offset();
        if let Err(e) = writer.write(bytes, self.target_file_size) {
            if let Err(te) = writer.truncate() {
                panic!(
                    "error when truncate {} after error: {}, get: {}",
                    seq, e, te
                );
            }
            return Err(e);
        }
        let handle = FileBlockHandle {
            id: FileId {
                queue: self.queue,
                seq,
            },
            offset: start_offset as u64,
            len: writer.offset() - start_offset,
        };
        for listener in &self.listeners {
            listener.on_append_log_file(handle);
        }
        Ok(handle)
    }

    fn maybe_sync(&self, force: bool) -> Result<()> {
        let mut active_file = self.active_file.lock();
        let seq = active_file.seq;
        let writer = &mut active_file.writer;
        if writer.offset() >= self.target_file_size {
            if let Err(e) = self.rotate_imp(&mut active_file) {
                panic!("error when rotate [{:?}:{}]: {}", self.queue, seq, e);
            }
        } else if writer.since_last_sync() >= self.bytes_per_sync || force {
            let _t = StopWatch::new(perf_context!(log_sync_duration));
            if let Err(e) = writer.sync() {
                panic!("error when sync [{:?}:{}]: {}", self.queue, seq, e,);
            }
        }

        Ok(())
    }

    fn file_span(&self) -> (FileSeq, FileSeq) {
        let files = self.files.read();
        (
            files.first_seq_in_use,
            files.first_seq + files.fds.len() as u64 - 1,
        )
    }

    fn expired_file_count(&self) -> usize {
        let files = self.files.read();
        (files.first_seq_in_use - files.first_seq) as usize
    }

    fn total_size(&self) -> usize {
        let files = self.files.read();
        files.fds.len() * self.target_file_size
    }

    fn rotate(&self) -> Result<()> {
        self.rotate_imp(&mut self.active_file.lock())
    }

    fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (
            first_purge_seq, /* first seq for purging */
            purged,          /* count of purged files */
            recycled,        /* count of recycled files */
            remained,        /* count of remained files */
        ) = {
            let mut files = self.files.write();
            if file_seq >= files.first_seq + files.fds.len() as u64 {
                return Err(box_err!("Purge active or newer files"));
            }
            let mut first_purge_seq = 0;
            let end_offset: usize;
            let mut recycled: usize = 0;
            if files.capacity == 0 {
                // Not capable for Recycle
                first_purge_seq = files.first_seq;
                end_offset = file_seq.saturating_sub(files.first_seq) as usize;
                files.fds.drain(..end_offset);
                files.first_seq = file_seq;
                files.first_seq_in_use = file_seq;
            } else {
                // Capable for recycling log files. It means that the
                // FileCollections has a finite volume for storing recycled files.
                // [1] if `reset_volume` <= `expected_purge_count`, it means
                //     that the FileCollection for recycling can not recycle
                //     all files which need to be purged, we should purge
                //     several files and recycle the others.
                // [2] if `reset_volume` > `expected_purge_count`, it means
                //     that the FileCollection has enough space for recycling
                //     all files which need to be purged.
                let reset_volume =
                    files.capacity - (files.first_seq_in_use - files.first_seq) as usize;
                let expected_purge_count = file_seq.saturating_sub(files.first_seq_in_use) as usize;
                if expected_purge_count >= reset_volume {
                    first_purge_seq = files.first_seq;
                    end_offset = expected_purge_count - reset_volume;
                    recycled = reset_volume;
                    files.first_seq += end_offset as u64;
                    files.first_seq_in_use = file_seq;
                } else {
                    end_offset = 0;
                    recycled = file_seq.saturating_sub(files.first_seq_in_use) as usize;
                    files.first_seq_in_use = file_seq;
                }
                files.fds.drain(..end_offset);
            }
            (first_purge_seq, end_offset, recycled, files.fds.len())
        };
        self.flush_metrics(remained);
        for seq in first_purge_seq..first_purge_seq + purged as u64 {
            let file_id = FileId {
                queue: self.queue,
                seq,
            };
            let path = file_id.build_file_path(&self.dir);
            #[cfg(feature = "failpoints")]
            {
                let remove_skipped = || {
                    fail::fail_point!("file_pipe_log::remove_file_skipped", |_| true);
                    false
                };
                if remove_skipped() {
                    continue;
                }
            }
            self.file_system.delete(&path)?;
        }
        Ok(purged + recycled)
    }

    fn fetch_active_file(&self) -> LogFileContext {
        let active_file = self.active_file.lock();
        LogFileContext::new(
            FileId {
                queue: self.queue,
                seq: active_file.seq,
            },
            active_file.writer.header.version(),
        )
    }
}

/// A [`PipeLog`] implementation that stores data in filesystem.
pub struct DualPipes<F: FileSystem> {
    pipes: [SinglePipe<F>; 2],

    _dir_lock: File,
}

impl<F: FileSystem> DualPipes<F> {
    /// Open a new [`DualPipes`]. Assumes the two [`SinglePipe`]s share the
    /// same directory, and that directory is locked by `dir_lock`.
    pub(super) fn open(
        dir_lock: File,
        appender: SinglePipe<F>,
        rewriter: SinglePipe<F>,
    ) -> Result<Self> {
        // TODO: remove this dependency.
        debug_assert_eq!(LogQueue::Append as usize, 0);
        debug_assert_eq!(LogQueue::Rewrite as usize, 1);

        Ok(Self {
            pipes: [appender, rewriter],
            _dir_lock: dir_lock,
        })
    }

    #[cfg(test)]
    pub fn file_system(&self) -> Arc<F> {
        self.pipes[0].file_system.clone()
    }
}

impl<F: FileSystem> PipeLog for DualPipes<F> {
    #[inline]
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        self.pipes[handle.id.queue as usize].read_bytes(handle)
    }

    #[inline]
    fn append(&self, queue: LogQueue, bytes: &[u8]) -> Result<FileBlockHandle> {
        self.pipes[queue as usize].append(bytes)
    }

    #[inline]
    fn maybe_sync(&self, queue: LogQueue, force: bool) -> Result<()> {
        self.pipes[queue as usize].maybe_sync(force)
    }

    #[inline]
    fn file_span(&self, queue: LogQueue) -> (FileSeq, FileSeq) {
        self.pipes[queue as usize].file_span()
    }

    #[inline]
    fn expired_file_count(&self, queue: LogQueue) -> usize {
        self.pipes[queue as usize].expired_file_count()
    }

    #[inline]
    fn total_size(&self, queue: LogQueue) -> usize {
        self.pipes[queue as usize].total_size()
    }

    #[inline]
    fn rotate(&self, queue: LogQueue) -> Result<()> {
        self.pipes[queue as usize].rotate()
    }

    #[inline]
    fn purge_to(&self, file_id: FileId) -> Result<usize> {
        self.pipes[file_id.queue as usize].purge_to(file_id.seq)
    }

    #[inline]
    fn fetch_active_file(&self, queue: LogQueue) -> LogFileContext {
        self.pipes[queue as usize].fetch_active_file()
    }

    #[inline]
    fn fetch_format_version(&self, file_id: FileId) -> Result<Version> {
        self.pipes[file_id.queue as usize].get_format_version(file_id.seq)
    }
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::super::format::LogFileFormat;
    use super::super::pipe_builder::lock_dir;
    use super::*;
    use crate::env::{DefaultFileSystem, WriteExt};
    use crate::util::ReadableSize;
    use std::io::{Read, Seek, SeekFrom, Write};

    fn new_test_pipe(cfg: &Config, queue: LogQueue) -> Result<SinglePipe<DefaultFileSystem>> {
        SinglePipe::open(
            cfg,
            Arc::new(DefaultFileSystem),
            Vec::new(),
            queue,
            0,
            VecDeque::new(),
            match queue {
                LogQueue::Append => cfg.recycle_capacity(),
                LogQueue::Rewrite => 0,
            },
        )
    }

    fn new_test_pipes(cfg: &Config) -> Result<DualPipes<DefaultFileSystem>> {
        DualPipes::open(
            lock_dir(&cfg.dir)?,
            new_test_pipe(cfg, LogQueue::Append)?,
            new_test_pipe(cfg, LogQueue::Rewrite)?,
        )
    }

    #[test]
    fn test_dir_lock() {
        let dir = Builder::new().prefix("test_dir_lock").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let cfg = Config {
            dir: path.to_owned(),
            ..Default::default()
        };

        let _r1 = new_test_pipes(&cfg).unwrap();

        // Only one thread can hold file lock
        let r2 = new_test_pipes(&cfg);

        assert!(format!("{}", r2.err().unwrap())
            .contains("maybe another instance is using this directory"));
    }

    #[test]
    fn test_pipe_log() {
        let dir = Builder::new().prefix("test_pipe_log").tempdir().unwrap();
        let path = dir.path().to_str().unwrap();
        let cfg = Config {
            dir: path.to_owned(),
            target_file_size: ReadableSize::kb(1),
            bytes_per_sync: ReadableSize::kb(32),
            ..Default::default()
        };
        let queue = LogQueue::Append;

        let pipe_log = new_test_pipes(&cfg).unwrap();
        assert_eq!(pipe_log.file_span(queue), (1, 1));

        let header_size = LogFileFormat::len() as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        let file_handle = pipe_log.append(queue, &content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 1);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 2);

        let file_handle = pipe_log.append(queue, &content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 2);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 3);

        // purge file 1
        assert_eq!(pipe_log.purge_to(FileId { queue, seq: 2 }).unwrap(), 1);
        assert_eq!(pipe_log.file_span(queue).0, 2);

        // cannot purge active file
        assert!(pipe_log.purge_to(FileId { queue, seq: 4 }).is_err());

        // append position
        let s_content = b"short content".to_vec();
        let file_handle = pipe_log.append(queue, &s_content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 3);
        assert_eq!(file_handle.offset, header_size);

        let file_handle = pipe_log.append(queue, &s_content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 3);
        assert_eq!(
            file_handle.offset,
            header_size as u64 + s_content.len() as u64
        );

        let content_readed = pipe_log
            .read_bytes(FileBlockHandle {
                id: FileId { queue, seq: 3 },
                offset: header_size as u64,
                len: s_content.len(),
            })
            .unwrap();
        assert_eq!(content_readed, s_content);
        // try to fetch abnormal entry
        let abnormal_content_readed = pipe_log.read_bytes(FileBlockHandle {
            id: FileId { queue, seq: 12 }, // abnormal seq
            offset: header_size as u64,
            len: s_content.len(),
        });
        assert!(abnormal_content_readed.is_err());

        // leave only 1 file to truncate
        assert!(pipe_log.purge_to(FileId { queue, seq: 3 }).is_ok());
        assert_eq!(pipe_log.file_span(queue), (3, 3));

        // fetch active file
        let file_context = pipe_log.fetch_active_file(LogQueue::Append);
        assert_eq!(file_context.version, Version::default());
        assert_eq!(file_context.id.seq, 3);
    }

    #[test]
    fn test_recycle_file_collections() {
        let dir = Builder::new()
            .prefix("test_recycle_file_collections")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let default_file_size = 32 * 1024 * 1024; // 32MB as default
        let file_system = Arc::new(DefaultFileSystem);
        // test FileCollection with Default(Invalid)
        {
            let mut recycle_collections = FileCollection::<DefaultFileSystem>::default();
            assert_eq!(recycle_collections.first_seq, 0);
            assert_eq!(recycle_collections.first_seq_in_use, 0);
            assert_eq!(recycle_collections.capacity, 0);
            assert_eq!(recycle_collections.fds.len(), 0);
            assert!(!recycle_collections.recycle_one_file(
                &file_system,
                path,
                FileId::dummy(LogQueue::Append)
            ));
        }
        // test FileCollection with a valid file
        {
            let data = vec![b'x'; 1024];
            let prepare_file =
                |file_id: FileId,
                 data: &[u8]|
                 -> Result<Arc<<DefaultFileSystem as FileSystem>::Handle>> {
                    let fd = Arc::new(file_system.create(&file_id.build_file_path(path))?);
                    let mut writer = file_system.new_writer(fd.clone())?;
                    writer.allocate(0, default_file_size)?;
                    writer.write_all(data)?;
                    writer.sync()?;
                    Ok(fd)
                };
            let validate_content_of_file =
                |file_id: FileId, expected_data: &[u8]| -> Result<bool> {
                    let data_len = expected_data.len();
                    let mut buf = vec![0; 1024];
                    let fd = Arc::new(file_system.open(&file_id.build_file_path(path)).unwrap());
                    let mut new_reader = file_system.new_reader(fd).unwrap();
                    let actual_len = new_reader.read(&mut buf[..]).unwrap();
                    Ok(if actual_len != data_len {
                        false
                    } else {
                        buf[..] == expected_data[..]
                    })
                };
            let old_file_id = FileId {
                queue: LogQueue::Append,
                seq: 12,
            };
            let cur_file_id = FileId {
                queue: LogQueue::Append,
                seq: 13,
            };
            let new_file_id = FileId {
                queue: LogQueue::Append,
                seq: cur_file_id.seq + 1,
            };
            // mock
            let _ = prepare_file(old_file_id, &data[..]); // prepare old file
            let mut recycle_collections = FileCollection::<DefaultFileSystem> {
                first_seq: old_file_id.seq,
                first_seq_in_use: old_file_id.seq,
                capacity: 3,
                ..Default::default()
            };
            recycle_collections.fds.push_back(FileHandler {
                handle: Arc::new(
                    file_system
                        .open(&old_file_id.build_file_path(path))
                        .unwrap(),
                ),
                context: LogFileContext::dummy(LogQueue::Append),
            });
            // recycle an old file
            assert!(!recycle_collections.recycle_one_file(&file_system, path, new_file_id));
            // update the reycle collection
            {
                recycle_collections.fds.push_back(FileHandler {
                    handle: Arc::new(
                        file_system
                            .open(&old_file_id.build_file_path(path))
                            .unwrap(),
                    ),
                    context: LogFileContext::dummy(LogQueue::Append),
                });
                recycle_collections.first_seq_in_use = cur_file_id.seq;
            }
            // recycle an old file
            assert!(recycle_collections.recycle_one_file(&file_system, path, new_file_id));
            // validate the content of recycled file
            assert!(validate_content_of_file(new_file_id, &data[..]).unwrap());
            // rewrite and validate the cotent
            {
                let refreshed_data = vec![b'm'; 1024];
                let fd = Arc::new(
                    file_system
                        .create(&new_file_id.build_file_path(path))
                        .unwrap(),
                );
                let mut new_writer = file_system.new_writer(fd).unwrap();
                assert!(new_writer.seek(SeekFrom::Start(0)).is_ok());
                assert_eq!(new_writer.write(&refreshed_data[..]).unwrap(), 1024);
                assert!(new_writer.sync().is_ok());
                assert!(validate_content_of_file(new_file_id, &refreshed_data[..]).unwrap());
            }
        }
    }

    #[test]
    fn test_pipe_log_with_recycle() {
        let dir = Builder::new()
            .prefix("test_pipe_log_with_recycle")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let cfg = Config {
            dir: path.to_owned(),
            target_file_size: ReadableSize::kb(1),
            bytes_per_sync: ReadableSize::kb(32),
            purge_threshold: ReadableSize::mb(1),
            allow_recycle: true,
            ..Default::default()
        };
        let queue = LogQueue::Append;

        let pipe_log = new_test_pipes(&cfg).unwrap();
        assert_eq!(pipe_log.file_span(queue), (1, 1));

        let header_size = LogFileFormat::len() as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        let file_handle = pipe_log.append(queue, &content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 1);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 2);

        let file_handle = pipe_log.append(queue, &content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 2);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 3);

        // purge file 1
        assert_eq!(pipe_log.purge_to(FileId { queue, seq: 2 }).unwrap(), 1);
        assert_eq!(pipe_log.expired_file_count(queue), 1);
        assert_eq!(pipe_log.file_span(queue).0, 2);

        // cannot purge active file
        assert!(pipe_log.purge_to(FileId { queue, seq: 4 }).is_err());
        assert_eq!(pipe_log.expired_file_count(queue), 1);

        // append position
        let s_content = b"short content".to_vec();
        let file_handle = pipe_log.append(queue, &s_content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 3);
        assert_eq!(file_handle.offset, header_size);

        let file_handle = pipe_log.append(queue, &s_content).unwrap();
        pipe_log.maybe_sync(queue, false).unwrap();
        assert_eq!(file_handle.id.seq, 3);
        assert_eq!(
            file_handle.offset,
            header_size as u64 + s_content.len() as u64
        );

        let content_readed = pipe_log
            .read_bytes(FileBlockHandle {
                id: FileId { queue, seq: 3 },
                offset: header_size as u64,
                len: s_content.len(),
            })
            .unwrap();
        assert_eq!(content_readed, s_content);
        // try to fetch abnormal entry
        let abnormal_content_readed = pipe_log.read_bytes(FileBlockHandle {
            id: FileId { queue, seq: 12 }, // abnormal seq
            offset: header_size as u64,
            len: s_content.len(),
        });
        assert!(abnormal_content_readed.is_err());

        // leave only 1 file to truncate
        assert!(pipe_log.purge_to(FileId { queue, seq: 3 }).is_ok());
        assert_eq!(pipe_log.file_span(queue), (3, 3));
        assert_eq!(pipe_log.expired_file_count(queue), 2);

        // fetch active file
        let file_context = pipe_log.fetch_active_file(LogQueue::Append);
        assert_eq!(file_context.version, Version::default());
        assert_eq!(file_context.id.seq, 3);
    }
}
