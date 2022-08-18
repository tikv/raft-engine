// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::utils::CachePadded;
use fail::fail_point;
use log::error;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::config::Config;
use crate::env::FileSystem;
use crate::event_listener::EventListener;
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogFileContext, LogQueue, PipeLog};
use crate::{perf_context, Error, Result};

use super::format::{FileNameExt, LogFileFormat};
use super::log_file::{build_file_reader, build_file_writer, LogFileWriter};

#[derive(Debug)]
pub struct FileWithFormat<F: FileSystem> {
    pub handle: Arc<F::Handle>,
    pub format: LogFileFormat,
}

struct FileCollection<F: FileSystem> {
    /// Sequence number of the first file.
    first_seq: FileSeq,
    /// Sequence number of the first file that is in use.
    first_seq_in_use: FileSeq,
    fds: VecDeque<FileWithFormat<F>>,
    /// A hint to control the amount of stale files.
    /// Unless `active_len() > capacity`, `len() <= capacity`.
    capacity: usize,
}

#[derive(PartialEq, Eq, Debug)]
struct FileState {
    first_seq: FileSeq,
    first_seq_in_use: FileSeq,
    total_len: usize,
}

/// Note: create a method for any mutable operations.
impl<F: FileSystem> FileCollection<F> {
    /// Takes a stale file if there is one.
    #[inline]
    fn recycle_one_file(&mut self) -> Option<FileSeq> {
        debug_assert!(self.first_seq <= self.first_seq_in_use);
        debug_assert!(!self.fds.is_empty());
        if self.first_seq < self.first_seq_in_use {
            let seq = self.first_seq;
            self.fds.pop_front().unwrap();
            self.first_seq += 1;
            Some(seq)
        } else {
            None
        }
    }

    #[inline]
    fn push(&mut self, file: FileWithFormat<F>) -> FileState {
        self.fds.push_back(file);
        FileState {
            first_seq: self.first_seq,
            first_seq_in_use: self.first_seq_in_use,
            total_len: self.fds.len(),
        }
    }

    #[inline]
    fn logical_purge(&mut self, file_seq: FileSeq) -> (FileState, FileState) {
        let prev = FileState {
            first_seq: self.first_seq,
            first_seq_in_use: self.first_seq_in_use,
            total_len: self.fds.len(),
        };
        if (self.first_seq_in_use..self.first_seq_in_use + self.fds.len() as u64)
            .contains(&file_seq)
        {
            // Remove some obsolete files if capacity is exceeded.
            let obsolete_files = (file_seq - self.first_seq) as usize;
            // When capacity is zero, always remove logically deleted files.
            let capacity_exceeded = self.fds.len().saturating_sub(self.capacity);
            let mut purged = std::cmp::min(capacity_exceeded, obsolete_files);
            // The files with format_version `V1` cannot be chosen as recycle
            // candidates, which should also be removed.
            // Find the newest obsolete `V1` file and refresh purge count.
            for i in (purged..obsolete_files).rev() {
                if !self.fds[i].format.version.has_log_signing() {
                    purged = i + 1;
                    break;
                }
            }
            self.first_seq += purged as u64;
            self.first_seq_in_use = file_seq;
            self.fds.drain(..purged);
        }
        let current = FileState {
            first_seq: self.first_seq,
            first_seq_in_use: self.first_seq_in_use,
            total_len: self.fds.len(),
        };
        (prev, current)
    }
}

struct ActiveFile<F: FileSystem> {
    seq: FileSeq,
    writer: LogFileWriter<F>,
    format: LogFileFormat,
}

/// A file-based log storage that arranges files as one single queue.
pub(super) struct SinglePipe<F: FileSystem> {
    queue: LogQueue,
    dir: String,
    file_format: LogFileFormat,
    target_file_size: usize,
    bytes_per_sync: usize,
    file_system: Arc<F>,
    listeners: Vec<Arc<dyn EventListener>>,

    files: CachePadded<RwLock<FileCollection<F>>>,
    /// The log file opened for write.
    ///
    /// `active_file` must be locked first to acquire both `files` and
    /// `active_file`
    active_file: CachePadded<Mutex<ActiveFile<F>>>,
}

impl<F: FileSystem> Drop for SinglePipe<F> {
    fn drop(&mut self) {
        let mut active_file = self.active_file.lock();
        if let Err(e) = active_file.writer.close() {
            error!("error while closing the active writer: {}", e);
        }
        // Release the unnecessary disk space occupied by stale files. It also reduces
        // recovery time.
        let files = self.files.read();
        for seq in files.first_seq..files.first_seq_in_use {
            let file_id = FileId {
                queue: self.queue,
                seq,
            };
            let path = file_id.build_file_path(&self.dir);
            if let Err(e) = self.file_system.delete(&path) {
                error!(
                    "error while deleting stale file: {}, err_msg: {}",
                    path.display(),
                    e
                )
            }
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
        mut fds: VecDeque<FileWithFormat<F>>,
        capacity: usize,
    ) -> Result<Self> {
        #[allow(unused_mut)]
        let mut alignment = 0;
        #[cfg(feature = "failpoints")]
        {
            let force_set_aligned_layout = || {
                fail_point!("file_pipe_log::open::force_set_aligned_layout", |_| {
                    true
                });
                false
            };
            if force_set_aligned_layout() {
                alignment = 16;
            }
        }

        let create_file = first_seq == 0;
        let active_seq = if create_file {
            first_seq = 1;
            let file_id = FileId {
                queue,
                seq: first_seq,
            };
            let fd = Arc::new(file_system.create(&file_id.build_file_path(&cfg.dir))?);
            fds.push_back(FileWithFormat {
                handle: fd,
                format: LogFileFormat::new(cfg.format_version, alignment),
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
            writer: build_file_writer(
                file_system.as_ref(),
                active_fd.handle.clone(),
                active_fd.format,
                false, /* force_reset */
            )?,
            format: active_fd.format,
        };

        let total_files = fds.len();
        let pipe = Self {
            queue,
            dir: cfg.dir.clone(),
            file_format: LogFileFormat::new(cfg.format_version, alignment),
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
        // TODO(tabokie): test case
        if !(files.first_seq_in_use..files.first_seq_in_use + files.fds.len() as u64)
            .contains(&file_seq)
        {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq_in_use) as usize]
            .handle
            .clone())
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
            if let Some(seq) = self.files.write().recycle_one_file() {
                let src_file_id = FileId {
                    queue: self.queue,
                    seq,
                };
                let src_path = src_file_id.build_file_path(&self.dir);
                let dst_path = file_id.build_file_path(&self.dir);
                // TODO(tabokie)
                if let Err(e) = self.file_system.reuse(&src_path, &dst_path) {
                    error!("error while trying to recycle one expired file: {}", e);
                    self.file_system
                        .delete(&src_path)
                        .expect("remove stale file");
                    Arc::new(self.file_system.create(&path)?)
                } else {
                    Arc::new(self.file_system.open(&path)?)
                }
            } else {
                Arc::new(self.file_system.create(&path)?)
            }
        };
        let mut new_file = ActiveFile {
            seq,
            // The file might generated from a recycled stale-file, always reset the file
            // header of it.
            writer: build_file_writer(
                self.file_system.as_ref(),
                fd.clone(),
                self.file_format,
                true, /* force_reset */
            )?,
            format: self.file_format,
        };
        // File header must be persisted. This way we can recover gracefully if power
        // loss before a new entry is written.
        new_file.writer.sync()?;
        self.sync_dir()?;
        let version = new_file.format.version;
        let alignment = new_file.format.alignment;
        **active_file = new_file;

        let state = self.files.write().push(FileWithFormat {
            handle: fd,
            format: LogFileFormat::new(version, alignment),
        });
        for listener in &self.listeners {
            listener.post_new_log_file(FileId {
                queue: self.queue,
                seq,
            });
        }
        self.flush_metrics(state.total_len);
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
        let mut reader = build_file_reader(self.file_system.as_ref(), fd)?;
        reader.read(handle)
    }

    fn append(&self, bytes: &[u8]) -> Result<FileBlockHandle> {
        fail_point!("file_pipe_log::append");
        let mut active_file = self.active_file.lock();
        let seq = active_file.seq;
        #[cfg(feature = "failpoints")]
        let format = active_file.format;
        let writer = &mut active_file.writer;

        #[cfg(feature = "failpoints")]
        {
            use crate::util::round_up;

            let corrupted_padding = || {
                fail_point!("file_pipe_log::append::corrupted_padding", |_| true);
                false
            };
            if format.version.has_log_signing() && format.alignment > 0 {
                let s_off = round_up(writer.offset(), format.alignment as usize);
                if s_off > writer.offset() {
                    let len = s_off - writer.offset();
                    let mut zeros = vec![0; len];
                    if corrupted_padding() {
                        zeros[len - 1] = 8_u8;
                    }
                    writer.write(&zeros, self.target_file_size)?;
                }
            }
        }
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

    fn total_size(&self) -> usize {
        let files = self.files.read();
        files.fds.len() * self.target_file_size
    }

    fn rotate(&self) -> Result<()> {
        self.rotate_imp(&mut self.active_file.lock())
    }

    /// Purge obsolete log files to the specific `FileSeq`.
    ///
    /// Return the actual removed count of purged files.
    fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (prev, current) = self.files.write().logical_purge(file_seq);
        if file_seq > prev.first_seq + prev.total_len as u64 - 1 {
            debug_assert_eq!(prev, current);
            return Err(box_err!("Purge active or newer files"));
        } else if prev == current {
            return Ok(0);
        }
        for seq in prev.first_seq..current.first_seq {
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
        self.flush_metrics(current.total_len);
        Ok((current.first_seq_in_use - prev.first_seq_in_use) as usize)
    }

    fn fetch_active_file(&self) -> LogFileContext {
        let files = self.files.read();
        LogFileContext {
            id: FileId::new(self.queue, files.first_seq + files.fds.len() as u64 - 1),
            version: files.fds.back().unwrap().format.version,
        }
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
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::super::format::LogFileFormat;
    use super::super::pipe_builder::lock_dir;
    use super::*;
    use crate::env::DefaultFileSystem;
    use crate::pipe_log::Version;
    use crate::util::ReadableSize;

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

        let header_size = LogFileFormat::encode_len(Version::default()) as u64;

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
        pipe_log.purge_to(FileId { queue, seq: 3 }).unwrap();
        assert_eq!(pipe_log.file_span(queue), (3, 3));

        // fetch active file
        let file_context = pipe_log.fetch_active_file(LogQueue::Append);
        assert_eq!(file_context.version, Version::default());
        assert_eq!(file_context.id.seq, 3);
    }

    #[test]
    fn test_file_collection() {
        fn new_file_handler(
            path: &str,
            file_id: FileId,
            version: Version,
        ) -> FileWithFormat<DefaultFileSystem> {
            FileWithFormat {
                handle: Arc::new(
                    DefaultFileSystem
                        .create(&file_id.build_file_path(path))
                        .unwrap(),
                ),
                format: LogFileFormat::new(version, 0 /* alignment */),
            }
        }
        let dir = Builder::new()
            .prefix("test_file_collection")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        // | 12
        let mut files = FileCollection {
            first_seq: 12,
            first_seq_in_use: 12,
            capacity: 5,
            fds: vec![new_file_handler(
                path,
                FileId::new(LogQueue::Append, 12),
                Version::V2,
            )]
            .into(),
        };
        assert_eq!(files.recycle_one_file(), None);
        // | 12 13 14
        files.push(new_file_handler(
            path,
            FileId::new(LogQueue::Append, 13),
            Version::V2,
        ));
        files.push(new_file_handler(
            path,
            FileId::new(LogQueue::Append, 14),
            Version::V1,
        ));
        // 12 13 | 14
        files.logical_purge(14);
        // 13 | 14
        assert_eq!(files.recycle_one_file().unwrap(), 12);
        // 13 | 14 15
        files.push(new_file_handler(
            path,
            FileId::new(LogQueue::Append, 15),
            Version::V2,
        ));
        // V1 file will not be kept around.
        // | 15
        files.logical_purge(15);
        assert_eq!(files.recycle_one_file(), None);
        // | 15 16 17 18 19 20
        for i in 16..=20 {
            files.push(new_file_handler(
                path,
                FileId::new(LogQueue::Append, i),
                Version::V2,
            ));
        }
        assert_eq!(files.recycle_one_file(), None);
        // 16 17 18 | 19 20
        files.logical_purge(19);
        // 17 18 | 19 20
        assert_eq!(files.recycle_one_file().unwrap(), 16);
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
            target_file_size: ReadableSize(1),
            bytes_per_sync: ReadableSize::kb(32),
            // super large capacity for recycling
            purge_threshold: ReadableSize::mb(100),
            enable_log_recycle: true,
            format_version: Version::V2,
            ..Default::default()
        };
        let queue = LogQueue::Append;

        let pipe_log = new_test_pipes(&cfg).unwrap();
        assert_eq!(pipe_log.file_span(queue), (1, 1));

        let content: Vec<u8> = vec![b'a'; 16];
        let mut handles = Vec::new();
        for _ in 0..10 {
            handles.push(pipe_log.append(queue, &content).unwrap());
            pipe_log.maybe_sync(queue, true).unwrap();
        }
        let (first, last) = pipe_log.file_span(queue);
        assert_eq!(
            pipe_log.purge_to(FileId::new(queue, last)).unwrap() as u64,
            last - first
        );
        // Try to read stale file.
        let fs = pipe_log.file_system();
        for handle in handles {
            assert!(pipe_log.read_bytes(handle).is_err());
            // Bypass pipe log
            let mut reader = build_file_reader(
                &fs,
                Arc::new(fs.open(handle.id.build_file_path(path)).unwrap()),
            )
            .unwrap();
            assert_eq!(reader.read(handle).unwrap(), content);
        }
    }
}
