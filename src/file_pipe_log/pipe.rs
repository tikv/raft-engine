// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

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
use crate::pipe_log::{
    FileBlockHandle, FileId, FileSeq, LogFileContext, LogQueue, PipeLog, ReactiveBytes,
};
use crate::{perf_context, Result};

use super::file_mgr::{
    ActiveFileCollection, FileCollectionMgr, FileWithFormat, StaleFileCollection,
};
use super::format::{DummyFileExt, FileNameExt, LogFileFormat};
use super::log_file::{build_file_reader, build_file_writer, LogFileWriter};

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
    capacity: usize,
    file_system: Arc<F>,
    listeners: Vec<Arc<dyn EventListener>>,

    active_files: CachePadded<RwLock<ActiveFileCollection<F>>>,
    stale_files: CachePadded<RwLock<StaleFileCollection<F>>>,
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
        // Manually release the StaleFileCollection by `destory` before `drop`.
        self.stale_files
            .write()
            .destroy(self.file_system.clone(), &self.dir, self.queue);
    }
}

impl<F: FileSystem> SinglePipe<F> {
    /// Opens a new [`SinglePipe`].
    pub fn open(
        cfg: &Config,
        file_system: Arc<F>,
        listeners: Vec<Arc<dyn EventListener>>,
        queue: LogQueue,
        mut active_files: ActiveFileCollection<F>,
        mut stale_files: StaleFileCollection<F>,
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

        let create_file = active_files.first_seq == 0;
        let active_seq = if create_file {
            active_files.first_seq = stale_files.file_span().1 + 1;
            let file_id = FileId {
                queue,
                seq: active_files.first_seq,
            };
            let fd = Arc::new(
                if let Some((seq, is_dummy_file)) = stale_files.recycle_one_file() {
                    let dummy_file_id = FileId { queue, seq };
                    let src_path = if is_dummy_file {
                        dummy_file_id.build_dummy_file_path(&cfg.dir)
                    } else {
                        dummy_file_id.build_file_path(&cfg.dir)
                    };
                    let dst_path = file_id.build_file_path(&cfg.dir);
                    if let Err(e) = file_system.reuse(&src_path, &dst_path) {
                        error!("error while trying to reuse one dummy file: {}", e);
                        if let Err(e) = file_system.delete(&src_path) {
                            error!("error while trying to delete one dummy file: {}", e);
                        }
                        file_system.create(&dst_path)?
                    } else {
                        file_system.open(&dst_path)?
                    }
                } else {
                    file_system.create(&file_id.build_file_path(&cfg.dir))?
                },
            );
            active_files.fds.push_back(FileWithFormat {
                handle: fd,
                format: LogFileFormat::new(cfg.format_version, alignment),
            });
            active_files.first_seq
        } else {
            active_files.first_seq + active_files.fds.len() as FileSeq - 1
        };

        for seq in active_files.first_seq..=active_seq {
            for listener in &listeners {
                listener.post_new_log_file(FileId { queue, seq });
            }
        }
        let active_fd = active_files.fds.back().unwrap();
        let active_file = ActiveFile {
            seq: active_seq,
            writer: build_file_writer(
                file_system.as_ref(),
                active_fd.handle.clone(),
                active_fd.format,
                create_file, /* force_reset */
            )?,
            format: active_fd.format,
        };

        let total_files = active_files.fds.len();
        let pipe = Self {
            queue,
            dir: cfg.dir.clone(),
            file_format: LogFileFormat::new(cfg.format_version, alignment),
            target_file_size: cfg.target_file_size.0 as usize,
            capacity,
            file_system,
            listeners,
            active_files: CachePadded::new(RwLock::new(active_files)),
            stale_files: CachePadded::new(RwLock::new(stale_files)),
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
        self.active_files.read().get_fd(file_seq)
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
        let fd = Arc::new(
            if let Some((seq, is_dummy_file)) = self.stale_files.write().recycle_one_file() {
                let stale_file_id = FileId {
                    seq,
                    queue: self.queue,
                };
                let src_path = if is_dummy_file {
                    stale_file_id.build_dummy_file_path(&self.dir)
                } else {
                    stale_file_id.build_file_path(&self.dir)
                };
                let dst_path = file_id.build_file_path(&self.dir);
                if let Err(e) = self.file_system.reuse(&src_path, &dst_path) {
                    error!("error while trying to reuse one stale file, err: {}", e);
                    if let Err(e) = self.file_system.delete(&src_path) {
                        error!("error while trying to delete one stale file, err: {}", e);
                    }
                    self.file_system.create(&path)?
                } else {
                    self.file_system.open(&path)?
                }
            } else {
                self.file_system.create(&path)?
            },
        );
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

        let state = self.active_files.write().push(FileWithFormat {
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

    fn append<T: ReactiveBytes + ?Sized>(&self, bytes: &mut T) -> Result<FileBlockHandle> {
        fail_point!("file_pipe_log::append");
        let mut active_file = self.active_file.lock();
        if active_file.writer.offset() >= self.target_file_size {
            if let Err(e) = self.rotate_imp(&mut active_file) {
                panic!(
                    "error when rotate [{:?}:{}]: {}",
                    self.queue, active_file.seq, e
                );
            }
        }

        let seq = active_file.seq;
        let format = active_file.format;
        let ctx = LogFileContext {
            id: FileId::new(self.queue, seq),
            version: format.version,
        };
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
        if let Err(e) = writer.write(bytes.as_bytes(&ctx), self.target_file_size) {
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

    fn sync(&self) -> Result<()> {
        let mut active_file = self.active_file.lock();
        let seq = active_file.seq;
        let writer = &mut active_file.writer;
        {
            let _t = StopWatch::new(perf_context!(log_sync_duration));
            if let Err(e) = writer.sync() {
                panic!("error when sync [{:?}:{}]: {}", self.queue, seq, e,);
            }
        }

        Ok(())
    }

    fn file_span(&self) -> (FileSeq, FileSeq) {
        self.active_files.read().file_span()
    }

    fn stale_file_span(&self) -> (FileSeq, FileSeq) {
        self.stale_files.read().file_span()
    }

    fn total_size(&self) -> usize {
        self.active_files.read().len() as usize * self.target_file_size
    }

    fn rotate(&self) -> Result<()> {
        self.rotate_imp(&mut self.active_file.lock())
    }

    /// Purge obsolete log files to the specific `FileSeq`.
    ///
    /// Return the actual removed count of purged files.
    fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (prev, current, purged_files) = self.active_files.write().logical_purge(file_seq);
        if file_seq > prev.first_seq + prev.total_len as u64 - 1 {
            debug_assert_eq!(prev, current);
            return Err(box_err!("Purge active or newer files"));
        } else if prev == current {
            return Ok(0);
        }

        // If there exists several stale files for purging in `self.stale_files`,
        // we should check and remove them to avoid the `size` of whole files
        // beyond `self.capacity`.
        {
            let mut stale_files = self.stale_files.write();
            stale_files.concat(prev.first_seq, purged_files);
            let (before, after, last_dummy_seq) = stale_files
                .logical_purge(self.capacity.saturating_sub(self.active_files.read().len()));
            for seq in before.first_seq..after.first_seq {
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
                let file_id = FileId {
                    queue: self.queue,
                    seq,
                };
                // If the current `seq` is less than `last_dummy_seq`, it means that it
                // is a dummy raft log.
                let path = if seq > last_dummy_seq {
                    file_id.build_file_path(&self.dir)
                } else {
                    file_id.build_dummy_file_path(&self.dir)
                };
                self.file_system.delete(&path)?;
            }
        }
        self.flush_metrics(current.total_len);
        Ok((current.first_seq - prev.first_seq) as usize)
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
    fn append<T: ReactiveBytes + ?Sized>(
        &self,
        queue: LogQueue,
        bytes: &mut T,
    ) -> Result<FileBlockHandle> {
        self.pipes[queue as usize].append(bytes)
    }

    #[inline]
    fn sync(&self, queue: LogQueue) -> Result<()> {
        self.pipes[queue as usize].sync()
    }

    #[inline]
    fn file_span(&self, queue: LogQueue) -> (FileSeq, FileSeq) {
        self.pipes[queue as usize].file_span()
    }

    #[inline]
    fn stale_file_span(&self, queue: LogQueue) -> (FileSeq, FileSeq) {
        self.pipes[queue as usize].stale_file_span()
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
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use tempfile::Builder;

    use super::super::format::LogFileFormat;
    use super::super::pipe_builder::lock_dir;
    use super::*;
    use crate::env::{DefaultFileSystem, ObfuscatedFileSystem};
    use crate::pipe_log::Version;
    use crate::util::ReadableSize;

    fn new_test_pipe<F: FileSystem>(
        cfg: &Config,
        queue: LogQueue,
        fs: Arc<F>,
    ) -> Result<SinglePipe<F>> {
        let capacity = match queue {
            LogQueue::Append => cfg.recycle_capacity(),
            LogQueue::Rewrite => 0,
        };
        SinglePipe::open(
            cfg,
            fs,
            Vec::new(),
            queue,
            ActiveFileCollection::new(0, VecDeque::new()),
            StaleFileCollection::new(0, 0, VecDeque::new()),
            capacity,
        )
    }

    fn new_test_pipes(cfg: &Config) -> Result<DualPipes<DefaultFileSystem>> {
        DualPipes::open(
            lock_dir(&cfg.dir)?,
            new_test_pipe(cfg, LogQueue::Append, Arc::new(DefaultFileSystem))?,
            new_test_pipe(cfg, LogQueue::Rewrite, Arc::new(DefaultFileSystem))?,
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
            ..Default::default()
        };
        let queue = LogQueue::Append;

        let pipe_log = new_test_pipes(&cfg).unwrap();
        assert_eq!(pipe_log.file_span(queue), (1, 1));

        let header_size = LogFileFormat::encoded_len(cfg.format_version) as u64;

        // generate file 1, 2, 3
        let content: Vec<u8> = vec![b'a'; 1024];
        let file_handle = pipe_log.append(queue, &mut &content).unwrap();
        assert_eq!(file_handle.id.seq, 1);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 1);

        let file_handle = pipe_log.append(queue, &mut &content).unwrap();
        assert_eq!(file_handle.id.seq, 2);
        assert_eq!(file_handle.offset, header_size);
        assert_eq!(pipe_log.file_span(queue).1, 2);

        pipe_log.rotate(queue).unwrap();

        // purge file 1
        assert_eq!(pipe_log.purge_to(FileId { queue, seq: 2 }).unwrap(), 1);
        assert_eq!(pipe_log.file_span(queue).0, 2);

        // cannot purge active file
        assert!(pipe_log.purge_to(FileId { queue, seq: 4 }).is_err());

        // append position
        let s_content = b"short content".to_vec();
        let file_handle = pipe_log.append(queue, &mut &s_content).unwrap();
        assert_eq!(file_handle.id.seq, 3);
        assert_eq!(file_handle.offset, header_size);

        let file_handle = pipe_log.append(queue, &mut &s_content).unwrap();
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
            // super large capacity for recycling
            purge_threshold: ReadableSize::mb(100),
            enable_log_recycle: true,
            format_version: Version::V2,
            ..Default::default()
        };
        let queue = LogQueue::Append;
        let fs = Arc::new(ObfuscatedFileSystem::default());
        let pipe_log = new_test_pipe(&cfg, queue, fs.clone()).unwrap();
        assert_eq!(pipe_log.file_span(), (1, 1));

        fn content(i: usize) -> Vec<u8> {
            vec![(i % (u8::MAX as usize)) as u8; 16]
        }
        let mut handles = Vec::new();
        for i in 0..10 {
            handles.push(pipe_log.append(&mut &content(i)).unwrap());
            pipe_log.sync().unwrap();
        }
        pipe_log.rotate().unwrap();
        let (first, last) = pipe_log.file_span();
        assert_eq!(pipe_log.purge_to(last).unwrap() as u64, last - first);
        // Try to read stale file.
        for (i, handle) in handles.into_iter().enumerate() {
            assert!(pipe_log.read_bytes(handle).is_err());
            // Bypass pipe log
            let mut reader = build_file_reader(
                fs.as_ref(),
                Arc::new(fs.open(handle.id.build_file_path(path)).unwrap()),
            )
            .unwrap();
            assert_eq!(reader.read(handle).unwrap(), content(i));
            // Delete file so that it cannot be reused.
            fs.delete(handle.id.build_file_path(path)).unwrap();
        }
        // Try to reuse.
        let mut handles = Vec::new();
        for i in 0..10 {
            handles.push(pipe_log.append(&mut &content(i + 1)).unwrap());
            pipe_log.sync().unwrap();
        }
        // Verify the data.
        for (i, handle) in handles.into_iter().enumerate() {
            assert_eq!(pipe_log.read_bytes(handle).unwrap(), content(i + 1));
        }
    }
}
