// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as SyncMutex;

use crossbeam::utils::CachePadded;
use fail::fail_point;
use libc::aiocb;
use log::error;
use parking_lot::{Mutex, MutexGuard, RwLock};
use protobuf::{parse_from_bytes, Message};

use crate::config::Config;
use crate::env::{AioContext, AsyncContext, DefaultFileSystem, FileSystem};
use crate::event_listener::EventListener;
use crate::memtable::EntryIndex;
use crate::metrics::*;
use crate::pipe_log::{
    FileBlockHandle, FileId, FileSeq, LogFileContext, LogQueue, PipeLog, ReactiveBytes,
};
use crate::{perf_context, Error, LogBatch, MessageExt, Result};

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
    /// `fds.len()` should be no larger than `capacity` unless it is full of
    /// active files.
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
        if (self.first_seq_in_use..self.first_seq + self.fds.len() as u64).contains(&file_seq) {
            // Remove some obsolete files if capacity is exceeded.
            let obsolete_files = (file_seq - self.first_seq) as usize;
            // When capacity is zero, always remove logically deleted files.
            let capacity_exceeded = self.fds.len().saturating_sub(self.capacity);
            let mut purged = std::cmp::min(capacity_exceeded, obsolete_files);
            // The files with format_version `V1` cannot be chosen as recycle
            // candidates. We will simply make sure there's no V1 stale files in the
            // collection.
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
        if !(files.first_seq_in_use..files.first_seq_in_use + files.fds.len() as u64)
            .contains(&file_seq)
        {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq) as usize]
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
        let fd = Arc::new(if let Some(seq) = self.files.write().recycle_one_file() {
            let src_file_id = FileId {
                queue: self.queue,
                seq,
            };
            let src_path = src_file_id.build_file_path(&self.dir);
            let dst_path = file_id.build_file_path(&self.dir);
            if let Err(e) = self.file_system.reuse(&src_path, &dst_path) {
                error!("error while trying to reuse one expired file: {}", e);
                if let Err(e) = self.file_system.delete(&src_path) {
                    error!("error while trying to delete one expired file: {}", e);
                }
                self.file_system.create(&path)?
            } else {
                self.file_system.open(&path)?
            }
        } else {
            self.file_system.create(&path)?
        });
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

    fn submit_read_req(
        &self,
        handle: &mut FileBlockHandle,
        ctx: &mut F::AsyncIoContext,
    ) {
        let fd = self.get_fd(handle.id.seq).unwrap();
        let mut buf = vec![0 as u8; handle.len];
        self.file_system.as_ref().new_async_reader(fd, ctx).unwrap();
        ctx.submit_read_req(buf, handle.offset).unwrap();
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
    fn async_entry_read<M: Message + MessageExt<Entry = M>>(
        &self,
        ents_idx: &mut Vec<EntryIndex>,
        vec: &mut Vec<M::Entry>,
    ) -> Result<()> {
        let mut handles: Vec<FileBlockHandle> = vec![];
        for (t, i) in ents_idx.iter().enumerate() {
            if t == 0 || (i.entries.unwrap() != ents_idx[t - 1].entries.unwrap()) {
                handles.push(i.entries.unwrap());
            }
        }

        let mut ctx_append = self.pipes[LogQueue::Append as usize]
            .file_system
            .new_async_io_context(handles.len() as usize)
            .unwrap();
        let mut ctx_rewrite = self.pipes[LogQueue::Rewrite as usize]
            .file_system
            .new_async_io_context(handles.len() as usize)
            .unwrap();

        for handle in handles.iter_mut() {
            match handle.id.queue {
                LogQueue::Append => {
                    self.pipes[LogQueue::Append as usize].submit_read_req(
                        handle,
                        &mut ctx_append,
                    );
                }
                LogQueue::Rewrite => {
                    self.pipes[LogQueue::Rewrite as usize].submit_read_req(
                        handle,
                        &mut ctx_rewrite,
                    );
                }
            }
        }

        let mut decode_buf = vec![];
        let mut seq_append: i32 = -1;
        let mut seq_rewrite: i32 = -1;

        for (t, i) in ents_idx.iter().enumerate() {
            decode_buf =
                match t == 0 || ents_idx[t - 1].entries.unwrap() != ents_idx[t].entries.unwrap() {
                    true => match ents_idx[t].entries.unwrap().id.queue {
                        LogQueue::Append => {
                            seq_append += 1;
                            ctx_append.single_wait(seq_append as usize).unwrap();
                            LogBatch::decode_entries_block(
                                &ctx_append.data(seq_append as usize),
                                i.entries.unwrap(),
                                i.compression_type,
                            )
                            .unwrap()
                        }
                        LogQueue::Rewrite => {
                            seq_rewrite += 1;
                            ctx_rewrite.single_wait(seq_rewrite as usize).unwrap();
                            LogBatch::decode_entries_block(
                                &ctx_rewrite.data(seq_rewrite as usize),
                                i.entries.unwrap(),
                                i.compression_type,
                            )
                            .unwrap()
                        }
                    },
                    false => decode_buf,
                };

            vec.push(
                parse_from_bytes::<M>(
                    &mut decode_buf
                        [(i.entry_offset) as usize..(i.entry_offset + i.entry_len) as usize],
                )
                .unwrap(),
            );
        }
        Ok(())
    }
    #[inline]
    fn async_read_bytes(&self, handles: &mut Vec<FileBlockHandle>) -> Result<Vec<Vec<u8>>> {
        let mut res: Vec<Vec<u8>> = vec![];

        let mut ctx_append = self.pipes[LogQueue::Append as usize]
            .file_system
            .new_async_io_context(handles.len() as usize)
            .unwrap();
        let mut ctx_rewrite = self.pipes[LogQueue::Rewrite as usize]
            .file_system
            .new_async_io_context(handles.len() as usize)
            .unwrap();

        for (seq, handle) in handles.iter_mut().enumerate() {
            match handle.id.queue {
                LogQueue::Append => {
                    self.pipes[LogQueue::Append as usize].submit_read_req(
                        handle,
                        &mut ctx_append,
                    );
                }
                LogQueue::Rewrite => {
                    self.pipes[LogQueue::Rewrite as usize].submit_read_req(
                        handle,
                        &mut ctx_rewrite,
                    );
                }
            }
        }
        let mut seq_append = 0;
        let mut seq_rewrite = 0;
        for handle in handles.iter_mut(){
            match handle.id.queue {
                LogQueue::Append => {
                    ctx_append.single_wait(seq_append);
                    res.push(ctx_append.data(seq_append));
                    seq_append += 1;
                }
                LogQueue::Rewrite => {
                    ctx_rewrite.single_wait(seq_rewrite);
                    res.push(ctx_rewrite.data(seq_rewrite));
                    seq_rewrite += 1;
                }
            }
        }

        Ok(res)
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
        SinglePipe::open(
            cfg,
            fs,
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
