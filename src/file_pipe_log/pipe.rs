// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs::File as StdFile;
use std::path::{Path, PathBuf};
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
use crate::{perf_context, Error, Result};

use super::format::{build_recycled_file_name, FileNameExt, LogFileFormat};
use super::log_file::build_file_reader;
use super::log_file::{build_file_writer, LogFileWriter};

pub const DEFAULT_PATH_ID: PathId = 0;

pub type PathId = usize;
pub type Paths = Vec<PathBuf>;

#[derive(Debug)]
pub struct File<F: FileSystem> {
    pub seq: FileSeq,
    pub handle: Arc<F::Handle>,
    pub format: LogFileFormat,
    pub path_id: PathId,
}

struct WritableFile<F: FileSystem> {
    pub seq: FileSeq,
    pub writer: LogFileWriter<F>,
    pub format: LogFileFormat,
}

/// A file-based log storage that arranges files as one single queue.
pub(super) struct SinglePipe<F: FileSystem> {
    queue: LogQueue,
    paths: Paths,
    file_system: Arc<F>,
    listeners: Vec<Arc<dyn EventListener>>,
    default_format: LogFileFormat,
    target_file_size: usize,

    capacity: usize,
    active_files: CachePadded<RwLock<VecDeque<File<F>>>>,
    recycled_files: CachePadded<RwLock<VecDeque<File<F>>>>,

    /// The log file opened for write.
    ///
    /// `writable_file` must be locked first to acquire both `files` and
    /// `writable_file`
    writable_file: CachePadded<Mutex<WritableFile<F>>>,
}

impl<F: FileSystem> Drop for SinglePipe<F> {
    fn drop(&mut self) {
        let mut writable_file = self.writable_file.lock();
        if let Err(e) = writable_file.writer.close() {
            error!("error while closing the active writer: {}", e);
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
        mut active_files: Vec<File<F>>,
        recycled_files: Vec<File<F>>,
    ) -> Result<Self> {
        let paths = vec![Path::new(&cfg.dir).to_path_buf()];
        let alignment = || {
            fail_point!("file_pipe_log::open::force_set_alignment", |_| { 16 });
            0
        };
        let default_format = LogFileFormat::new(cfg.format_version, alignment());

        // Open or create active file.
        let no_active_files = active_files.is_empty();
        if no_active_files {
            let path_id = DEFAULT_PATH_ID;
            let file_id = FileId::new(queue, 1_u64);
            let path = file_id.build_file_path(&paths[path_id]);
            active_files.push(File {
                seq: file_id.seq,
                handle: file_system.create(&path)?.into(),
                format: default_format,
                path_id,
            });
        }
        let f = active_files.last().unwrap();
        // If starting from active_files.emtpy(), we should reset the first file with
        // given file format.
        let writable_file = WritableFile {
            seq: f.seq,
            writer: build_file_writer(
                file_system.as_ref(),
                f.handle.clone(),
                f.format,
                no_active_files, /* force_reset */
            )?,
            format: f.format,
        };

        let len = active_files.len();
        for f in active_files.iter() {
            for listener in &listeners {
                listener.post_new_log_file(FileId { queue, seq: f.seq });
            }
        }

        let pipe = Self {
            queue,
            paths,
            file_system,
            listeners,
            default_format,
            target_file_size: cfg.target_file_size.0 as usize,
            capacity: if queue == LogQueue::Append {
                cfg.recycle_capacity()
            } else {
                0
            },
            active_files: RwLock::new(active_files.into()).into(),
            recycled_files: RwLock::new(recycled_files.into()).into(),
            writable_file: Mutex::new(writable_file).into(),
        };
        pipe.flush_metrics(len);
        Ok(pipe)
    }

    /// Synchronizes all metadatas associated with the working directory to the
    /// filesystem.
    fn sync_dir(&self) -> Result<()> {
        debug_assert!(!self.paths.is_empty());
        let path = PathBuf::from(&self.paths[0]);
        std::fs::File::open(path).and_then(|d| d.sync_all())?;
        Ok(())
    }

    fn new_file(&self, seq: FileSeq) -> Result<(PathId, F::Handle)> {
        let new_file_id = FileId {
            seq,
            queue: self.queue,
        };
        if let Some(f) = self.recycled_files.write().pop_front() {
            let src_path = self.paths[f.path_id].join(build_recycled_file_name(f.seq));
            let dst_path = new_file_id.build_file_path(&self.paths[f.path_id]);
            if let Err(e) = self.file_system.reuse(&src_path, &dst_path) {
                error!("error while trying to reuse recycled file, err: {}", e);
                if let Err(e) = self.file_system.delete(&src_path) {
                    error!("error while trying to delete recycled file, err: {}", e);
                }
            } else {
                return Ok((f.path_id, self.file_system.open(&dst_path)?));
            }
        }
        let path_id = DEFAULT_PATH_ID;
        let dst_path = new_file_id.build_file_path(&self.paths[path_id]);
        Ok((path_id, self.file_system.create(&dst_path)?))
    }

    /// Creates a new file for write, and rotates the active log file.
    ///
    /// This operation is atomic in face of errors.
    fn rotate_imp(&self, writable_file: &mut MutexGuard<WritableFile<F>>) -> Result<()> {
        let _t = StopWatch::new((
            &*LOG_ROTATE_DURATION_HISTOGRAM,
            perf_context!(log_rotate_duration),
        ));
        let new_seq = writable_file.seq + 1;
        debug_assert!(new_seq > 1);

        writable_file.writer.close()?;

        let (path_id, handle) = self.new_file(new_seq)?;
        let f = File::<F> {
            seq: new_seq,
            handle: handle.into(),
            format: self.default_format,
            path_id,
        };
        let mut new_file = WritableFile {
            seq: new_seq,
            writer: build_file_writer(
                self.file_system.as_ref(),
                f.handle.clone(),
                f.format,
                true, /* force_reset */
            )?,
            format: f.format,
        };
        // File header must be persisted. This way we can recover gracefully if power
        // loss before a new entry is written.
        new_file.writer.sync()?;
        self.sync_dir()?;

        **writable_file = new_file;
        let len = {
            let mut files = self.active_files.write();
            files.push_back(f);
            files.len()
        };
        self.flush_metrics(len);
        for listener in &self.listeners {
            listener.post_new_log_file(FileId {
                queue: self.queue,
                seq: new_seq,
            });
        }
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

    /// Returns a shared [`LogFd`] for the specified file sequence number.
    fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<F::Handle>> {
        let files = self.active_files.read();
        if !(files[0].seq..files[0].seq + files.len() as u64).contains(&file_seq) {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files[(file_seq - files[0].seq) as usize].handle.clone())
    }

    fn append<T: ReactiveBytes + ?Sized>(&self, bytes: &mut T) -> Result<FileBlockHandle> {
        fail_point!("file_pipe_log::append");
        let mut writable_file = self.writable_file.lock();
        if writable_file.writer.offset() >= self.target_file_size {
            if let Err(e) = self.rotate_imp(&mut writable_file) {
                panic!(
                    "error when rotate [{:?}:{}]: {}",
                    self.queue, writable_file.seq, e
                );
            }
        }

        let seq = writable_file.seq;
        let format = writable_file.format;
        let ctx = LogFileContext {
            id: FileId::new(self.queue, seq),
            version: format.version,
        };
        let writer = &mut writable_file.writer;

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
        let mut writable_file = self.writable_file.lock();
        let seq = writable_file.seq;
        let writer = &mut writable_file.writer;
        {
            let _t = StopWatch::new(perf_context!(log_sync_duration));
            if let Err(e) = writer.sync() {
                panic!("error when sync [{:?}:{}]: {}", self.queue, seq, e,);
            }
        }

        Ok(())
    }

    fn file_span(&self) -> (FileSeq, FileSeq) {
        let files = self.active_files.read();
        (files[0].seq, files[files.len() - 1].seq)
    }

    fn total_size(&self) -> usize {
        let (first_seq, last_seq) = self.file_span();
        (last_seq - first_seq + 1) as usize * self.target_file_size
    }

    fn rotate(&self) -> Result<()> {
        self.rotate_imp(&mut self.writable_file.lock())
    }

    fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (len, purged_files) = {
            let mut files = self.active_files.write();
            if !(files[0].seq..files[0].seq + files.len() as u64).contains(&file_seq) {
                return Err(box_err!("FileSeq out of range, cannot be purged"));
            }
            let off = (file_seq - files[0].seq) as usize;
            let mut tail = files.split_off(off);
            std::mem::swap(&mut tail, &mut files);
            (files.len(), tail)
        };
        let purged_len = purged_files.len();
        if purged_len > 0 {
            let remains_capacity = self.capacity.saturating_sub(len);
            let (recycled_start, mut recycled_len) = {
                let files = self.recycled_files.read();
                files
                    .front()
                    .map_or_else(|| (1, 0), |f| (f.seq, files.len()))
            };
            let mut new_recycled = VecDeque::new();
            // The newly purged files from `self.active_files` should be renamed
            // to recycled files with LOG_APPEND_RESERVED_SUFFIX suffix, to reduce the
            // unnecessary recovery timecost when restarting.
            for f in purged_files {
                let file_id = FileId {
                    seq: f.seq,
                    queue: self.queue,
                };
                let path = file_id.build_file_path(&self.paths[f.path_id]);
                // Recycle purged files whose version meets the requirement.
                if f.format.version.has_log_signing() && recycled_len < remains_capacity {
                    let recycled_seq = recycled_start + recycled_len as u64;
                    let dst_path =
                        self.paths[f.path_id].join(build_recycled_file_name(recycled_seq));
                    match self.file_system.reuse_and_open(&path, &dst_path) {
                        Ok(handle) => {
                            new_recycled.push_back(File {
                                seq: recycled_seq,
                                handle: handle.into(),
                                path_id: f.path_id,
                                format: f.format,
                            });
                            recycled_len += 1;
                            continue;
                        }
                        Err(e) => error!("failed to reuse purged file {:?}", e),
                    }
                }
                // Remove purged files which are out of capacity and files whose version is
                // marked not recycled.
                self.file_system.delete(&path)?;
            }
            self.recycled_files.write().append(&mut new_recycled);
        }
        self.flush_metrics(len);
        Ok(purged_len)
    }
}

/// A [`PipeLog`] implementation that stores data in filesystem.
pub struct DualPipes<F: FileSystem> {
    pipes: [SinglePipe<F>; 2],

    _dir_lock: StdFile,
}

impl<F: FileSystem> DualPipes<F> {
    /// Open a new [`DualPipes`]. Assumes the two [`SinglePipe`]s share the
    /// same directory, and that directory is locked by `dir_lock`.
    pub(super) fn open(
        dir_lock: StdFile,
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
        SinglePipe::open(cfg, fs, Vec::new(), queue, Vec::new(), Vec::new())
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
        let pipe_log = new_test_pipe(&cfg, queue, fs).unwrap();
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
        // By `purge`, all stale files will be automatically renamed to recycled files.
        assert_eq!(pipe_log.purge_to(last).unwrap() as u64, last - first);
        // Try to read recycled file.
        for (_, handle) in handles.into_iter().enumerate() {
            assert!(pipe_log.read_bytes(handle).is_err());
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
