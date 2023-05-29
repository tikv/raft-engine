// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs::File as StdFile;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::utils::CachePadded;
use fail::fail_point;
use log::error;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::config::Config;
use crate::env::{FileSystem, Permission};
use crate::errors::is_no_space_err;
use crate::event_listener::EventListener;
use crate::metrics::*;
use crate::pipe_log::{
    FileBlockHandle, FileId, FileSeq, LogFileContext, LogQueue, PipeLog, ReactiveBytes,
};
use crate::{perf_context, Error, Result};

use super::format::{build_reserved_file_name, FileNameExt, LogFileFormat};
use super::log_file::build_file_reader;
use super::log_file::{build_file_writer, LogFileWriter};

pub type PathId = usize;
pub type Paths = Vec<PathBuf>;

/// Main directory path id.
pub const DEFAULT_PATH_ID: PathId = 0;
/// FileSeq of logs must start from `1` by default to keep backward
/// compatibility.
pub const DEFAULT_FIRST_FILE_SEQ: FileSeq = 1;

#[derive(Debug)]
pub struct File<F: FileSystem> {
    pub seq: FileSeq,
    pub handle: Arc<F::Handle>,
    pub format: LogFileFormat,
    pub path_id: PathId,
    /// Flag remarking whether it is a reserved file or not.
    pub reserved: bool,
}

struct WritableFile<F: FileSystem> {
    pub seq: FileSeq,
    pub writer: LogFileWriter<F>,
    pub format: LogFileFormat,
}

/// Collection of recycled files, consists of two parts:
/// - reserved files when enabling `prefill-for-recycle`.
/// - stale files when purging obsolete files.
struct RecycledFilesCollection<F: FileSystem> {
    /// Reserved files if enable `prefill-for-recycle`.
    reserved_files: VecDeque<File<F>>,
    /// Stale files which could be recycled.
    stale_files: VecDeque<File<F>>,
}

impl<F: FileSystem> RecycledFilesCollection<F> {
    fn new(reserved_files: Vec<File<F>>) -> Self {
        Self {
            reserved_files: reserved_files.into(),
            stale_files: VecDeque::new(),
        }
    }

    fn len(&self) -> usize {
        self.reserved_files.len() + self.stale_files.len()
    }

    /// Return the droppable files (`stale_files`).
    fn droppable_files(&self) -> &VecDeque<File<F>> {
        &self.stale_files
    }

    fn pop_front(&mut self) -> Option<File<F>> {
        if !self.reserved_files.is_empty() {
            return self.reserved_files.pop_front();
        }
        self.stale_files.pop_front()
    }

    fn append(&mut self, files: &mut VecDeque<File<F>>) {
        self.stale_files.append(files);
    }
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
    recycled_files: CachePadded<RwLock<RecycledFilesCollection<F>>>,

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
            error!("error while closing the active writer: {e}");
        }
        // Release the unnecessary disk space occupied by stale files. It also reduces
        // recovery time.
        let files = self.recycled_files.read();
        for f in files.droppable_files() {
            let file_id = FileId {
                queue: self.queue,
                seq: f.seq,
            };
            let path = file_id.build_file_path(&self.paths[f.path_id]);
            if let Err(e) = self.file_system.delete(&path) {
                error!("error while deleting stale file: {path:?}, err_msg: {e}");
            }
        }
    }
}

impl<F: FileSystem> SinglePipe<F> {
    /// Opens a new [`SinglePipe`].
    pub fn open(
        cfg: &Config,
        paths: Paths,
        file_system: Arc<F>,
        listeners: Vec<Arc<dyn EventListener>>,
        queue: LogQueue,
        mut active_files: Vec<File<F>>,
        reserved_files: Vec<File<F>>,
    ) -> Result<Self> {
        let alignment = || {
            fail_point!("file_pipe_log::open::force_set_alignment", |_| { 16 });
            0
        };
        let default_format = LogFileFormat::new(cfg.format_version, alignment());

        // Open or create active file.
        let no_active_files = active_files.is_empty();
        if no_active_files {
            let path_id = find_available_dir(&paths, cfg.target_file_size.0 as usize);
            let file_id = FileId::new(queue, DEFAULT_FIRST_FILE_SEQ);
            let path = file_id.build_file_path(&paths[path_id]);
            active_files.push(File {
                seq: file_id.seq,
                handle: file_system.create(&path)?.into(),
                format: default_format,
                path_id,
                reserved: false,
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

        let (len, recycled_len) = (active_files.len(), reserved_files.len());
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
            recycled_files: RwLock::new(RecycledFilesCollection::new(reserved_files)).into(),
            writable_file: Mutex::new(writable_file).into(),
        };
        pipe.flush_metrics(len);
        pipe.flush_recycle_metrics(recycled_len);
        Ok(pipe)
    }

    /// Synchronizes all metadatas associated with the working directory to the
    /// filesystem.
    fn sync_dir(&self, path_id: PathId) -> Result<()> {
        debug_assert!(!self.paths.is_empty());
        std::fs::File::open(PathBuf::from(&self.paths[path_id])).and_then(|d| d.sync_all())?;
        Ok(())
    }

    /// Recycles one obsolete file from the recycled file list and return its
    /// [`PathId`] and [`F::Handle`] if success.
    fn recycle_file(&self, seq: FileSeq) -> Option<Result<(PathId, F::Handle)>> {
        let new_file_id = FileId {
            seq,
            queue: self.queue,
        };
        let (recycle_file, recycle_len) = {
            let mut recycled_files = self.recycled_files.write();
            (recycled_files.pop_front(), recycled_files.len())
        };
        if let Some(f) = recycle_file {
            let src_path = if f.reserved {
                self.paths[f.path_id].join(build_reserved_file_name(f.seq))
            } else {
                let stale_file_id = FileId {
                    seq: f.seq,
                    queue: self.queue,
                };
                stale_file_id.build_file_path(&self.paths[f.path_id])
            };
            let dst_path = new_file_id.build_file_path(&self.paths[f.path_id]);
            if let Err(e) = self.file_system.reuse(&src_path, &dst_path) {
                error!("error while trying to reuse recycled file, err: {e}");
                if let Err(e) = self.file_system.delete(&src_path) {
                    error!("error while trying to delete recycled file, err: {e}");
                }
            } else {
                self.flush_recycle_metrics(recycle_len);
                return match self.file_system.open(&dst_path, Permission::ReadWrite) {
                    Ok(handle) => Some(Ok((f.path_id, handle))),
                    Err(e) => Some(Err(e.into())),
                };
            }
        }
        None
    }

    /// Creates a new log file according to the given [`FileSeq`].
    fn new_file(&self, seq: FileSeq) -> Result<(PathId, F::Handle)> {
        let new_file_id = FileId {
            seq,
            queue: self.queue,
        };
        let path_id = find_available_dir(&self.paths, self.target_file_size);
        let path = new_file_id.build_file_path(&self.paths[path_id]);
        Ok((path_id, self.file_system.create(path)?))
    }

    /// Returns a shared [`LogFd`] for the specified file sequence number.
    fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<F::Handle>> {
        let files = self.active_files.read();
        if !(files[0].seq..files[0].seq + files.len() as u64).contains(&file_seq) {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files[(file_seq - files[0].seq) as usize].handle.clone())
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
        debug_assert!(new_seq > DEFAULT_FIRST_FILE_SEQ);

        writable_file.writer.close()?;

        let (path_id, handle) = self
            .recycle_file(new_seq)
            .unwrap_or_else(|| self.new_file(new_seq))?;
        let f = File::<F> {
            seq: new_seq,
            handle: handle.into(),
            format: self.default_format,
            path_id,
            reserved: false,
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
        self.sync_dir(path_id)?;

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

    /// Synchronizes current recycled states to related metrics.
    fn flush_recycle_metrics(&self, len: usize) {
        match self.queue {
            LogQueue::Append => RECYCLED_FILE_COUNT.append.set(len as i64),
            LogQueue::Rewrite => RECYCLED_FILE_COUNT.rewrite.set(len as i64),
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
        let mut writable_file = self.writable_file.lock();
        if writable_file.writer.offset() >= self.target_file_size {
            if let Err(e) = self.rotate_imp(&mut writable_file) {
                panic!(
                    "error when rotate [{:?}:{}]: {e}",
                    self.queue, writable_file.seq,
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
                panic!("error when truncate {seq} after error: {e}, get: {}", te);
            }
            if is_no_space_err(&e) {
                // TODO: There exists several corner cases should be tackled if
                // `bytes.len()` > `target_file_size`. For example,
                // - [1] main-dir has no recycled logs, and spill-dir have several recycled
                //   logs.
                // - [2] main-dir has several recycled logs, and sum(recycled_logs.size()) <
                //   expected_file_size, but no recycled logs exist in spill-dir.
                // - [3] Both main-dir and spill-dir have several recycled logs.
                // But as `bytes.len()` is always smaller than `target_file_size` in common
                // cases, this issue will be ignored temprorarily.
                if let Err(e) = self.rotate_imp(&mut writable_file) {
                    panic!(
                        "error when rotate [{:?}:{}]: {e}",
                        self.queue, writable_file.seq
                    );
                }
                // If there still exists free space for this record, rotate the file
                // and return a special TryAgain Err (for retry) to the caller.
                return Err(Error::TryAgain(format!(
                    "error when append [{:?}:{seq}]: {e}",
                    self.queue,
                )));
            }
            return Err(Error::Io(e));
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
                panic!("error when sync [{:?}:{seq}]: {e}", self.queue);
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
        let (len, mut purged_files) = {
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
            let mut recycled_len = self.recycled_files.read().len();
            let mut final_purged_idx = 0_usize;
            for (i, f) in purged_files.iter().rev().enumerate() {
                // Recycle purged files whose version meets the requirement.
                // - [1] The files with format_version `V1` cannot be chosen as recycle
                // candidates.
                // - [2] Capacity of recycled files should be under the limit.
                if !f.format.version.has_log_signing() || recycled_len >= remains_capacity {
                    final_purged_idx = purged_len - i;
                    break;
                }
                recycled_len += 1;
            }
            // Remove purged files which are out of capacity and files whose version is
            // marked not recycled.
            for f in purged_files
                .drain(..final_purged_idx)
                .collect::<VecDeque<File<F>>>()
            {
                let file_id = FileId {
                    seq: f.seq,
                    queue: self.queue,
                };
                let path = file_id.build_file_path(&self.paths[f.path_id]);

                self.file_system.delete(&path)?;
            }
            debug_assert!(recycled_len <= remains_capacity);
            self.recycled_files.write().append(&mut purged_files);
            self.flush_recycle_metrics(recycled_len);
        }
        self.flush_metrics(len);
        Ok(purged_len)
    }
}

/// A [`PipeLog`] implementation that stores data in filesystem.
pub struct DualPipes<F: FileSystem> {
    pipes: [SinglePipe<F>; 2],

    _dir_locks: Vec<StdFile>,
}

impl<F: FileSystem> DualPipes<F> {
    /// Open a new [`DualPipes`]. Assumes the two [`SinglePipe`]s share the
    /// same directory, and that directory is locked by `dir_lock`.
    pub(super) fn open(
        dir_locks: Vec<StdFile>,
        appender: SinglePipe<F>,
        rewriter: SinglePipe<F>,
    ) -> Result<Self> {
        // TODO: remove this dependency.
        debug_assert_eq!(LogQueue::Append as usize, 0);
        debug_assert_eq!(LogQueue::Rewrite as usize, 1);

        Ok(Self {
            pipes: [appender, rewriter],
            _dir_locks: dir_locks,
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

/// Fetch and return a valid `PathId` of the specific directories.
pub(crate) fn find_available_dir(paths: &Paths, target_size: usize) -> PathId {
    fail_point!("file_pipe_log::force_choose_dir", |s| s
        .map_or(DEFAULT_PATH_ID, |n| n.parse::<usize>().unwrap()));
    // Only if one single dir is set by `Config::dir`, can it skip the check of disk
    // space usage.
    if paths.len() > 1 {
        for (t, p) in paths.iter().enumerate() {
            if let Ok(disk_stats) = fs2::statvfs(p) {
                if target_size <= disk_stats.available_space() as usize {
                    return t;
                }
            }
        }
    }
    DEFAULT_PATH_ID
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use tempfile::Builder;

    use super::super::format::LogFileFormat;
    use super::super::pipe_builder::lock_dir;
    use super::*;
    use crate::env::{DefaultFileSystem, ObfuscatedFileSystem};
    use crate::pipe_log::Version;
    use crate::util::ReadableSize;

    fn new_test_pipe<F: FileSystem>(
        cfg: &Config,
        paths: Paths,
        queue: LogQueue,
        fs: Arc<F>,
    ) -> Result<SinglePipe<F>> {
        SinglePipe::open(cfg, paths, fs, Vec::new(), queue, Vec::new(), Vec::new())
    }

    fn new_test_pipes(cfg: &Config) -> Result<DualPipes<DefaultFileSystem>> {
        DualPipes::open(
            vec![lock_dir(&cfg.dir)?],
            new_test_pipe(
                cfg,
                vec![Path::new(&cfg.dir).to_path_buf()],
                LogQueue::Append,
                Arc::new(DefaultFileSystem),
            )?,
            new_test_pipe(
                cfg,
                vec![Path::new(&cfg.dir).to_path_buf()],
                LogQueue::Rewrite,
                Arc::new(DefaultFileSystem),
            )?,
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
        assert_eq!(file_handle.offset, header_size + s_content.len() as u64);

        let content_readed = pipe_log
            .read_bytes(FileBlockHandle {
                id: FileId { queue, seq: 3 },
                offset: header_size,
                len: s_content.len(),
            })
            .unwrap();
        assert_eq!(content_readed, s_content);
        // try to fetch abnormal entry
        let abnormal_content_readed = pipe_log.read_bytes(FileBlockHandle {
            id: FileId { queue, seq: 12 }, // abnormal seq
            offset: header_size,
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
        let pipe_log =
            new_test_pipe(&cfg, vec![Path::new(&cfg.dir).to_path_buf()], queue, fs).unwrap();
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
        // Cannot purge already expired logs or not existsed logs.
        assert!(pipe_log.purge_to(first - 1).is_err());
        assert!(pipe_log.purge_to(last + 1).is_err());
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
