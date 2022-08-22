// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::utils::CachePadded;
use fail::fail_point;
use log::error;
use parking_lot::{Mutex, MutexGuard, RwLock};
use strum::{EnumIter, IntoEnumIterator};

use crate::config::Config;
use crate::env::FileSystem;
use crate::event_listener::EventListener;
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogFileContext, LogQueue, PipeLog};
use crate::{perf_context, Error, Result};

use super::format::{FileNameExt, LogFileFormat};
use super::log_file::{build_file_reader, build_file_writer, LogFileWriter};

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Debug, EnumIter)]
pub enum StorageDirType {
    Main = 0,
    Secondary = 1,
}

/// Represents the info of storage dirs, including `main dir` and
/// `secondary dir`.
struct StorageInfo {
    storage: Vec<String>,
}

impl StorageInfo {
    fn new(dir: String, secondary_dir: Option<String>) -> Self {
        let mut storage = vec![dir; 1];
        if let Some(sec_dir) = secondary_dir {
            storage.push(sec_dir);
        }
        Self { storage }
    }

    fn get_free_dir(&self, target_size: usize) -> Option<(&str, StorageDirType)> {
        #[cfg(feature = "failpoints")]
        {
            fail::fail_point!("file_pipe_log::force_use_secondary_dir", |_| {
                Some((
                    self.storage[StorageDirType::Secondary as usize].as_str(),
                    StorageDirType::Secondary,
                ))
            });
            fail::fail_point!("file_pipe_log::force_no_free_space", |_| { None });
        }
        for t in StorageDirType::iter() {
            let idx = t as usize;
            if idx >= self.storage.len() {
                break;
            }
            let disk_stats = match fs2::statvfs(&self.storage[idx]) {
                Err(e) => {
                    error!(
                        "get disk stat for raft engine failed, dir_path: {}, err: {}",
                        &self.storage[idx], e
                    );
                    return None;
                }
                Ok(stats) => stats,
            };
            if target_size <= disk_stats.available_space() as usize {
                return Some((&self.storage[idx], t));
            }
        }
        None
    }

    #[inline]
    fn get_dir(&self, storage_type: StorageDirType) -> Option<&str> {
        let idx = storage_type as usize;
        if idx >= self.storage.len() {
            None
        } else {
            Some(&self.storage[idx])
        }
    }

    fn sync_all_dir(&mut self) -> Result<()> {
        for t in StorageDirType::iter() {
            let idx = t as usize;
            if idx >= self.storage.len() {
                break;
            }
            let path = PathBuf::from(&self.storage[idx]);
            std::fs::File::open(path).and_then(|d| d.sync_all())?;
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct FileWithFormat<F: FileSystem> {
    pub handle: Arc<F::Handle>,
    pub format: LogFileFormat,
    pub storage_type: StorageDirType,
}

struct FileCollection<F: FileSystem> {
    /// Sequence number of the first file.
    first_seq: FileSeq,
    /// Sequence number of the first file that is in use.
    first_seq_in_use: FileSeq,
    fds: VecDeque<FileWithFormat<F>>,
    /// `0` => no capbility for recycling stale files
    /// `_` => finite volume for recycling stale files
    capacity: usize,
    /// Info of storage dir.
    storage: StorageInfo,
}

impl<F: FileSystem> FileCollection<F> {
    /// Recycles the first obsolete file and renewed with new FileId.
    ///
    /// Attention please, the recycled file would be automatically `renamed` in
    /// this func.
    fn recycle_one_file(
        &mut self,
        file_system: &F,
        dst_fd: FileId,
    ) -> (bool, Option<StorageDirType>) {
        debug_assert!(self.first_seq <= self.first_seq_in_use);
        debug_assert!(!self.fds.is_empty());
        if self.first_seq < self.first_seq_in_use {
            let first_file_id = FileId {
                queue: dst_fd.queue,
                seq: self.first_seq,
            };
            let storage_type = self.fds[0].storage_type;
            let dir = self.storage.get_dir(storage_type).unwrap();
            let src_path = first_file_id.build_file_path(dir); // src filepath
            let dst_path = dst_fd.build_file_path(dir); // dst filepath
            if let Err(e) = file_system.reuse(&src_path, &dst_path) {
                error!("error while trying to recycle one expired file: {}", e);
            } else {
                // Only if `rename` made sense, could we update the first_seq and return
                // success.
                self.fds.pop_front().unwrap();
                self.first_seq += 1;
                return (true, Some(storage_type));
            }
        }
        (false, None)
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
    file_format: LogFileFormat,
    target_file_size: usize,
    bytes_per_sync: usize,
    file_system: Arc<F>,
    listeners: Vec<Arc<dyn EventListener>>,

    /// All log files.
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
        // Here, we also should release the unnecessary disk space
        // occupied by stale files.
        let files = self.files.write();
        for seq in files.first_seq..files.first_seq_in_use {
            let file_id = FileId {
                queue: self.queue,
                seq,
            };
            let dir = files
                .storage
                .get_dir(files.fds[(seq - files.first_seq) as usize].storage_type);
            debug_assert!(dir.is_some());
            let path = file_id.build_file_path(dir.unwrap());
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

        let storage = StorageInfo::new(cfg.dir.clone(), cfg.secondary_dir.clone());
        let create_file = first_seq == 0;
        let active_seq = if create_file {
            first_seq = 1;
            let (dir, dir_type) = match storage.get_free_dir(cfg.target_file_size.0 as usize) {
                Some((d, t)) => (d, t),
                None => {
                    // No space for writing.
                    return Err(Error::Other(box_err!(
                        "no free space for recording new logs."
                    )));
                }
            };
            let file_id = FileId {
                queue,
                seq: first_seq,
            };
            let fd = Arc::new(file_system.create(&file_id.build_file_path(&dir))?);
            fds.push_back(FileWithFormat {
                handle: fd,
                format: LogFileFormat::new(cfg.format_version, alignment),
                storage_type: dir_type,
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
                active_fd.format,
                false, /* force_reset */
            )?,
            format: active_fd.format,
        };

        let total_files = fds.len();
        let pipe = Self {
            queue,
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
                storage,
            })),
            active_file: CachePadded::new(Mutex::new(active_file)),
        };
        pipe.flush_metrics(total_files);
        Ok(pipe)
    }

    /// Synchronizes all metadatas associated with the working directory to the
    /// filesystem.
    fn sync_dir(&self) -> Result<()> {
        let mut files = self.files.write();
        files.storage.sync_all_dir()
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
        // Generate a new fd from a newly chosen file, might be reused from a stale
        // file or generated from a newly created file.
        let (fd, storage_type) = {
            // Create the fd by recycling stale files or creating a new file.
            let mut files = self.files.write();
            let (recycle, storage_type) = files.recycle_one_file(&self.file_system, file_id);
            if recycle {
                // Open the recycled file(file is already renamed)
                debug_assert!(storage_type.is_some());
                let storage_type = storage_type.unwrap();
                let path = file_id.build_file_path(files.storage.get_dir(storage_type).unwrap());
                (Arc::new(self.file_system.open(&path)?), storage_type)
            } else if let Some((d, t)) = files.storage.get_free_dir(self.target_file_size) {
                // Has free space for newly writing, a new file is introduced.
                let path = file_id.build_file_path(&d);
                (Arc::new(self.file_system.create(&path)?), t)
            } else {
                // Neither has stale files nor has space for writing.
                return Err(Error::Other(box_err!(
                    "no free space for recording new logs."
                )));
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

        let len = {
            let mut files = self.files.write();
            debug_assert!(files.first_seq + files.fds.len() as u64 == seq);
            files.fds.push_back(FileWithFormat {
                handle: fd,
                format: LogFileFormat::new(version, alignment),
                storage_type,
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
            // TODO: Refine the following judgement if the error type
            // `ErrorKind::StorageFull` is stable.
            let no_space_err = {
                if_chain::if_chain! {
                    if let Error::Io(ref e) = e;
                    let err_msg = format!("{}", e.get_ref().unwrap());
                    if err_msg.contains("nospace");
                    then {
                        true
                    } else {
                        false
                    }
                }
            };
            let has_free_space = {
                let files = self.files.read();
                files.first_seq < files.first_seq_in_use /* has stale files */
                    || files.storage.get_free_dir(self.target_file_size).is_some()
            };
            // If there still exists free space for this record, a special Err will
            // be returned to the caller.
            if no_space_err && has_free_space {
                return Err(Error::Other(box_err!(
                    "failed to write {} file, get {} try to flush it to other dir",
                    seq,
                    e
                )));
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
        let (
            purged_files, /* list of purged files */
            remained,     /* count of remained files */
        ) = {
            let mut files = self.files.write();
            if file_seq >= files.first_seq + files.fds.len() as u64 {
                return Err(box_err!("Purge active or newer files"));
            } else if file_seq <= files.first_seq_in_use {
                return Ok(0);
            }

            // TODO: move these under FileCollection.
            // Remove some obsolete files if capacity is exceeded.
            let obsolete_files = (file_seq - files.first_seq) as usize;
            // When capacity is zero, always remove logically deleted files.
            let capacity_exceeded = files.fds.len().saturating_sub(files.capacity);
            let mut purged = std::cmp::min(capacity_exceeded, obsolete_files);
            // The files with format_version `V1` cannot be chosen as recycle
            // candidates, which should also be removed.
            // Find the newest obsolete `V1` file and refresh purge count.
            for i in (purged..obsolete_files).rev() {
                if !files.fds[i].format.version.has_log_signing() {
                    purged = i + 1;
                    break;
                }
            }
            // Assemble the info of purged files.
            let old_first_seq = files.first_seq;
            let mut purged_files = Vec::<(FileSeq, String)>::default();
            purged_files.reserve(purged);
            for i in 0..purged {
                purged_files.push((
                    i as u64 + old_first_seq,
                    files
                        .storage
                        .get_dir(files.fds[i].storage_type)
                        .unwrap()
                        .to_owned(),
                ));
            }
            // Update metadata of files
            files.first_seq += purged as u64;
            files.first_seq_in_use = file_seq;
            files.fds.drain(..purged);
            (purged_files, files.fds.len())
        };
        self.flush_metrics(remained);
        // for seq in first_purge_seq..first_purge_seq + purged as u64 {
        for (seq, dir) in purged_files.iter() {
            let file_id = FileId {
                queue: self.queue,
                seq: *seq,
            };
            let path = file_id.build_file_path(dir);
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
        Ok(purged_files.len())
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
    use crate::env::{DefaultFileSystem, WriteExt};
    use crate::pipe_log::Version;
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
    fn test_recycle_file_collections() {
        fn prepare_file<F: FileSystem>(
            file_system: &F,
            path: &str,
            file_id: FileId,
            data: &[u8],
        ) -> Result<Arc<F::Handle>> {
            let fd = Arc::new(file_system.create(&file_id.build_file_path(path))?);
            let mut writer = file_system.new_writer(fd.clone())?;
            writer.allocate(0, 32 * 1024 * 1024)?; // 32MB as default
            writer.write_all(data)?;
            writer.sync()?;
            Ok(fd)
        }
        fn validate_content_of_file<F: FileSystem>(
            file_system: &F,
            path: &str,
            file_id: FileId,
            expected_data: &[u8],
        ) -> Result<bool> {
            let data_len = expected_data.len();
            let mut buf = vec![0; 1024];
            let fd = Arc::new(file_system.open(&file_id.build_file_path(path)).unwrap());
            let mut new_reader = file_system.new_reader(fd).unwrap();
            let actual_len = new_reader.read(&mut buf).unwrap();
            Ok(if actual_len != data_len {
                false
            } else {
                buf == expected_data
            })
        }
        fn new_file_handler(path: &str, file_id: FileId) -> FileWithFormat<DefaultFileSystem> {
            FileWithFormat {
                handle: Arc::new(
                    DefaultFileSystem
                        .open(&file_id.build_file_path(path))
                        .unwrap(),
                ),
                format: LogFileFormat::default(),
                storage_type: StorageDirType::Main,
            }
        }
        let dir = Builder::new()
            .prefix("test_recycle_file_collections")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let data = vec![b'x'; 1024];
        let file_system = Arc::new(DefaultFileSystem);
        // test FileCollection with a valid file
        {
            // mock
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
            let _ = prepare_file(file_system.as_ref(), path, old_file_id, &data); // prepare old file
            let mut recycle_collections = FileCollection::<DefaultFileSystem> {
                first_seq: old_file_id.seq,
                first_seq_in_use: old_file_id.seq,
                capacity: 3,
                fds: vec![new_file_handler(path, old_file_id)].into(),
                storage: StorageInfo::new(path.to_owned(), None),
            };
            // recycle an old file
            assert!(
                !recycle_collections
                    .recycle_one_file(&file_system, new_file_id)
                    .0
            );
            // update the reycle collection
            {
                recycle_collections
                    .fds
                    .push_back(new_file_handler(path, old_file_id));
                recycle_collections.first_seq_in_use = cur_file_id.seq;
            }
            // recycle an old file
            assert!(
                recycle_collections
                    .recycle_one_file(&file_system, new_file_id)
                    .0
            );
            // validate the content of recycled file
            assert!(
                validate_content_of_file(file_system.as_ref(), path, new_file_id, &data).unwrap()
            );
            // rewrite then rename with validation on the content.
            {
                let refreshed_data = vec![b'm'; 1024];
                let fd = Arc::new(
                    file_system
                        .create(&new_file_id.build_file_path(path))
                        .unwrap(),
                );
                let mut new_writer = file_system.new_writer(fd).unwrap();
                new_writer.seek(SeekFrom::Start(0)).unwrap();
                assert_eq!(new_writer.write(&refreshed_data).unwrap(), 1024);
                new_writer.sync().unwrap();
                assert!(validate_content_of_file(
                    file_system.as_ref(),
                    path,
                    new_file_id,
                    &refreshed_data
                )
                .unwrap());
            }
        }
        // Test FileCollection with abnormal `recycle_one_file`.
        {
            let fake_file_id = FileId {
                queue: LogQueue::Append,
                seq: 11,
            };
            let _ = prepare_file(file_system.as_ref(), path, fake_file_id, &data); // prepare old file
            let mut recycle_collections = FileCollection::<DefaultFileSystem> {
                first_seq: fake_file_id.seq,
                first_seq_in_use: fake_file_id.seq + 1,
                capacity: 2,
                fds: vec![new_file_handler(path, fake_file_id)].into(),
                storage: StorageInfo::new(path.to_owned(), None),
            };
            // mock the failure on `rename`
            file_system
                .delete(&fake_file_id.build_file_path(path))
                .unwrap();
            let new_file_id = FileId {
                queue: LogQueue::Append,
                seq: 13,
            };
            // `rename` is failed, and no stale files in recycle_collections could be
            // recycled.
            assert!(
                !recycle_collections
                    .recycle_one_file(&file_system, new_file_id)
                    .0
            );
            assert_eq!(recycle_collections.fds.len(), 1);
            // rebuild the file for recycle
            prepare_file(file_system.as_ref(), path, fake_file_id, &data).unwrap();
            assert!(
                recycle_collections
                    .recycle_one_file(&file_system, new_file_id)
                    .0
            );
            assert!(recycle_collections.fds.is_empty());
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
            enable_log_recycle: true,
            format_version: Version::V2,
            ..Default::default()
        };
        let queue = LogQueue::Append;

        let pipe_log = new_test_pipes(&cfg).unwrap();
        assert_eq!(pipe_log.file_span(queue), (1, 1));

        let header_size = LogFileFormat::encode_len(cfg.format_version) as u64;

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

        // Purge to file 1, this file would be recycled
        assert_eq!(pipe_log.purge_to(FileId { queue, seq: 2 }).unwrap(), 0);
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
        assert_eq!(file_context.version, Version::V2);
        assert_eq!(file_context.id.seq, 3);
    }

    #[test]
    fn test_storage_info() {
        let dir = Builder::new()
            .prefix("test_storage_info_main_dir")
            .tempdir()
            .unwrap();
        let secondary_dir = Builder::new()
            .prefix("test_storage_info_sec_dir")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        let sec_path = secondary_dir.path().to_str().unwrap();
        {
            // Test StorageInfo with main dir only.
            let storage = StorageInfo::new(path.to_owned(), None);
            assert_eq!(storage.get_dir(StorageDirType::Main).unwrap(), path);
            assert!(storage.get_dir(StorageDirType::Secondary).is_none());
        }
        {
            // Test StorageInfo both with main dir and secondary dir.
            let storage = StorageInfo::new(path.to_owned(), Some(sec_path.to_owned()));
            assert_eq!(storage.get_dir(StorageDirType::Main).unwrap(), path);
            assert_eq!(
                storage.get_dir(StorageDirType::Secondary).unwrap(),
                sec_path
            );
        }
    }
}
