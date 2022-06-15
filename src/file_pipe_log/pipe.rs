// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::utils::CachePadded;
use fail::fail_point;
use log::{error, warn};
use num_traits::FromPrimitive;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::config::Config;
use crate::env::FileSystem;
use crate::event_listener::EventListener;
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue, PipeLog};
use crate::{Error, Result};

use super::format::{FileNameExt, Version};
use super::log_file::{build_file_reader, build_file_writer, FileHandler, LogFileWriter};

struct FileCollection<F: FileSystem> {
    first_seq: FileSeq,
    active_seq: FileSeq,
    fds: VecDeque<FileHandler<F>>,
}

struct ActiveFile<F: FileSystem> {
    seq: FileSeq,
    writer: LogFileWriter<F>,
}

/// Collection of staled Files.
struct RecycleFileCollection {
    /// capacity of RecycleFiles
    capacity: Option<usize>,
    /// list of recycled files
    fds: VecDeque<FileId>,
}

impl RecycleFileCollection {
    pub fn new(capacity: Option<usize>) -> Self {
        Self {
            capacity,
            fds: VecDeque::new(),
        }
    }

    /// Check the RecycleFileCollection is open or not.
    ///
    /// Only when had the capacity been set with a valid value, the
    /// `RecycleFileCollection` is valid.
    pub fn valid(&self) -> bool {
        self.capacity.is_some()
    }

    pub fn valid_to_push(&self, count: usize) -> bool {
        match self.capacity {
            Some(0) => true, // the capacity is infinite.
            Some(p) => p >= count + self.fds.len(),
            None => false,
        }
    }

    /// Support update the capacity dynamically.
    pub fn upd_capacity(&mut self, count: i64) {
        if self.capacity.is_some() {
            let cur_capacity = self.capacity.unwrap() as i64;
            if cur_capacity + count > 0 {
                self.capacity = Some((cur_capacity + count) as usize);
            } else {
                self.capacity = None;
            }
        }
    }

    /// Push one log file into the recycle collection.
    ///
    /// Attention, the given `[FileId]` must be confirmed that it was stale.
    pub fn push_one_file(&mut self, fd: FileId) -> bool {
        let ret = self.valid_to_push(1); // push one file
        if ret {
            self.fds.push_back(fd);
        }
        ret
    }

    /// Pop one FileId from the recycle list.
    pub fn pop_one_file(&mut self) -> Option<FileId> {
        if !self.valid() {
            None
        } else {
            self.fds.pop_front()
        }
    }

    /// Recycle one exipired or purged file with new FileId.
    ///
    /// Attention please, the recycled file would be automatically `renamed` in
    /// this func.
    pub fn recycle_one_file<F: FileSystem>(
        &mut self,
        file_system: &Arc<F>,
        dir_path: &str,
        dst_fd: FileId,
    ) -> bool {
        let mut ret = false;
        while let Some(fd) = self.pop_one_file() {
            let src_path = fd.build_file_path(dir_path); // src filepath
            let dst_path = dst_fd.build_file_path(dir_path); // dst filepath
            match file_system.rename(src_path, dst_path) {
                Ok(_) => {
                    ret = true;
                }
                Err(e) => {
                    ret = false;
                    error!("error while trying to recycle one expired file: {}", e);
                }
            }
        }
        /* Only if the `rename` made sense, we could return success. */
        ret
    }

    pub fn size(&self) -> usize {
        self.fds.len()
    }
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
    /// All recycled log files waited to be reused.
    recycled_files: CachePadded<RwLock<RecycleFileCollection>>,
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
        recycle_capacity: Option<usize>,
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
                version: Version::from_u64(cfg.format_version).unwrap(),
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
                active_fd.version,
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
                active_seq,
                fds,
            })),
            // collection of recycled files
            recycled_files: CachePadded::new(RwLock::new(RecycleFileCollection::new(
                if let Some(capacity) = recycle_capacity {
                    if capacity >= total_files {
                        Some(capacity - total_files)
                    } else {
                        None
                    }
                } else {
                    None
                },
            ))),
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
        if file_seq < files.first_seq || file_seq > files.active_seq {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq) as usize]
            .handle
            .clone())
    }

    /// Returns a shared [`Version`] for the specified file sequence number.
    fn get_format_version(&self, file_seq: FileSeq) -> Result<Version> {
        let files = self.files.read();
        if file_seq < files.first_seq || file_seq > files.active_seq {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(files.fds[(file_seq - files.first_seq) as usize].version)
    }

    /// Creates a new file for write, and rotates the active log file.
    ///
    /// This operation is atomic in face of errors.
    fn rotate_imp(&self, active_file: &mut MutexGuard<ActiveFile<F>>) -> Result<()> {
        let _t = StopWatch::new(&LOG_ROTATE_DURATION_HISTOGRAM);
        let seq = active_file.seq + 1;
        debug_assert!(seq > 1);

        active_file.writer.close()?;

        let file_id = FileId {
            queue: self.queue,
            seq,
        };
        let path = file_id.build_file_path(&self.dir);
        let mut recycled_files = self.recycled_files.write();
        // If there still existed extra free files in `RecycleFileCollection`,
        // pop one as the active file, with `rename` by the func `recycle_one_file`.
        let fd = if recycled_files.recycle_one_file(&self.file_system, &self.dir, file_id) {
            // Open the recycled file(file is already renamed)
            Arc::new(self.file_system.open(&path)?)
        } else {
            // A new file is introduced, so we should update the capacity of
            // `ReycleFileCollection` by "current_capacity - 1".
            recycled_files.upd_capacity(-1);
            Arc::new(self.file_system.create(&path)?)
        };
        let mut new_file = ActiveFile {
            seq,
            writer: build_file_writer(self.file_system.as_ref(), fd.clone(), self.format_version)?,
        };
        // File header must be persisted. This way we can recover gracefully if power
        // loss before a new entry is written.
        new_file.writer.sync()?;
        self.sync_dir()?;
        let active_file_format_version = new_file.writer.header.version();
        **active_file = new_file;

        let len = {
            let mut files = self.files.write();
            debug_assert!(files.active_seq + 1 == seq);
            files.active_seq = seq;
            files.fds.push_back(FileHandler {
                handle: fd,
                version: active_file_format_version,
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
            if let Err(e) = writer.sync() {
                panic!("error when sync [{:?}:{}]: {}", self.queue, seq, e,);
            }
        }

        Ok(())
    }

    fn file_span(&self) -> (FileSeq, FileSeq) {
        let files = self.files.read();
        (files.first_seq, files.active_seq)
    }

    fn total_size(&self) -> usize {
        let files = self.files.read();
        let recycled_files = self.recycled_files.read();
        ((files.active_seq - files.first_seq + 1) as usize + recycled_files.size())
            * self.target_file_size
    }

    fn rotate(&self) -> Result<()> {
        self.rotate_imp(&mut self.active_file.lock())
    }

    fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (purged, remained) = {
            let mut files = self.files.write();
            if file_seq > files.active_seq {
                return Err(box_err!("Purge active or newer files"));
            }
            let end_offset = file_seq.saturating_sub(files.first_seq) as usize;
            files.fds.drain(..end_offset);
            files.first_seq = file_seq;
            (end_offset, files.fds.len())
        };
        self.flush_metrics(remained);
        for seq in file_seq - purged as u64..file_seq {
            let file_id = FileId {
                queue: self.queue,
                seq,
            };
            let mut path = file_id.build_file_path(&self.dir);
            #[cfg(feature = "failpoints")]
            {
                let remove_failure = || {
                    fail::fail_point!("file_pipe_log::remove_file_failure", |_| true);
                    false
                };
                if remove_failure() {
                    continue;
                }
            }
            let mut recycled_files = self.recycled_files.write();
            // If we could recycle the file, we would put the file_id into the
            // `RecycleFileCollections` for the future reusing.
            if recycled_files.valid_to_push(1) {
                // A stale file is purged, so we should update the capacity of
                // `ReycleFileCollection` by "current_capacity + 1".
                recycled_files.upd_capacity(1);
                recycled_files.push_one_file(file_id);
                continue;
            } else {
                // If we could not push the stale file into the RecycleFileCollections, it means
                // that:
                // [1]: the RecycleFileCollections is invalid for inserting, that is, it's not
                //      set by configuration.
                // [2]: the RecycleFileCollections has no extra space for the staled file.
                let first_stale_fid = recycled_files.pop_one_file();
                match first_stale_fid {
                    None => {
                        // Invalid RecycleFileCollections
                        // do nothing.
                    }
                    Some(fid) => {
                        // Delete the first stale file in RecycleFileCollections to free space
                        // for the incoming stale file.
                        path = fid.build_file_path(&self.dir);
                        recycled_files.push_one_file(file_id);
                    }
                }
            }
            if let Err(e) = fs::remove_file(&path) {
                warn!("Remove purged log file {:?} failed: {}", path, e);
            }
        }
        Ok(purged)
    }

    fn fetch_active_file(&self) -> (Version, FileId) {
        let active_file = self.active_file.lock();
        (
            active_file.writer.header.version(),
            FileId {
                queue: self.queue,
                seq: active_file.seq,
            },
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
    fn fetch_active_file(&self, queue: LogQueue) -> (Version, FileId) {
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
        let capacity_of_recycle =
            |default_file_size: usize, purge_threshold: usize| -> Option<usize> {
                if default_file_size == 0 || purge_threshold == 0 {
                    Option::None
                } else {
                    Option::Some(purge_threshold / default_file_size)
                }
            };
        SinglePipe::open(
            cfg,
            Arc::new(DefaultFileSystem),
            Vec::new(),
            queue,
            0,
            VecDeque::new(),
            match queue {
                LogQueue::Append => capacity_of_recycle(
                    cfg.target_file_size.0 as usize,
                    cfg.purge_threshold.0 as usize,
                ),
                LogQueue::Rewrite => None,
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
        let (format_version, file_seq) = pipe_log.fetch_active_file(LogQueue::Append);
        assert_eq!(format_version, Version::default());
        assert_eq!(file_seq.seq, 3);
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
        // test RecycleFileCollection with None(Invalid)
        {
            let mut recycle_collections = RecycleFileCollection::new(None);
            assert!(!recycle_collections.valid());
            assert!(!recycle_collections.valid_to_push(1));
            assert!(!recycle_collections.push_one_file(FileId::dummy(LogQueue::Append)));
            assert!(recycle_collections.pop_one_file().is_none());
            assert!(!recycle_collections.recycle_one_file(
                &file_system,
                path,
                FileId::dummy(LogQueue::Append)
            ));
        }
        // test RecycleFileCollection with a valid file
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
            let mut recycle_collections = RecycleFileCollection::new(Some(1));
            let invalid_file_id = FileId {
                queue: LogQueue::Append,
                seq: 1,
            };
            let old_file_id = FileId {
                queue: LogQueue::Append,
                seq: 12,
            };
            let new_file_id = FileId {
                queue: LogQueue::Append,
                seq: old_file_id.seq + 1,
            };
            assert!(recycle_collections.valid());
            assert_eq!(recycle_collections.size(), 0);
            assert!(recycle_collections.valid_to_push(1));
            assert!(recycle_collections.push_one_file(invalid_file_id));
            assert_eq!(recycle_collections.size(), 1);
            assert!(!recycle_collections.valid_to_push(1)); // full
            recycle_collections.upd_capacity(1);
            assert!(recycle_collections.valid_to_push(1));
            recycle_collections.upd_capacity(-1);
            assert!(!recycle_collections.valid_to_push(1)); // full
            let ret = recycle_collections.pop_one_file();
            assert_eq!(recycle_collections.size(), 0);
            assert!(ret.is_some());
            assert_eq!(invalid_file_id, ret.unwrap());
            // prepare old file
            let _ = prepare_file(old_file_id, &data[..]);
            // recycle old file
            assert!(recycle_collections.push_one_file(old_file_id));
            assert!(recycle_collections.recycle_one_file(&file_system, path, new_file_id));
            assert_eq!(recycle_collections.size(), 0);
            assert!(recycle_collections.valid_to_push(1));
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
        let (format_version, file_seq) = pipe_log.fetch_active_file(LogQueue::Append);
        assert_eq!(format_version, Version::default());
        assert_eq!(file_seq.seq, 3);
    }
}
