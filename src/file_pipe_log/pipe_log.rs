// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs;
use std::fs::File;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Read, Seek, Write};
use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::utils::CachePadded;
use fail::fail_point;
use fs2::FileExt;
use log::warn;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::config::Config;
use crate::event_listener::EventListener;
use crate::file_builder::FileBuilder;
use crate::metrics::*;
use crate::pipe_log::{FileBlockHandle, FileId, FileSeq, LogQueue, PipeLog};
use crate::{Error, Result};

use super::format::{lock_file_path, FileNameExt, LogFileHeader};
use super::log_file::{LogFd, LogFile};

const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

struct FileCollection {
    first_seq: FileSeq,
    active_seq: FileSeq,
    all_files: VecDeque<Arc<LogFd>>,
}

struct ActiveFile<W: Seek + Write> {
    seq: FileSeq,
    fd: Arc<LogFd>,
    writer: W,

    written: usize,
    capacity: usize,
    last_sync: usize,
}

impl<W: Seek + Write> ActiveFile<W> {
    fn open(seq: FileSeq, fd: Arc<LogFd>, writer: W) -> Result<Self> {
        let file_size = fd.file_size()?;
        let mut f = Self {
            seq,
            fd,
            writer,
            written: file_size,
            capacity: file_size,
            last_sync: file_size,
        };
        if file_size < LogFileHeader::len() {
            f.write_header()?;
        } else {
            f.writer.seek(std::io::SeekFrom::Start(file_size as u64))?;
        }
        Ok(f)
    }

    fn new(seq: FileSeq, fd: Arc<LogFd>, writer: W) -> Result<Self> {
        let mut f = Self {
            seq,
            fd,
            writer,
            written: 0,
            capacity: 0,
            last_sync: 0,
        };
        f.write_header()?;
        Ok(f)
    }

    fn truncate(&mut self) -> Result<()> {
        if self.written < self.capacity {
            self.fd.truncate(self.written)?;
            self.capacity = self.written;
        }
        Ok(())
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.seek(std::io::SeekFrom::Start(0))?;
        self.written = 0;
        let mut buf = Vec::with_capacity(LogFileHeader::len());
        LogFileHeader::default().encode(&mut buf)?;
        self.write(&buf, 0)
    }

    fn write(&mut self, buf: &[u8], target_file_size: usize) -> Result<()> {
        if self.written + buf.len() > self.capacity {
            // Use fallocate to pre-allocate disk space for active file.
            let alloc = std::cmp::max(
                self.written + buf.len() - self.capacity,
                std::cmp::min(
                    FILE_ALLOCATE_SIZE,
                    target_file_size.saturating_sub(self.capacity),
                ),
            );
            if alloc > 0 {
                self.fd.allocate(self.capacity, alloc)?;
                self.capacity += alloc;
            }
        }
        self.writer.write_all(buf)?;
        self.written += buf.len();
        Ok(())
    }

    fn sync(&mut self) -> Result<()> {
        if self.last_sync < self.written {
            StopWatch::new(&LOG_SYNC_DURATION_HISTOGRAM);
            self.fd.sync()?;
            self.last_sync = self.written;
        }
        Ok(())
    }

    fn since_last_sync(&self) -> usize {
        self.written - self.last_sync
    }
}

pub struct FilePipeLogImp<B: FileBuilder> {
    queue: LogQueue,
    dir: String,
    rotate_size: usize,
    bytes_per_sync: usize,
    file_builder: Arc<B>,
    listeners: Vec<Arc<dyn EventListener>>,

    files: CachePadded<RwLock<FileCollection>>,
    active_file: CachePadded<Mutex<ActiveFile<B::Writer<LogFile>>>>,
}

impl<B: FileBuilder> FilePipeLogImp<B> {
    pub fn open(
        cfg: &Config,
        file_builder: Arc<B>,
        listeners: Vec<Arc<dyn EventListener>>,
        queue: LogQueue,
        mut first_seq: FileSeq,
        mut all_files: VecDeque<Arc<LogFd>>,
    ) -> Result<Self> {
        let create_file = first_seq == 0;
        let active_seq = if create_file {
            first_seq = 1;
            let file_id = FileId {
                queue,
                seq: first_seq,
            };
            let fd = Arc::new(LogFd::create(&file_id.build_file_path(&cfg.dir))?);
            all_files.push_back(fd);
            first_seq
        } else {
            first_seq + all_files.len() as u64 - 1
        };

        for seq in first_seq..=active_seq {
            for listener in &listeners {
                listener.post_new_log_file(FileId { queue, seq });
            }
        }

        let active_fd = all_files.back().unwrap().clone();
        let file_id = FileId {
            queue,
            seq: active_seq,
        };
        let active_file = ActiveFile::open(
            active_seq,
            active_fd.clone(),
            file_builder.build_writer(
                &file_id.build_file_path(&cfg.dir),
                LogFile::new(active_fd),
                create_file,
            )?,
        )?;

        let total_files = all_files.len();
        let pipe = Self {
            queue,
            dir: cfg.dir.clone(),
            rotate_size: cfg.target_file_size.0 as usize,
            bytes_per_sync: cfg.bytes_per_sync.0 as usize,
            file_builder,
            listeners,

            files: CachePadded::new(RwLock::new(FileCollection {
                first_seq,
                active_seq,
                all_files,
            })),
            active_file: CachePadded::new(Mutex::new(active_file)),
        };
        pipe.flush_metrics(total_files);
        Ok(pipe)
    }

    fn sync_dir(&self) -> Result<()> {
        let path = PathBuf::from(&self.dir);
        std::fs::File::open(path).and_then(|d| d.sync_all())?;
        Ok(())
    }

    fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<LogFd>> {
        let files = self.files.read();
        if file_seq < files.first_seq || file_seq > files.active_seq {
            return Err(Error::Io(IoError::new(
                IoErrorKind::NotFound,
                "file seqno out of range",
            )));
        }
        Ok(files.all_files[(file_seq - files.first_seq) as usize].clone())
    }

    fn rotate_imp(
        &self,
        active_file: &mut MutexGuard<ActiveFile<B::Writer<LogFile>>>,
    ) -> Result<()> {
        let seq = active_file.seq + 1;
        debug_assert!(seq > 1);
        // Necessary to truncate extra zeros from fallocate().
        active_file.truncate()?;
        active_file.sync()?;

        let file_id = FileId {
            queue: self.queue,
            seq,
        };
        let path = file_id.build_file_path(&self.dir);
        let fd = Arc::new(LogFd::create(&path)?);
        self.sync_dir()?;

        **active_file = ActiveFile::new(
            seq,
            fd.clone(),
            self.file_builder.build_writer(
                &path,
                LogFile::new(fd.clone()),
                true, /*create*/
            )?,
        )?;
        let len = {
            let mut files = self.files.write();
            files.active_seq = seq;
            files.all_files.push_back(fd);
            for listener in &self.listeners {
                listener.post_new_log_file(FileId {
                    queue: self.queue,
                    seq,
                });
            }
            files.all_files.len()
        };
        self.flush_metrics(len);
        Ok(())
    }

    fn flush_metrics(&self, len: usize) {
        match self.queue {
            LogQueue::Append => LOG_FILE_COUNT.append.set(len as i64),
            LogQueue::Rewrite => LOG_FILE_COUNT.rewrite.set(len as i64),
        }
    }
}

impl<B: FileBuilder> FilePipeLogImp<B> {
    fn read_bytes(&self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let fd = self.get_fd(handle.id.seq)?;
        let mut reader = self
            .file_builder
            .build_reader(&handle.id.build_file_path(&self.dir), LogFile::new(fd))?;
        reader.seek(std::io::SeekFrom::Start(handle.offset))?;
        let mut buf = vec![0; handle.len];
        let size = reader.read(&mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }

    fn append(&self, bytes: &[u8]) -> Result<FileBlockHandle> {
        fail_point!("file_pipe_log::append");
        let mut active_file = self.active_file.lock();
        let offset = active_file.written as u64;
        if let Err(e) = active_file.write(bytes, self.rotate_size) {
            if let Err(te) = active_file.truncate() {
                panic!(
                    "error when truncate {} after error: {}, get: {}",
                    active_file.seq, e, te
                );
            }
            return Err(e);
        }
        let handle = FileBlockHandle {
            id: FileId {
                queue: self.queue,
                seq: active_file.seq,
            },
            offset,
            len: active_file.written - offset as usize,
        };
        for listener in &self.listeners {
            listener.on_append_log_file(handle);
        }
        Ok(handle)
    }

    fn maybe_sync(&self, force: bool) -> Result<()> {
        let mut active_file = self.active_file.lock();
        if active_file.written >= self.rotate_size {
            if let Err(e) = self.rotate_imp(&mut active_file) {
                panic!(
                    "error when rotate [{:?}:{}]: {}",
                    self.queue, active_file.seq, e
                );
            }
        } else if active_file.since_last_sync() >= self.bytes_per_sync || force {
            if let Err(e) = active_file.sync() {
                panic!(
                    "error when sync [{:?}:{}]: {}",
                    self.queue, active_file.seq, e,
                );
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
        (files.active_seq - files.first_seq + 1) as usize * self.rotate_size
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
            files.all_files.drain(..end_offset);
            files.first_seq = file_seq;
            (end_offset, files.all_files.len())
        };
        self.flush_metrics(remained);
        for seq in file_seq - purged as u64..file_seq {
            let file_id = FileId {
                queue: self.queue,
                seq,
            };
            let path = file_id.build_file_path(&self.dir);
            if let Err(e) = fs::remove_file(&path) {
                // TODO: handle this case in recovery.
                warn!("Remove purged log file {:?} failed: {}", path, e);
            }
        }
        Ok(purged)
    }
}

pub struct FilePipeLog<B: FileBuilder> {
    pipes: [FilePipeLogImp<B>; 2],

    _lock_file: File,
}

impl<B: FileBuilder> FilePipeLog<B> {
    pub fn open(
        dir: &str,
        appender: FilePipeLogImp<B>,
        rewriter: FilePipeLogImp<B>,
    ) -> Result<Self> {
        let lock_file = File::create(lock_file_path(dir))?;
        lock_file.try_lock_exclusive().map_err(|e| {
            Error::Other(box_err!(
                "Failed to lock file: {}, maybe another instance is using this directory.",
                e
            ))
        })?;

        // TODO: remove this dependency.
        debug_assert_eq!(LogQueue::Append as usize, 0);
        debug_assert_eq!(LogQueue::Rewrite as usize, 1);
        Ok(Self {
            pipes: [appender, rewriter],
            _lock_file: lock_file,
        })
    }

    #[cfg(test)]
    pub fn file_builder(&self) -> Arc<B> {
        self.pipes[0].file_builder.clone()
    }
}

impl<B: FileBuilder> PipeLog for FilePipeLog<B> {
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
}

#[cfg(test)]
mod tests {
    use tempfile::Builder;

    use super::*;
    use crate::file_builder::DefaultFileBuilder;
    use crate::util::ReadableSize;

    fn new_test_pipe(cfg: &Config, queue: LogQueue) -> Result<FilePipeLogImp<DefaultFileBuilder>> {
        FilePipeLogImp::open(
            cfg,
            Arc::new(DefaultFileBuilder {}),
            Vec::new(),
            queue,
            0,
            VecDeque::new(),
        )
    }

    fn new_test_pipe_log(cfg: &Config) -> Result<FilePipeLog<DefaultFileBuilder>> {
        FilePipeLog::open(
            &cfg.dir,
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

        let _r1 = new_test_pipe_log(&cfg).unwrap();

        // Only one thread can hold file lock
        let r2 = new_test_pipe_log(&cfg);

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

        let pipe_log = new_test_pipe_log(&cfg).unwrap();
        assert_eq!(pipe_log.file_span(queue), (1, 1));

        let header_size = LogFileHeader::len() as u64;

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

        // leave only 1 file to truncate
        assert!(pipe_log.purge_to(FileId { queue, seq: 3 }).is_ok());
        assert_eq!(pipe_log.file_span(queue), (3, 3));
    }
}
