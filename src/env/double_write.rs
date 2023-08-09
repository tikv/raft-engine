// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::file_pipe_log::log_file::build_file_reader;
use crate::file_pipe_log::pipe::File;
use crate::file_pipe_log::pipe_builder::FileName;
use crate::file_pipe_log::reader::LogItemBatchFileReader;
use crate::file_pipe_log::FileNameExt;
use crate::internals::parse_reserved_file_name;
use crate::internals::FileId;
use crate::internals::LogQueue;
use crate::{Error, Result};
use crossbeam::channel::unbounded;
use crossbeam::channel::Sender;
use fail::fail_point;
use log::{info, warn, Log};
use std::fs;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::RwLock;
use std::thread;

use crate::env::default::LogFd;
use crate::env::DefaultFileSystem;
use crate::env::{FileSystem, Handle, Permission, WriteExt};
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::select;

use either::Either;

type Callback<T> = Box<dyn FnOnce(IoResult<Option<T>>) + Send>;

#[derive(PartialEq)]
enum Task {
    Create(PathBuf),
    Open {
        path: PathBuf,
        perm: Permission,
    },
    Delete(PathBuf),
    Rename {
        src_path: PathBuf,
        dst_path: PathBuf,
    },
    Stop,
}

#[derive(Default)]
struct Files {
    prefix: PathBuf,
    append_file: Vec<FileName>,
    rewrite_file: Vec<FileName>,
    recycled_file: Vec<FileName>,
}

fn replace_path(path: &Path, from: &Path, to: &Path) -> PathBuf {
    if let Ok(file) = path.strip_prefix(from) {
        to.to_path_buf().join(file)
    } else {
        panic!("Invalid path: {:?}", path);
    }
}

pub struct HedgedFileSystem<F: FileSystem> {
    inner: Arc<F>,

    path1: PathBuf,
    path2: PathBuf,
    disk1: Sender<(Task, Callback<LogFd>)>,
    disk2: Sender<(Task, Callback<LogFd>)>,

    handle1: Option<thread::JoinHandle<()>>,
    handle2: Option<thread::JoinHandle<()>>,
}

// TODO: read both dir at recovery, maybe no need? cause operations are to both
// disks TODO: consider encryption

impl<F: FileSystem> HedgedFileSystem<F> {
    pub fn new(inner: Arc<F>, path1: PathBuf, path2: PathBuf) -> Self {
        let (tx1, rx1) = unbounded::<(Task, Callback<LogFd>)>();
        let (tx2, rx2) = unbounded::<(Task, Callback<LogFd>)>();
        let fs1 = inner.clone();
        let handle1 = thread::spawn(|| {
            for (task, cb) in rx1 {
                if task == Task::Stop {
                    break;
                }
                fail_point!("double_write::thread1");
                let res = Self::handle(&fs1, task);
                cb(res);
            }
        });
        let fs2 = inner.clone();
        let handle2 = thread::spawn(|| {
            let fs = DefaultFileSystem {};
            for (task, cb) in rx2 {
                if task == Task::Stop {
                    break;
                }
                let res = Self::handle(&fs2, task);
                cb(res);
            }
        });
        Self {
            inner,
            path1,
            path2,
            disk1: tx1,
            disk2: tx2,
            handle1: Some(handle1),
            handle2: Some(handle2),
        }
    }

    pub fn bootstrap(&self) -> Result<()> {
        // catch up diff
        let files1 = self.get_files(&self.path1)?;
        let files2 = self.get_files(&self.path2)?;

        let count1 = self.get_latest_valid_seq(&files1)?;
        let count2 = self.get_latest_valid_seq(&files2)?;

        match count1.cmp(&count2) {
            std::cmp::Ordering::Equal => {
                // TODO: still need to catch up
                return Ok(());
            }
            std::cmp::Ordering::Less => {
                self.catch_up_diff(files2, files1)?;
            }
            std::cmp::Ordering::Greater => {
                self.catch_up_diff(files1, files2)?;
            }
        }
        Ok(())
    }

    fn catch_up_diff(&self, fromFiles: Files, toFiles: Files) -> Result<()> {
        let check_files = |from: &Vec<FileName>, to: &Vec<FileName>| -> IoResult<()> {
            let mut iter1 = from.iter().peekable();
            let mut iter2 = to.iter().peekable();
            // compare files of from and to, if the file in from is not in to, copy it to
            // to, and if the file in to is not in from, delete it
            loop {
                match (iter1.peek(), iter2.peek()) {
                    (None, None) => break,
                    (Some(f1), None) => {
                        let to = replace_path(
                            f1.path.as_ref(),
                            fromFiles.prefix.as_ref(),
                            toFiles.prefix.as_ref(),
                        );
                        fs::copy(&f1.path, to)?;
                        iter1.next();
                    }
                    (None, Some(f2)) => {
                        fs::remove_file(&f2.path)?;
                        iter2.next();
                    }
                    (Some(f1), Some(f2)) => {
                        match f1.seq.cmp(&f2.seq) {
                            std::cmp::Ordering::Equal => {
                                // TODO: do we need to check file size?
                                // if f1.handle.file_size() != f2.handle.file_size() {
                                //     let to = replace_path(f1.path.as_ref(),
                                // fromFiles.prefix.as_ref(), toFiles.prefix.as_ref());
                                //     fs::copy(&f1.path, &to)?;
                                // }
                                iter1.next();
                                iter2.next();
                            }
                            std::cmp::Ordering::Less => {
                                let to = replace_path(
                                    f1.path.as_ref(),
                                    fromFiles.prefix.as_ref(),
                                    toFiles.prefix.as_ref(),
                                );
                                fs::copy(&f1.path, to)?;
                                iter1.next();
                            }
                            std::cmp::Ordering::Greater => {
                                fs::remove_file(&f2.path)?;
                                iter2.next();
                            }
                        }
                    }
                }
            }
            Ok(())
        };

        check_files(&fromFiles.append_file, &toFiles.append_file)?;
        check_files(&fromFiles.rewrite_file, &toFiles.rewrite_file)?;
        check_files(&fromFiles.recycled_file, &toFiles.recycled_file)?;

        // check file size is not enough, treat the last files differently considering
        // the recycle, always copy the last file
        if let Some(last_file) = fromFiles.append_file.last() {
            let to = replace_path(
                last_file.path.as_ref(),
                fromFiles.prefix.as_ref(),
                toFiles.prefix.as_ref(),
            );
            fs::copy(&last_file.path, to)?;
        }
        if let Some(last_file) = fromFiles.rewrite_file.last() {
            let to = replace_path(
                last_file.path.as_ref(),
                fromFiles.prefix.as_ref(),
                toFiles.prefix.as_ref(),
            );
            fs::copy(&last_file.path, to)?;
        }
        if let Some(last_file) = fromFiles.recycled_file.last() {
            let to = replace_path(
                last_file.path.as_ref(),
                fromFiles.prefix.as_ref(),
                toFiles.prefix.as_ref(),
            );
            fs::copy(&last_file.path, to)?;
        }

        Ok(())
    }

    fn get_files(&self, path: &PathBuf) -> IoResult<Files> {
        let mut files = Files {
            prefix: path.clone(),
            ..Default::default()
        };
        if !path.exists() {
            info!("Create raft log directory: {}", path.display());
            fs::create_dir(path).unwrap();
        }

        fs::read_dir(path)
            .unwrap()
            .try_for_each(|e| -> IoResult<()> {
                let dir_entry = e?;
                let p = dir_entry.path();
                if !p.is_file() {
                    return Ok(());
                }
                let file_name = p.file_name().unwrap().to_str().unwrap();
                match FileId::parse_file_name(file_name) {
                    Some(FileId {
                        queue: LogQueue::Append,
                        seq,
                    }) => files.append_file.push(FileName {
                        seq,
                        path: p,
                        path_id: 0,
                    }),
                    Some(FileId {
                        queue: LogQueue::Rewrite,
                        seq,
                    }) => files.rewrite_file.push(FileName {
                        seq,
                        path: p,
                        path_id: 0,
                    }),
                    _ => {
                        if let Some(seq) = parse_reserved_file_name(file_name) {
                            files.recycled_file.push(FileName {
                                seq,
                                path: p,
                                path_id: 0,
                            })
                        }
                    }
                }
                Ok(())
            })
            .unwrap();
        files.append_file.sort_by(|a, b| a.seq.cmp(&b.seq));
        files.rewrite_file.sort_by(|a, b| a.seq.cmp(&b.seq));
        files.recycled_file.sort_by(|a, b| a.seq.cmp(&b.seq));
        Ok(files)
    }

    fn get_latest_valid_seq(&self, files: &Files) -> Result<usize> {
        let mut count = 0;
        if let Some(f) = files.append_file.last() {
            let recovery_read_block_size = 1024;
            let mut reader = LogItemBatchFileReader::new(recovery_read_block_size);
            // TODO: change file system
            let handle = Arc::new(DefaultFileSystem {}.open(&f.path, Permission::ReadOnly)?);
            let file_reader = build_file_reader(&DefaultFileSystem {}, handle)?;
            match reader.open(
                FileId {
                    queue: LogQueue::Append,
                    seq: f.seq,
                },
                file_reader,
            ) {
                Err(e) if matches!(e, Error::Io(_)) => return Err(e),
                Err(e) => {
                    return Ok(0);
                }
                Ok(format) => {
                    // Do nothing
                }
            }
            loop {
                match reader.next() {
                    Ok(Some(item_batch)) => {
                        count += 1;
                    }
                    Ok(None) => break,
                    Err(e) => break,
                }
            }
        }

        Ok(count)
    }

    async fn wait_handle(&self, task1: Task, task2: Task) -> IoResult<HedgedHandle> {
        let (cb1, mut f1) = paired_future_callback();
        let (cb2, mut f2) = paired_future_callback();
        self.disk1.send((task1, cb1)).unwrap();
        self.disk2.send((task2, cb2)).unwrap();

        select! {
            res1 = f1 => res1.unwrap().map(|h| HedgedHandle::new(
                Either::Right(h.unwrap()), Either::Left(f2) )),
            res2 = f2 => res2.unwrap().map(|h| HedgedHandle::new(
                Either::Left(f1), Either::Right(h.unwrap()) )),
        }
    }

    async fn wait_one(&self, task1: Task, task2: Task) -> IoResult<()> {
        let (cb1, mut f1) = paired_future_callback();
        let (cb2, mut f2) = paired_future_callback();
        self.disk1.send((task1, cb1)).unwrap();
        self.disk2.send((task2, cb2)).unwrap();

        select! {
            res1 = f1 => res1.unwrap().map(|_| ()),
            res2 = f2 => res2.unwrap().map(|_| ()),
        }
    }

    #[inline]
    fn handle(file_system: &F, task: Task) -> IoResult<Option<LogFd>> {
        match task {
            Task::Create(path) => file_system.create(path).map(Some),
            Task::Open { path, perm } => file_system.open(path, perm).map(Some),
            Task::Delete(path) => file_system.delete(path).map(|_| None),
            Task::Rename { src_path, dst_path } => {
                file_system.rename(src_path, dst_path).map(|_| None)
            }
            Task::Stop => unreachable!(),
        }
    }
}

impl<F: FileSystem> Drop for HedgedFileSystem<F> {
    fn drop(&mut self) {
        self.disk1.send((Task::Stop, Box::new(|_| {}))).unwrap();
        self.disk2.send((Task::Stop, Box::new(|_| {}))).unwrap();
        self.handle1.take().unwrap().join().unwrap();
        self.handle2.take().unwrap().join().unwrap();
    }
}

impl<F: FileSystem> FileSystem for HedgedFileSystem<F> {
    type Handle = HedgedHandle;
    type Reader = HedgedReader;
    type Writer = HedgedWriter;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        block_on(self.wait_handle(
            Task::Create(path.as_ref().to_path_buf()),
            Task::Create(replace_path(
                path.as_ref(),
                self.path1.as_ref(),
                self.path2.as_ref(),
            )),
        ))
    }

    fn open<P: AsRef<Path>>(&self, path: P, perm: Permission) -> IoResult<Self::Handle> {
        block_on(self.wait_handle(
            Task::Open {
                path: path.as_ref().to_path_buf(),
                perm,
            },
            Task::Open {
                path: replace_path(path.as_ref(), self.path1.as_ref(), self.path2.as_ref()),
                perm,
            },
        ))
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        block_on(self.wait_one(
            Task::Delete(path.as_ref().to_path_buf()),
            Task::Delete(replace_path(
                path.as_ref(),
                self.path1.as_ref(),
                self.path2.as_ref(),
            )),
        ))
    }

    fn rename<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> IoResult<()> {
        block_on(self.wait_one(
            Task::Rename {
                src_path: src_path.as_ref().to_path_buf(),
                dst_path: dst_path.as_ref().to_path_buf(),
            },
            Task::Rename {
                src_path: replace_path(src_path.as_ref(), self.path1.as_ref(), self.path2.as_ref()),
                dst_path: replace_path(dst_path.as_ref(), self.path1.as_ref(), self.path2.as_ref()),
            },
        ))
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        Ok(HedgedReader::new(handle))
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        Ok(HedgedWriter::new(handle))
    }
}

#[derive(Clone, PartialEq)]
enum FileTask {
    Truncate(usize),
    FileSize,
    Sync,
    Write { offset: usize, bytes: Vec<u8> },
    Allocate { offset: usize, size: usize },
    Stop,
}

pub struct HedgedHandle {
    disk1: Sender<(FileTask, Callback<usize>)>,
    disk2: Sender<(FileTask, Callback<usize>)>,
    counter1: Arc<AtomicU64>,
    counter2: Arc<AtomicU64>,
    fd1: Arc<RwLock<Option<Arc<LogFd>>>>,
    fd2: Arc<RwLock<Option<Arc<LogFd>>>>,

    handle1: Option<thread::JoinHandle<()>>,
    handle2: Option<thread::JoinHandle<()>>,
}

impl HedgedHandle {
    pub fn new(
        file1: Either<oneshot::Receiver<IoResult<Option<LogFd>>>, LogFd>,
        file2: Either<oneshot::Receiver<IoResult<Option<LogFd>>>, LogFd>,
    ) -> Self {
        let (tx1, rx1) = unbounded::<(FileTask, Callback<usize>)>();
        let (tx2, rx2) = unbounded::<(FileTask, Callback<usize>)>();
        let counter1 = Arc::new(AtomicU64::new(0));
        let counter2 = Arc::new(AtomicU64::new(0));
        let fd1 = Arc::new(RwLock::new(None));
        let fd2 = Arc::new(RwLock::new(None));

        let handle1 = {
            let fd1 = fd1.clone();
            let counter1 = counter1.clone();
            thread::spawn(move || {
                let fd = Arc::new(Self::resolve(file1));
                fd1.write().unwrap().replace(fd.clone());
                for (task, cb) in rx1 {
                    if task == FileTask::Stop {
                        break;
                    }
                    let res = Self::handle(&fd, task);
                    counter1.fetch_add(1, Ordering::Relaxed);
                    cb(res);
                }
            })
        };
        let handle2 = {
            let fd2 = fd2.clone();
            let counter2 = counter2.clone();
            thread::spawn(move || {
                let fd = Arc::new(Self::resolve(file2));
                fd2.write().unwrap().replace(fd.clone());
                for (task, cb) in rx2 {
                    if task == FileTask::Stop {
                        break;
                    }
                    let res = Self::handle(&fd, task);
                    counter2.fetch_add(1, Ordering::Relaxed);
                    cb(res);
                }
            })
        };
        Self {
            disk1: tx1,
            disk2: tx2,
            counter1,
            counter2,
            fd1,
            fd2,
            handle1: Some(handle1),
            handle2: Some(handle2),
        }
    }

    fn resolve(file: Either<oneshot::Receiver<IoResult<Option<LogFd>>>, LogFd>) -> LogFd {
        match file {
            Either::Left(f) => {
                // TODO: should we handle the second disk io error
                block_on(f).unwrap().unwrap().unwrap()
            }
            Either::Right(fd) => fd,
        }
    }

    fn handle(fd: &LogFd, task: FileTask) -> IoResult<Option<usize>> {
        match task {
            FileTask::Truncate(offset) => fd.truncate(offset).map(|_| None),
            FileTask::FileSize => fd.file_size().map(Some),
            FileTask::Sync => fd.sync().map(|_| None),
            FileTask::Write { offset, bytes } => fd.write(offset, &bytes).map(Some),
            FileTask::Allocate { offset, size } => fd.allocate(offset, size).map(|_| None),
            FileTask::Stop => unreachable!(),
        }
    }

    fn read(&self, offset: usize, buf: &mut [u8]) -> IoResult<usize> {
        // TODO: read simultaneously from both disks
        // choose latest to perform read
        let count1 = self.counter1.load(Ordering::Relaxed);
        let count2 = self.counter2.load(Ordering::Relaxed);
        match count1.cmp(&count2) {
            std::cmp::Ordering::Equal => {
                if let Some(fd) = self.fd1.read().unwrap().as_ref() {
                    fd.read(offset, buf)
                } else if let Some(fd) = self.fd2.read().unwrap().as_ref() {
                    fd.read(offset, buf)
                } else {
                    panic!("Both fd1 and fd2 are None");
                }
            }
            std::cmp::Ordering::Greater => {
                self.fd1.read().unwrap().as_ref().unwrap().read(offset, buf)
            }
            std::cmp::Ordering::Less => {
                self.fd2.read().unwrap().as_ref().unwrap().read(offset, buf)
            }
        }
    }

    fn write(&self, offset: usize, content: &[u8]) -> IoResult<usize> {
        block_on(self.wait_one(FileTask::Write {
            offset,
            bytes: content.to_vec(),
        }))
        .map(|x| x.unwrap())
    }

    fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
        block_on(self.wait_one(FileTask::Allocate { offset, size })).map(|_| ())
    }

    async fn wait_one(&self, task: FileTask) -> IoResult<Option<usize>> {
        let (cb1, mut f1) = paired_future_callback();
        let (cb2, mut f2) = paired_future_callback();
        self.disk1.send((task.clone(), cb1)).unwrap();
        self.disk2.send((task, cb2)).unwrap();

        select! {
            res1 = f1 => res1.unwrap(),
            res2 = f2 => res2.unwrap(),
        }
    }
}

impl Drop for HedgedHandle {
    fn drop(&mut self) {
        self.disk1.send((FileTask::Stop, Box::new(|_| {}))).unwrap();
        self.disk2.send((FileTask::Stop, Box::new(|_| {}))).unwrap();
        self.handle1.take().unwrap().join().unwrap();
        self.handle2.take().unwrap().join().unwrap();
    }
}

impl Handle for HedgedHandle {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        block_on(self.wait_one(FileTask::Truncate(offset))).map(|_| ())
    }

    fn file_size(&self) -> IoResult<usize> {
        block_on(self.wait_one(FileTask::FileSize)).map(|x| x.unwrap())
    }

    fn sync(&self) -> IoResult<()> {
        block_on(self.wait_one(FileTask::Sync)).map(|_| ())
    }
}

pub struct HedgedWriter {
    inner: Arc<HedgedHandle>,
    offset: usize,
}

impl HedgedWriter {
    pub fn new(handle: Arc<HedgedHandle>) -> Self {
        Self {
            inner: handle,
            offset: 0,
        }
    }
}

impl Write for HedgedWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let len = self.inner.write(self.offset, buf)?;
        self.offset += len;
        Ok(len)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl WriteExt for HedgedWriter {
    fn truncate(&mut self, offset: usize) -> IoResult<()> {
        self.inner.truncate(offset)?;
        self.offset = offset;
        Ok(())
    }

    fn allocate(&mut self, offset: usize, size: usize) -> IoResult<()> {
        self.inner.allocate(offset, size)
    }
}

impl Seek for HedgedWriter {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        match pos {
            SeekFrom::Start(offset) => self.offset = offset as usize,
            SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as usize,
            SeekFrom::End(i) => self.offset = (self.inner.file_size()? as i64 + i) as usize,
        }
        Ok(self.offset as u64)
    }
}

pub struct HedgedReader {
    inner: Arc<HedgedHandle>,
    offset: usize,
}

impl HedgedReader {
    pub fn new(handle: Arc<HedgedHandle>) -> Self {
        Self {
            inner: handle,
            offset: 0,
        }
    }
}

impl Seek for HedgedReader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        match pos {
            SeekFrom::Start(offset) => self.offset = offset as usize,
            SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as usize,
            SeekFrom::End(i) => self.offset = (self.inner.file_size()? as i64 + i) as usize,
        }
        Ok(self.offset as u64)
    }
}

impl Read for HedgedReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let len = self.inner.read(self.offset, buf)?;
        self.offset += len;
        Ok(len)
    }
}

pub fn paired_future_callback<T: Send + 'static>(
) -> (Callback<T>, oneshot::Receiver<IoResult<Option<T>>>) {
    let (tx, future) = oneshot::channel();
    let callback = Box::new(move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    });
    (callback, future)
}
