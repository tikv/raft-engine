// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::env::default::LogFile;
use crate::env::RecoverExt;
use crate::file_pipe_log::log_file::build_file_reader;
use crate::file_pipe_log::pipe_builder::FileName;
use crate::file_pipe_log::reader::LogItemBatchFileReader;
use crate::file_pipe_log::FileNameExt;
use crate::internals::parse_reserved_file_name;
use crate::internals::FileId;
use crate::internals::LogQueue;
use crate::Error;
use crossbeam::channel::unbounded;
use crossbeam::channel::{Receiver, Sender};
use fail::fail_point;
use log::{info, warn};
use std::cell::UnsafeCell;
use std::fs;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::RwLock;
use std::thread;
use std::thread::JoinHandle;

use crate::env::default::LogFd;
use crate::env::DefaultFileSystem;
use crate::env::{FileSystem, Handle, Permission, WriteExt};
use futures::channel::oneshot;
use futures::executor::block_on;
use futures::{join, select};

use either::Either;

type Callback<T> = Box<dyn FnOnce(IoResult<T>) + Send>;

struct SeqTask {
    inner: Task,
    seq: u64,
}

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
    Truncate {
        handle: Arc<FutureHandle>,
        offset: usize,
    },
    FileSize(Arc<FutureHandle>),
    Sync(Arc<FutureHandle>),
    Write {
        handle: Arc<FutureHandle>,
        offset: usize,
        bytes: Vec<u8>,
    },
    Allocate {
        handle: Arc<FutureHandle>,
        offset: usize,
        size: usize,
    },
    Pause,
    Snapshot(PathBuf),
    Stop,
}

impl SeqTask {
    fn process(self, file_system: &DefaultFileSystem) -> IoResult<TaskRes> {
        match self.inner {
            Task::Create(path) => file_system.create(&path).map(|h| TaskRes::Create {
                fd: h,
                is_for_rewrite: path.extension().map_or(false, |ext| ext == "rewrite"),
            }),
            Task::Open { path, perm } => file_system.open(&path, perm).map(|h| TaskRes::Open {
                fd: h,
                is_for_rewrite: path.extension().map_or(false, |ext| ext == "rewrite"),
            }),
            Task::Delete(path) => file_system.delete(path).map(|_| TaskRes::Delete),
            Task::Rename { src_path, dst_path } => file_system
                .rename(src_path, dst_path)
                .map(|_| TaskRes::Rename),
            Task::Snapshot(path) => {
                let mut files = HedgedFileSystem::get_files(&path).unwrap(); // TODO: handle error
                files.append_files = files
                    .append_files
                    .into_iter()
                    .map(|f| f.into_handle(file_system))
                    .collect();
                // exclude rewrite files, as they are always synced
                files.reserved_files = files
                    .reserved_files
                    .into_iter()
                    .map(|f| f.into_handle(file_system))
                    .collect();
                Ok(TaskRes::Snapshot(files))
            }
            Task::Stop | Task::Pause => unreachable!(),
            _ => self.handle_process(),
        }
    }

    fn handle_process(self) -> IoResult<TaskRes> {
        match self.inner {
            Task::Truncate { handle, offset } => {
                handle.get().truncate(offset).map(|_| TaskRes::Truncate)
            }
            Task::FileSize(handle) => handle.get().file_size().map(|s| TaskRes::FileSize(s)),
            Task::Sync(handle) => handle.get().sync().map(|_| TaskRes::Sync),
            Task::Write {
                handle,
                offset,
                bytes,
            } => handle
                .get()
                .write(offset, &bytes)
                .map(|s| TaskRes::Write(s)),
            Task::Allocate {
                handle,
                offset,
                size,
            } => handle
                .get()
                .allocate(offset, size)
                .map(|_| TaskRes::Allocate),
            _ => unreachable!(),
        }
    }
}

enum TaskRes {
    Create { fd: LogFd, is_for_rewrite: bool },
    Open { fd: LogFd, is_for_rewrite: bool },
    Delete,
    Rename,
    Truncate,
    FileSize(usize),
    Sync,
    Write(usize),
    Allocate,
    Snapshot(Files),
    Stop,
}

#[derive(Default)]
struct Files {
    prefix: PathBuf,
    append_files: Vec<SeqFile>,
    rewrite_files: Vec<SeqFile>,
    reserved_files: Vec<SeqFile>,
}

enum SeqFile {
    Path(FileName),
    Handle((FileName, Arc<LogFd>)),
}

impl SeqFile {
    fn seq(&self) -> u64 {
        match self {
            SeqFile::Path(f) => f.seq,
            SeqFile::Handle((f, _)) => f.seq,
        }
    }

    fn path(&self) -> &PathBuf {
        match self {
            SeqFile::Path(f) => &f.path,
            SeqFile::Handle((f, _)) => &f.path,
        }
    }

    fn remove(&self) -> IoResult<()> {
        match self {
            SeqFile::Path(f) => fs::remove_file(&f.path),
            SeqFile::Handle((f, _)) => fs::remove_file(&f.path),
        }
    }

    fn copy(&self, file_system: &DefaultFileSystem, to: &PathBuf) -> IoResult<u64> {
        match self {
            SeqFile::Path(f) => fs::copy(&f.path, to.as_path()),
            SeqFile::Handle((_, fd)) => {
                let mut reader = LogFile::new(fd.clone());
                let mut writer = LogFile::new(Arc::new(file_system.create(to)?));
                std::io::copy(&mut reader, &mut writer)
            }
        }
    }

    fn into_handle(mut self, file_system: &DefaultFileSystem) -> Self {
        if let SeqFile::Path(f) = self {
            let fd = Arc::new(file_system.open(&f.path, Permission::ReadOnly).unwrap());
            self = SeqFile::Handle((f, fd));
        }
        self
    }
}

fn replace_path(path: &Path, from: &Path, to: &Path) -> PathBuf {
    if let Ok(file) = path.strip_prefix(from) {
        to.to_path_buf().join(file)
    } else {
        panic!("Invalid path: {:?}", path);
    }
}

// let say the average entry size is 100B, then the total size of the log in the
// channel is 1GB,
const ABORT_THRESHOLD: usize = 10000;

// Make sure the task is sent to two disks' channel atomically, otherwise the
// ordering of the tasks in two disks' channels are not same.
#[derive(Clone)]
struct HedgedSender(Arc<Mutex<HedgedSenderInner>>);

struct HedgedSenderInner {
    disk1: Sender<(SeqTask, Callback<TaskRes>)>,
    disk2: Sender<(SeqTask, Callback<TaskRes>)>,
    seq: u64,
    state: Arc<RwLock<RecoveryState>>,
}

impl HedgedSender {
    fn new(
        disk1: Sender<(SeqTask, Callback<TaskRes>)>,
        disk2: Sender<(SeqTask, Callback<TaskRes>)>,
        state: Arc<RwLock<RecoveryState>>,
    ) -> Self {
        Self(Arc::new(Mutex::new(HedgedSenderInner {
            disk1,
            disk2,
            seq: 0,
            state,
        })))
    }

    fn state(&self) -> Arc<RwLock<RecoveryState>> {
        self.0.lock().unwrap().state.clone()
    }

    fn send(&self, task1: Task, task2: Task, cb1: Callback<TaskRes>, cb2: Callback<TaskRes>) {
        if matches!(task1, Task::Pause | Task::Snapshot(_)) {
            unreachable!();
        }

        let mut inner = self.0.lock().unwrap();
        inner.seq += 1;
        let task1 = SeqTask {
            inner: task1,
            seq: inner.seq,
        };
        let task2 = SeqTask {
            inner: task2,
            seq: inner.seq,
        };
        let state = inner.state.read().unwrap();
        if matches!(*state, RecoveryState::Normal) {
            let check1 = inner.disk1.len() > ABORT_THRESHOLD;
            let check2 = inner.disk2.len() > ABORT_THRESHOLD;
            match (check1, check2) {
                (true, true) => {
                    panic!("Both channels of disk1 and disk2 are full")
                }
                (true, false) => {
                    *inner.state.write().unwrap() = RecoveryState::Paused1;
                    inner
                        .disk1
                        .send((
                            SeqTask {
                                inner: Task::Pause,
                                seq: 0,
                            },
                            Box::new(|_| {}),
                        ))
                        .unwrap();
                }
                (false, true) => {
                    *inner.state.write().unwrap() = RecoveryState::Paused2;
                    inner
                        .disk2
                        .send((
                            SeqTask {
                                inner: Task::Pause,
                                seq: 0,
                            },
                            Box::new(|_| {}),
                        ))
                        .unwrap();
                }
                _ => {}
            }
        }
        if !matches!(
            *state,
            RecoveryState::Paused1 | RecoveryState::WaitRecover1(_)
        ) {
            inner.disk1.send((task1, cb1)).unwrap();
        }
        if !matches!(
            *state,
            RecoveryState::Paused2 | RecoveryState::WaitRecover2(_)
        ) {
            inner.disk2.send((task2, cb2)).unwrap();
        }
    }

    fn send_snapshot(&self, index: u8, task: Task, cb: Callback<TaskRes>) {
        assert!(matches!(task, Task::Snapshot(_)));

        let inner = self.0.lock().unwrap();
        if index == 1 {
            inner
                .disk1
                .send((
                    SeqTask {
                        inner: task,
                        seq: 0,
                    },
                    cb,
                ))
                .unwrap();
        } else {
            inner
                .disk2
                .send((
                    SeqTask {
                        inner: task,
                        seq: 0,
                    },
                    cb,
                ))
                .unwrap();
        }
    }
}

enum RecoveryState {
    Normal,
    Paused1, /* When the length of channel of disk1 reaches threshold, a
              * `Pause` task is sent and no more later task will be sent
              * to disk1 */
    Paused2, // no more task will be sent to disk2
    WaitRecover1(oneshot::Sender<()>),
    WaitRecover2(oneshot::Sender<()>),
    Recovering,
}

pub struct HedgedFileSystem {
    base: Arc<DefaultFileSystem>,

    path1: PathBuf,
    path2: PathBuf,

    sender: HedgedSender,

    seqno1: Arc<AtomicU64>,
    seqno2: Arc<AtomicU64>,

    thread1: Option<JoinHandle<()>>,
    thread2: Option<JoinHandle<()>>,

    state: Arc<RwLock<RecoveryState>>,
}

struct TaskRunner {
    id: u8,
    path: PathBuf,
    fs: Arc<DefaultFileSystem>,
    rx: Receiver<(SeqTask, Callback<TaskRes>)>,
    seqno: Arc<AtomicU64>,
    state: Arc<RwLock<RecoveryState>>,
}

impl TaskRunner {
    fn new(
        id: u8,
        path: PathBuf,
        fs: Arc<DefaultFileSystem>,
        rx: Receiver<(SeqTask, Callback<TaskRes>)>,
        seqno: Arc<AtomicU64>,
        state: Arc<RwLock<RecoveryState>>,
    ) -> Self {
        Self {
            id,
            path,
            fs,
            rx,
            seqno,
            state,
        }
    }

    fn spawn(self) -> JoinHandle<()> {
        thread::Builder::new()
            .name(format!("raft-engine-disk{}", self.id))
            .spawn(move || {
                for (task, cb) in self.rx {
                    if let Task::Stop = task.inner {
                        cb(Ok(TaskRes::Stop));
                        break;
                    }
                    if let Task::Pause = task.inner {
                        let (tx, rx) = oneshot::channel();
                        *self.state.write().unwrap() = if self.id == 1 {
                            RecoveryState::WaitRecover1(tx)
                        } else {
                            RecoveryState::WaitRecover2(tx)
                        };
                        let _ = block_on(rx);
                        // indicate the pause is done
                        // do not update seqno for pause task
                        continue;
                    }
                    if self.id == 1 {
                        fail_point!("double_write::thread1");
                    }
                    let seq = task.seq;
                    let res = task.process(&self.fs);
                    // seqno should be updated before the write callback is called, otherwise one
                    // read may be performed right after the write is finished. Then the read may be
                    // performed on the other disk not having the data because the seqno for this
                    // disk is not updated yet.
                    if seq != 0 {
                        self.seqno.store(seq, Ordering::Relaxed);
                    }
                    cb(res);
                }
            })
            .unwrap()
    }
}

// TODO: read both dir at recovery, maybe no need? cause operations are to both
// disks TODO: consider encryption
impl HedgedFileSystem {
    pub fn new(base: Arc<DefaultFileSystem>, path1: PathBuf, path2: PathBuf) -> Self {
        let (tx1, rx1) = unbounded::<(SeqTask, Callback<TaskRes>)>();
        let (tx2, rx2) = unbounded::<(SeqTask, Callback<TaskRes>)>();
        let state = Arc::new(RwLock::new(RecoveryState::Normal));
        let seqno1 = Arc::new(AtomicU64::new(0));
        let seqno2 = Arc::new(AtomicU64::new(0));
        let runner1 = TaskRunner::new(
            1,
            path1.clone(),
            base.clone(),
            rx1,
            seqno1.clone(),
            state.clone(),
        );
        let runner2 = TaskRunner::new(
            2,
            path2.clone(),
            base.clone(),
            rx2,
            seqno2.clone(),
            state.clone(),
        );
        let thread1 = runner1.spawn();
        let thread2 = runner2.spawn();
        let sender = HedgedSender::new(tx1, tx2, state.clone());
        Self {
            base,
            path1,
            path2,
            sender,
            seqno1,
            seqno2,
            thread1: Some(thread1),
            thread2: Some(thread2),
            state,
        }
    }

    fn catch_up_diff(
        &self,
        mut from_files: Files,
        mut to_files: Files,
        skip_rewrite: bool,
    ) -> IoResult<()> {
        from_files
            .append_files
            .sort_by(|a, b| a.seq().cmp(&b.seq()));
        to_files.append_files.sort_by(|a, b| a.seq().cmp(&b.seq()));
        from_files
            .rewrite_files
            .sort_by(|a, b| a.seq().cmp(&b.seq()));
        to_files.rewrite_files.sort_by(|a, b| a.seq().cmp(&b.seq()));
        from_files
            .reserved_files
            .sort_by(|a, b| a.seq().cmp(&b.seq()));
        to_files
            .reserved_files
            .sort_by(|a, b| a.seq().cmp(&b.seq()));

        let check_files = |from: &Vec<SeqFile>, to: &Vec<SeqFile>| -> IoResult<()> {
            let last_from_seq = from.last().map(|f| f.seq()).unwrap_or(0);

            let mut iter1 = from.iter().peekable();
            let mut iter2 = to.iter().peekable();
            // compare files of from and to, if the file in from is not in to, copy it to
            // to, and if the file in to is not in from, delete it
            loop {
                match (iter1.peek(), iter2.peek()) {
                    (None, None) => break,
                    (Some(f1), None) => {
                        let to = replace_path(
                            f1.path().as_ref(),
                            from_files.prefix.as_ref(),
                            to_files.prefix.as_ref(),
                        );
                        f1.copy(&self.base, &to)?;
                        iter1.next();
                    }
                    (None, Some(f2)) => {
                        f2.remove()?;
                        iter2.next();
                    }
                    (Some(f1), Some(f2)) => {
                        match f1.seq().cmp(&f2.seq()) {
                            std::cmp::Ordering::Equal => {
                                // check file size is not enough, treat the last files differently
                                // considering the recycle, always copy the last file
                                // TODO: only copy diff part
                                if f1.seq() == last_from_seq {
                                    let to = replace_path(
                                        f1.path().as_ref(),
                                        from_files.prefix.as_ref(),
                                        to_files.prefix.as_ref(),
                                    );
                                    f1.copy(&self.base, &to)?;
                                }
                                iter1.next();
                                iter2.next();
                            }
                            std::cmp::Ordering::Less => {
                                let to = replace_path(
                                    f1.path().as_ref(),
                                    from_files.prefix.as_ref(),
                                    to_files.prefix.as_ref(),
                                );
                                f1.copy(&self.base, &to)?;
                                iter1.next();
                            }
                            std::cmp::Ordering::Greater => {
                                f2.remove()?;
                                iter2.next();
                            }
                        }
                    }
                }
            }
            Ok(())
        };

        check_files(&from_files.append_files, &to_files.append_files)?;
        if !skip_rewrite {
            check_files(&from_files.rewrite_files, &to_files.rewrite_files)?;
        }
        check_files(&from_files.reserved_files, &to_files.reserved_files)?;
        Ok(())
    }

    fn get_files(path: &PathBuf) -> IoResult<Files> {
        assert!(path.exists());

        let mut files = Files {
            prefix: path.clone(),
            ..Default::default()
        };

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
                    }) => files.append_files.push(SeqFile::Path(FileName {
                        seq,
                        path: p,
                        path_id: 0,
                    })),
                    Some(FileId {
                        queue: LogQueue::Rewrite,
                        seq,
                    }) => files.rewrite_files.push(SeqFile::Path(FileName {
                        seq,
                        path: p,
                        path_id: 0,
                    })),
                    _ => {
                        if let Some(seq) = parse_reserved_file_name(file_name) {
                            files.reserved_files.push(SeqFile::Path(FileName {
                                seq,
                                path: p,
                                path_id: 0,
                            }))
                        }
                    }
                }
                Ok(())
            })
            .unwrap();

        Ok(files)
    }

    fn get_latest_valid_seq(&self, files: &Files) -> IoResult<usize> {
        let mut count = 0;
        if let Some(f) = files.append_files.last() {
            let recovery_read_block_size = 1024;
            let mut reader = LogItemBatchFileReader::new(recovery_read_block_size);
            let handle = Arc::new(self.base.open(&f.path(), Permission::ReadOnly)?);
            let file_reader = build_file_reader(self.base.as_ref(), handle)?;
            match reader.open(
                FileId {
                    queue: LogQueue::Append,
                    seq: f.seq(),
                },
                file_reader,
            ) {
                Err(e) => match e {
                    Error::Io(err) => return Err(err),
                    _ => return Ok(0),
                },
                Ok(_) => {
                    // Do nothing
                }
            }
            loop {
                match reader.next() {
                    Ok(Some(_)) => {
                        count += 1;
                    }
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
        }

        Ok(count)
    }

    async fn wait_handle(&self, task1: Task, task2: Task) -> IoResult<HedgedHandle> {
        let (cb1, mut f1) = paired_future_callback();
        let (cb2, mut f2) = paired_future_callback();
        self.sender.send(task1, task2, cb1, cb2);

        let resolve = |res: TaskRes| -> (LogFd, bool) {
            match res {
                TaskRes::Create { fd, is_for_rewrite } => (fd, is_for_rewrite),
                TaskRes::Open { fd, is_for_rewrite } => (fd, is_for_rewrite),
                _ => unreachable!(),
            }
        };
        select! {
            res1 = f1 => res1.unwrap().map(|res| {
                let (fd, is_for_rewrite) = resolve(res);
                HedgedHandle::new(
                    is_for_rewrite,
                    self.sender.clone(),
                    FutureHandle::new_owned(fd),
                    FutureHandle::new(f2),
                    self.seqno1.clone(),
                    self.seqno2.clone(),
                )
            }),
            res2 = f2 => res2.unwrap().map(|res| {
                let (fd, is_for_rewrite) = resolve(res);
                HedgedHandle::new(
                    is_for_rewrite,
                    self.sender.clone(),
                    FutureHandle::new(f1),
                    FutureHandle::new_owned(fd) ,
                    self.seqno1.clone(),
                    self.seqno2.clone(),
                )
            }),
        }
    }

    async fn wait(&self, task1: Task, task2: Task) -> IoResult<()> {
        let (cb1, mut f1) = paired_future_callback();
        let (cb2, mut f2) = paired_future_callback();
        self.sender.send(task1, task2, cb1, cb2);

        select! {
            res1 = f1 => res1.unwrap().map(|_| ()),
            res2 = f2 => res2.unwrap().map(|_| ()),
        }
    }
}

impl Drop for HedgedFileSystem {
    fn drop(&mut self) {
        block_on(self.wait(Task::Stop, Task::Stop)).unwrap();

        let t1 = self.thread1.take().unwrap();
        let t2 = self.thread2.take().unwrap();
        let mut times = 0;
        loop {
            // wait 1s
            if t1.is_finished() && t2.is_finished() {
                t1.join().unwrap();
                t2.join().unwrap();
                break;
            }
            times += 1;
            if times > 100 {
                // one disk may be blocked for a long time,
                // to avoid block shutdown process for a long time, do not join the threads
                // here, only need at least to ensure one thread is exited
                if t1.is_finished() || t2.is_finished() {
                    if t1.is_finished() {
                        t1.join().unwrap();
                    } else {
                        t2.join().unwrap();
                    }
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
    }
}

impl RecoverExt for HedgedFileSystem {
    fn bootstrap(&self) -> IoResult<()> {
        // catch up diff
        if !self.path1.exists() {
            info!("Create raft log directory: {}", self.path1.display());
            fs::create_dir(&self.path1).unwrap();
        }
        if !self.path2.exists() {
            info!("Create raft log directory: {}", self.path2.display());
            fs::create_dir(&self.path2).unwrap();
        }
        let files1 = HedgedFileSystem::get_files(&self.path1)?;
        let files2 = HedgedFileSystem::get_files(&self.path2)?;

        let count1 = self.get_latest_valid_seq(&files1)?;
        let count2 = self.get_latest_valid_seq(&files2)?;

        match count1.cmp(&count2) {
            std::cmp::Ordering::Equal => {
                // still need to catch up, but only diff
                self.catch_up_diff(files1, files2, false)?;
                return Ok(());
            }
            std::cmp::Ordering::Less => {
                self.catch_up_diff(files2, files1, false)?;
            }
            std::cmp::Ordering::Greater => {
                self.catch_up_diff(files1, files2, false)?;
            }
        }
        Ok(())
    }

    fn need_recover(&self) -> bool {
        // in wait recover state, the task should be still dropped
        let res = matches!(
            *self.state.read().unwrap(),
            RecoveryState::WaitRecover1(_) | RecoveryState::WaitRecover2(_)
        );
        if res {
            // in recovering stat, the task can keep sending
            *self.state.write().unwrap() = RecoveryState::Recovering;
        }
        res
    }

    fn is_in_recover(&self) -> bool {
        matches!(*self.state.read().unwrap(), RecoveryState::Recovering)
    }

    fn trigger_recover(&self) {
        // let (cb, f) = paired_future_callback();
        // let to_files = match *self.state.read().unwrap() {
        //     RecoveryState::WaitRecover1(tx) => {
        //         self.sender
        //             .send_snapshot(1, Task::Snapshot(self.path2.clone()), cb);
        //         self.get_files(&self.path1).unwrap() // TODO: handle error
        //     }
        //     RecoveryState::WaitRecover2(tx) => {
        //         self.sender
        //             .send_snapshot(2, Task::Snapshot(self.path1.clone()), cb);
        //         self.get_files(&self.path2).unwrap()
        //     }
        //     _ => unreachable!(),
        // };

        // let from_files = block_on(f).unwrap().map(|res| {
        //     if let TaskRes::Snapshot(files) = res {
        //         files
        //     } else {
        //         unreachable!()
        //     }
        // }).unwrap(); // TODO: handle error

        // TODO: async
        // exclude rewrite files because rewrite files are always synced
        // self.catch_up_diff(from_files, to_files, true);

        // when
        *self.state.write().unwrap() = RecoveryState::Normal;
        // tx.send(()).unwrap();
    }
}

impl FileSystem for HedgedFileSystem {
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
        block_on(self.wait(
            Task::Delete(path.as_ref().to_path_buf()),
            Task::Delete(replace_path(
                path.as_ref(),
                self.path1.as_ref(),
                self.path2.as_ref(),
            )),
        ))
    }

    fn rename<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> IoResult<()> {
        block_on(self.wait(
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

pub struct FutureHandle {
    inner: UnsafeCell<Either<oneshot::Receiver<IoResult<TaskRes>>, Arc<LogFd>>>,
}

unsafe impl Send for FutureHandle {}

// To avoid using `Mutex`
// Safety:
// For write, all writes are serialized to one channel, so only one thread will
// update the inner. For read, multiple readers and one writer and may visit
// try_get() concurrently to get the fd from receiver. The receiver is `Sync`,
// so only one of them will get the fd, and update the inner to Arc<LogFd>.
unsafe impl Sync for FutureHandle {}

impl FutureHandle {
    fn new(rx: oneshot::Receiver<IoResult<TaskRes>>) -> Self {
        Self {
            inner: UnsafeCell::new(Either::Left(rx)),
        }
    }
    fn new_owned(h: LogFd) -> Self {
        Self {
            inner: UnsafeCell::new(Either::Right(Arc::new(h))),
        }
    }

    fn get(&self) -> Arc<LogFd> {
        let mut set = false;
        let fd = match unsafe { &mut *self.inner.get() } {
            Either::Left(rx) => {
                set = true;
                // TODO: should we handle the second disk io error
                match block_on(rx).unwrap().unwrap() {
                    TaskRes::Open { fd, .. } => Arc::new(fd),
                    TaskRes::Create { fd, .. } => Arc::new(fd),
                    _ => unreachable!(),
                }
            }
            Either::Right(w) => w.clone(),
        };
        if set {
            unsafe {
                *self.inner.get() = Either::Right(fd.clone());
            }
        }
        fd
    }

    fn try_get(&self) -> Option<Arc<LogFd>> {
        let mut set = false;
        let fd = match unsafe { &mut *self.inner.get() } {
            Either::Left(rx) => {
                set = true;
                // TODO: should we handle the second disk io error
                match rx.try_recv().unwrap() {
                    None => return None,
                    Some(Err(_)) => panic!(),
                    Some(Ok(TaskRes::Open { fd, .. })) => Arc::new(fd),
                    Some(Ok(TaskRes::Create { fd, .. })) => Arc::new(fd),
                    _ => unreachable!(),
                }
            }
            Either::Right(w) => w.clone(),
        };
        if set {
            unsafe {
                *self.inner.get() = Either::Right(fd.clone());
            }
        }
        Some(fd)
    }
}

pub struct HedgedHandle {
    strong_consistent: bool,

    sender: HedgedSender,

    handle1: Arc<FutureHandle>,
    handle2: Arc<FutureHandle>,

    seqno1: Arc<AtomicU64>,
    seqno2: Arc<AtomicU64>,

    thread1: Option<JoinHandle<()>>,
    thread2: Option<JoinHandle<()>>,
}

impl HedgedHandle {
    fn new(
        strong_consistent: bool,
        mut sender: HedgedSender,
        handle1: FutureHandle,
        handle2: FutureHandle,
        mut seqno1: Arc<AtomicU64>,
        mut seqno2: Arc<AtomicU64>,
    ) -> Self {
        let mut thread1 = None;
        let mut thread2 = None;
        if strong_consistent {
            // use two separated threads for both wait
            let (tx1, rx1) = unbounded::<(SeqTask, Callback<TaskRes>)>();
            let (tx2, rx2) = unbounded::<(SeqTask, Callback<TaskRes>)>();
            // replace the seqno with self owned, then in `read` the seqno from two disks
            // should be always the same. It's just to reuse the logic without
            // adding special check in `read`
            seqno1 = Arc::new(AtomicU64::new(0));
            seqno2 = Arc::new(AtomicU64::new(0));
            let seqno1_clone = seqno1.clone();
            thread1 = Some(thread::spawn(move || {
                for (task, cb) in rx1 {
                    if let Task::Stop = task.inner {
                        break;
                    }
                    let res = task.handle_process();
                    seqno1_clone.fetch_add(1, Ordering::Relaxed);
                    cb(res);
                }
            }));
            let seqno2_clone = seqno2.clone();
            thread2 = Some(thread::spawn(move || {
                for (task, cb) in rx2 {
                    if let Task::Stop = task.inner {
                        break;
                    }
                    let res = task.handle_process();
                    seqno2_clone.fetch_add(1, Ordering::Relaxed);
                    cb(res);
                }
            }));
            sender = HedgedSender::new(tx1, tx2, sender.state());
        }

        Self {
            strong_consistent,
            sender,
            handle1: Arc::new(handle1),
            handle2: Arc::new(handle2),
            seqno1,
            seqno2,
            thread1,
            thread2,
        }
    }

    fn read(&self, offset: usize, buf: &mut [u8]) -> IoResult<usize> {
        // Raft engine promises that the offset would be read only after the write is
        // finished and memtable is updated. And the hedged file system promises that
        // the write is done when either one of the disk finishes the write. Here the
        // read data must be present in at least one of the disks. So choose the disk of
        // largest seqno to read.
        //
        // Safety: the get for these two seqno is not necessary to be atomic.
        // What if the seqno2 is updated after getting seqno1? It's fine, let's say
        // - T1 denotes the time of getting seqno1, the actual seqno for disk1 and disk2
        //   is S1, S2
        // - T2 denotes the time of getting seqno2, the actual seqno for disk1 and disk2
        //   is S1', S2'
        // Assume disk2 is just slightly slower than disk1, here is a possible case:
        // - T1: S1 = 10, S2 = 9
        // - T2: S1'= 12, S2'= 11
        // Then, what we get would be seq1=10, seq2=11, and the read would be performed
        // on disk2. But disk2 is slower than disk1. The data may not be written yet.
        // Would the read on a slower disk is safe?
        // Yes, it's safe because at T1 we know the data can be read at least with a
        // seqno of S1, then at T2, S2' > S1, so the data must be already written in the
        // disk2, even if it's the slow disk.
        let seq1 = self.seqno1.load(Ordering::Relaxed);
        let seq2 = self.seqno2.load(Ordering::Relaxed);
        match seq1.cmp(&seq2) {
            std::cmp::Ordering::Equal => {
                // TODO: read simultaneously from both disks and return the faster one
                if let Some(fd) = self.handle1.try_get() {
                    fd.read(offset, buf)
                } else if let Some(fd) = self.handle2.try_get() {
                    fd.read(offset, buf)
                } else {
                    panic!("Both fd1 and fd2 are None");
                }
            }
            std::cmp::Ordering::Greater => self.handle1.try_get().unwrap().read(offset, buf),
            std::cmp::Ordering::Less => self.handle2.try_get().unwrap().read(offset, buf),
        }
    }

    fn write(&self, offset: usize, content: &[u8]) -> IoResult<usize> {
        block_on(self.wait(
            Task::Write {
                handle: self.handle1.clone(),
                offset,
                bytes: content.to_vec(),
            },
            Task::Write {
                handle: self.handle2.clone(),
                offset,
                bytes: content.to_vec(),
            },
        ))
        .map(|res| {
            if let TaskRes::Write(size) = res {
                size
            } else {
                unreachable!()
            }
        })
    }

    fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
        block_on(self.wait(
            Task::Allocate {
                handle: self.handle1.clone(),
                offset,
                size,
            },
            Task::Allocate {
                handle: self.handle2.clone(),
                offset,
                size,
            },
        ))
        .map(|_| ())
    }

    async fn wait_one(&self, task1: Task, task2: Task) -> IoResult<TaskRes> {
        let (cb1, mut f1) = paired_future_callback();
        let (cb2, mut f2) = paired_future_callback();
        self.sender.send(task1, task2, cb1, cb2);

        select! {
            res1 = f1 => res1.unwrap(),
            res2 = f2 => res2.unwrap(),
        }
    }

    async fn wait_both(&self, task1: Task, task2: Task) -> IoResult<TaskRes> {
        let (cb1, f1) = paired_future_callback();
        let (cb2, f2) = paired_future_callback();
        self.sender.send(task1, task2, cb1, cb2);

        let (res1, res2) = join!(f1, f2);
        match (res1.unwrap(), res2.unwrap()) {
            (res @ Ok(_), Ok(_)) => res,
            (Err(e), Err(_)) => Err(e),
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
        }
    }

    async fn wait(&self, task1: Task, task2: Task) -> IoResult<TaskRes> {
        if self.strong_consistent {
            self.wait_both(task1, task2).await
        } else {
            self.wait_one(task1, task2).await
        }
    }
}

impl Handle for HedgedHandle {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        block_on(self.wait(
            Task::Truncate {
                handle: self.handle1.clone(),
                offset,
            },
            Task::Truncate {
                handle: self.handle2.clone(),
                offset,
            },
        ))
        .map(|_| ())
    }

    fn file_size(&self) -> IoResult<usize> {
        block_on(self.wait(
            Task::FileSize(self.handle1.clone()),
            Task::FileSize(self.handle2.clone()),
        ))
        .map(|res| {
            if let TaskRes::FileSize(size) = res {
                size
            } else {
                unreachable!()
            }
        })
    }

    fn sync(&self) -> IoResult<()> {
        block_on(self.wait(
            Task::Sync(self.handle1.clone()),
            Task::Sync(self.handle2.clone()),
        ))
        .map(|_| ())
    }
}

impl Drop for HedgedHandle {
    fn drop(&mut self) {
        if self.strong_consistent {
            self.sender
                .send(Task::Stop, Task::Stop, Box::new(|_| {}), Box::new(|_| {}));
            self.thread1.take().unwrap().join().unwrap();
            self.thread2.take().unwrap().join().unwrap();
        }
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

pub fn paired_future_callback<T: Send + 'static>() -> (Callback<T>, oneshot::Receiver<IoResult<T>>)
{
    let (tx, future) = oneshot::channel();
    let callback = Box::new(move |result| {
        let r = tx.send(result);
        if r.is_err() {
            warn!("paired_future_callback: Failed to send result to the future rx, discarded.");
        }
    });
    (callback, future)
}
