// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::cell::UnsafeCell;
use std::io::Result as IoResult;
use std::path::PathBuf;
use std::sync::Arc;

use crate::env::default::LogFd;
use crate::env::DefaultFileSystem;
use crate::env::{FileSystem, Handle, Permission};
use futures::channel::oneshot::{self, Canceled};
use futures::executor::block_on;

use super::recover::{self, Files};

use either::Either;

pub(crate) type Callback = Box<dyn FnOnce(IoResult<TaskRes>) -> IoResult<TaskRes> + Send>;

pub(crate) fn empty_callback() -> Callback {
    Box::new(|_| Ok(TaskRes::Noop))
}

pub(crate) fn paired_future_callback() -> (Callback, oneshot::Receiver<IoResult<TaskRes>>) {
    let (tx, future) = oneshot::channel();
    let callback = Box::new(move |result| -> IoResult<TaskRes> {
        if let Err(result) = tx.send(result) {
            return result;
        }
        Ok(TaskRes::Noop)
    });
    (callback, future)
}

pub(crate) struct SeqTask {
    pub inner: Task,
    pub seq: u64,
}

#[derive(Clone)]
pub(crate) enum Task {
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
    Snapshot,
    Stop,
}

impl SeqTask {
    pub fn process(self, file_system: &DefaultFileSystem, path: &PathBuf) -> IoResult<TaskRes> {
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
            Task::Snapshot => {
                let mut files = recover::get_files(&path)?;
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
                Ok(TaskRes::Snapshot((self.seq, files)))
            }
            Task::Stop | Task::Pause => unreachable!(),
            _ => self.handle_process(file_system),
        }
    }

    pub fn handle_process(self, file_system: &DefaultFileSystem) -> IoResult<TaskRes> {
        match self.inner {
            Task::Truncate { handle, offset } => handle
                .get(file_system)?
                .truncate(offset)
                .map(|_| TaskRes::Truncate),
            Task::FileSize(handle) => handle
                .get(file_system)?
                .file_size()
                .map(|s| TaskRes::FileSize(s)),
            Task::Sync(handle) => handle.get(file_system)?.sync().map(|_| TaskRes::Sync),
            Task::Write {
                handle,
                offset,
                bytes,
            } => handle
                .get(file_system)?
                .write(offset, &bytes)
                .map(|s| TaskRes::Write(s)),
            Task::Allocate {
                handle,
                offset,
                size,
            } => handle
                .get(file_system)?
                .allocate(offset, size)
                .map(|_| TaskRes::Allocate),
            _ => unreachable!(),
        }
    }
}

pub(crate) enum TaskRes {
    Noop,
    Create { fd: LogFd, is_for_rewrite: bool },
    Open { fd: LogFd, is_for_rewrite: bool },
    Delete,
    Rename,
    Truncate,
    FileSize(usize),
    Sync,
    Write(usize),
    Allocate,
    Snapshot((u64, Files)),
    Stop,
}

pub(crate) struct FutureHandle {
    inner: UnsafeCell<Either<oneshot::Receiver<IoResult<TaskRes>>, Arc<LogFd>>>,
    task: Option<Task>,
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
    pub fn new(rx: oneshot::Receiver<IoResult<TaskRes>>, task: Task) -> Self {
        Self {
            inner: UnsafeCell::new(Either::Left(rx)),
            task: Some(task),
        }
    }
    pub fn new_owned(h: LogFd) -> Self {
        Self {
            inner: UnsafeCell::new(Either::Right(Arc::new(h))),
            task: None,
        }
    }

    pub fn get(&self, file_system: &DefaultFileSystem) -> IoResult<Arc<LogFd>> {
        let mut set = false;
        let fd = match unsafe { &mut *self.inner.get() } {
            Either::Left(rx) => {
                set = true;
                match block_on(rx) {
                    Err(Canceled) => self.retry_canceled(file_system)?,
                    Ok(res) => match res? {
                        TaskRes::Open { fd, .. } => Arc::new(fd),
                        TaskRes::Create { fd, .. } => Arc::new(fd),
                        _ => unreachable!(),
                    },
                }
            }
            Either::Right(w) => w.clone(),
        };
        if set {
            unsafe {
                *self.inner.get() = Either::Right(fd.clone());
            }
        }
        Ok(fd)
    }

    pub fn try_get(&self, file_system: &DefaultFileSystem) -> IoResult<Option<Arc<LogFd>>> {
        let mut set = false;
        let fd = match unsafe { &mut *self.inner.get() } {
            Either::Left(rx) => {
                set = true;
                match rx.try_recv() {
                    Err(Canceled) => self.retry_canceled(file_system)?,
                    Ok(None) => return Ok(None),
                    Ok(Some(res)) => match res? {
                        TaskRes::Open { fd, .. } => Arc::new(fd),
                        TaskRes::Create { fd, .. } => Arc::new(fd),
                        _ => unreachable!(),
                    },
                }
            }
            Either::Right(w) => w.clone(),
        };
        if set {
            unsafe {
                *self.inner.get() = Either::Right(fd.clone());
            }
        }
        Ok(Some(fd))
    }

    fn retry_canceled(&self, file_system: &DefaultFileSystem) -> IoResult<Arc<LogFd>> {
        // Canceled is caused by the task is dropped when in paused state,
        // so we should retry the task now
        Ok(match self.task.as_ref().unwrap() {
            Task::Create(path) => {
                // has been already created, so just open
                let fd = file_system.open(path, Permission::ReadWrite)?;
                Arc::new(fd)
            }
            Task::Open { path, perm } => {
                let fd = file_system.open(path, *perm)?;
                Arc::new(fd)
            }
            _ => unreachable!(),
        })
    }
}
