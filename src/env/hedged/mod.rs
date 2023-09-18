// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crossbeam::channel::unbounded;
use crossbeam::channel::Receiver;
use log::info;
use std::fs;
use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::env::default::LogFd;
use crate::env::DefaultFileSystem;
use crate::env::{FileSystem, Handle, Permission, WriteExt};
use futures::executor::block_on;
use futures::{join, select};

mod recover;
mod runner;
mod sender;
mod task;
mod util;

use runner::TaskRunner;
use sender::HedgedSender;
use task::{
    empty_callback, paired_future_callback, Callback, FutureHandle, SeqTask, Task, TaskRes,
};
use util::replace_path;

pub use sender::State;

// TODO: add metrics
// TODO: handle specially on config change(upgrade and downgrade)

// In cloud environment, cloud disk IO may get stuck for a while due to cloud
// vendor infrastructure issues. This may affect the foreground latency
// dramatically. Raft log apply doesn't sync mostly, so it wouldn't be a
// problem. While raft log append is synced every time. To alleviate that, we
// can hedge raft log to two different cloud disks. If either one of them is
// synced, the raft log append is considered finished. Thus when one of the
// cloud disk IO is stuck, the other one can still work and doesn't affect
// foreground write flow.

//Under the hood, the HedgedFileSystem manages two directories on different
// cloud disks. All operations of the interface are serialized by one channel
// for each disk and wait until either one of the channels is consumed. With
// that, if one of the disk's io is slow for a long time, the other can still
// serve the operations without any delay. And once the disk comes back to
// normal, it can catch up with the accumulated operations record in the
// channel. Then the states of the two disks can be synced again.

// It relays on some raft-engine assumptions:
// 1. Raft log is append only.
// 2. Raft log is read-only once it's sealed.

// For raft engine write thread model, only one thread writes WAL at one point.
// So not supporting writing WAL concurrently is not a big deal. But for the
// rewrite(GC), it is concurrent to WAL write. Making GC write operations
// serialized with WAL write may affect the performance pretty much. To avoid
// that, we can treat rewrite files especially that make rewrite operations wait
// both disks because rewrite is a background job that doesn’t affect foreground
// latency. As the rewrite files are the partial order

pub struct HedgedFileSystem {
    base: Arc<DefaultFileSystem>,

    path1: PathBuf,
    path2: PathBuf,

    sender: HedgedSender,

    seqno1: Arc<AtomicU64>,
    seqno2: Arc<AtomicU64>,

    thread1: Option<JoinHandle<()>>,
    thread2: Option<JoinHandle<()>>,
}

// TODO: consider encryption
impl HedgedFileSystem {
    pub fn new(base: Arc<DefaultFileSystem>, path1: PathBuf, path2: PathBuf) -> Self {
        let (tx1, rx1) = unbounded::<(SeqTask, Callback)>();
        let (tx2, rx2) = unbounded::<(SeqTask, Callback)>();
        let sender = HedgedSender::new(tx1, tx2);

        let seqno1 = Arc::new(AtomicU64::new(0));
        let seqno2 = Arc::new(AtomicU64::new(0));
        let runner1 = TaskRunner::new(
            1,
            path1.clone(),
            base.clone(),
            rx1,
            sender.clone(),
            seqno1.clone(),
        );
        let runner2 = TaskRunner::new(
            2,
            path2.clone(),
            base.clone(),
            rx2,
            sender.clone(),
            seqno2.clone(),
        );
        let thread1 = runner1.spawn();
        let thread2 = runner2.spawn();
        Self {
            base,
            path1,
            path2,
            sender,
            seqno1,
            seqno2,
            thread1: Some(thread1),
            thread2: Some(thread2),
        }
    }

    pub fn state(&self) -> State {
        self.sender.state()
    }

    async fn wait_handle(&self, task1: Task, task2: Task) -> IoResult<HedgedHandle> {
        let (cb1, mut f1) = paired_future_callback();
        let (cb2, mut f2) = paired_future_callback();
        self.sender.send(task1.clone(), task2.clone(), cb1, cb2);

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
                    self.base.clone(),
                    is_for_rewrite,
                    self.sender.clone(),
                    FutureHandle::new_owned(fd),
                    FutureHandle::new(f2, task2),
                    self.seqno1.clone(),
                    self.seqno2.clone(),
                )
            }),
            res2 = f2 => res2.unwrap().map(|res| {
                let (fd, is_for_rewrite) = resolve(res);
                HedgedHandle::new(
                    self.base.clone(),
                    is_for_rewrite,
                    self.sender.clone(),
                    FutureHandle::new(f1, task1),
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
            if t1.is_finished() && t2.is_finished() {
                t1.join().unwrap();
                t2.join().unwrap();
                break;
            }
            times += 1;
            if times > 100 {
                // wait 1s
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

impl FileSystem for HedgedFileSystem {
    type Handle = HedgedHandle;
    type Reader = HedgedReader;
    type Writer = HedgedWriter;

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
        let files1 = recover::get_files(&self.path1)?;
        let files2 = recover::get_files(&self.path2)?;

        let count1 = recover::get_latest_valid_seq(&self.base, &files1)?;
        let count2 = recover::get_latest_valid_seq(&self.base, &files2)?;

        match count1.cmp(&count2) {
            std::cmp::Ordering::Equal => {
                // still need to catch up, but only diff
                recover::catch_up_diff(&self.base, files1, files2, false)?;
                return Ok(());
            }
            std::cmp::Ordering::Less => {
                recover::catch_up_diff(&self.base, files2, files1, false)?;
            }
            std::cmp::Ordering::Greater => {
                recover::catch_up_diff(&self.base, files1, files2, false)?;
            }
        }
        Ok(())
    }

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

pub struct HedgedHandle {
    base: Arc<DefaultFileSystem>,

    // for rewrite file, all the operations should wait both disks finished
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
        base: Arc<DefaultFileSystem>,
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
            let (tx1, rx1) = unbounded::<(SeqTask, Callback)>();
            let (tx2, rx2) = unbounded::<(SeqTask, Callback)>();
            // replace the seqno with self owned, then in `read` the seqno from two disks
            // should be always the same. It's just to reuse the logic without
            // adding special check in `read`
            seqno1 = Arc::new(AtomicU64::new(0));
            seqno2 = Arc::new(AtomicU64::new(0));
            let poll = |(rx, seqno, fs): (
                Receiver<(SeqTask, Callback)>,
                Arc<AtomicU64>,
                Arc<DefaultFileSystem>,
            )| {
                for (task, cb) in rx {
                    if let Task::Stop = task.inner {
                        break;
                    }
                    assert!(!matches!(task.inner, Task::Pause | Task::Snapshot));
                    let res = task.handle_process(&fs);
                    seqno.fetch_add(1, Ordering::Relaxed);
                    cb(res).unwrap();
                }
            };
            let args1 = (rx1, seqno1.clone(), base.clone());
            thread1 = Some(thread::spawn(move || {
                poll(args1);
            }));
            let args2 = (rx2, seqno2.clone(), base.clone());
            thread2 = Some(thread::spawn(move || {
                poll(args2);
            }));
            sender = HedgedSender::new(tx1, tx2);
        }

        Self {
            base,
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
                if let Some(fd) = self.handle1.try_get(&self.base)? {
                    fd.read(offset, buf)
                } else if let Some(fd) = self.handle2.try_get(&self.base)? {
                    fd.read(offset, buf)
                } else {
                    panic!("Both fd1 and fd2 are None");
                }
            }
            std::cmp::Ordering::Greater => {
                self.handle1.try_get(&self.base)?.unwrap().read(offset, buf)
            }
            std::cmp::Ordering::Less => {
                self.handle2.try_get(&self.base)?.unwrap().read(offset, buf)
            }
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
                .send(Task::Stop, Task::Stop, empty_callback(), empty_callback());
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
