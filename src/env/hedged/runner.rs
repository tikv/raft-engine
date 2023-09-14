// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crossbeam::channel::Receiver;
use fail::fail_point;
use std::io::Result as IoResult;
use std::path::PathBuf;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::thread;
use std::thread::JoinHandle;

use crate::env::DefaultFileSystem;
use futures::executor::block_on;

use super::recover;
use super::sender::HedgedSender;
use super::task::paired_future_callback;
use super::task::{Callback, SeqTask, Task, TaskRes};

pub(crate) struct TaskRunner {
    id: u8,
    path: PathBuf,
    fs: Arc<DefaultFileSystem>,
    rx: Receiver<(SeqTask, Callback)>,
    sender: HedgedSender,
    seqno: Arc<AtomicU64>,
}

impl TaskRunner {
    pub fn new(
        id: u8,
        path: PathBuf,
        fs: Arc<DefaultFileSystem>,
        rx: Receiver<(SeqTask, Callback)>,
        sender: HedgedSender,
        seqno: Arc<AtomicU64>,
    ) -> Self {
        Self {
            id,
            path,
            fs,
            rx,
            sender,
            seqno,
        }
    }

    pub fn spawn(self) -> JoinHandle<()> {
        let id = self.id;
        thread::Builder::new()
            .name(format!("raft-engine-disk{}", id))
            .spawn(move || {
                if let Err(e) = self.poll() {
                    panic!("disk {} failed: {:?}", id, e);
                }
            })
            .unwrap()
    }

    fn poll(self) -> IoResult<()> {
        let mut last_seq = 0;
        let mut snap_seq = None;
        for (task, cb) in self.rx {
            if let Task::Stop = task.inner {
                cb(Ok(TaskRes::Stop))?;
                break;
            }
            if let Task::Pause = task.inner {
                // Encountering `Pause`, indicate the disk may not slow anymore
                let (cb, f) = paired_future_callback();
                self.sender.send_snapshot(cb);
                let to_files = recover::get_files(&self.path)?;
                let from_files = block_on(f).unwrap().map(|res| {
                    if let TaskRes::Snapshot((seq, files)) = res {
                        snap_seq = Some(seq);
                        files
                    } else {
                        unreachable!()
                    }
                })?;

                // Snapshot doesn't include the file size, so it would copy more data than
                // the data seen at the time of snapshot. But it's okay, as the data is
                // written with specific offset, so the data written
                // of no necessity will be overwritten by the latter writes.
                // Exclude rewrite files because rewrite files are always synced.
                recover::catch_up_diff(&self.fs, from_files, to_files, true)?;

                self.sender.finish_snapshot();
                self.seqno.store(snap_seq.unwrap(), Ordering::Relaxed);
                last_seq = snap_seq.unwrap();
                continue;
            }
            if self.id == 1 {
                fail_point!("hedged::task_runner::thread1");
            }
            let seq = task.seq;
            assert_ne!(seq, 0);
            if let Some(snap) = snap_seq.as_ref() {
                // the change already included in the snapshot
                if seq < *snap {
                    continue;
                } else if seq == *snap {
                    unreachable!();
                } else if seq == *snap + 1 {
                    snap_seq = None;
                } else {
                    panic!("seqno {} is larger than snapshot seqno {}", seq, *snap);
                }
            }

            assert_eq!(last_seq + 1, seq);
            last_seq = seq;
            let res = task.process(&self.fs, &self.path);
            // seqno should be updated before the write callback is called, otherwise one
            // read may be performed right after the write is finished. Then the read may be
            // performed on the other disk not having the data because the seqno for this
            // disk is not updated yet.
            self.seqno.store(seq, Ordering::Relaxed);
            cb(res)?;
        }
        Ok(())
    }
}
