// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crossbeam::channel::Sender;
use fail::fail_point;
use std::sync::Arc;
use std::sync::Mutex;

use super::task::{empty_callback, Callback, SeqTask, Task};

// let say the average entry size is 100B, then the total size of the log in the
// channel is 1GB,
const PAUSE_THRESHOLD: usize = 10000;

fn get_pause_threshold() -> usize {
    fail_point!("hedged::pause_threshold", |s| s
        .unwrap()
        .parse::<usize>()
        .unwrap());
    PAUSE_THRESHOLD
}

#[derive(Debug, PartialEq, Clone)]
pub enum State {
    Normal,
    Paused1, /* When the length of channel of disk1 reaches threshold, a
              * `Pause` task is sent and no more later task will be sent
              * to disk1 */
    Paused2, // no more task will be sent to disk2
    Recovering,
}

// Make sure the task is sent to two disks' channel atomically, otherwise the
// ordering of the tasks in two disks' channels are not same.
#[derive(Clone)]
pub(crate) struct HedgedSender(Arc<Mutex<HedgedSenderInner>>);

struct HedgedSenderInner {
    disk1: Sender<(SeqTask, Callback)>,
    disk2: Sender<(SeqTask, Callback)>,
    seq: u64,
    state: State,
}

impl HedgedSender {
    pub fn new(disk1: Sender<(SeqTask, Callback)>, disk2: Sender<(SeqTask, Callback)>) -> Self {
        Self(Arc::new(Mutex::new(HedgedSenderInner {
            disk1,
            disk2,
            seq: 0,
            state: State::Normal,
        })))
    }

    pub fn state(&self) -> State {
        self.0.lock().unwrap().state.clone()
    }

    pub fn send(&self, task1: Task, task2: Task, cb1: Callback, cb2: Callback) {
        if matches!(task1, Task::Pause | Task::Snapshot) {
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
        if matches!(inner.state, State::Normal) {
            let check1 = inner.disk1.len() > get_pause_threshold();
            let check2 = inner.disk2.len() > get_pause_threshold();
            match (check1, check2) {
                (true, true) => {
                    panic!("Both channels of disk1 and disk2 are full")
                }
                (true, false) => {
                    inner.state = State::Paused1;
                    inner
                        .disk1
                        .send((
                            SeqTask {
                                inner: Task::Pause,
                                seq: 0,
                            },
                            empty_callback(),
                        ))
                        .unwrap();
                }
                (false, true) => {
                    inner.state = State::Paused2;
                    inner
                        .disk2
                        .send((
                            SeqTask {
                                inner: Task::Pause,
                                seq: 0,
                            },
                            empty_callback(),
                        ))
                        .unwrap();
                }
                _ => {}
            }
        }
        if !matches!(inner.state, State::Paused1) {
            inner.disk1.send((task1, cb1)).unwrap();
        }
        if !matches!(inner.state, State::Paused2) {
            inner.disk2.send((task2, cb2)).unwrap();
        }
    }

    pub fn send_snapshot(&self, cb: Callback) {
        let mut inner = self.0.lock().unwrap();
        inner.seq += 1;
        let task = SeqTask {
            inner: Task::Snapshot,
            seq: inner.seq,
        };
        match inner.state {
            State::Paused1 => {
                inner.disk2.send((task, cb)).unwrap();
            }
            State::Paused2 => {
                inner.disk1.send((task, cb)).unwrap();
            }
            _ => unreachable!(),
        }
        inner.state = State::Recovering;
    }

    pub fn finish_snapshot(&self) {
        let mut inner = self.0.lock().unwrap();
        inner.state = State::Normal;
    }
}
