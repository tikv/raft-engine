// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Synchronizer of writes.
//!
//! This module relies heavily on unsafe codes. Extra call site constraints are
//! required to maintain memory safety. Use it with great caution.

use std::cell::Cell;
use std::marker::PhantomData;
use std::ptr::NonNull;
use std::time::Instant;

use fail::fail_point;
use parking_lot::{Condvar, Mutex};

type Ptr<T> = Option<NonNull<T>>;

///
pub struct Writer<P, O> {
    next: Cell<Ptr<Writer<P, O>>>,
    payload: *mut P,
    output: Option<O>,

    pub(crate) sync: bool,
    pub(crate) start_time: Instant,
}

impl<P, O> Writer<P, O> {
    /// Creates a new writer.
    ///
    /// # Safety
    ///
    /// Data pointed by `payload` is mutably referenced by this writer. Do not
    /// access the payload by its original name during this writer's lifetime.
    pub fn new(payload: &mut P, sync: bool, start_time: Instant) -> Self {
        Writer {
            next: Cell::new(None),
            payload: payload as *mut _,
            output: None,
            sync,
            start_time,
        }
    }

    /// Returns an immutable reference to the payload.
    pub fn get_payload(&self) -> &P {
        unsafe { &*self.payload }
    }

    /// Sets the output. This method is re-entrant.
    pub fn set_output(&mut self, output: O) {
        self.output = Some(output);
    }

    /// Consumes itself and yields an output.
    ///
    /// # Panics
    ///
    /// Panics if called before being processed by a [`WriteBarrier`] or setting
    /// the output itself.
    pub fn finish(mut self) -> O {
        self.output.take().unwrap()
    }

    fn get_next(&self) -> Ptr<Writer<P, O>> {
        self.next.get()
    }

    fn set_next(&self, next: Ptr<Writer<P, O>>) {
        self.next.set(next);
    }
}

/// A collection of writers. User thread (leader) that receives a [`WriteGroup`]
/// is responsible for processing its containing writers.
pub struct WriteGroup<'a, 'b, P: 'a, O: 'a> {
    start: Ptr<Writer<P, O>>,
    back: Ptr<Writer<P, O>>,

    ref_barrier: &'a WriteBarrier<P, O>,
    marker: PhantomData<&'b Writer<P, O>>,
}

impl<'a, 'b, P, O> WriteGroup<'a, 'b, P, O> {
    pub fn iter_mut(&mut self) -> WriterIter<'_, 'a, 'b, P, O> {
        WriterIter {
            start: self.start,
            back: self.back,
            marker: PhantomData,
        }
    }
}

impl<'a, 'b, P, O> Drop for WriteGroup<'a, 'b, P, O> {
    fn drop(&mut self) {
        self.ref_barrier.leader_exit();
    }
}

/// An iterator over the [`Writer`]s in one [`WriteGroup`].
pub struct WriterIter<'a, 'b, 'c, P: 'c, O: 'c> {
    start: Ptr<Writer<P, O>>,
    back: Ptr<Writer<P, O>>,
    marker: PhantomData<&'a WriteGroup<'b, 'c, P, O>>,
}

impl<'a, 'b, 'c, P, O> Iterator for WriterIter<'a, 'b, 'c, P, O> {
    type Item = &'a mut Writer<P, O>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.start.is_none() {
            None
        } else {
            let writer = unsafe { self.start.unwrap().as_mut() };
            if self.start == self.back {
                self.start = None;
            } else {
                self.start = writer.get_next();
            }
            Some(writer)
        }
    }
}

struct WriteBarrierInner<P, O> {
    head: Cell<Ptr<Writer<P, O>>>,
    tail: Cell<Ptr<Writer<P, O>>>,

    pending_leader: Cell<Ptr<Writer<P, O>>>,
    pending_index: Cell<usize>,
}

unsafe impl<P: Send, O: Send> Send for WriteBarrierInner<P, O> {}

impl<P, O> Default for WriteBarrierInner<P, O> {
    fn default() -> Self {
        WriteBarrierInner {
            head: Cell::new(None),
            tail: Cell::new(None),
            pending_leader: Cell::new(None),
            pending_index: Cell::new(0),
        }
    }
}

/// A synchronizer of [`Writer`]s.
pub struct WriteBarrier<P, O> {
    inner: Mutex<WriteBarrierInner<P, O>>,
    leader_cv: Condvar,
    follower_cvs: [Condvar; 2],
}

impl<P, O> Default for WriteBarrier<P, O> {
    fn default() -> Self {
        WriteBarrier {
            leader_cv: Condvar::new(),
            follower_cvs: [Condvar::new(), Condvar::new()],
            inner: Mutex::new(WriteBarrierInner::default()),
        }
    }
}

impl<P, O> WriteBarrier<P, O> {
    /// Waits until the caller should perform some work. If `writer` has become
    /// the leader of a set of writers, returns a [`WriteGroup`] that contains
    /// them, `writer` included.
    pub fn enter<'a>(&self, writer: &'a mut Writer<P, O>) -> Option<WriteGroup<'_, 'a, P, O>> {
        let node = unsafe { Some(NonNull::new_unchecked(writer)) };
        let mut inner = self.inner.lock();
        if let Some(tail) = inner.tail.get() {
            unsafe {
                tail.as_ref().set_next(node);
            }
            inner.tail.set(node);

            if inner.pending_leader.get().is_some() {
                // follower of next write group.
                self.follower_cvs[inner.pending_index.get() % 2].wait(&mut inner);
                return None;
            } else {
                // leader of next write group.
                inner.pending_leader.set(node);
                inner
                    .pending_index
                    .set(inner.pending_index.get().wrapping_add(1));
                //
                self.leader_cv.wait(&mut inner);
                inner.pending_leader.set(None);
            }
        } else {
            // leader of a empty write group. proceed directly.
            debug_assert!(inner.pending_leader.get().is_none());
            inner.head.set(node);
            inner.tail.set(node);
        }

        Some(WriteGroup {
            start: node,
            back: inner.tail.get(),
            ref_barrier: self,
            marker: PhantomData,
        })
    }

    /// Must called when write group leader finishes processing its responsible
    /// writers, and next write group should be formed.
    fn leader_exit(&self) {
        fail_point!("write_barrier::leader_exit", |_| {});
        let inner = self.inner.lock();
        if let Some(leader) = inner.pending_leader.get() {
            // wake up leader of next write group.
            self.leader_cv.notify_one();
            // wake up follower of current write group.
            self.follower_cvs[inner.pending_index.get().wrapping_sub(1) % 2].notify_all();
            inner.head.set(Some(leader));
        } else {
            // wake up follower of current write group.
            self.follower_cvs[inner.pending_index.get() % 2].notify_all();
            inner.head.set(None);
            inner.tail.set(None);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::thread::{self, Builder as ThreadBuilder};
    use std::time::Duration;

    #[test]
    fn test_sequential_groups() {
        let barrier: WriteBarrier<(), u32> = Default::default();
        let mut payload = ();
        let mut leaders = 0;
        let mut processed_writers = 0;

        for _ in 0..4 {
            let mut writer = Writer::new(&mut payload, false, Instant::now());
            {
                let mut wg = barrier.enter(&mut writer).unwrap();
                leaders += 1;
                for writer in wg.iter_mut() {
                    writer.set_output(7);
                    processed_writers += 1;
                }
            }
            assert_eq!(writer.finish(), 7);
        }

        assert_eq!(processed_writers, 4);
        assert_eq!(leaders, 4);
    }

    struct ConcurrentWriteContext {
        barrier: Arc<WriteBarrier<u32, u32>>,

        writer_seq: u32,
        ths: Vec<thread::JoinHandle<()>>,
        tx: mpsc::SyncSender<()>,
        rx: mpsc::Receiver<()>,
    }

    impl ConcurrentWriteContext {
        fn new() -> Self {
            let (tx, rx) = mpsc::sync_channel(0);
            Self {
                barrier: Default::default(),
                writer_seq: 0,
                ths: Vec::new(),
                tx,
                rx,
            }
        }

        // 1) create `n` writers and form a new write group
        // 2) current active write group finishes writing and exits
        // 3) the new write group enters writing phrase
        fn step(&mut self, n: usize) {
            let (ready_tx, ready_rx) = mpsc::channel();
            if self.ths.is_empty() {
                // ensure there is one active write group.
                self.writer_seq += 1;
                let barrier_clone = self.barrier.clone();
                let tx_clone = self.tx.clone();
                let ready_tx_clone = ready_tx.clone();
                let mut seq = self.writer_seq;
                self.ths.push(
                    ThreadBuilder::new()
                        .spawn(move || {
                            let mut writer = Writer::new(&mut seq, false, Instant::now());
                            ready_tx_clone.send(()).unwrap();
                            {
                                let mut wg = barrier_clone.enter(&mut writer).unwrap();
                                let mut idx = 0;
                                for w in wg.iter_mut() {
                                    w.set_output(*w.get_payload());
                                    idx += 1;
                                }
                                assert_eq!(idx, 1);
                                tx_clone.send(()).unwrap();
                            }
                            assert_eq!(writer.finish(), seq);
                        })
                        .unwrap(),
                );
                ready_rx.recv().unwrap();
            }
            let prev_writers = self.ths.len();
            for _ in 0..n {
                self.writer_seq += 1;
                let barrier_clone = self.barrier.clone();
                let tx_clone = self.tx.clone();
                let ready_tx_clone = ready_tx.clone();
                let mut seq = self.writer_seq;
                self.ths.push(
                    ThreadBuilder::new()
                        .spawn(move || {
                            let mut writer = Writer::new(&mut seq, false, Instant::now());
                            ready_tx_clone.send(()).unwrap();
                            if let Some(mut wg) = barrier_clone.enter(&mut writer) {
                                let mut idx = 0;
                                for w in wg.iter_mut() {
                                    w.set_output(*w.get_payload());
                                    idx += 1;
                                }
                                assert_eq!(idx, n as u32);
                                tx_clone.send(()).unwrap();
                            }
                            assert_eq!(writer.finish(), seq);
                        })
                        .unwrap(),
                );
            }
            for _ in 0..n {
                ready_rx.recv().unwrap();
            }
            std::thread::sleep(Duration::from_millis(5));
            // unblock current leader
            self.rx.recv().unwrap();
            for th in self.ths.drain(0..prev_writers) {
                th.join().unwrap();
            }
        }

        fn join(&mut self) {
            self.rx.recv().unwrap();
            for th in self.ths.drain(..) {
                th.join().unwrap();
            }
        }
    }

    #[test]
    fn test_parallel_groups() {
        let mut ctx = ConcurrentWriteContext::new();
        for i in 1..5 {
            ctx.step(i);
        }
        ctx.join();
    }
}
