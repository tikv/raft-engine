// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

pub use std::collections::hash_map::Entry as HashMapEntry;
use std::collections::{HashMap as StdHashMap, VecDeque};
use std::fmt::{self, Write};
use std::hash::BuildHasherDefault;
use std::ops::{Div, Mul};
use std::str::FromStr;
use std::sync::Arc;
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::{Builder as ThreadBuilder, JoinHandle};
use std::time::Duration;

use crossbeam::channel::{bounded, unbounded, Receiver, RecvTimeoutError, Sender};
use log::warn;
use serde::de::{self, Unexpected, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

pub use crossbeam::channel::SendError as ScheduleError;
pub type HashMap<K, V> = StdHashMap<K, V, BuildHasherDefault<fxhash::FxHasher>>;

const UNIT: u64 = 1;
const DATA_MAGNITUDE: u64 = 1024;
pub const KB: u64 = UNIT * DATA_MAGNITUDE;
pub const MB: u64 = KB * DATA_MAGNITUDE;
pub const GB: u64 = MB * DATA_MAGNITUDE;
pub const TB: u64 = (GB as u64) * (DATA_MAGNITUDE as u64);
pub const PB: u64 = (TB as u64) * (DATA_MAGNITUDE as u64);

#[derive(Clone, Debug, Copy, PartialEq)]
pub struct ReadableSize(pub u64);

impl ReadableSize {
    pub const fn kb(count: u64) -> ReadableSize {
        ReadableSize(count * KB)
    }

    pub const fn mb(count: u64) -> ReadableSize {
        ReadableSize(count * MB)
    }

    pub const fn gb(count: u64) -> ReadableSize {
        ReadableSize(count * GB)
    }

    pub const fn as_mb(self) -> u64 {
        self.0 / MB
    }
}

impl Div<u64> for ReadableSize {
    type Output = ReadableSize;

    fn div(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 / rhs)
    }
}

impl Div<ReadableSize> for ReadableSize {
    type Output = u64;

    fn div(self, rhs: ReadableSize) -> u64 {
        self.0 / rhs.0
    }
}

impl Mul<u64> for ReadableSize {
    type Output = ReadableSize;

    fn mul(self, rhs: u64) -> ReadableSize {
        ReadableSize(self.0 * rhs)
    }
}

impl Serialize for ReadableSize {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let size = self.0;
        let mut buffer = String::new();
        if size == 0 {
            write!(buffer, "{}KiB", size).unwrap();
        } else if size % PB == 0 {
            write!(buffer, "{}PiB", size / PB).unwrap();
        } else if size % TB == 0 {
            write!(buffer, "{}TiB", size / TB).unwrap();
        } else if size % GB as u64 == 0 {
            write!(buffer, "{}GiB", size / GB).unwrap();
        } else if size % MB as u64 == 0 {
            write!(buffer, "{}MiB", size / MB).unwrap();
        } else if size % KB as u64 == 0 {
            write!(buffer, "{}KiB", size / KB).unwrap();
        } else {
            return serializer.serialize_u64(size);
        }
        serializer.serialize_str(&buffer)
    }
}

impl FromStr for ReadableSize {
    type Err = String;

    fn from_str(s: &str) -> Result<ReadableSize, String> {
        let size_str = s.trim();
        if size_str.is_empty() {
            return Err(format!("{:?} is not a valid size.", s));
        }

        if !size_str.is_ascii() {
            return Err(format!("ASCII string is expected, but got {:?}", s));
        }

        // size: digits and '.' as decimal separator
        let size_len = size_str
            .to_string()
            .chars()
            .take_while(|c| char::is_ascii_digit(c) || *c == '.')
            .count();

        // unit: alphabetic characters
        let (size, unit) = size_str.split_at(size_len);

        let unit = match unit.trim() {
            "K" | "KB" | "KiB" => KB,
            "M" | "MB" | "MiB" => MB,
            "G" | "GB" | "GiB" => GB,
            "T" | "TB" | "TiB" => TB,
            "P" | "PB" | "PiB" => PB,
            "B" | "" => UNIT,
            _ => {
                return Err(format!(
                    "only B, KB, KiB, MB, MiB, GB, GiB, TB, TiB, PB, and PiB are supported: {:?}",
                    s
                ));
            }
        };

        match size.parse::<f64>() {
            Ok(n) => Ok(ReadableSize((n * unit as f64) as u64)),
            Err(_) => Err(format!("invalid size string: {:?}", s)),
        }
    }
}

impl<'de> Deserialize<'de> for ReadableSize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct SizeVisitor;

        impl<'de> Visitor<'de> for SizeVisitor {
            type Value = ReadableSize;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("valid size")
            }

            fn visit_i64<E>(self, size: i64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                if size >= 0 {
                    self.visit_u64(size as u64)
                } else {
                    Err(E::invalid_value(Unexpected::Signed(size), &self))
                }
            }

            fn visit_u64<E>(self, size: u64) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                Ok(ReadableSize(size))
            }

            fn visit_str<E>(self, size_str: &str) -> Result<ReadableSize, E>
            where
                E: de::Error,
            {
                size_str.parse().map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

/// Take slices in the range.
///
/// ### Panics
///
/// if [low, high) is out of bound.
pub fn slices_in_range<T>(entry: &VecDeque<T>, low: usize, high: usize) -> (&[T], &[T]) {
    let (first, second) = entry.as_slices();
    if low >= first.len() {
        (&second[low - first.len()..high - first.len()], &[])
    } else if high <= first.len() {
        (&first[low..high], &[])
    } else {
        (&first[low..], &second[..high - first.len()])
    }
}

pub trait HandyRwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<'_, T>;
    fn rl(&self) -> RwLockReadGuard<'_, T>;
}

impl<T> HandyRwLock<T> for RwLock<T> {
    fn wl(&self) -> RwLockWriteGuard<'_, T> {
        self.write().unwrap()
    }
    fn rl(&self) -> RwLockReadGuard<'_, T> {
        self.read().unwrap()
    }
}

pub trait Runnable<T> {
    fn run(&mut self, task: T) -> bool;
    fn on_tick(&mut self);
    fn shutdown(&mut self) {}
}

#[derive(Clone)]
pub struct Scheduler<T> {
    name: Arc<String>,
    sender: Sender<Option<T>>,
}

impl<T> Scheduler<T> {
    pub fn schedule(&self, task: T) -> Result<(), ScheduleError<T>> {
        if let Err(ScheduleError(e)) = self.sender.send(Some(task)) {
            return Err(ScheduleError(e.unwrap()));
        }
        Ok(())
    }
}

pub struct Worker<T: Clone> {
    scheduler: Scheduler<T>,
    receiver: Option<Receiver<Option<T>>>,
    handle: Option<JoinHandle<()>>,
}

// `scheduler` is `!Sync`, but we didn't uses the field.
// unsafe impl<T> Sync for Worker<T> {}

impl<T: Clone> Worker<T> {
    pub fn new(name: String, capacity: Option<usize>) -> Worker<T> {
        let (tx, rx) = match capacity {
            Some(capacity) => bounded(capacity),
            None => unbounded(),
        };
        let scheduler = Scheduler {
            name: Arc::new(name),
            sender: tx,
        };
        Worker {
            scheduler,
            receiver: Some(rx),
            handle: None,
        }
    }
    pub fn scheduler(&self) -> Scheduler<T> {
        self.scheduler.clone()
    }

    #[cfg(test)]
    pub fn take_receiver(&mut self) -> Receiver<Option<T>> {
        self.receiver.take().unwrap()
    }

    pub fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            let _ = self.scheduler.sender.send(None);
            if let Err(e) = handle.join() {
                warn!("Cache evictor aborts with {:?}", e);
            }
        }
    }
}

impl<T: Clone + Send + 'static> Worker<T> {
    pub fn start<R>(&mut self, runner: R, tick: Option<Duration>) -> bool
    where
        R: Runnable<T> + Send + 'static,
    {
        let tick = tick.unwrap_or_else(|| Duration::from_secs(u64::MAX));
        let receiver = match self.receiver.take() {
            Some(rx) => rx,
            None => return false,
        };
        let name = self.scheduler.name.as_ref().clone();
        let th = ThreadBuilder::new()
            .name(name)
            .spawn(move || poll(runner, receiver, tick))
            .unwrap();
        self.handle = Some(th);
        true
    }
}

fn poll<T, R: Runnable<T>>(mut runner: R, receiver: Receiver<Option<T>>, tick: Duration) {
    loop {
        match receiver.recv_timeout(tick) {
            Ok(None) | Err(RecvTimeoutError::Disconnected) => return,
            Ok(Some(task)) => {
                if runner.run(task) {
                    runner.on_tick();
                }
            }
            Err(RecvTimeoutError::Timeout) => runner.on_tick(),
        }
    }
}

impl<T: Clone> Drop for Worker<T> {
    fn drop(&mut self) {
        self.stop();
    }
}

#[derive(Clone, Debug, Copy, PartialEq, Default)]
pub struct Statistic {
    pub wal_cost: usize,
    pub sync_cost: usize,
    pub avg_write_cost: usize,
    pub max_wal_cost: usize,
    pub max_sync_cost: usize,
    pub max_write_cost: usize,
    pub freq: usize,
}

fn max(left: usize, right: usize) -> usize {
    if left > right {
        left
    } else {
        right
    }
}

impl Statistic {
    pub fn add(&mut self, other: &Self) {
        self.wal_cost += other.wal_cost;
        self.sync_cost += other.sync_cost;
        self.freq += other.freq;
        self.max_wal_cost = max(self.max_wal_cost, other.max_wal_cost);
        self.max_write_cost = max(self.max_write_cost, other.max_write_cost);
        self.max_sync_cost = max(self.max_sync_cost, other.max_sync_cost);
    }

    pub fn clear(&mut self) {
        self.wal_cost = 0;
        self.sync_cost = 0;
        self.avg_write_cost = 0;
        self.max_wal_cost = 0;
        self.max_sync_cost = 0;
        self.max_write_cost = 0;
        self.freq = 0;
    }

    #[inline]
    pub fn add_wal(&mut self, wal: usize) {
        self.wal_cost += wal;
        self.max_wal_cost = max(self.max_wal_cost, wal);
    }

    #[inline]
    pub fn add_sync(&mut self, sync: usize) {
        self.sync_cost += sync;
        self.max_sync_cost = max(self.max_sync_cost, sync);
    }

    #[inline]
    pub fn add_one(&mut self) {
        self.freq += 1;
    }
}
