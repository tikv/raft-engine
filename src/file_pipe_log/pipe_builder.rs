// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

//! Helper types to recover in-memory states from log files.

use std::cmp;
use std::fs::{self, File as StdFile};
use std::io::Write;
use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use fs2::FileExt;
use log::{error, info, warn};
use rayon::prelude::*;

use crate::config::{Config, RecoveryMode};
use crate::env::FileSystem;
use crate::env::Handle;
use crate::event_listener::EventListener;
use crate::log_batch::LogItemBatch;
use crate::pipe_log::{FileId, LogQueue};
use crate::util::{Factory, ReadableSize};
use crate::{Error, Result};

use super::format::{
    build_recycled_file_name, lock_file_path, parse_recycled_file_name, FileNameExt, LogFileFormat,
};
use super::log_file::build_file_reader;
use super::pipe::{fetch_dir, DualPipes, File, Paths, SinglePipe, DEFAULT_FIRST_FILE_SEQ};
use super::reader::LogItemBatchFileReader;

const PREFILL_BUFFER_SIZE: usize = ReadableSize::mb(16).0 as usize;
const MAX_PREFILL_SIZE: usize = ReadableSize::gb(12).0 as usize;

/// `ReplayMachine` is a type of deterministic state machine that obeys
/// associative law.
///
/// Sequentially arranged log items can be divided and replayed to several
/// [`ReplayMachine`]s, and their merged state will be the same as when
/// replayed to one single [`ReplayMachine`].
///
/// This abstraction is useful for recovery in parallel: a set of log files can
/// be replayed in a divide-and-conquer fashion.
pub trait ReplayMachine: Send {
    /// Inputs a batch of log items from the given file to this machine.
    /// Returns whether the input sequence up till now is accepted.
    fn replay(&mut self, item_batch: LogItemBatch, file_id: FileId) -> Result<()>;

    /// Merges with another [`ReplayMachine`] that has consumed newer log items
    /// in the same input sequence.
    fn merge(&mut self, rhs: Self, queue: LogQueue) -> Result<()>;
}

/// A factory of [`ReplayMachine`]s that can be default constructed.
#[derive(Clone, Default)]
pub struct DefaultMachineFactory<M>(PhantomData<std::sync::Mutex<M>>);

impl<M: ReplayMachine + Default> Factory<M> for DefaultMachineFactory<M> {
    fn new_target(&self) -> M {
        M::default()
    }
}

/// Container for basic settings on recovery.
pub struct RecoveryConfig {
    pub queue: LogQueue,
    pub mode: RecoveryMode,
    pub concurrency: usize,
    pub read_block_size: u64,
}

/// [`DualPipes`] factory that can also recover other customized memory states.
pub struct DualPipesBuilder<F: FileSystem> {
    cfg: Config,
    file_system: Arc<F>,
    listeners: Vec<Arc<dyn EventListener>>,

    /// Only filled after a successful call of `DualPipesBuilder::scan`.
    dirs: Paths,
    dir_locks: Vec<StdFile>,
    append_files: Vec<File<F>>,
    rewrite_files: Vec<File<F>>,
    recycled_files: Vec<File<F>>,
}

impl<F: FileSystem> DualPipesBuilder<F> {
    /// Creates a new builder.
    pub fn new(cfg: Config, file_system: Arc<F>, listeners: Vec<Arc<dyn EventListener>>) -> Self {
        Self {
            cfg,
            file_system,
            listeners,
            dirs: Vec::new(),
            dir_locks: Vec::new(),
            append_files: Vec::new(),
            rewrite_files: Vec::new(),
            recycled_files: Vec::new(),
        }
    }

    /// Scans for all log files under the working directory. The directory will
    /// be created if not exists.
    pub fn scan(&mut self) -> Result<()> {
        self.scan_dir(self.cfg.dir.clone())?; // scan Main dir
        if let Some(dir) = self.cfg.auxiliary_dir.as_ref() {
            let auxiliary_dir = dir.clone();
            self.scan_dir(auxiliary_dir)?; // scan Auxiliary dir
        }
        // Sorts the expected `file_list` according to `file_seq`.
        self.append_files.sort_by(|a, b| a.seq.cmp(&b.seq));
        self.rewrite_files.sort_by(|a, b| a.seq.cmp(&b.seq));
        self.recycled_files.sort_by(|a, b| a.seq.cmp(&b.seq));

        // Validate and clear obsolete metadata and log files.
        for (queue, files, is_recycled_file) in [
            (LogQueue::Append, &mut self.append_files, false),
            (LogQueue::Rewrite, &mut self.rewrite_files, false),
            (LogQueue::Append, &mut self.recycled_files, true),
        ] {
            if files.is_empty() {
                continue;
            }
            // Check the file_list and remove the hole of files.
            let mut current_seq = files[0].seq;
            let mut invalid_files = 0_usize;
            debug_assert!(current_seq > 0);
            for (i, f) in files.iter().enumerate() {
                if f.seq > current_seq + (i - invalid_files) as u64 {
                    warn!(
                        "Detected a hole when scanning directory, discarding files before file_seq {}.",
                        f.seq,
                    );
                    current_seq = f.seq + 1;
                    invalid_files = i;
                } else if f.seq < current_seq {
                    return Err(Error::InvalidArgument("Duplicate file".to_string()));
                }
            }
            files.drain(..invalid_files);
            // Try to cleanup stale metadata left by the previous version.
            if files.is_empty() {
                continue;
            }
            let mut cleared = 0_u64;
            let clear_start: u64 = {
                // TODO: Need a more efficient way to remove sparse stale metadata,
                // without iterating one by one.
                1
            };
            let dir = &self.dirs[files[0].path_id];
            for seq in (clear_start..files[0].seq).rev() {
                let file_id = FileId { queue, seq };
                let path = if is_recycled_file {
                    dir.join(build_recycled_file_name(seq))
                } else {
                    file_id.build_file_path(dir)
                };
                if self.file_system.exists_metadata(&path) {
                    if let Err(e) = self.file_system.delete_metadata(&path) {
                        error!("failed to delete metadata of {}: {}.", path.display(), e);
                        break;
                    }
                    cleared += 1;
                }
            }
            if cleared > 0 {
                warn!(
                    "clear {} stale files of {:?} in range [{}, {}).",
                    cleared, queue, 0, files[0].seq,
                );
            }
        }
        Ok(())
    }

    fn scan_dir(&mut self, dir: String) -> Result<()> {
        let dir = Path::new(&dir);
        if !dir.exists() {
            info!("Create raft log directory: {}", dir.display());
            fs::create_dir(dir)?;
            self.dir_locks.push(lock_dir(dir)?);
            self.dirs.push(dir.to_path_buf());
            return Ok(());
        }
        if !dir.is_dir() {
            return Err(box_err!("Not directory: {}", dir.display()));
        }
        self.dir_locks.push(lock_dir(dir)?);
        self.dirs.push(dir.to_path_buf());
        assert_eq!(self.dirs.len(), self.dir_locks.len());
        assert!(!self.dirs.is_empty());
        let path_id = self.dirs.len() - 1; // path_id to current dir.

        fs::read_dir(dir)?.try_for_each(|e| -> Result<()> {
            if let Ok(e) = e {
                let p = e.path();
                if !p.is_file() {
                    return Ok(());
                }
                let file_name = p.file_name().unwrap().to_str().unwrap();
                match FileId::parse_file_name(file_name) {
                    Some(FileId {
                        queue: LogQueue::Append,
                        seq,
                    }) => self.append_files.push(File {
                        seq,
                        handle: Arc::new(self.file_system.open(&p)?),
                        format: LogFileFormat::default(),
                        path_id,
                    }),
                    Some(FileId {
                        queue: LogQueue::Rewrite,
                        seq,
                    }) => self.rewrite_files.push(File {
                        seq,
                        handle: Arc::new(self.file_system.open(&p)?),
                        format: LogFileFormat::default(),
                        path_id,
                    }),
                    _ => {
                        if let Some(seq) = parse_recycled_file_name(file_name) {
                            self.recycled_files.push(File {
                                seq,
                                handle: Arc::new(self.file_system.open(&p)?),
                                format: LogFileFormat::default(),
                                path_id,
                            })
                        }
                    }
                }
            }
            Ok(())
        })
    }

    /// Reads through log items in all available log files, and replays them to
    /// specific [`ReplayMachine`]s that can be constructed via
    /// `machine_factory`.
    pub fn recover<M: ReplayMachine, FA: Factory<M>>(
        &mut self,
        machine_factory: &FA,
    ) -> Result<(M, M)> {
        if self.append_files.is_empty() && self.rewrite_files.is_empty() {
            // Avoid creating a thread pool.
            return Ok((machine_factory.new_target(), machine_factory.new_target()));
        }
        let threads = std::cmp::min(
            self.cfg.recovery_threads,
            self.append_files.len() + self.rewrite_files.len(),
        );
        let pool = rayon::ThreadPoolBuilder::new()
            .num_threads(threads)
            .build()
            .unwrap();
        let (append_concurrency, rewrite_concurrency) =
            match (self.append_files.len(), self.rewrite_files.len()) {
                (a, b) if a > 0 && b > 0 => {
                    let a_threads = std::cmp::max(1, threads * a / (a + b));
                    let b_threads = std::cmp::max(1, threads.saturating_sub(a_threads));
                    (a_threads, b_threads)
                }
                _ => (threads, threads),
            };
        let append_recovery_cfg = RecoveryConfig {
            queue: LogQueue::Append,
            mode: self.cfg.recovery_mode,
            concurrency: append_concurrency,
            read_block_size: self.cfg.recovery_read_block_size.0,
        };
        let rewrite_recovery_cfg = RecoveryConfig {
            queue: LogQueue::Rewrite,
            concurrency: rewrite_concurrency,
            ..append_recovery_cfg
        };
        let append_files = &mut self.append_files;
        let rewrite_files = &mut self.rewrite_files;
        let file_system = self.file_system.clone();
        // As the `recover_queue` would update the `LogFileFormat` of each log file
        // in `apend_files` and `rewrite_files`, we re-design the implementation on
        // `recover_queue` to make it compatiable to concurrent processing
        // with ThreadPool.
        let (append, rewrite) = pool.join(
            || {
                DualPipesBuilder::recover_queue_imp(
                    file_system.clone(),
                    append_recovery_cfg,
                    append_files,
                    machine_factory,
                )
            },
            || {
                DualPipesBuilder::recover_queue_imp(
                    file_system.clone(),
                    rewrite_recovery_cfg,
                    rewrite_files,
                    machine_factory,
                )
            },
        );
        Ok((append?, rewrite?))
    }

    /// Manually reads through log items in all available log files of the
    /// specified queue, and replays them to specific [`ReplayMachine`]s
    /// that can be constructed via `machine_factory`.
    fn recover_queue_imp<M: ReplayMachine, FA: Factory<M>>(
        file_system: Arc<F>,
        recovery_cfg: RecoveryConfig,
        files: &mut Vec<File<F>>,
        machine_factory: &FA,
    ) -> Result<M> {
        if recovery_cfg.concurrency == 0 || files.is_empty() {
            return Ok(machine_factory.new_target());
        }
        let queue = recovery_cfg.queue;
        let concurrency = recovery_cfg.concurrency;
        let recovery_mode = recovery_cfg.mode;
        let recovery_read_block_size = recovery_cfg.read_block_size as usize;

        let max_chunk_size = std::cmp::max((files.len() + concurrency - 1) / concurrency, 1);
        let chunks = files.par_chunks_mut(max_chunk_size);
        let chunk_count = chunks.len();
        debug_assert!(chunk_count <= concurrency);
        let machine = chunks
            .enumerate()
            .map(|(index, chunk)| {
                let mut reader =
                    LogItemBatchFileReader::new(recovery_read_block_size);
                let mut machine = machine_factory.new_target();
                let file_count = chunk.len();
                for (i, f) in chunk.iter_mut().enumerate() {
                    let is_last_file = index == chunk_count - 1 && i == file_count - 1;
                    let mut file_reader = build_file_reader(file_system.as_ref(), f.handle.clone())?;
                    match file_reader.parse_format() {
                        Err(e) => {
                            // TODO: More reliable tail detection.
                            if recovery_mode == RecoveryMode::TolerateAnyCorruption
                              || recovery_mode == RecoveryMode::TolerateTailCorruption
                                && is_last_file {
                                warn!(
                                    "File header is corrupted but ignored: {:?}:{}, {}",
                                    queue, f.seq, e
                                );
                                f.handle.truncate(0)?;
                                f.format = LogFileFormat::default();
                                continue;
                            } else {
                                error!(
                                    "Failed to open log file due to broken header: {:?}:{}",
                                    queue, f.seq
                                );
                                return Err(e);
                            }
                        },
                        Ok(format) => {
                            f.format = format;
                            reader.open(FileId { queue, seq: f.seq }, format, file_reader)?;
                        }
                    }
                    loop {
                        match reader.next() {
                            Ok(Some(item_batch)) => {
                                machine
                                    .replay(item_batch, FileId { queue, seq: f.seq })?;
                            }
                            Ok(None) => break,
                            Err(e)
                                if recovery_mode == RecoveryMode::TolerateTailCorruption
                                    && is_last_file =>
                            {
                                warn!(
                                    "The last log file is corrupted but ignored: {:?}:{}, {}",
                                    queue, f.seq, e
                                );
                                f.handle.truncate(reader.valid_offset())?;
                                break;
                            }
                            Err(e) if recovery_mode == RecoveryMode::TolerateAnyCorruption => {
                                warn!(
                                    "File is corrupted but ignored: {:?}:{}, {}",
                                    queue, f.seq, e
                                );
                                f.handle.truncate(reader.valid_offset())?;
                                break;
                            }
                            Err(e) => {
                                error!(
                                    "Failed to open log file due to broken entry: {:?}:{} offset={}",
                                    queue, f.seq, reader.valid_offset()
                                );
                                return Err(e);
                            }
                        }
                    }
                }
                Ok(machine)
            })
            .try_reduce(
                || machine_factory.new_target(),
                |mut lhs, rhs| {
                    lhs.merge(rhs, queue)?;
                    Ok(lhs)
                },
            )?;

        Ok(machine)
    }

    /// Manually reads through log items in all available log files of the
    /// specified `[LogQueue]`, and replays them to specific
    /// [`ReplayMachine`]s that can be constructed via `machine_factory`.
    #[allow(dead_code)]
    pub fn recover_queue<M: ReplayMachine, FA: Factory<M>>(
        &mut self,
        file_system: Arc<F>,
        recovery_cfg: RecoveryConfig,
        replay_machine_factory: &FA,
    ) -> Result<M> {
        let files = if recovery_cfg.queue == LogQueue::Append {
            &mut self.append_files
        } else {
            &mut self.rewrite_files
        };
        DualPipesBuilder::recover_queue_imp(
            file_system,
            recovery_cfg,
            files,
            replay_machine_factory,
        )
    }

    fn initialize_files(&mut self) -> Result<()> {
        let target_file_size = self.cfg.target_file_size.0 as usize;
        let mut target = if self.cfg.prefill_for_recycle {
            self.cfg
                .recycle_capacity()
                .saturating_sub(self.append_files.len())
        } else {
            0
        };
        target = cmp::min(target, MAX_PREFILL_SIZE / target_file_size);
        let to_create = target.saturating_sub(self.recycled_files.len());
        if to_create > 0 {
            let now = Instant::now();
            let mut no_enough_space = false;
            for _ in 0..to_create {
                let seq = self
                    .recycled_files
                    .last()
                    .map(|f| f.seq + 1)
                    .unwrap_or_else(|| DEFAULT_FIRST_FILE_SEQ);
                let path_id = fetch_dir(&self.dirs, target_file_size);
                let root_path = &self.dirs[path_id];
                let path = root_path.join(build_recycled_file_name(seq));
                let handle = Arc::new(self.file_system.create(&path)?);
                let mut writer = self.file_system.new_writer(handle.clone())?;
                let mut written = 0;
                let buf = vec![0; std::cmp::min(PREFILL_BUFFER_SIZE, target_file_size)];
                while written < target_file_size {
                    writer.write_all(&buf).unwrap_or_else(|e| {
                        warn!("failed to build recycled file, err: {}", e);
                        no_enough_space = true;
                    });
                    written += buf.len();
                }
                if no_enough_space {
                    warn!("no enough space for preparing recycled logs");
                    // Clear the incompletely prefilled recycled log.
                    let _ = self.file_system.delete(&path);
                    // Checks whether several prefilled logs need to be cleared to ensure that
                    // there exists enough space for successfully building the `Pipe`.
                    [self.append_files.is_empty(), self.rewrite_files.is_empty()].map(|is_empty| {
                        if is_empty {
                            target = target.saturating_sub(1);
                        }
                    });
                    break;
                }
                self.recycled_files.push(File {
                    seq,
                    handle,
                    format: LogFileFormat::default(),
                    path_id,
                });
            }
            info!(
                "prefill logs takes {:?}, created {} files",
                now.elapsed(),
                to_create
            );
        }
        // If target recycled capacity has been changed when restarting by manually
        // modifications, such as setting `Config::enable-log-recycle` from TRUE to
        // FALSE, setting `Config::prefill-for-recycle` from TRUE to FALSE or
        // changing the recycle capacity, we should remove redundant
        // recycled files in advance.
        while self.recycled_files.len() > target {
            let f = self.recycled_files.pop().unwrap();
            let root_path = &self.dirs[f.path_id];
            let path = root_path.join(build_recycled_file_name(f.seq));
            let _ = self.file_system.delete(&path);
        }
        Ok(())
    }

    /// Builds a [`DualPipes`] that contains all available log files.
    pub fn finish(mut self) -> Result<DualPipes<F>> {
        self.initialize_files()?;
        let appender = SinglePipe::open(
            &self.cfg,
            self.dirs.clone(),
            self.file_system.clone(),
            self.listeners.clone(),
            LogQueue::Append,
            self.append_files,
            self.recycled_files,
        )?;
        let rewriter = SinglePipe::open(
            &self.cfg,
            self.dirs.clone(),
            self.file_system.clone(),
            self.listeners.clone(),
            LogQueue::Rewrite,
            self.rewrite_files,
            Vec::new(),
        )?;
        DualPipes::open(self.dir_locks, appender, rewriter)
    }
}

/// Creates and exclusively locks a lock file under the given directory.
pub(super) fn lock_dir<P: AsRef<Path>>(dir: P) -> Result<StdFile> {
    let lock_file = StdFile::create(lock_file_path(dir))?;
    lock_file.try_lock_exclusive().map_err(|e| {
        Error::Other(box_err!(
            "Failed to lock file: {}, maybe another instance is using this directory.",
            e
        ))
    })?;
    Ok(lock_file)
}
