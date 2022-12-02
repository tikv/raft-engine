// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::io::Write;
use std::sync::Arc;
use std::time::Instant;

use crossbeam::utils::CachePadded;
use log::{error, info};
use parking_lot::RwLock;

use crate::env::FileSystem;
use crate::pipe_log::{FileId, FileSeq, LogQueue, Version};
use crate::{Error, Result};

use super::format::{FileNameExt, LogFileFormat, StaleFileNameExt};

/// Default buffer size for building stale file, unit: byte.
const LOG_STALE_FILE_BUF_SIZE: usize = 16 * 1024 * 1024;
/// Default path id for log files. 0 => Main dir
const LOG_DEFAULT_PATH_ID: PathId = 0;

pub type PathId = usize;
pub type Paths = [String; 1];

#[derive(Debug)]
pub struct FileWithFormat<F: FileSystem> {
    pub handle: Arc<F::Handle>,
    pub format: LogFileFormat,
    pub path_id: PathId,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FileState {
    pub first_seq: FileSeq,
    pub total_len: usize,
}

pub struct FileList<F: FileSystem> {
    /// Sequence number of the first file.
    first_seq: FileSeq,
    fds: VecDeque<FileWithFormat<F>>,
}

impl<F: FileSystem> FileList<F> {
    #[inline]
    pub fn new(first_seq: FileSeq, fds: VecDeque<FileWithFormat<F>>) -> Self {
        // If fds was empty, we just set first_seq with `1` to make
        // `first_seq` + `fds.len()` remake the next file directly.
        let first_seq = if fds.is_empty() { 1 } else { first_seq };
        Self { first_seq, fds }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.fds.len()
    }

    #[inline]
    pub fn span(&self) -> Option<(FileSeq, FileSeq)> {
        if !self.fds.is_empty() {
            return Some((self.first_seq, self.first_seq + self.fds.len() as u64 - 1));
        }
        None
    }

    #[inline]
    pub fn state(&self) -> FileState {
        // If current file list was empty, the returned state remarks
        // the next and expected file.
        FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        }
    }

    #[inline]
    pub fn get(&self, file_seq: FileSeq) -> Result<Arc<F::Handle>> {
        if !(self.first_seq..self.first_seq + self.fds.len() as u64).contains(&file_seq) {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(self.fds[(file_seq - self.first_seq) as usize]
            .handle
            .clone())
    }

    #[inline]
    pub fn back(&self) -> Option<&FileWithFormat<F>> {
        self.fds.back()
    }

    #[inline]
    pub fn push_back(&mut self, file_seq: FileSeq, handle: FileWithFormat<F>) -> FileState {
        if self.fds.is_empty() {
            self.first_seq = file_seq;
        }
        self.fds.push_back(handle);
        debug_assert_eq!(file_seq, self.first_seq + (self.fds.len() - 1) as FileSeq);
        FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        }
    }

    #[inline]
    pub fn pop_front(&mut self) -> Option<(FileSeq, FileWithFormat<F>)> {
        match self.fds.pop_front() {
            Some(fd) => {
                let seq = self.first_seq;
                self.first_seq += 1;
                Some((seq, fd))
            }
            None => None,
        }
    }

    #[inline]
    pub fn append(&mut self, mut file_list: FileList<F>) -> FileState {
        if file_list.len() > 0 {
            if self.fds.is_empty() {
                self.first_seq = file_list.first_seq;
            } else {
                debug_assert_eq!(
                    self.first_seq + self.fds.len() as FileSeq,
                    file_list.first_seq
                );
            }
            self.fds.append(&mut file_list.fds);
        }
        FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        }
    }

    /// Splits current file list.
    ///
    /// Returns the first splitted part of current file list.
    #[inline]
    pub fn split_by(&mut self, file_seq: FileSeq) -> (FileState, FileList<F>) {
        let splitted_list =
            if (self.first_seq..self.first_seq + self.fds.len() as u64).contains(&file_seq) {
                let purged = file_seq.saturating_sub(self.first_seq) as usize;
                let purged_file_list =
                    FileList::new(self.first_seq, self.fds.drain(..purged).collect());
                self.first_seq = file_seq;
                purged_file_list
            } else {
                FileList::new(0, VecDeque::default())
            };
        (
            FileState {
                first_seq: self.first_seq,
                total_len: self.fds.len(),
            },
            splitted_list,
        )
    }
}

/// A collection of files for managing all log files.
///
/// Log files consist of two parts:
/// - `Stale` part, obsolete files but temporarily stashed for recycling.
/// - `Active` part, active files for accessing.
pub struct FileCollection<F: FileSystem> {
    file_system: Arc<F>,
    queue: LogQueue,
    paths: Paths,
    file_format: LogFileFormat,
    target_file_size: usize,
    capacity: usize,
    /// File list to collect all active files
    active_files: CachePadded<RwLock<FileList<F>>>,
    /// File list to collect all stale files
    stale_files: CachePadded<RwLock<FileList<F>>>,
}

impl<F: FileSystem> FileCollection<F> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        file_system: Arc<F>,
        queue: LogQueue,
        paths: Paths,
        file_format: LogFileFormat,
        target_file_size: usize,
        capacity: usize,
        active_files: FileList<F>,
        stale_files: FileList<F>,
    ) -> Self {
        Self {
            file_system,
            queue,
            paths,
            file_format,
            target_file_size,
            capacity,
            active_files: CachePadded::new(RwLock::new(active_files)),
            stale_files: CachePadded::new(RwLock::new(stale_files)),
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.active_files.read().len() + self.stale_files.read().len()
    }

    #[inline]
    pub fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<F::Handle>> {
        self.active_files.read().get(file_seq)
    }

    #[inline]
    pub fn back(&self) -> Option<FileWithFormat<F>> {
        self.active_files.read().back().map(|fd| FileWithFormat {
            handle: fd.handle.clone(),
            ..*fd
        })
    }

    #[inline]
    pub fn active_file_span(&self) -> Option<(FileSeq, FileSeq)> {
        self.active_files.read().span()
    }

    #[inline]
    #[cfg(test)]
    fn stale_file_span(&self) -> Option<(FileSeq, FileSeq)> {
        self.stale_files.read().span()
    }

    #[inline]
    pub fn push_back(&self, seq: FileSeq, handle: FileWithFormat<F>) -> FileState {
        self.active_files.write().push_back(seq, handle)
    }

    #[inline]
    pub fn get_target_file_size(&self) -> usize {
        self.target_file_size
    }

    /// Rotate a new file handle and return it to the caller.
    ///
    /// Returns the relevant `FileSeq`, and `FileWithFormat` of the file
    /// handle to the caller.
    /// Attention please, the returned fd should be manually appended to current
    /// `FileCollection`.
    pub fn rotate(&self) -> Result<(FileSeq, FileWithFormat<F>)> {
        let active_state = self.active_files.read().state();
        let new_file_id = FileId {
            seq: active_state.first_seq + active_state.total_len as FileSeq,
            queue: self.queue,
        };
        let new_file_hanle = if let Some((seq, fd)) = self.stale_files.write().pop_front() {
            debug_assert_eq!(fd.path_id, 0);
            let stale_file_id = FileId {
                seq,
                queue: self.queue,
            };
            let src_path = stale_file_id.build_stale_file_path(&self.paths[fd.path_id]);
            let dst_path = new_file_id.build_file_path(&self.paths[fd.path_id]);
            if let Err(e) = self.file_system.reuse(&src_path, &dst_path) {
                error!("error while trying to reuse one stale file, err: {}", e);
                if let Err(e) = self.file_system.delete(&src_path) {
                    error!("error while trying to delete one stale file, err: {}", e);
                }
                FileWithFormat {
                    handle: Arc::new(self.file_system.create(&dst_path)?),
                    path_id: fd.path_id,
                    format: self.file_format,
                }
            } else {
                FileWithFormat {
                    handle: Arc::new(self.file_system.open(&dst_path)?),
                    path_id: fd.path_id,
                    format: self.file_format,
                }
            }
        } else {
            let path_id = FileCollection::<F>::get_valid_path(&self.paths, self.target_file_size);
            let path = new_file_id.build_file_path(&self.paths[path_id]);
            FileWithFormat {
                handle: Arc::new(self.file_system.create(&path)?),
                path_id,
                format: self.file_format,
            }
        };
        Ok((new_file_id.seq, new_file_hanle))
    }

    /// Purges files to the specific file_seq.
    ///
    /// Returns the purged count of active files.
    pub fn purge_to(&self, file_seq: FileSeq) -> Result<usize> {
        let (active_state, mut purged_files) = self.active_files.write().split_by(file_seq);
        if file_seq >= active_state.first_seq + active_state.total_len as FileSeq {
            debug_assert_eq!(purged_files.len(), 0);
            return Err(box_err!("Purge active or newer files"));
        } else if purged_files.len() == 0 {
            return Ok(0);
        }
        let logical_purged_count = purged_files.len();
        {
            let remains_capacity = self.capacity.saturating_sub(active_state.total_len);
            // We get the FileState of `self.stale_files` in advance to reduce the lock
            // racing for later processing.
            let mut stale_state = self.stale_files.write().state();
            let mut files_to_stale = FileList::<F>::new(0, VecDeque::default());
            // The newly purged files from `self.active_files` should be RENAME
            // to stale files with `.raftlog.stale` suffix, to reduce the unnecessary
            // recovery timecost when RESTART.
            while let Some((seq, file)) = purged_files.pop_front() {
                let file_id = FileId {
                    seq,
                    queue: self.queue,
                };
                let path = file_id.build_file_path(&self.paths[file.path_id]);
                if file.format.version.has_log_signing() && stale_state.total_len < remains_capacity
                {
                    let stale_file_id = FileId {
                        seq: stale_state.first_seq + stale_state.total_len as FileSeq,
                        queue: self.queue,
                    };
                    let stale_path = stale_file_id.build_stale_file_path(&self.paths[file.path_id]);
                    self.file_system.reuse(&path, &stale_path)?;
                    files_to_stale.push_back(
                        stale_file_id.seq,
                        FileWithFormat {
                            handle: Arc::new(self.file_system.open(&stale_path)?),
                            path_id: file.path_id,
                            format: self.file_format,
                        },
                    );
                    stale_state.total_len += 1;
                } else {
                    // The files with format_version `V1` cannot be chosen as recycle candidates.
                    // We will simply make sure there's no `V1` stale files in the collection.
                    self.file_system.delete(&path)?;
                }
            }
            // If there exists several stale files out of space, contained in
            // `self.stale_files` and `purged_files`, we should check and remove them
            // to avoid the `size` of whole files beyond `self.capacity`.
            if stale_state.total_len > remains_capacity {
                let (_, mut clear_list) = self.stale_files.write().split_by(
                    stale_state.first_seq + (stale_state.total_len - remains_capacity) as FileSeq,
                );
                while let Some((seq, file)) = clear_list.pop_front() {
                    let file_id = FileId {
                        seq,
                        queue: self.queue,
                    };
                    let path = file_id.build_stale_file_path(&self.paths[file.path_id]);
                    self.file_system.delete(&path)?;
                }
            }
            self.stale_files.write().append(files_to_stale);
        }
        Ok(logical_purged_count)
    }

    /// Initialize current file collection by preparing several
    /// stale files for later log recycling in advance.
    ///
    /// Attention, this function only makes sense when
    /// `Config.enable-log-recycle` is true.
    pub fn initialize(&mut self) -> Result<()> {
        let now = Instant::now();
        let stale_capacity = self.capacity.saturating_sub(self.active_files.read().len());
        // If `stale_capacity` > 0, we should prepare stale files for later
        // log recycling in advance.
        if stale_capacity > 0 {
            // Concurrent prepraring will bring more time consumption on racing. So, we just
            // introduce a serial processing for preparing progress.
            let mut stale_files = self.stale_files.write();
            let (prepare_first_seq, prepare_stale_files_count) = (
                {
                    let state = stale_files.state();
                    state.first_seq + state.total_len as FileSeq
                },
                stale_capacity.saturating_sub(stale_files.len()),
            );
            for seq in prepare_first_seq..prepare_first_seq + prepare_stale_files_count as FileSeq {
                let file_id = FileId {
                    queue: self.queue,
                    seq,
                };
                stale_files.push_back(
                    seq,
                    self.build_stale_file(
                        file_id,
                        LogFileFormat::new(Version::V2, 0 /* alignment */),
                    )?,
                );
            }
        }
        info!(
            "LogQueue: {:?} preparing stale raft logs takes {:?}, prepared count: {}",
            self.queue,
            now.elapsed(),
            self.stale_files.read().len(),
        );
        Ok(())
    }

    /// Generates a Fake log used for recycling.
    ///
    /// Attention, this function is only called when `Config.enable-log-recycle`
    /// is true.
    fn build_stale_file(
        &self,
        file_id: FileId,
        format: LogFileFormat,
    ) -> Result<FileWithFormat<F>> {
        let path_id = FileCollection::<F>::get_valid_path(&self.paths, self.target_file_size);
        let file_path = file_id.build_stale_file_path(&self.paths[path_id]);
        let fd = Arc::new(self.file_system.create(&file_path)?);
        let mut writer = self.file_system.new_writer(fd.clone())?;
        let mut written = 0_usize;
        let buf = vec![0; std::cmp::min(LOG_STALE_FILE_BUF_SIZE, self.target_file_size)];
        while written <= self.target_file_size {
            writer.write_all(&buf).unwrap_or_else(|e| {
                panic!("failed to prepare stale file: {}", e);
            });
            written += buf.len();
        }
        // Metadata of stale files are not what we're truely concerned. So,
        // they can be ignored by clear them here.
        Ok(FileWithFormat {
            handle: fd,
            format,
            path_id,
        })
    }

    /// Returns a valid path for dumping new files.
    #[inline]
    pub fn get_valid_path(paths: &Paths, _target_file_size: usize) -> PathId {
        debug_assert!(paths.len() <= 2);
        LOG_DEFAULT_PATH_ID
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use tempfile::Builder;

    use super::super::format::{FileNameExt, LogFileFormat};
    use super::*;
    use crate::env::DefaultFileSystem;
    use crate::pipe_log::Version;
    use crate::util::ReadableSize;

    fn new_file_handler(
        path: &str,
        file_id: FileId,
        version: Version,
        is_stale: bool,
    ) -> FileWithFormat<DefaultFileSystem> {
        let file_path = if !is_stale {
            file_id.build_file_path(path)
        } else {
            file_id.build_stale_file_path(path)
        };
        FileWithFormat {
            handle: Arc::new(DefaultFileSystem.create(&file_path).unwrap()),
            format: LogFileFormat::new(version, 0 /* alignment */),
            path_id: 0, /* default with Main dir */
        }
    }

    #[test]
    fn test_file_list() {
        let dir = Builder::new().prefix("test_file_list").tempdir().unwrap();
        let path = String::from(dir.path().to_str().unwrap());
        // | 12
        let mut active_files = FileList::new(
            12,
            vec![new_file_handler(
                &path,
                FileId::new(LogQueue::Append, 12),
                Version::V2,
                true, /* forcely marked with stale */
            )]
            .into(),
        );
        assert_eq!(
            active_files.state(),
            FileState {
                first_seq: 12,
                total_len: 1,
            }
        );
        assert_eq!(active_files.len(), 1);
        assert_eq!(active_files.span().unwrap(), (12, 12));
        let mut stale_files = FileList::new(0, VecDeque::default());
        assert_eq!(stale_files.len(), 0);
        assert!(stale_files.span().is_none());
        assert!(stale_files.pop_front().is_none());
        // 11 | 12
        assert_eq!(
            stale_files.push_back(
                11,
                new_file_handler(&path, FileId::new(LogQueue::Append, 11), Version::V2, true),
            ),
            FileState {
                first_seq: 11,
                total_len: 1
            }
        );
        // 11 | 12 13
        active_files.push_back(
            13,
            new_file_handler(&path, FileId::new(LogQueue::Append, 13), Version::V2, false),
        );
        assert_eq!(active_files.span().unwrap(), (12, 13));
        // 11 12 | 13
        let (state, files) = active_files.split_by(13);
        assert_eq!(state.first_seq, 13);
        assert_eq!(files.span().unwrap(), (12, 12));
        for (i, fd) in files.fds.iter().enumerate() {
            stale_files.push_back(
                files.first_seq + i as FileSeq,
                FileWithFormat {
                    handle: fd.handle.clone(),
                    ..*fd
                },
            );
        }
        assert_eq!(stale_files.span().unwrap(), (11, 12));
        assert!(stale_files.pop_front().is_some());
        assert_eq!(stale_files.span().unwrap(), (12, 12));
        assert!(stale_files.back().is_some());
        // 11 12 | 13 14 15
        let mut append_list = FileList::new(0, VecDeque::default());
        for seq in 14..=15 {
            append_list.push_back(
                seq,
                new_file_handler(
                    &path,
                    FileId::new(LogQueue::Append, seq),
                    Version::V2,
                    false,
                ),
            );
        }
        assert_eq!(
            active_files.append(append_list),
            FileState {
                first_seq: 13,
                total_len: 3
            }
        );
    }

    #[test]
    fn test_file_collection() {
        let dir = Builder::new()
            .prefix("test_file_collection")
            .tempdir()
            .unwrap();
        let path = String::from(dir.path().to_str().unwrap());
        let target_file_size = ReadableSize(1);
        // null | null
        let file_collection = FileCollection::new(
            Arc::new(DefaultFileSystem),
            LogQueue::Append,
            [path.clone()],
            LogFileFormat::new(Version::V2, 0),
            target_file_size.0 as usize,
            5,
            FileList::new(0, VecDeque::default()),
            FileList::new(0, VecDeque::default()),
        );
        assert_eq!(file_collection.len(), 0);
        assert!(file_collection.active_file_span().is_none());
        assert!(file_collection.stale_file_span().is_none());
        assert!(file_collection.back().is_none());
        // null | 11 12 13
        for seq in 11..=13 {
            file_collection.push_back(
                seq,
                new_file_handler(
                    &path,
                    FileId::new(LogQueue::Append, seq),
                    Version::V2,
                    false,
                ),
            );
        }
        assert_eq!(file_collection.len(), 3);
        assert_eq!(file_collection.active_file_span().unwrap(), (11, 13));
        assert!(file_collection.stale_file_span().is_none());
        // 1 2 | 13
        assert_eq!(file_collection.purge_to(13).unwrap(), 2);
        assert_eq!(file_collection.len(), 3);
        assert_eq!(file_collection.active_file_span().unwrap(), (13, 13));
        assert_eq!(file_collection.stale_file_span().unwrap(), (1, 2));
        // 1 2 | 13 14
        assert_eq!(
            file_collection.push_back(
                14,
                new_file_handler(&path, FileId::new(LogQueue::Append, 14), Version::V1, false),
            ),
            FileState {
                first_seq: 13,
                total_len: 2,
            }
        );
        // 1 2 | 13 14 15
        file_collection.push_back(
            15,
            new_file_handler(&path, FileId::new(LogQueue::Append, 15), Version::V2, false),
        );
        // V1 file will not be kept around.
        // 1 2 3 | 15
        assert_eq!(2, file_collection.purge_to(15).unwrap());
        assert_eq!(file_collection.len(), 4);
        assert_eq!(file_collection.stale_file_span().unwrap(), (1, 3));
        assert_eq!(file_collection.active_file_span().unwrap(), (15, 15));
        // 1 2 3 | 15 16
        file_collection.push_back(
            16,
            new_file_handler(&path, FileId::new(LogQueue::Append, 16), Version::V1, false),
        );
        // Stale file with seqno 15 will be reused to `.raftlog.stale` with seqno 1.
        // 1 2 3 4 | 16
        assert_eq!(1, file_collection.purge_to(16).unwrap());
        assert_eq!(file_collection.len(), 5);
        assert_eq!(file_collection.stale_file_span().unwrap(), (1, 4));
        assert_eq!(file_collection.active_file_span().unwrap(), (16, 16));
        // 1 2 3 4 | 16 17 18 19 20
        for i in 17..=20 {
            file_collection.push_back(
                i as FileSeq,
                new_file_handler(&path, FileId::new(LogQueue::Append, i), Version::V2, false),
            );
        }
        assert_eq!(file_collection.stale_file_span().unwrap(), (1, 4));
        assert_eq!(file_collection.active_file_span().unwrap(), (16, 20));
        // V1 file will not be kept around.
        // 2 3 4 | 19 20
        assert_eq!(3, file_collection.purge_to(19).unwrap());
        assert_eq!(file_collection.len(), 5);
        assert_eq!(file_collection.stale_file_span().unwrap(), (2, 4));
        assert_eq!(file_collection.active_file_span().unwrap(), (19, 20));
        // 3 4 | 19 20
        let (file_seq, fd) = file_collection.rotate().unwrap();
        assert_eq!(file_collection.len(), 4);
        assert_eq!(file_collection.stale_file_span().unwrap(), (3, 4));
        assert_eq!(file_collection.active_file_span().unwrap(), (19, 20));
        assert_eq!(file_seq, 21);
        // 3 4 | 19 20 21
        file_collection.push_back(file_seq, fd);
        assert_eq!(file_collection.len(), 5);
        assert_eq!(file_collection.stale_file_span().unwrap(), (3, 4));
        assert_eq!(file_collection.active_file_span().unwrap(), (19, 21));
    }
}
