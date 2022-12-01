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

use super::format::{max_stale_log_count, FileNameExt, LogFileFormat, StaleFileNameExt};

/// Default buffer size for building stale file, unit: byte.
const LOG_STALE_FILE_BUF_SIZE: usize = 16 * 1024 * 1024;

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
        let first_seq = if fds.is_empty() { 0 } else { first_seq };
        Self { first_seq, fds }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.fds.len()
    }

    #[inline]
    pub fn span(&self) -> (FileSeq, FileSeq) {
        if !self.fds.is_empty() {
            (self.first_seq, self.first_seq + self.fds.len() as u64 - 1)
        } else {
            (0, 0)
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
    #[cfg(test)]
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
    target_file_size: usize,
    capacity: usize,
    /// File list to collect all active files
    active_files: CachePadded<RwLock<FileList<F>>>,
    /// File list to collect all stale files
    stale_files: CachePadded<RwLock<FileList<F>>>,
}

impl<F: FileSystem> FileCollection<F> {
    pub fn new(
        file_system: Arc<F>,
        queue: LogQueue,
        paths: Paths,
        target_file_size: usize,
        capacity: usize,
        active_files: FileList<F>,
        stale_files: FileList<F>,
    ) -> Self {
        Self {
            file_system,
            queue,
            paths,
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
    pub fn active_file_span(&self) -> (FileSeq, FileSeq) {
        self.active_files.read().span()
    }

    #[inline]
    #[cfg(test)]
    pub fn stale_file_span(&self) -> (FileSeq, FileSeq) {
        self.stale_files.read().span()
    }

    #[inline]
    pub fn push_back(&mut self, seq: FileSeq, handle: FileWithFormat<F>) -> FileState {
        self.active_files.write().push_back(seq, handle)
    }

    /// Rotate a new file handle and return it to the caller.
    ///
    /// Returns the relevant `FileSeq`, `PathId` and `F::Handle` of the file
    /// handle to the caller.
    /// Attention please, the returned fd should be manually appended to current
    /// `FileCollection`.
    pub fn rotate(&mut self) -> Result<(FileSeq, PathId, Arc<F::Handle>)> {
        let new_file_id = FileId {
            seq: self.active_files.read().span().1 + 1,
            queue: self.queue,
        };
        let (path_id, new_fd) = if let Some((seq, fd)) = self.stale_files.write().pop_front() {
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
                (fd.path_id, Arc::new(self.file_system.create(&dst_path)?))
            } else {
                (fd.path_id, Arc::new(self.file_system.open(&dst_path)?))
            }
        } else {
            let path_id = FileCollection::<F>::get_valid_path(&self.paths, self.target_file_size);
            let path = new_file_id.build_file_path(&self.paths[path_id]);
            (path_id, Arc::new(self.file_system.create(&path)?))
        };
        Ok((new_file_id.seq, path_id, new_fd))
    }

    /// Purges files to the specific file_seq.
    ///
    /// Returns the purged count of active files.
    pub fn purge_to(&mut self, file_seq: FileSeq) -> Result<usize> {
        let (active_state, mut purged_files) = self.active_files.write().split_by(file_seq);
        if file_seq >= active_state.first_seq + active_state.total_len as FileSeq {
            debug_assert_eq!(purged_files.len(), 0);
            return Err(box_err!("Purge active or newer files"));
        } else if purged_files.len() == 0 {
            return Ok(0);
        }
        let logical_purged_count = purged_files.len();
        {
            let mut stale_files = self.stale_files.write();
            // The files with format_version `V1` cannot be chosen as recycle candidates.
            // We will simply make sure there's no `V1` stale files in the collection.
            let invalid_count = {
                let mut invalid_idx = 0_usize;
                for (i, fd) in purged_files.fds.iter().enumerate() {
                    if !fd.format.version.has_log_signing() {
                        invalid_idx = i + 1;
                        break;
                    }
                }
                invalid_idx
            };
            let out_of_space = if invalid_count > 0 && self.capacity > 0 {
                // If `invalid_count` exists, it means that part of stale files with
                // seqno earlier than this flag should be cleared.
                invalid_count + stale_files.len()
            } else {
                // If there exists several stale files out of space, contained in
                // `self.stale_files` and `purged_files`, we should check and remove them
                // to avoid the `size` of whole files beyond `self.capacity`.
                (active_state.total_len + logical_purged_count + stale_files.len())
                    .saturating_sub(self.capacity)
            };
            for _ in 0..out_of_space {
                let path = {
                    if let Some((seq, fd)) = stale_files.pop_front() {
                        let file_id = FileId {
                            seq,
                            queue: self.queue,
                        };
                        file_id.build_stale_file_path(&self.paths[fd.path_id])
                    } else if let Some((seq, fd)) = purged_files.pop_front() {
                        let file_id = FileId {
                            seq,
                            queue: self.queue,
                        };
                        file_id.build_file_path(&self.paths[fd.path_id])
                    } else {
                        break;
                    }
                };
                self.file_system.delete(&path)?;
            }
            // Meanwhile, the new purged files from `self.active_files` should be RENAME
            // to stale files with `.raftlog.stale` suffix, to reduce the unnecessary
            // recovery timecost when RESTART.
            for (i, fd) in purged_files.fds.iter().enumerate() {
                let file_id = FileId {
                    seq: purged_files.first_seq + i as FileSeq,
                    queue: self.queue,
                };
                let stale_file_id = FileId {
                    seq: stale_files.span().1 + 1_u64,
                    queue: self.queue,
                };
                let path = file_id.build_file_path(&self.paths[fd.path_id]);
                let stale_path = stale_file_id.build_stale_file_path(&self.paths[fd.path_id]);
                self.file_system.reuse(&path, &stale_path)?;
                stale_files.push_back(
                    stale_file_id.seq,
                    FileWithFormat {
                        handle: Arc::new(self.file_system.open(&stale_path)?),
                        path_id: fd.path_id,
                        format: fd.format,
                    },
                );
            }
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
        let stale_capacity = std::cmp::min(
            self.capacity.saturating_sub(self.active_files.read().len()),
            max_stale_log_count(),
        );
        // If `stale_capacity` > 0, we should prepare stale files for later
        // log recycling in advance.
        if stale_capacity > 0 {
            // Concurrent prepraring will bring more time consumption on racing. So, we just
            // introduce a serial processing for preparing progress.
            let mut stale_files = self.stale_files.write();
            let (prepare_first_seq, prepare_stale_files_count) = (
                stale_files.span().1 + 1,
                stale_capacity.saturating_sub(stale_files.len()),
            );
            for seq in prepare_first_seq..prepare_first_seq + prepare_stale_files_count as FileSeq {
                let file_id = FileId {
                    queue: self.queue,
                    seq,
                };
                stale_files.push_back(
                    seq,
                    FileCollection::build_stale_file(
                        self.file_system.as_ref(),
                        &self.paths,
                        file_id,
                        LogFileFormat::new(Version::V2, 0 /* alignment */),
                        self.target_file_size,
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
        file_system: &F,
        paths: &Paths,
        file_id: FileId,
        format: LogFileFormat,
        target_file_size: usize,
    ) -> Result<FileWithFormat<F>> {
        let path_id = FileCollection::<F>::get_valid_path(paths, target_file_size);
        let file_path = file_id.build_stale_file_path(&paths[path_id]);
        let fd = Arc::new(file_system.create(&file_path)?);
        let mut writer = file_system.new_writer(fd.clone())?;
        let mut written = 0_usize;
        let buf = vec![0; std::cmp::min(LOG_STALE_FILE_BUF_SIZE, target_file_size)];
        while written <= target_file_size {
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
    ///
    /// Default: 0 -> default index of the main directory.
    pub fn get_valid_path(paths: &Paths, _target_file_size: usize) -> PathId {
        debug_assert!(paths.len() <= 2);
        0
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

    #[test]
    fn test_file_collection() {
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
        let dir = Builder::new()
            .prefix("test_file_collection")
            .tempdir()
            .unwrap();
        let path = String::from(dir.path().to_str().unwrap());
        let target_file_size = ReadableSize(1);
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
        assert_eq!(active_files.len(), 1);
        assert_eq!(active_files.span(), (12, 12));
        let mut stale_files = FileList::new(0, VecDeque::default());
        assert_eq!(stale_files.len(), 0);
        assert_eq!(stale_files.span(), (0, 0));
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
        assert_eq!(active_files.span(), (12, 13));
        // 11 12 | 13
        let (state, files) = active_files.split_by(13);
        assert_eq!(state.first_seq, 13);
        assert_eq!(files.span(), (12, 12));
        assert_eq!(
            stale_files.append(files),
            FileState {
                first_seq: 11,
                total_len: 2,
            }
        );
        let mut file_collection = FileCollection::new(
            Arc::new(DefaultFileSystem),
            LogQueue::Append,
            [path.clone()],
            target_file_size.0 as usize,
            5,
            active_files,
            stale_files,
        );
        // 11 12 | 13 14
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
        // 11 12 | 13 14 15
        file_collection.push_back(
            15,
            new_file_handler(&path, FileId::new(LogQueue::Append, 15), Version::V2, false),
        );
        // | 15
        // V1 file will not be kept around.
        assert_eq!(2, file_collection.purge_to(15).unwrap());
        assert_eq!(file_collection.len(), 1);
        assert_eq!(file_collection.stale_file_span(), (0, 0));
        assert_eq!(file_collection.active_file_span(), (15, 15));
        // | 15 16
        file_collection.push_back(
            16,
            new_file_handler(&path, FileId::new(LogQueue::Append, 16), Version::V2, false),
        );
        // Stale file with seqno 15 will be reused to `.raftlog.stale` with seqno 1.
        // 1 | 16
        assert_eq!(1, file_collection.purge_to(16).unwrap());
        assert_eq!(file_collection.len(), 2);
        assert_eq!(file_collection.stale_file_span(), (1, 1));
        assert_eq!(file_collection.active_file_span(), (16, 16));
        // 1 | 16 17 18 19 20
        for i in 17..=20 {
            file_collection.push_back(
                i as FileSeq,
                new_file_handler(&path, FileId::new(LogQueue::Append, i), Version::V2, false),
            );
        }
        assert_eq!(file_collection.stale_file_span(), (1, 1));
        assert_eq!(file_collection.active_file_span(), (16, 20));
        // 1 2 3 | 19 20
        assert_eq!(3, file_collection.purge_to(19).unwrap());
        assert_eq!(file_collection.len(), 5);
        assert_eq!(file_collection.stale_file_span(), (1, 3));
        assert_eq!(file_collection.active_file_span(), (19, 20));
        // 2 3 | 19 20
        let (file_seq, path_id, fd) = file_collection.rotate().unwrap();
        assert_eq!(file_collection.len(), 4);
        assert_eq!(file_seq, 21);
        // 2 3 | 19 20 21
        file_collection.push_back(
            file_seq,
            FileWithFormat {
                handle: fd,
                format: LogFileFormat::new(Version::V2, 0),
                path_id,
            },
        );
        assert_eq!(file_collection.len(), 5);
        assert_eq!(file_collection.stale_file_span(), (2, 3));
        assert_eq!(file_collection.active_file_span(), (19, 21));
    }
}
