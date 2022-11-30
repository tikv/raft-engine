// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use log::{error, info};

use crate::env::FileSystem;
use crate::pipe_log::{FileId, FileSeq, LogQueue, Version};
use crate::{Error, Result};

use super::format::{max_stale_log_count, FileNameExt, LogFileFormat, StaleFileNameExt};
use super::log_file::build_file_writer;

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
    pub fn split_by(&mut self, file_seq: FileSeq) -> FileList<F> {
        if (self.first_seq..self.first_seq + self.fds.len() as u64).contains(&file_seq) {
            let purged = file_seq.saturating_sub(self.first_seq);
            let purged_file_list =
                FileList::new(self.first_seq, self.fds.drain(..purged as usize).collect());
            self.first_seq = file_seq;
            purged_file_list
        } else {
            FileList::new(0, VecDeque::default())
        }
    }

    /// Purges current file list according to the given `capacity`.
    ///
    /// Returns the purged file list. Attention please, this function
    /// is a specialized one for stale files.
    pub fn purge(&mut self, capacity: usize) -> FileList<F> {
        let obsolete_files = self.fds.len();
        // When capacity is zero, always remove logically deleted files.
        let mut purged = obsolete_files.saturating_sub(capacity);
        // The files with format_version `V1` cannot be chosen as recycle
        // candidates. We will simply make sure there's no V1 stale files in the
        // collection.
        for i in (purged..obsolete_files).rev() {
            if !self.fds[i].format.version.has_log_signing() {
                purged = i + 1;
                break;
            }
        }
        self.first_seq += purged as FileSeq;
        FileList {
            first_seq: self.first_seq - purged as FileSeq,
            fds: self.fds.drain(..purged).collect(),
        }
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
    capacity: usize,
    /// File list to collect all active files
    active_files: FileList<F>,
    /// File list to collect all stale files
    stale_files: FileList<F>,
}

impl<F: FileSystem> FileCollection<F> {
    pub fn new(
        file_system: Arc<F>,
        queue: LogQueue,
        paths: Paths,
        capacity: usize,
        active_files: FileList<F>,
        stale_files: FileList<F>,
    ) -> Self {
        Self {
            file_system,
            queue,
            paths,
            capacity,
            active_files,
            stale_files,
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.active_files.len() + self.stale_files.len()
    }

    #[inline]
    pub fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<F::Handle>> {
        self.active_files.get(file_seq)
    }

    #[inline]
    pub fn back(&self) -> Option<&FileWithFormat<F>> {
        self.active_files.back()
    }

    #[inline]
    pub fn active_file_span(&self) -> (FileSeq, FileSeq) {
        self.active_files.span()
    }

    #[inline]
    pub fn stale_file_span(&self) -> (FileSeq, FileSeq) {
        self.stale_files.span()
    }

    #[inline]
    pub fn push_back(&mut self, seq: FileSeq, handle: FileWithFormat<F>) -> FileState {
        self.active_files.push_back(seq, handle)
    }

    /// Rotate a new file handle and return it to the caller.
    ///
    /// Returns the relevant `FileSeq`, `PathId` and `F::Handle` of the file
    /// handle to the caller.
    /// Attention please, the returned fd should be manually appended to current
    /// `FileCollection`.
    pub fn rotate(&mut self, target_file_size: usize) -> Result<(FileSeq, PathId, Arc<F::Handle>)> {
        let new_file_id = FileId {
            seq: if self.active_files.len() == 0 {
                self.stale_files.span().1 + 1
            } else {
                self.active_files.span().1 + 1
            },
            queue: self.queue,
        };
        let (path_id, new_fd) = if let Some((seq, fd)) = self.stale_files.pop_front() {
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
            let path_id = FileCollection::<F>::get_valid_path(&self.paths, target_file_size);
            let path = new_file_id.build_file_path(&self.paths[path_id]);
            (path_id, Arc::new(self.file_system.create(&path)?))
        };
        Ok((new_file_id.seq, path_id, new_fd))
    }

    /// Purges files to the specific file_seq.
    ///
    /// Returns the purged count of active files.
    pub fn purge_to(&mut self, file_seq: FileSeq) -> Result<usize> {
        let purged_files = self.active_files.split_by(file_seq);
        if file_seq > self.active_files.span().1 {
            debug_assert_eq!(purged_files.len(), 0);
            return Err(box_err!("Purge active or newer files"));
        } else if purged_files.len() == 0 {
            return Ok(0);
        }
        let (logical_purged_first_seq, logical_purged_count) =
            (purged_files.first_seq, purged_files.len());
        let logical_purged_path_list: Vec<PathId> =
            purged_files.fds.iter().map(|f| f.path_id).collect();
        // If there exists several stale files for purging in `self.stale_files`,
        // we should check and remove them to avoid the `size` of whole files
        // beyond `self.capacity`.
        let clear_list = {
            self.stale_files.append(purged_files);
            self.stale_files
                .purge(self.capacity.saturating_sub(self.active_files.len()))
        };
        for (i, fd) in clear_list.fds.iter().enumerate() {
            #[cfg(feature = "failpoints")]
            {
                let remove_skipped = || {
                    fail::fail_point!("file_pipe_log::remove_file_skipped", |_| true);
                    false
                };
                if remove_skipped() {
                    continue;
                }
            }
            let seq = clear_list.first_seq + i as FileSeq;
            let file_id = FileId {
                queue: self.queue,
                seq,
            };
            // If seq less than the first seq of `purged_files`, it should be a renamed
            // stale file, named with `.raftlog.stale` suffix.
            let path = if seq < logical_purged_first_seq {
                file_id.build_stale_file_path(&self.paths[fd.path_id])
            } else {
                file_id.build_file_path(&self.paths[fd.path_id])
            };
            self.file_system.delete(&path)?;
        }
        // Meanwhile, the new supplemented files from `self.active_files` should
        // be RENAME to stale files with `.raftlog.stale` suffix, to reduce the
        // unnecessary recovery timecost when RESTART.
        {
            let mv_first_seq = std::cmp::max(
                logical_purged_first_seq,
                clear_list.span().0 + clear_list.len() as FileSeq,
            );
            let mv_total_len = std::cmp::min(logical_purged_count, self.stale_files.len());
            for seq in mv_first_seq..mv_first_seq + mv_total_len as FileSeq {
                let file_id = FileId {
                    queue: self.queue,
                    seq,
                };
                let path_id = logical_purged_path_list[(seq - logical_purged_first_seq) as usize];
                let path = file_id.build_file_path(&self.paths[path_id]);
                let stale_path = file_id.build_stale_file_path(&self.paths[path_id]);
                self.file_system.reuse(&path, &stale_path)?;
            }
        }
        Ok(logical_purged_count)
    }

    /// Initialize current file collection by preparing several
    /// stale files for later log recycling in advance.
    ///
    /// Attention, this function only makes sense when
    /// `Config.enable-log-recycle` is true.
    pub fn initialize(&mut self, target_file_size: usize) -> Result<()> {
        let now = Instant::now();
        let stale_capacity = std::cmp::min(
            self.capacity.saturating_sub(self.active_files.len()),
            max_stale_log_count(),
        );
        let mut stale_first_seq: FileSeq = self.stale_files.span().0;
        // If `stale_capacity` > 0, we should prepare stale files for later
        // log recycling in advance.
        if stale_capacity > 0 {
            let (prepare_first_seq, prepare_stale_files_count) =
                match (self.active_files.len(), self.stale_files.len()) {
                    (0, 0) => {
                        // Both stale and active files are empty, it will fully
                        // fill the list of stale files with `capacity`.
                        // Udate first_seq of stale files.
                        stale_first_seq = 1;
                        (1, stale_capacity)
                    }
                    (0, _) => {
                        // Exists several stale files, but no active files found.
                        (self.stale_files.span().1 + 1, stale_capacity)
                    }
                    (_, _) => {
                        // Both exists stale files and active files. It should
                        // fill the list of stale files according to the following
                        // strategy.
                        let active_first_seq = self.active_file_span().0;
                        let exist_stale_count = self.stale_files.len();
                        let max_supply_count =
                            std::cmp::min((active_first_seq - 1) as usize, stale_capacity)
                                .saturating_sub(exist_stale_count);
                        // Calibrate the sequence of existing stale logs.
                        if active_first_seq - max_supply_count as FileSeq
                            != stale_first_seq + exist_stale_count as FileSeq
                        {
                            let expected_first_seq = active_first_seq
                                .saturating_sub((max_supply_count + exist_stale_count) as FileSeq);
                            for idx in 0..exist_stale_count {
                                let src_file_id = FileId {
                                    seq: stale_first_seq + idx as FileSeq,
                                    queue: self.queue,
                                };
                                let dst_file_id = FileId {
                                    seq: expected_first_seq + idx as FileSeq,
                                    queue: self.queue,
                                };
                                let path = &self.paths[self.stale_files.fds[idx].path_id];
                                self.file_system.reuse(
                                    src_file_id.build_stale_file_path(path),
                                    dst_file_id.build_stale_file_path(path),
                                )?;
                            }
                        }
                        // Record the calibrated first seq of stale files
                        stale_first_seq =
                            active_first_seq - (max_supply_count + exist_stale_count) as FileSeq;
                        (
                            active_first_seq - max_supply_count as FileSeq,
                            max_supply_count,
                        )
                    }
                };
            // Update first seq of stale files
            self.stale_files.first_seq = stale_first_seq;
            // Concurrent prepraring will bring more time consumption on racing. So, we just
            // introduce a serial processing for preparing progress.
            for seq in prepare_first_seq..prepare_first_seq + prepare_stale_files_count as FileSeq {
                let file_id = FileId {
                    queue: self.queue,
                    seq,
                };
                self.stale_files.push_back(
                    seq,
                    FileCollection::build_stale_file(
                        self.file_system.as_ref(),
                        &self.paths,
                        file_id,
                        LogFileFormat::new(Version::V2, 0 /* alignment */),
                        target_file_size,
                    )?,
                );
            }
        }
        info!(
            "LogQueue: {:?} preparing stale raft logs takes {:?}, prepared count: {}",
            self.queue,
            now.elapsed(),
            self.stale_files.len()
        );
        Ok(())
    }

    /// Scans all stale files from the specific directory.
    ///
    /// Returns a stale `FileList`.
    pub fn scan_stale_files(file_system: &F, path: &str) -> Result<FileList<F>> {
        let path = Path::new(path);
        debug_assert!(path.exists() && path.is_dir());
        let mut first_seq: FileSeq = 0;
        let (mut min_stale_id, mut max_stale_id) = (u64::MAX, 0);
        fs::read_dir(path)?.for_each(|e| {
            if let Ok(e) = e {
                let p = e.path();
                if p.is_file() {
                    if let Some(FileId {
                        queue: LogQueue::Append,
                        seq,
                    }) = FileId::parse_stale_file_name(p.file_name().unwrap().to_str().unwrap())
                    {
                        min_stale_id = std::cmp::min(min_stale_id, seq);
                        max_stale_id = std::cmp::max(max_stale_id, seq);
                    }
                }
            }
        });
        let mut files: VecDeque<FileWithFormat<F>> = VecDeque::default();
        if max_stale_id > 0 {
            files.reserve((max_stale_id - min_stale_id) as usize + 1);
            for seq in min_stale_id..=max_stale_id {
                let file_id = FileId {
                    queue: LogQueue::Append,
                    seq,
                };
                let path = file_id.build_stale_file_path(path);
                if !path.exists() {
                    files.clear();
                } else {
                    let handle = Arc::new(file_system.open(&path)?);
                    // It's not necessary to record the metadata of stale files.
                    file_system.delete_metadata(&path)?;
                    files.push_back(FileWithFormat {
                        handle,
                        format: LogFileFormat::new(Version::V2, 0 /* alignment */),
                        path_id: 0,
                    });
                }
            }
            first_seq = max_stale_id - files.len() as FileSeq + 1;
        }
        Ok(FileList {
            first_seq,
            fds: files,
        })
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
        let mut file = build_file_writer(file_system, fd.clone(), format, true)?;
        let mut written = LogFileFormat::encoded_len(format.version);
        let buf = vec![0; 4096];
        while written <= target_file_size {
            file.write(&buf, target_file_size)?;
            written += buf.len();
        }
        file.close()?;
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
        let files = active_files.split_by(13);
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
        // 15 | 16
        assert_eq!(1, file_collection.purge_to(16).unwrap());
        assert_eq!(file_collection.len(), 2);
        assert_eq!(file_collection.stale_file_span(), (15, 15));
        assert_eq!(file_collection.active_file_span(), (16, 16));
        // 15 | 16 17 18 19 20
        for i in 17..=20 {
            file_collection.push_back(
                i as FileSeq,
                new_file_handler(&path, FileId::new(LogQueue::Append, i), Version::V2, false),
            );
        }
        // 16 17 18 | 19 20
        assert_eq!(3, file_collection.purge_to(19).unwrap());
        assert_eq!(file_collection.len(), 5);
        assert_eq!(file_collection.stale_file_span(), (16, 18));
        assert_eq!(file_collection.active_file_span(), (19, 20));
        // 17 18 | 19 20 21
        let (file_seq, path_id, fd) = file_collection.rotate(1).unwrap();
        assert_eq!(file_collection.len(), 4);
        assert_eq!(file_seq, 21);
        file_collection.push_back(
            file_seq,
            FileWithFormat {
                handle: fd,
                format: LogFileFormat::new(Version::V2, 0),
                path_id,
            },
        );
        assert_eq!(file_collection.len(), 5);
        assert_eq!(file_collection.stale_file_span(), (17, 18));
        assert_eq!(file_collection.active_file_span(), (19, 21));
    }
}
