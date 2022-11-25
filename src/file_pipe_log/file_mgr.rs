// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::collections::VecDeque;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use log::info;

use crate::config::Config;
use crate::env::FileSystem;
use crate::pipe_log::{FileId, FileSeq, LogQueue, Version};
use crate::{Error, Result};

use super::format::{max_dummy_log_count, DummyFileExt, LogFileFormat};
use super::log_file::build_file_writer;

#[derive(Debug)]
pub struct FileWithFormat<F: FileSystem> {
    pub handle: Arc<F::Handle>,
    pub format: LogFileFormat,
}

#[derive(Clone, PartialEq, Eq, Debug)]
pub struct FileState {
    pub first_seq: FileSeq,
    pub total_len: usize,
}

/// A collection of files for managing active files.
pub struct ActiveFileCollection<F: FileSystem> {
    /// Sequence number of the first file.
    pub first_seq: FileSeq,
    pub fds: VecDeque<FileWithFormat<F>>,
}

impl<F: FileSystem> ActiveFileCollection<F> {
    #[inline]
    pub fn new(first_seq: FileSeq, fds: VecDeque<FileWithFormat<F>>) -> Self {
        Self { first_seq, fds }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.fds.len()
    }

    #[inline]
    pub fn get_fd(&self, file_seq: FileSeq) -> Result<Arc<F::Handle>> {
        if !(self.first_seq..self.first_seq + self.fds.len() as u64).contains(&file_seq) {
            return Err(Error::Corruption("file seqno out of range".to_owned()));
        }
        Ok(self.fds[(file_seq - self.first_seq) as usize]
            .handle
            .clone())
    }

    #[inline]
    pub fn file_span(&self) -> (FileSeq, FileSeq) {
        if !self.fds.is_empty() {
            (self.first_seq, self.first_seq + self.fds.len() as u64 - 1)
        } else {
            (0, 0)
        }
    }

    #[inline]
    pub fn push(&mut self, file: FileWithFormat<F>) -> FileState {
        self.fds.push_back(file);
        FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        }
    }

    #[inline]
    pub fn logical_purge(
        &mut self,
        file_seq: FileSeq,
    ) -> (FileState, FileState, VecDeque<FileWithFormat<F>>) {
        let prev = FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        };
        if (self.first_seq..self.first_seq + self.fds.len() as u64).contains(&file_seq) {
            let purged = file_seq.saturating_sub(self.first_seq);
            let purged_files = self.fds.drain(..purged as usize).collect();
            self.first_seq = file_seq;
            let current = FileState {
                first_seq: self.first_seq,
                total_len: self.fds.len(),
            };
            return (prev, current, purged_files);
        }
        (prev.clone(), prev, VecDeque::<_>::default())
    }
}

/// A collection of files for managing stale files.
///
/// Stale files are named with `.raftlog.dummy` suffix,
/// coming from two parts:
/// - `Dummy` part, prepared when starting.
/// - `Expired` part, obsolete log files manually purged by callers.
pub struct StaleFileCollection<F: FileSystem> {
    /// Sequence number of the first file.
    pub first_seq: FileSeq,
    pub fds: VecDeque<FileWithFormat<F>>,
}

impl<F: FileSystem> StaleFileCollection<F> {
    #[inline]
    pub fn new(first_seq: FileSeq, fds: VecDeque<FileWithFormat<F>>) -> Self {
        Self { first_seq, fds }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.fds.len()
    }

    #[inline]
    #[cfg(test)]
    pub fn push(&mut self, file: FileWithFormat<F>) -> FileState {
        self.fds.push_back(file);
        FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        }
    }

    #[inline]
    pub fn file_span(&self) -> (FileSeq, FileSeq) {
        if !self.fds.is_empty() {
            (self.first_seq, self.first_seq + self.fds.len() as u64 - 1)
        } else {
            (0, 0)
        }
    }

    #[inline]
    pub fn recycle_one_file(&mut self) -> Option<FileSeq> {
        if !self.fds.is_empty() {
            let seq = self.first_seq;
            self.fds.pop_front().unwrap();
            self.first_seq += 1;
            return Some(seq);
        }
        None
    }

    #[inline]
    pub fn logical_purge(&mut self, capacity: usize) -> (FileState, FileState) {
        let prev = FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        };
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
        self.first_seq += purged as u64;
        self.fds.drain(..purged);
        let current = FileState {
            first_seq: self.first_seq,
            total_len: self.fds.len(),
        };
        (prev, current)
    }

    /// Concatenates the given files into stale files list.
    ///
    /// This function shoud only be used for moving inactive files from
    /// `ActiveFileCollection` into `StaleFileCollection`.
    #[inline]
    pub fn concat(&mut self, file_seq: FileSeq, mut files: VecDeque<FileWithFormat<F>>) {
        if !files.is_empty() {
            let (_, last) = self.file_span();
            if last == 0 {
                self.first_seq = file_seq;
            } else {
                debug_assert_eq!(last + 1, file_seq);
            }
            self.fds.append(&mut files);
        }
    }

    /// Scans all dummy files from the specific directory.
    ///
    /// Returns the first `FileSeq` of dummy files and the related file list by
    /// `VecDeque`.
    pub fn scan_dummpy_files(
        file_system: &F,
        path: &str,
    ) -> Result<(FileSeq, VecDeque<FileWithFormat<F>>)> {
        let path = Path::new(path);
        debug_assert!(path.exists() && path.is_dir());
        let mut first_seq: FileSeq = 0;
        let (mut min_dummy_id, mut max_dummy_id) = (u64::MAX, 0);
        fs::read_dir(path)?.for_each(|e| {
            if let Ok(e) = e {
                let p = e.path();
                if p.is_file() {
                    if let Some(FileId {
                        queue: LogQueue::Append,
                        seq,
                    }) = FileId::parse_dummy_file_name(p.file_name().unwrap().to_str().unwrap())
                    {
                        min_dummy_id = std::cmp::min(min_dummy_id, seq);
                        max_dummy_id = std::cmp::max(max_dummy_id, seq);
                    }
                }
            }
        });
        let mut files: VecDeque<FileWithFormat<F>> = VecDeque::default();
        if max_dummy_id > 0 {
            files.reserve((max_dummy_id - min_dummy_id) as usize + 1);
            for seq in min_dummy_id..=max_dummy_id {
                let file_id = FileId {
                    queue: LogQueue::Append,
                    seq,
                };
                let path = file_id.build_dummy_file_path(path);
                if !path.exists() {
                    files.clear();
                } else {
                    let handle = Arc::new(file_system.open(&path)?);
                    // It's not necessary to record the metadata of dummy files.
                    file_system.delete_metadata(&path)?;
                    files.push_back(FileWithFormat {
                        handle,
                        format: LogFileFormat::new(Version::default(), 0),
                    });
                }
            }
            first_seq = max_dummy_id - files.len() as FileSeq + 1;
        }
        Ok((first_seq, files))
    }

    /// Prepares several dummy files for later log recycling.
    ///
    /// Returns the first `FileSeq` of dummy files.
    /// Attention, this function is only called when `Config.enable-log-recycle`
    /// is true.
    pub fn prepare_dummy_logs_for_recycle(
        cfg: &Config,
        file_system: &F,
        queue: LogQueue,
        capacity: usize,
        first_seq: FileSeq,
        dummy_first_seq: FileSeq,
        dummy_files: &mut VecDeque<FileWithFormat<F>>,
    ) -> Result<FileSeq> {
        let now = Instant::now();
        let capacity = std::cmp::min(capacity, max_dummy_log_count());
        let mut dummy_first_seq: FileSeq = dummy_first_seq;
        if capacity > 0 {
            let (prepare_first_seq, prepare_stale_files_count) = match first_seq {
                0 => {
                    // Update first_seq
                    dummy_first_seq = 1;
                    (1, capacity)
                }
                seq => {
                    let max_supply_count = std::cmp::min((seq - 1) as usize, capacity)
                        .saturating_sub(dummy_files.len());
                    // Calibrate the sequence of existing dummy logs.
                    if seq - max_supply_count as FileSeq
                        != dummy_first_seq + dummy_files.len() as FileSeq
                    {
                        let expected_first_seq =
                            seq.saturating_sub((max_supply_count + dummy_files.len()) as FileSeq);
                        for idx in 0..dummy_files.len() {
                            let src_file_id = FileId {
                                seq: dummy_first_seq + idx as FileSeq,
                                queue,
                            };
                            let dst_file_id = FileId {
                                seq: expected_first_seq + idx as FileSeq,
                                queue,
                            };
                            file_system.rename(
                                src_file_id.build_dummy_file_path(&cfg.dir),
                                dst_file_id.build_dummy_file_path(&cfg.dir),
                            )?;
                        }
                    }
                    // Update first seq of dummy files
                    dummy_first_seq = seq - (max_supply_count + dummy_files.len()) as FileSeq;
                    (seq - max_supply_count as FileSeq, max_supply_count)
                }
            };
            // Concurrent prepraring will bring more time consumption on racing. So, we just
            // introduce a serial processing for preparing progress.
            for seq in prepare_first_seq..prepare_first_seq + prepare_stale_files_count as FileSeq {
                let file_id = FileId {
                    queue: LogQueue::Append,
                    seq,
                };
                dummy_files.push_back(
                    StaleFileCollection::gen_fake_file(
                        file_system,
                        &cfg.dir,
                        file_id,
                        cfg.format_version,
                        cfg.target_file_size.0,
                    )
                    .unwrap(),
                );
            }
        }
        info!(
            "preparing dummy raft logs takes {:?}, prepared count: {}",
            now.elapsed(),
            dummy_files.len()
        );
        Ok(dummy_first_seq)
    }

    /// Generates a Fake log used for recycling.
    ///
    /// Attention, this function is only called when `Config.enable-log-recycle`
    /// is true.
    fn gen_fake_file(
        file_system: &F,
        path: &str,
        file_id: FileId,
        version: Version,
        target_file_size: u64,
    ) -> Result<FileWithFormat<F>> {
        let format = LogFileFormat::new(version, 0 /* alignment */);
        let file_path = file_id.build_dummy_file_path(path);
        let fd = Arc::new(file_system.create(&file_path)?);
        let mut file = build_file_writer(file_system, fd.clone(), format, true)?;
        let mut written = LogFileFormat::encoded_len(version) as u64;
        let buf = vec![0; 4096];
        while written <= target_file_size {
            file.write(&buf, target_file_size as usize)?;
            written += buf.len() as u64;
        }
        file.close()?;
        // Metadata of dummy files are not what we're truely concerned. So,
        // they can be ignored by clear them here.
        Ok(FileWithFormat { handle: fd, format })
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
        ) -> FileWithFormat<DefaultFileSystem> {
            FileWithFormat {
                handle: Arc::new(
                    DefaultFileSystem
                        .create(&file_id.build_file_path(path))
                        .unwrap(),
                ),
                format: LogFileFormat::new(version, 0 /* alignment */),
            }
        }
        let dir = Builder::new()
            .prefix("test_file_collection")
            .tempdir()
            .unwrap();
        let path = dir.path().to_str().unwrap();
        // | 12
        let mut active_files = ActiveFileCollection::new(
            12,
            vec![new_file_handler(
                path,
                FileId::new(LogQueue::Append, 12),
                Version::V2,
            )]
            .into(),
        );
        let mut stale_files = StaleFileCollection::new(11, VecDeque::default());
        assert_eq!(stale_files.len(), 0);
        assert_eq!(stale_files.file_span(), (0, 0));
        assert_eq!(stale_files.recycle_one_file(), None);
        // 11 | 12
        stale_files.push(new_file_handler(
            path,
            FileId::new(LogQueue::Append, 11),
            Version::V2,
        ));
        // 11 | 12 13
        active_files.push(new_file_handler(
            path,
            FileId::new(LogQueue::Append, 13),
            Version::V2,
        ));
        // 11 12 | 13
        let (prev, curr, files) = active_files.logical_purge(13);
        assert_eq!(
            prev,
            FileState {
                first_seq: 12,
                total_len: 2
            }
        );
        assert_eq!(curr.total_len, 1);
        assert_eq!(files.len(), 1);
        stale_files.concat(prev.first_seq, files);
        assert_eq!(stale_files.file_span(), (11, 12));
        // 11 12 | 13 14
        active_files.push(new_file_handler(
            path,
            FileId::new(LogQueue::Append, 14),
            Version::V1,
        ));
        assert_eq!(stale_files.recycle_one_file().unwrap(), 11);
        assert_eq!(stale_files.file_span(), (12, 12));
        // 12 | 13 14 15
        active_files.push(new_file_handler(
            path,
            FileId::new(LogQueue::Append, 15),
            Version::V2,
        ));
        // 12 13 14 | 15
        let (prev, curr, files) = active_files.logical_purge(15);
        assert_eq!(curr.total_len, 1);
        assert_eq!(active_files.len(), 1);
        stale_files.concat(prev.first_seq, files);
        // V1 file will not be kept around.
        let (prev, curr) = stale_files.logical_purge(2);
        assert_eq!(
            prev,
            FileState {
                first_seq: 12,
                total_len: 3,
            }
        );
        assert_eq!(curr.total_len, 0);
        assert_eq!(stale_files.recycle_one_file(), None);
        // | 15 16 17 18 19 20
        for i in 16..=20 {
            active_files.push(new_file_handler(
                path,
                FileId::new(LogQueue::Append, i),
                Version::V2,
            ));
        }
        assert_eq!(stale_files.recycle_one_file(), None);
        // 15 16 17 18 | 19 20
        let (prev, curr, files) = active_files.logical_purge(19);
        assert_eq!(curr.total_len, 2);
        assert_eq!(active_files.len(), 2);
        stale_files.concat(prev.first_seq, files);
        assert_eq!(stale_files.file_span(), (15, 18));
        // 16 17 18 | 19 20
        assert_eq!(stale_files.recycle_one_file().unwrap(), 15);
    }
}
