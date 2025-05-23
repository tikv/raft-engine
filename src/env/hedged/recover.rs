// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use crate::env::default::LogFile;
use crate::file_pipe_log::log_file::build_file_reader;
use crate::file_pipe_log::pipe_builder::FileName;
use crate::file_pipe_log::reader::LogItemBatchFileReader;
use crate::file_pipe_log::FileNameExt;
use crate::internals::parse_reserved_file_name;
use crate::internals::FileId;
use crate::internals::LogQueue;
use crate::Error;
use std::fs;
use std::io::Result as IoResult;
use std::path::PathBuf;
use std::sync::Arc;

use crate::env::log_fd::LogFd;
use crate::env::DefaultFileSystem;
use crate::env::{FileSystem, Permission};

use super::util::replace_path;

#[derive(Default)]
pub(crate) struct Files {
    pub prefix: PathBuf,
    pub append_files: Vec<SeqFile>,
    pub rewrite_files: Vec<SeqFile>,
    pub reserved_files: Vec<SeqFile>,
}

pub(crate) enum SeqFile {
    Path(FileName),
    Handle((FileName, Arc<LogFd>)),
}

impl SeqFile {
    pub fn seq(&self) -> u64 {
        match self {
            SeqFile::Path(f) => f.seq,
            SeqFile::Handle((f, _)) => f.seq,
        }
    }

    pub fn path(&self) -> &PathBuf {
        match self {
            SeqFile::Path(f) => &f.path,
            SeqFile::Handle((f, _)) => &f.path,
        }
    }

    pub fn remove(&self) -> IoResult<()> {
        match self {
            SeqFile::Path(f) => fs::remove_file(&f.path),
            SeqFile::Handle((f, _)) => fs::remove_file(&f.path),
        }
    }

    pub fn copy(&self, file_system: &DefaultFileSystem, to: &PathBuf) -> IoResult<u64> {
        match self {
            SeqFile::Path(f) => fs::copy(&f.path, to.as_path()),
            SeqFile::Handle((_, fd)) => {
                let mut reader = LogFile::new(fd.clone());
                let mut writer = LogFile::new(Arc::new(file_system.create(to)?));
                std::io::copy(&mut reader, &mut writer)
            }
        }
    }

    pub fn into_handle(mut self, file_system: &DefaultFileSystem) -> Self {
        if let SeqFile::Path(f) = self {
            let fd = Arc::new(file_system.open(&f.path, Permission::ReadOnly).unwrap());
            self = SeqFile::Handle((f, fd));
        }
        self
    }
}

pub(crate) fn catch_up_diff(
    file_system: &Arc<DefaultFileSystem>,
    mut from_files: Files,
    mut to_files: Files,
    skip_rewrite: bool,
) -> IoResult<()> {
    from_files
        .append_files
        .sort_by(|a, b| a.seq().cmp(&b.seq()));
    to_files.append_files.sort_by(|a, b| a.seq().cmp(&b.seq()));
    from_files
        .rewrite_files
        .sort_by(|a, b| a.seq().cmp(&b.seq()));
    to_files.rewrite_files.sort_by(|a, b| a.seq().cmp(&b.seq()));
    from_files
        .reserved_files
        .sort_by(|a, b| a.seq().cmp(&b.seq()));
    to_files
        .reserved_files
        .sort_by(|a, b| a.seq().cmp(&b.seq()));

    let check_files = |from: &Vec<SeqFile>, to: &Vec<SeqFile>| -> IoResult<()> {
        let last_from_seq = from.last().map(|f| f.seq()).unwrap_or(0);

        let mut iter1 = from.iter().peekable();
        let mut iter2 = to.iter().peekable();
        // compare files of from and to, if the file in from is not in to, copy it to
        // to, and if the file in to is not in from, delete it
        loop {
            match (iter1.peek(), iter2.peek()) {
                (None, None) => break,
                (Some(f1), None) => {
                    let to = replace_path(
                        f1.path().as_ref(),
                        from_files.prefix.as_ref(),
                        to_files.prefix.as_ref(),
                    );
                    f1.copy(file_system, &to)?;
                    iter1.next();
                }
                (None, Some(f2)) => {
                    f2.remove()?;
                    iter2.next();
                }
                (Some(f1), Some(f2)) => {
                    match f1.seq().cmp(&f2.seq()) {
                        std::cmp::Ordering::Equal => {
                            // check file size is not enough, treat the last files differently
                            // considering the recycle, always copy the last file
                            // TODO: only copy diff part
                            if f1.seq() == last_from_seq {
                                let to = replace_path(
                                    f1.path().as_ref(),
                                    from_files.prefix.as_ref(),
                                    to_files.prefix.as_ref(),
                                );
                                f1.copy(file_system, &to)?;
                            }
                            iter1.next();
                            iter2.next();
                        }
                        std::cmp::Ordering::Less => {
                            let to = replace_path(
                                f1.path().as_ref(),
                                from_files.prefix.as_ref(),
                                to_files.prefix.as_ref(),
                            );
                            f1.copy(file_system, &to)?;
                            iter1.next();
                        }
                        std::cmp::Ordering::Greater => {
                            f2.remove()?;
                            iter2.next();
                        }
                    }
                }
            }
        }
        Ok(())
    };

    check_files(&from_files.append_files, &to_files.append_files)?;
    if !skip_rewrite {
        check_files(&from_files.rewrite_files, &to_files.rewrite_files)?;
    }
    check_files(&from_files.reserved_files, &to_files.reserved_files)?;
    Ok(())
}

pub(crate) fn get_files(path: &PathBuf) -> IoResult<Files> {
    assert!(path.exists());

    let mut files = Files {
        prefix: path.clone(),
        ..Default::default()
    };

    fs::read_dir(path)
        .unwrap()
        .try_for_each(|e| -> IoResult<()> {
            let dir_entry = e?;
            let p = dir_entry.path();
            if !p.is_file() {
                return Ok(());
            }
            let file_name = p.file_name().unwrap().to_str().unwrap();
            match FileId::parse_file_name(file_name) {
                Some(FileId {
                    queue: LogQueue::Append,
                    seq,
                }) => files.append_files.push(SeqFile::Path(FileName {
                    seq,
                    path: p,
                    path_id: 0,
                })),
                Some(FileId {
                    queue: LogQueue::Rewrite,
                    seq,
                }) => files.rewrite_files.push(SeqFile::Path(FileName {
                    seq,
                    path: p,
                    path_id: 0,
                })),
                _ => {
                    if let Some(seq) = parse_reserved_file_name(file_name) {
                        files.reserved_files.push(SeqFile::Path(FileName {
                            seq,
                            path: p,
                            path_id: 0,
                        }))
                    }
                }
            }
            Ok(())
        })
        .unwrap();

    Ok(files)
}

pub(crate) fn get_latest_valid_seq(
    file_system: &Arc<DefaultFileSystem>,
    files: &Files,
) -> IoResult<usize> {
    let mut count = 0;
    if let Some(f) = files.append_files.last() {
        let recovery_read_block_size = 1024;
        let mut reader = LogItemBatchFileReader::new(recovery_read_block_size);
        let handle = Arc::new(file_system.open(&f.path(), Permission::ReadOnly)?);
        let file_reader = build_file_reader(file_system.as_ref(), handle)?;
        match reader.open(
            FileId {
                queue: LogQueue::Append,
                seq: f.seq(),
            },
            file_reader,
        ) {
            Err(e) => match e {
                Error::Io(err) => return Err(err),
                _ => return Ok(0),
            },
            Ok(_) => {
                // Do nothing
            }
        }
        loop {
            match reader.next() {
                Ok(Some(_)) => {
                    count += 1;
                }
                Ok(None) => break,
                Err(_) => break,
            }
        }
    }

    Ok(count)
}
