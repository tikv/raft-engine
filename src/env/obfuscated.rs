// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::env::{DefaultFileSystem, FileSystem, WriteExt};

pub struct ObfuscatedReader(<DefaultFileSystem as FileSystem>::Reader);

impl Read for ObfuscatedReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        if !buf.is_empty() {
            let len = self.0.read(&mut buf[..1])?;
            if len == 1 {
                buf[0] = buf[0].wrapping_sub(1);
            }
            Ok(len)
        } else {
            Ok(0)
        }
    }
}

impl Seek for ObfuscatedReader {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

pub struct ObfuscatedWriter(<DefaultFileSystem as FileSystem>::Writer);

impl Write for ObfuscatedWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        if !buf.is_empty() {
            let tmp_vec = vec![buf[0].wrapping_add(1)];
            self.0.write(&tmp_vec)
        } else {
            Ok(0)
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        self.0.flush()
    }
}

impl Seek for ObfuscatedWriter {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        self.0.seek(pos)
    }
}

impl WriteExt for ObfuscatedWriter {
    fn truncate(&mut self, offset: usize) -> IoResult<()> {
        self.0.truncate(offset)
    }

    fn sync(&mut self) -> IoResult<()> {
        self.0.sync()
    }

    fn allocate(&mut self, offset: usize, size: usize) -> IoResult<()> {
        self.0.allocate(offset, size)
    }
}

/// `[ObfuscatedFileSystem]` is a special implementation of `[FileSystem]`,
/// which is used for constructing and simulating an abnormal file system for
/// `[Read]` and `[Write]`.
pub struct ObfuscatedFileSystem(DefaultFileSystem);
impl Default for ObfuscatedFileSystem {
    fn default() -> Self {
        ObfuscatedFileSystem(DefaultFileSystem)
    }
}

impl FileSystem for ObfuscatedFileSystem {
    type Handle = <DefaultFileSystem as FileSystem>::Handle;
    type Reader = ObfuscatedReader;
    type Writer = ObfuscatedWriter;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        self.0.create(path)
    }

    fn open<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        self.0.open(path)
    }

    fn rename<P: AsRef<Path>>(&self, src: P, dst: P) -> IoResult<()> {
        self.0.rename(src, dst)
    }

    fn new_reader(&self, inner: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        Ok(ObfuscatedReader(self.0.new_reader(inner)?))
    }

    fn new_writer(&self, inner: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        Ok(ObfuscatedWriter(self.0.new_writer(inner)?))
    }
}
