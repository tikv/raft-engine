// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Arc;

use crate::env::{DefaultFileSystem, FileSystem, WriteExt};

pub struct ObfuscatedReader(<DefaultFileSystem as FileSystem>::Reader);

impl Read for ObfuscatedReader {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let len = self.0.read(buf)?;
        for c in buf {
            *c = c.wrapping_sub(1);
        }
        Ok(len)
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
        let mut new_buf = buf.to_owned();
        for c in &mut new_buf {
            *c = c.wrapping_add(1);
        }
        self.0.write(&new_buf)
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
    fn truncate(&self, offset: usize) -> IoResult<()> {
        self.0.truncate(offset)
    }

    fn sync(&self) -> IoResult<()> {
        self.0.sync()
    }

    fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
        self.0.allocate(offset, size)
    }
}

pub struct ObfuscatedFileSystem(DefaultFileSystem);

impl ObfuscatedFileSystem {
    #[allow(dead_code)]
    pub fn new() -> ObfuscatedFileSystem {
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

    fn new_reader(&self, inner: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        Ok(ObfuscatedReader(self.0.new_reader(inner)?))
    }

    fn new_writer(&self, inner: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        Ok(ObfuscatedWriter(self.0.new_writer(inner)?))
    }
}
