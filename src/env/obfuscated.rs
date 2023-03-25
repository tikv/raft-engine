// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::env::{DefaultFileSystem, FileSystem, WriteExt};

use crate::pipe_log::FileBlockHandle;
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

    fn allocate(&mut self, offset: usize, size: usize) -> IoResult<()> {
        self.0.allocate(offset, size)
    }
}

/// `[ObfuscatedFileSystem]` is a special implementation of `[FileSystem]`,
/// which is used for constructing and simulating an abnormal file system for
/// `[Read]` and `[Write]`.
pub struct ObfuscatedFileSystem {
    inner: DefaultFileSystem,
    files: AtomicUsize,
}

impl Default for ObfuscatedFileSystem {
    fn default() -> Self {
        ObfuscatedFileSystem {
            inner: DefaultFileSystem,
            files: AtomicUsize::new(0),
        }
    }
}

impl ObfuscatedFileSystem {
    pub fn file_count(&self) -> usize {
        self.files.load(Ordering::Relaxed)
    }
}

impl FileSystem for ObfuscatedFileSystem {
    type Handle = <DefaultFileSystem as FileSystem>::Handle;
    type Reader = ObfuscatedReader;
    type Writer = ObfuscatedWriter;
    type MultiReadContext = <DefaultFileSystem as FileSystem>::MultiReadContext;

    fn multi_read(
        &self,
        ctx: &mut Self::MultiReadContext,
        handle: Arc<Self::Handle>,
        block: &FileBlockHandle,
    ) -> IoResult<()> {
        self.inner.multi_read(ctx, handle, block)
    }

    fn async_finish(&self, ctx: Self::MultiReadContext) -> IoResult<Vec<Vec<u8>>> {
        let mut base = self.inner.async_finish(ctx).unwrap();

        for v in base.iter_mut() {
            for c in v.iter_mut() {
                // do obfuscation.
                *c = c.wrapping_sub(1);
            }
        }
        Ok(base)
    }

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        let r = self.inner.create(path);
        if r.is_ok() {
            self.files.fetch_add(1, Ordering::Relaxed);
        }
        r
    }

    fn open<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        self.inner.open(path)
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        let r = self.inner.delete(path);
        if r.is_ok() {
            self.files.fetch_sub(1, Ordering::Relaxed);
        }
        r
    }

    fn rename<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> IoResult<()> {
        self.inner.rename(src_path, dst_path)
    }

    fn reuse<P: AsRef<Path>>(&self, src_path: P, dst_path: P) -> IoResult<()> {
        self.delete(src_path)?;
        self.create(dst_path)?;
        Ok(())
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        Ok(ObfuscatedReader(self.inner.new_reader(handle)?))
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        Ok(ObfuscatedWriter(self.inner.new_writer(handle)?))
    }

    fn new_async_io_context(&self) -> IoResult<Self::MultiReadContext> {
        self.inner.new_async_io_context()
    }
}
