// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result, Seek, Write};
use std::path::Path;
use std::sync::Arc;

mod default;
mod obfuscated;

pub use default::DefaultFileSystem;
pub use obfuscated::ObfuscatedFileSystem;

/// FileSystem
pub trait FileSystem: Send + Sync {
    type Handle: Send + Sync + Handle;
    type Reader: Seek + Read + Send;
    type Writer: Seek + Write + Send + WriteExt;

    fn create<P: AsRef<Path>>(&self, path: P) -> Result<Self::Handle>;

    fn open<P: AsRef<Path>>(&self, path: P) -> Result<Self::Handle>;

    fn delete<P: AsRef<Path>>(&self, path: P) -> Result<()>;

    /// Deletes user implemented metadata associated with `path`. Returns
    /// `true` if any metadata is deleted.
    ///
    /// In older versions of Raft Engine, physical files are deleted without
    /// going through user implemented cleanup procedure. This method is used to
    /// detect and cleanup the user metadata that is no longer mapped to a
    /// physical file.
    fn delete_metadata<P: AsRef<Path>>(&self, _path: P) -> Result<()> {
        Ok(())
    }

    /// Returns whether there is any user metadata associated with given `path`.
    fn exists_metadata<P: AsRef<Path>>(&self, _path: P) -> bool {
        false
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> Result<Self::Reader>;

    fn new_writer(&self, handle: Arc<Self::Handle>) -> Result<Self::Writer>;
}

pub trait Handle {
    fn truncate(&self, offset: usize) -> Result<()>;

    /// Returns the current size of this file.
    fn file_size(&self) -> Result<usize>;
}

/// WriteExt is writer extension api
pub trait WriteExt {
    fn truncate(&mut self, offset: usize) -> Result<()>;
    fn sync(&mut self) -> Result<()>;
    fn allocate(&mut self, offset: usize, size: usize) -> Result<()>;
}
