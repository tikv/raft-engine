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
    fn new_reader(&self, handle: Arc<Self::Handle>) -> Result<Self::Reader>;
    fn new_writer(&self, handle: Arc<Self::Handle>) -> Result<Self::Writer>;
}

pub trait Handle {
    fn truncate(&self, offset: usize) -> Result<()>;
    fn file_size(&self) -> Result<usize>;
}

/// WriteExt is writer extension api
pub trait WriteExt {
    fn truncate(&self, offset: usize) -> Result<()>;
    fn sync(&self) -> Result<()>;
    fn allocate(&self, offset: usize, size: usize) -> Result<()>;
}
