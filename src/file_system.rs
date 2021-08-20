// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Seek, Write};
use std::path::Path;

pub trait Readable: Seek + Read + Send + Sync {}
impl<T: Seek + Read + Send + Sync> Readable for T {}

pub trait Writable: Seek + Write + Send + Sync {}
impl<T: Seek + Write + Send + Sync> Writable for T {}

/// An overlay abstraction for accessing files.
pub trait FileSystem: Send + Sync {
    fn open_file_reader(
        &self,
        path: &Path,
        reader: Box<dyn Readable>,
    ) -> Result<Box<dyn Readable>, Box<dyn std::error::Error>>;

    fn open_file_writer(
        &self,
        path: &Path,
        writer: Box<dyn Writable>,
    ) -> Result<Box<dyn Writable>, Box<dyn std::error::Error>>;

    fn create_file_writer(
        &self,
        path: &Path,
        writer: Box<dyn Writable>,
    ) -> Result<Box<dyn Writable>, Box<dyn std::error::Error>>;
}
