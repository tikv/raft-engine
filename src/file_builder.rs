// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Seek, Write};
use std::path::Path;

/// A `FileBuilder` generates file readers or writers that are composed upon
/// existing ones.
pub trait FileBuilder: Send + Sync {
    /// Types of outcome file reader/writer. They must not alter the length of
    /// bytes stream, nor buffer bytes between operations.
    type Reader<R: Seek + Read + Send>: Seek + Read + Send;
    type Writer<W: Seek + Write + Send>: Seek + Write + Send;

    fn build_reader<R>(&self, path: &Path, reader: R) -> Result<Self::Reader<R>, std::io::Error>
    where
        R: Seek + Read + Send;

    fn build_writer<W>(
        &self,
        path: &Path,
        writer: W,
        create: bool,
    ) -> Result<Self::Writer<W>, std::io::Error>
    where
        W: Seek + Write + Send;
}

/// `DefaultFileBuilder` is a `FileBuilder` that builds out the original
/// `Reader`/`Writer` as it is.
pub struct DefaultFileBuilder {}

impl FileBuilder for DefaultFileBuilder {
    type Reader<R: Seek + Read + Send> = R;
    type Writer<W: Seek + Write + Send> = W;

    fn build_reader<R>(&self, _path: &Path, reader: R) -> Result<Self::Reader<R>, std::io::Error>
    where
        R: Seek + Read + Send,
    {
        Ok(reader)
    }

    fn build_writer<W>(
        &self,
        _path: &Path,
        writer: W,
        _create: bool,
    ) -> Result<Self::Writer<W>, std::io::Error>
    where
        W: Seek + Write + Send,
    {
        Ok(writer)
    }
}
