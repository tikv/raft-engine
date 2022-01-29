// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result, Seek, Write};
use std::path::Path;

/// A `FileBuilder` generates file readers or writers that are stacked over
/// existing ones.
pub trait FileBuilder: Send + Sync {
    /// Types of output file reader/writer. They must not alter the length of
    /// bytes stream, nor buffer bytes between operations.
    type Reader<R: Seek + Read + Send>: Seek + Read + Send;
    type Writer<W: Seek + Write + Send>: Seek + Write + Send;

    /// Creates a new `Reader` stacked on top of `reader`.
    fn build_reader<R>(&self, path: &Path, reader: R) -> Result<Self::Reader<R>>
    where
        R: Seek + Read + Send;

    /// Creates a new `Writer` stacked on top of `writer`.
    fn build_writer<W>(&self, path: &Path, writer: W, create: bool) -> Result<Self::Writer<W>>
    where
        W: Seek + Write + Send;
}

/// `DefaultFileBuilder` is a [`FileBuilder`] that builds out the original
/// reader or writer as it is.
pub struct DefaultFileBuilder;

impl FileBuilder for DefaultFileBuilder {
    type Reader<R: Seek + Read + Send> = R;
    type Writer<W: Seek + Write + Send> = W;

    fn build_reader<R>(&self, _path: &Path, reader: R) -> Result<Self::Reader<R>>
    where
        R: Seek + Read + Send,
    {
        Ok(reader)
    }

    fn build_writer<W>(&self, _path: &Path, writer: W, _create: bool) -> Result<Self::Writer<W>>
    where
        W: Seek + Write + Send,
    {
        Ok(writer)
    }
}
