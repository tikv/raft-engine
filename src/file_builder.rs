// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Seek, Write};
use std::path::Path;

/// A `FileBuilder` generates file readers or writers that are composed upon
/// existing ones.
pub trait FileBuilder: Send + Sync {
    /// Types of outcome file reader/writer. They must not alter the length of
    /// bytes stream, nor buffer bytes between operations.
    type Reader<R: Seek + Read>: Seek + Read;
    type Writer<W: Seek + Write>: Seek + Write;

    fn build_reader<R>(
        &self,
        path: &Path,
        reader: R,
    ) -> Result<Self::Reader<R>, Box<dyn std::error::Error>>
    where
        R: Seek + Read;

    fn build_writer<W>(
        &self,
        path: &Path,
        writer: W,
        create: bool,
    ) -> Result<Self::Writer<W>, Box<dyn std::error::Error>>
    where
        W: Seek + Write;
}

/// `DefaultFileBuilder` is a `FileBuilder` that builds out the original
/// `Reader`/`Writer` as it is.
pub struct DefaultFileBuilder {}

impl FileBuilder for DefaultFileBuilder {
    type Reader<R: Seek + Read> = R;
    type Writer<W: Seek + Write> = W;

    fn build_reader<R>(
        &self,
        _path: &Path,
        reader: R,
    ) -> Result<Self::Reader<R>, Box<dyn std::error::Error>>
    where
        R: Seek + Read,
    {
        Ok(reader)
    }

    fn build_writer<W>(
        &self,
        _path: &Path,
        writer: W,
        _create: bool,
    ) -> Result<Self::Writer<W>, Box<dyn std::error::Error>>
    where
        W: Seek + Write,
    {
        Ok(writer)
    }
}
