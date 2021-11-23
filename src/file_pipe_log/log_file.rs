// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{Read, Result as IoResult, Seek, SeekFrom, Write};
use std::os::unix::io::RawFd;
use std::path::Path;
use std::sync::Arc;

use fail::fail_point;
use log::error;
use nix::errno::Errno;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{close, ftruncate, lseek, Whence};
use nix::NixPath;

use crate::file_builder::FileBuilder;
use crate::metrics::*;
use crate::FileBlockHandle;
use crate::Result;

use super::format::LogFileHeader;

const FILE_ALLOCATE_SIZE: usize = 2 * 1024 * 1024;

/// A `LogFd` is a RAII file that provides basic I/O functionality.
///
/// This implementation is a thin wrapper around `RawFd`, and primarily targets
/// UNIX-based systems.
pub struct LogFd(RawFd);

fn from_nix_error(e: nix::Error, custom: &'static str) -> std::io::Error {
    match e {
        nix::Error::Sys(no) => {
            let kind = std::io::Error::from(no).kind();
            std::io::Error::new(kind, custom)
        }
        e => std::io::Error::new(std::io::ErrorKind::Other, format!("{}: {:?}", custom, e)),
    }
}

impl LogFd {
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        fail_point!("log_fd::open::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        let flags = OFlag::O_RDWR;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        fail_point!("log_fd::open::fadvise_dontneed", |_| {
            let fd = LogFd(fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?);
            #[cfg(target_os = "linux")]
            unsafe {
                extern crate libc;
                libc::posix_fadvise64(fd.0, 0, fd.file_size()? as i64, libc::POSIX_FADV_DONTNEED);
            }
            Ok(fd)
        });
        Ok(LogFd(
            fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?,
        ))
    }

    pub fn create<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        fail_point!("log_fd::create::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        let flags = OFlag::O_RDWR | OFlag::O_CREAT;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        let fd = fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?;
        Ok(LogFd(fd))
    }

    pub fn close(&self) -> IoResult<()> {
        fail_point!("log_fd::close::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        close(self.0).map_err(|e| from_nix_error(e, "close"))
    }

    pub fn sync(&self) -> IoResult<()> {
        fail_point!("log_fd::sync::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        #[cfg(target_os = "linux")]
        {
            nix::unistd::fdatasync(self.0).map_err(|e| from_nix_error(e, "fdatasync"))
        }
        #[cfg(not(target_os = "linux"))]
        {
            nix::unistd::fsync(self.0).map_err(|e| from_nix_error(e, "fsync"))
        }
    }

    pub fn read(&self, mut offset: usize, buf: &mut [u8]) -> IoResult<usize> {
        let mut readed = 0;
        while readed < buf.len() {
            fail_point!("log_fd::read::err", |_| {
                Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
            });
            let bytes = match pread(self.0, &mut buf[readed..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
                Err(e) => return Err(from_nix_error(e, "pread")),
            };
            // EOF
            if bytes == 0 {
                break;
            }
            readed += bytes;
            offset += bytes;
        }
        Ok(readed)
    }

    pub fn write(&self, mut offset: usize, content: &[u8]) -> IoResult<usize> {
        fail_point!("log_fd::write::zero", |_| { Ok(0) });
        let mut written = 0;
        while written < content.len() {
            let bytes = match pwrite(self.0, &content[written..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
                Err(e) => return Err(from_nix_error(e, "pwrite")),
            };
            if bytes == 0 {
                break;
            }
            written += bytes;
            offset += bytes;
        }
        fail_point!("log_fd::write::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        Ok(written)
    }

    pub fn file_size(&self) -> IoResult<usize> {
        fail_point!("log_fd::file_size::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        lseek(self.0, 0, Whence::SeekEnd)
            .map(|n| n as usize)
            .map_err(|e| from_nix_error(e, "lseek"))
    }

    pub fn truncate(&self, offset: usize) -> IoResult<()> {
        fail_point!("log_fd::truncate::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        ftruncate(self.0, offset as i64).map_err(|e| from_nix_error(e, "ftruncate"))
    }

    #[allow(unused_variables)]
    pub fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
        fail_point!("log_fd::allocate::err", |_| {
            Err(from_nix_error(nix::Error::invalid_argument(), "fp"))
        });
        #[cfg(target_os = "linux")]
        {
            fcntl::fallocate(
                self.0,
                fcntl::FallocateFlags::empty(),
                offset as i64,
                size as i64,
            )
            .map_err(|e| from_nix_error(e, "fallocate"))
        }
        #[cfg(not(target_os = "linux"))]
        {
            Ok(())
        }
    }
}

impl Drop for LogFd {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("error while closing file: {}", e);
        }
    }
}

/// A `LogFile` is a `LogFd` wrapper that implements `Seek`, `Write` and `Read`.
pub struct LogFile {
    inner: Arc<LogFd>,
    offset: usize,
}

impl LogFile {
    pub fn new(fd: Arc<LogFd>) -> Self {
        Self {
            inner: fd,
            offset: 0,
        }
    }
}

impl Write for LogFile {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let len = self.inner.write(self.offset, buf)?;
        self.offset += len;
        Ok(len)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Read for LogFile {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let len = self.inner.read(self.offset, buf)?;
        self.offset += len;
        Ok(len)
    }
}

impl Seek for LogFile {
    fn seek(&mut self, pos: SeekFrom) -> IoResult<u64> {
        match pos {
            SeekFrom::Start(offset) => self.offset = offset as usize,
            SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as usize,
            SeekFrom::End(i) => self.offset = (self.inner.file_size()? as i64 + i) as usize,
        }
        Ok(self.offset as u64)
    }
}

pub fn build_file_writer<B: FileBuilder>(
    builder: &B,
    path: &Path,
    fd: Arc<LogFd>,
    create: bool,
) -> Result<LogFileWriter<B>> {
    let raw_writer = LogFile::new(fd.clone());
    let writer = builder.build_writer(path, raw_writer, create)?;
    LogFileWriter::open(fd, writer)
}

/// Append-only writer for log file.
pub struct LogFileWriter<B: FileBuilder> {
    fd: Arc<LogFd>,
    writer: B::Writer<LogFile>,

    written: usize,
    capacity: usize,
    last_sync: usize,
}

impl<B: FileBuilder> LogFileWriter<B> {
    fn open(fd: Arc<LogFd>, writer: B::Writer<LogFile>) -> Result<Self> {
        let file_size = fd.file_size()?;
        let mut f = Self {
            fd,
            writer,
            written: file_size,
            capacity: file_size,
            last_sync: file_size,
        };
        if file_size < LogFileHeader::len() {
            f.write_header()?;
        } else {
            f.writer.seek(SeekFrom::Start(file_size as u64))?;
        }
        Ok(f)
    }

    fn write_header(&mut self) -> Result<()> {
        self.writer.seek(SeekFrom::Start(0))?;
        self.written = 0;
        let mut buf = Vec::with_capacity(LogFileHeader::len());
        LogFileHeader::default().encode(&mut buf)?;
        self.write(&buf, 0)
    }

    pub fn close(&mut self) -> Result<()> {
        // Necessary to truncate extra zeros from fallocate().
        self.truncate()?;
        self.sync()
    }

    pub fn truncate(&mut self) -> Result<()> {
        if self.written < self.capacity {
            self.fd.truncate(self.written)?;
            self.capacity = self.written;
        }
        Ok(())
    }

    pub fn write(&mut self, buf: &[u8], target_size_hint: usize) -> Result<()> {
        let new_written = self.written + buf.len();
        if self.capacity < new_written {
            let _t = StopWatch::new(&LOG_ALLOCATE_DURATION_HISTOGRAM);
            let alloc = std::cmp::max(
                new_written - self.capacity,
                std::cmp::min(
                    FILE_ALLOCATE_SIZE,
                    target_size_hint.saturating_sub(self.capacity),
                ),
            );
            self.fd.allocate(self.capacity, alloc)?;
            self.capacity += alloc;
        }
        self.writer.write_all(buf)?;
        self.written = new_written;
        Ok(())
    }

    pub fn sync(&mut self) -> Result<()> {
        if self.last_sync < self.written {
            let _t = StopWatch::new(&LOG_SYNC_DURATION_HISTOGRAM);
            self.fd.sync()?;
            self.last_sync = self.written;
        }
        Ok(())
    }

    #[inline]
    pub fn since_last_sync(&self) -> usize {
        self.written - self.last_sync
    }

    #[inline]
    pub fn offset(&self) -> usize {
        self.written
    }
}

pub fn build_file_reader<B: FileBuilder>(
    builder: &B,
    path: &Path,
    fd: Arc<LogFd>,
) -> Result<LogFileReader<B>> {
    let raw_reader = LogFile::new(fd.clone());
    let reader = builder.build_reader(path, raw_reader)?;
    LogFileReader::open(fd, reader)
}

/// Random-access reader for log file.
pub struct LogFileReader<B: FileBuilder> {
    fd: Arc<LogFd>,
    reader: B::Reader<LogFile>,

    offset: usize,
}

impl<B: FileBuilder> LogFileReader<B> {
    fn open(fd: Arc<LogFd>, reader: B::Reader<LogFile>) -> Result<Self> {
        Ok(Self {
            fd,
            reader,
            // Set to an invalid offset to force a reseek at first read.
            offset: usize::MAX,
        })
    }

    pub fn read(&mut self, handle: FileBlockHandle) -> Result<Vec<u8>> {
        let mut buf = vec![0; handle.len];
        let size = self.read_to(handle.offset, &mut buf)?;
        buf.truncate(size);
        Ok(buf)
    }

    pub fn read_to(&mut self, offset: u64, buffer: &mut [u8]) -> Result<usize> {
        if offset != self.offset as u64 {
            self.reader.seek(SeekFrom::Start(offset))?;
            self.offset = offset as usize;
        }
        let size = self.reader.read(buffer)?;
        self.offset += size;
        Ok(size)
    }

    #[inline]
    pub fn file_size(&self) -> Result<usize> {
        Ok(self.fd.file_size()?)
    }
}
