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

use crate::env::{FileSystem, Handle, WriteExt};

fn from_nix_error(e: nix::Error, custom: &'static str) -> std::io::Error {
    let kind = std::io::Error::from(e).kind();
    std::io::Error::new(kind, custom)
}

/// A RAII-style low-level file. Errors occurred during automatic resource
/// release are logged and ignored.
///
/// A [`LogFd`] is essentially a thin wrapper around [`RawFd`]. It's only
/// supported on *Unix*, and primarily optimized for *Linux*.
///
/// All [`LogFd`] instances are opened with read and write permission.
pub struct LogFd(RawFd);

impl LogFd {
    /// Opens a file with the given `path`.
    pub fn open<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        fail_point!("log_fd::open::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
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

    /// Opens a file with the given `path`. The specified file will be created
    /// first if not exists.
    pub fn create<P: ?Sized + NixPath>(path: &P) -> IoResult<Self> {
        fail_point!("log_fd::create::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        let flags = OFlag::O_RDWR | OFlag::O_CREAT;
        // Permission 644
        let mode = Mode::S_IRUSR | Mode::S_IWUSR | Mode::S_IRGRP | Mode::S_IROTH;
        let fd = fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?;
        Ok(LogFd(fd))
    }

    /// Closes the file.
    pub fn close(&self) -> IoResult<()> {
        fail_point!("log_fd::close::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        close(self.0).map_err(|e| from_nix_error(e, "close"))
    }

    /// Synchronizes all in-memory data of the file except metadata to the
    /// filesystem.
    pub fn sync(&self) -> IoResult<()> {
        fail_point!("log_fd::sync::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
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

    /// Reads some bytes starting at `offset` from this file into the specified
    /// buffer. Returns how many bytes were read.
    pub fn read(&self, mut offset: usize, buf: &mut [u8]) -> IoResult<usize> {
        let mut readed = 0;
        while readed < buf.len() {
            fail_point!("log_fd::read::err", |_| {
                Err(from_nix_error(nix::Error::EINVAL, "fp"))
            });
            let bytes = match pread(self.0, &mut buf[readed..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EINTR => continue,
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

    /// Writes some bytes to this file starting at `offset`. Returns how many
    /// bytes were written.
    pub fn write(&self, mut offset: usize, content: &[u8]) -> IoResult<usize> {
        fail_point!("log_fd::write::zero", |_| { Ok(0) });
        let mut written = 0;
        while written < content.len() {
            let bytes = match pwrite(self.0, &content[written..], offset as i64) {
                Ok(bytes) => bytes,
                Err(e) if e == Errno::EINTR => continue,
                Err(e) => return Err(from_nix_error(e, "pwrite")),
            };
            if bytes == 0 {
                break;
            }
            written += bytes;
            offset += bytes;
        }
        fail_point!("log_fd::write::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        Ok(written)
    }

    /// Truncates all data after `offset`.
    pub fn truncate(&self, offset: usize) -> IoResult<()> {
        fail_point!("log_fd::truncate::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        ftruncate(self.0, offset as i64).map_err(|e| from_nix_error(e, "ftruncate"))
    }

    /// Attempts to allocate space for `size` bytes starting at `offset`.
    #[allow(unused_variables)]
    pub fn allocate(&self, offset: usize, size: usize) -> IoResult<()> {
        fail_point!("log_fd::allocate::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        #[cfg(target_os = "linux")]
        {
            if let Err(e) = fcntl::fallocate(
                self.0,
                fcntl::FallocateFlags::empty(),
                offset as i64,
                size as i64,
            ) {
                if e != nix::Error::EOPNOTSUPP {
                    return Err(from_nix_error(e, "fallocate"));
                }
            }
        }
        Ok(())
    }
}

impl Handle for LogFd {
    fn truncate(&self, offset: usize) -> IoResult<()> {
        fail_point!("log_fd::truncate::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        ftruncate(self.0, offset as i64).map_err(|e| from_nix_error(e, "ftruncate"))
    }

    fn file_size(&self) -> IoResult<usize> {
        fail_point!("log_fd::file_size::err", |_| {
            Err(from_nix_error(nix::Error::EINVAL, "fp"))
        });
        lseek(self.0, 0, Whence::SeekEnd)
            .map(|n| n as usize)
            .map_err(|e| from_nix_error(e, "lseek"))
    }
}

impl Drop for LogFd {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            error!("error while closing file: {}", e);
        }
    }
}

/// A low-level file adapted for standard interfaces including [`Seek`],
/// [`Write`] and [`Read`].
pub struct LogFile {
    inner: Arc<LogFd>,
    offset: usize,
}

impl LogFile {
    /// Creates a new [`LogFile`] from a shared [`LogFd`].
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
        fail_point!("log_file::seek::err", |_| {
            Err(std::io::Error::new(std::io::ErrorKind::Other, "fp"))
        });
        match pos {
            SeekFrom::Start(offset) => self.offset = offset as usize,
            SeekFrom::Current(i) => self.offset = (self.offset as i64 + i) as usize,
            SeekFrom::End(i) => self.offset = (self.inner.file_size()? as i64 + i) as usize,
        }
        Ok(self.offset as u64)
    }
}

impl WriteExt for LogFile {
    fn truncate(&mut self, offset: usize) -> IoResult<()> {
        self.inner.truncate(offset)?;
        self.offset = offset;
        Ok(())
    }

    fn sync(&mut self) -> IoResult<()> {
        self.inner.sync()
    }

    fn allocate(&mut self, offset: usize, size: usize) -> IoResult<()> {
        self.inner.allocate(offset, size)
    }
}

pub struct DefaultFileSystem;

impl FileSystem for DefaultFileSystem {
    type Handle = LogFd;
    type Reader = LogFile;
    type Writer = LogFile;

    fn create<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        LogFd::create(path.as_ref())
    }

    fn open<P: AsRef<Path>>(&self, path: P) -> IoResult<Self::Handle> {
        LogFd::open(path.as_ref())
    }

    fn delete<P: AsRef<Path>>(&self, path: P) -> IoResult<()> {
        std::fs::remove_file(path)
    }

    fn new_reader(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Reader> {
        Ok(LogFile::new(handle))
    }

    fn new_writer(&self, handle: Arc<Self::Handle>) -> IoResult<Self::Writer> {
        Ok(LogFile::new(handle))
    }
}
