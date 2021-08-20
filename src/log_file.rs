// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::io::{BufRead, Read, Result as IoResult, Seek, SeekFrom, Write};
use std::os::unix::io::RawFd;
use std::sync::Arc;
use std::time::Instant;

use fail::fail_point;
use log::warn;
use nix::errno::Errno;
use nix::fcntl::{self, OFlag};
use nix::sys::stat::Mode;
use nix::sys::uio::{pread, pwrite};
use nix::unistd::{close, fsync, ftruncate, lseek, Whence};
use nix::NixPath;
use num_derive::{FromPrimitive, ToPrimitive};
use num_traits::{FromPrimitive, ToPrimitive};

use crate::codec::{self, NumberEncoder};
use crate::metrics::*;
use crate::util::InstantExt;
use crate::{Error, Result};

const LOG_FILE_MAGIC_HEADER: &[u8] = b"RAFT-LOG-FILE-HEADER-9986AB3E47F320B394C8E84916EB0ED5";
pub const LOG_FILE_MIN_HEADER_LEN: usize = LOG_FILE_MAGIC_HEADER.len() + Version::len();
pub const LOG_FILE_MAX_HEADER_LEN: usize = LOG_FILE_MIN_HEADER_LEN;

#[derive(Clone, Copy, FromPrimitive, ToPrimitive)]
enum Version {
    V1 = 1,
}

impl Version {
    const fn current() -> Self {
        Self::V1
    }

    const fn len() -> usize {
        8
    }
}

pub struct LogFileHeader {}

impl LogFileHeader {
    pub fn new() -> Self {
        Self {}
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self> {
        if buf.len() < LOG_FILE_MIN_HEADER_LEN {
            return Err(Error::Corruption("log file header too short".to_owned()));
        }
        if !buf.starts_with(LOG_FILE_MAGIC_HEADER) {
            return Err(Error::Corruption(
                "log file magic header mismatch".to_owned(),
            ));
        }
        buf.consume(LOG_FILE_MAGIC_HEADER.len());
        let v = codec::decode_u64(buf)?;
        if Version::from_u64(v).is_none() {
            return Err(Error::Corruption(format!(
                "unrecognized log file version: {}",
                v
            )));
        }
        Ok(Self {})
    }

    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        buf.extend_from_slice(LOG_FILE_MAGIC_HEADER);
        buf.encode_u64(Version::current().to_u64().unwrap())?;
        Ok(())
    }
}

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
        let flags = OFlag::O_RDWR;
        let mode = Mode::S_IRWXU;
        fail_point!("log_fd_fadvise_dontneed", |_| {
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
        let flags = OFlag::O_RDWR | OFlag::O_CREAT;
        let mode = Mode::S_IRWXU;
        let fd = fcntl::open(path, flags, mode).map_err(|e| from_nix_error(e, "open"))?;
        Ok(LogFd(fd))
    }

    pub fn close(&self) -> IoResult<()> {
        close(self.0).map_err(|e| from_nix_error(e, "close"))
    }

    pub fn sync(&self) -> IoResult<()> {
        let start = Instant::now();
        let res = fsync(self.0).map_err(|e| from_nix_error(e, "fsync"));
        LOG_SYNC_TIME_HISTOGRAM.observe(start.saturating_elapsed().as_secs_f64());
        res
    }

    pub fn read(&self, mut offset: i64, buf: &mut [u8]) -> IoResult<usize> {
        let mut readed = 0;
        while readed < buf.len() {
            let bytes = match pread(self.0, &mut buf[readed..], offset) {
                Ok(bytes) => bytes,
                Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
                Err(e) => return Err(from_nix_error(e, "pread")),
            };
            // EOF
            if bytes == 0 {
                break;
            }
            readed += bytes;
            offset += bytes as i64;
        }
        Ok(readed)
    }

    pub fn write(&self, mut offset: i64, content: &[u8]) -> IoResult<usize> {
        let mut written = 0;
        while written < content.len() {
            let bytes = match pwrite(self.0, &content[written..], offset) {
                Ok(bytes) => bytes,
                Err(e) if e.as_errno() == Some(Errno::EAGAIN) => continue,
                Err(e) => return Err(from_nix_error(e, "pwrite")),
            };
            if bytes == 0 {
                break;
            }
            written += bytes;
            offset += bytes as i64;
        }
        Ok(written)
    }

    pub fn file_size(&self) -> IoResult<usize> {
        lseek(self.0, 0, Whence::SeekEnd)
            .map(|n| n as usize)
            .map_err(|e| from_nix_error(e, "lseek"))
    }

    pub fn truncate(&self, offset: i64) -> IoResult<()> {
        ftruncate(self.0, offset).map_err(|e| from_nix_error(e, "ftruncate"))
    }

    pub fn allocate(&self, offset: i64, size: i64) -> IoResult<()> {
        fcntl::fallocate(
            self.0,
            fcntl::FallocateFlags::FALLOC_FL_KEEP_SIZE,
            offset,
            size,
        )
        .map_err(|e| from_nix_error(e, "fallocate"))
    }
}

impl Drop for LogFd {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            warn!("close: {}", e);
        }
    }
}

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
        let len = self.inner.write(self.offset as i64, buf)?;
        self.offset += len;
        Ok(len)
    }

    fn flush(&mut self) -> IoResult<()> {
        Ok(())
    }
}

impl Read for LogFile {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<usize> {
        let len = self.inner.read(self.offset as i64, buf)?;
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
