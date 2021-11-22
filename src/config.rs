// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use log::warn;
use serde::{Deserialize, Serialize};

use crate::{util::ReadableSize, Result};

const MIN_RECOVERY_READ_BLOCK_SIZE: usize = 512;
const MIN_RECOVERY_THREADS: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryMode {
    AbsoluteConsistency,
    TolerateTailCorruption,
    TolerateAnyCorruption,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    /// Directory to store log files. Will create on startup if not exists.
    ///
    /// Default: ""
    pub dir: String,

    /// How to deal with file corruption during recovery.
    ///
    /// Default: "tolerate-tail-corruption".
    pub recovery_mode: RecoveryMode,
    /// Minimum I/O size for reading log files during recovery.
    ///
    /// Default: "4KB". Minimum: "512B".
    pub recovery_read_block_size: ReadableSize,
    /// The number of threads used to scan and recovery log files.
    ///
    /// Default: 4. Minimum: 1.
    pub recovery_threads: usize,

    /// Compress a log batch if its size exceeds this value. Setting it to zero
    /// disables compression.
    ///
    /// Default: "8KB"
    pub batch_compression_threshold: ReadableSize,
    /// Incrementally sync log files after specified bytes have been written.
    /// Setting it to zero disables incremental sync.
    ///
    /// Default: "4MB"
    pub bytes_per_sync: ReadableSize,
    /// Target file size for rotating log files.
    ///
    /// Default: "128MB"
    pub target_file_size: ReadableSize,

    /// Purge main log queue if its file size exceeds this value.
    ///
    /// Default: "10GB"
    pub purge_threshold: ReadableSize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            dir: "".to_owned(),
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            recovery_read_block_size: ReadableSize::kb(16),
            recovery_threads: 4,
            batch_compression_threshold: ReadableSize::kb(8),
            bytes_per_sync: ReadableSize::mb(4),
            target_file_size: ReadableSize::mb(128),
            purge_threshold: ReadableSize::gb(10),
        }
    }
}

impl Config {
    pub fn sanitize(&mut self) -> Result<()> {
        if self.purge_threshold.0 < self.target_file_size.0 {
            return Err(box_err!("purge-threshold < target-file-size"));
        }
        if self.bytes_per_sync.0 == 0 {
            self.bytes_per_sync = ReadableSize(u64::MAX);
        }
        let min_recovery_read_block_size = ReadableSize(MIN_RECOVERY_READ_BLOCK_SIZE as u64);
        if self.recovery_read_block_size < min_recovery_read_block_size {
            warn!(
                "recovery-read-block-size ({}) is too small, setting it to {}",
                self.recovery_read_block_size, min_recovery_read_block_size
            );
            self.recovery_read_block_size = min_recovery_read_block_size;
        }
        if self.recovery_threads < MIN_RECOVERY_THREADS {
            warn!(
                "recovery-threads ({}) is too small, setting it to {}",
                self.recovery_threads, MIN_RECOVERY_THREADS
            );
            self.recovery_threads = MIN_RECOVERY_THREADS;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serde() {
        let value = Config::default();
        let dump = toml::to_string_pretty(&value).unwrap();
        let load = toml::from_str(&dump).unwrap();
        assert_eq!(value, load);
    }

    #[test]
    fn test_custom() {
        let custom = r#"
            dir = "custom_dir"
            recovery-mode = "absolute-consistency"
            bytes-per-sync = "2KB"
            target-file-size = "1MB"
            purge-threshold = "3MB"
        "#;
        let load: Config = toml::from_str(custom).unwrap();
        assert_eq!(load.dir, "custom_dir");
        assert_eq!(load.recovery_mode, RecoveryMode::AbsoluteConsistency);
        assert_eq!(load.bytes_per_sync, ReadableSize::kb(2));
        assert_eq!(load.target_file_size, ReadableSize::mb(1));
        assert_eq!(load.purge_threshold, ReadableSize::mb(3));
    }

    #[test]
    fn test_invalid() {
        let hard_error = r#"
            target-file-size = "5MB"
            purge-threshold = "3MB"
        "#;
        let mut hard_load: Config = toml::from_str(hard_error).unwrap();
        assert!(hard_load.sanitize().is_err());

        let soft_error = r#"
            recovery-read-block-size = "1KB"
            recovery-threads = 0
        "#;
        let soft_load: Config = toml::from_str(soft_error).unwrap();
        let mut soft_sanitized = soft_load.clone();
        soft_sanitized.sanitize().unwrap();
        assert_ne!(soft_load, soft_sanitized);
    }
}
