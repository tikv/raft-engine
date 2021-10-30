// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use log::warn;
use serde::{Deserialize, Serialize};

use crate::{util::ReadableSize, Result};

const MIN_RECOVERY_READ_BLOCK_SIZE: usize = 512;
const MIN_RECOVERY_THREADS: usize = 1;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryMode {
    AbsoluteConsistency = 0,
    TolerateTailCorruption = 1,
    TolerateAnyCorruption = 2,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
#[serde(rename_all = "kebab-case")]
pub struct Config {
    pub dir: String,
    pub recovery_mode: RecoveryMode,
    pub bytes_per_sync: ReadableSize,
    pub target_file_size: ReadableSize,

    /// Only purge if disk file size is greater than `purge_threshold`.
    pub purge_threshold: ReadableSize,

    /// Compress a log batch if its size is greater than `batch_compression_threshold`.
    ///
    /// Set to `0` will disable compression.
    pub batch_compression_threshold: ReadableSize,

    /// Read block size for recovery. Default value: "4KB". Min value: "512B".
    pub recovery_read_block_size: ReadableSize,
    /// Parallel recovery concurrency. Default value: 4. Min value: 1.
    pub recovery_threads: usize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            dir: "".to_owned(),
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            bytes_per_sync: ReadableSize::kb(256),
            target_file_size: ReadableSize::mb(128),
            purge_threshold: ReadableSize::gb(10),
            batch_compression_threshold: ReadableSize::kb(8),
            recovery_read_block_size: ReadableSize::kb(16),
            recovery_threads: 4,
        }
    }
}

impl Config {
    pub fn sanitize(&mut self) -> Result<()> {
        if self.purge_threshold.0 < self.target_file_size.0 {
            return Err(box_err!("purge-threshold < target-file-size"));
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
