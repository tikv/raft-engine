// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

use crate::env::from_same_dev;
use crate::pipe_log::Version;
use crate::{util::ReadableSize, Result};

const MIN_RECOVERY_READ_BLOCK_SIZE: usize = 512;
const MIN_RECOVERY_THREADS: usize = 1;

/// Check the given `dir` is valid or not. If `dir` not exists,
/// it would be created automatically.
fn validate_dir(dir: &str) -> Result<()> {
    let path = Path::new(dir);
    if !path.exists() {
        info!("Create raft log directory: {}", dir);
        fs::create_dir(dir)?;
        return Ok(());
    }
    if !path.is_dir() {
        return Err(box_err!("Not directory: {}", dir));
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryMode {
    AbsoluteConsistency,
    // For backward compatibility.
    #[serde(
        alias = "tolerate-corrupted-tail-records",
        rename(serialize = "tolerate-corrupted-tail-records")
    )]
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

    /// Secondary directory to store log files. Will create on startup if
    /// set and not exists.
    ///
    /// Newly logs will be put into this dir when the main `dir` is full
    /// or not accessible.
    ///
    /// Default: ""
    pub secondary_dir: Option<String>,

    /// How to deal with file corruption during recovery.
    ///
    /// Default: "tolerate-tail-corruption".
    pub recovery_mode: RecoveryMode,
    /// Minimum I/O size for reading log files during recovery.
    ///
    /// Default: "16KB". Minimum: "512B".
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

    /// Version of the log file.
    ///
    /// Default: 1
    pub format_version: Version,

    /// Target file size for rotating log files.
    ///
    /// Default: "128MB"
    pub target_file_size: ReadableSize,

    /// Purge append log queue if its size exceeds this value.
    ///
    /// Default: "10GB"
    pub purge_threshold: ReadableSize,
    /// Purge rewrite log queue if its size exceeds this value.
    ///
    /// Default: MAX(`purge_threshold` / 10, `target_file_size`)
    pub purge_rewrite_threshold: Option<ReadableSize>,
    /// Purge rewrite log queue if its garbage ratio exceeds this value.
    ///
    /// Default: "0.6"
    pub purge_rewrite_garbage_ratio: f64,

    /// Maximum memory bytes allowed for the in-memory index.
    /// Effective under the `swap` feature only.
    ///
    /// Default: None
    pub memory_limit: Option<ReadableSize>,

    /// Whether to recycle stale log files.
    /// If `true`, logically purged log files will be reserved for recycling.
    /// Only available for `format_version` 2 and above.
    ///
    /// Default: false
    pub enable_log_recycle: bool,
}

impl Default for Config {
    fn default() -> Config {
        #[allow(unused_mut)]
        let mut cfg = Config {
            dir: "".to_owned(),
            secondary_dir: None,
            recovery_mode: RecoveryMode::TolerateTailCorruption,
            recovery_read_block_size: ReadableSize::kb(16),
            recovery_threads: 4,
            batch_compression_threshold: ReadableSize::kb(8),
            bytes_per_sync: ReadableSize::mb(4),
            format_version: Version::V1,
            target_file_size: ReadableSize::mb(128),
            purge_threshold: ReadableSize::gb(10),
            purge_rewrite_threshold: None,
            purge_rewrite_garbage_ratio: 0.6,
            memory_limit: None,
            enable_log_recycle: false,
        };
        // Test-specific configurations.
        #[cfg(test)]
        {
            cfg.memory_limit = Some(ReadableSize(0));
            cfg.format_version = Version::V2;
            cfg.enable_log_recycle = true;
        }
        cfg
    }
}

impl Config {
    pub fn sanitize(&mut self) -> Result<()> {
        if self.purge_threshold.0 < self.target_file_size.0 {
            return Err(box_err!("purge-threshold < target-file-size"));
        }
        if self.purge_rewrite_threshold.is_none() {
            self.purge_rewrite_threshold = Some(ReadableSize(std::cmp::max(
                self.purge_threshold.0 / 10,
                self.target_file_size.0,
            )));
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
        if self.enable_log_recycle && !self.format_version.has_log_signing() {
            return Err(box_err!(
                "format version {} doesn't support log recycle, use 2 or above",
                self.format_version
            ));
        }
        #[cfg(not(feature = "swap"))]
        if self.memory_limit.is_some() {
            warn!("memory-limit will be ignored because swap feature is disabled");
        }
        // Validate `dir`.
        if let Err(e) = validate_dir(&self.dir) {
            return Err(box_err!(
                "dir ({}) is invalid, err: {}, please check it again",
                self.dir,
                e
            ));
        }
        // Validate `secondary-dir`.
        if_chain::if_chain! {
            if let Some(secondary_dir) = self.secondary_dir.as_ref();
            if validate_dir(secondary_dir).is_ok();
            if let Ok(true) = from_same_dev(&self.dir, secondary_dir);
            then {
                warn!(
                    "secondary-dir ({}) and dir ({}) are on same device, recommend setting it to another device",
                    secondary_dir, self.dir
                );
            }
        }
        Ok(())
    }

    /// Returns the capacity for recycling log files.
    pub(crate) fn recycle_capacity(&self) -> usize {
        // Attention please, log files with Version::V1 could not be recycled, it might
        // cause LogBatchs in a mess in the recycled file, where the reader might get
        // an obsolete entries (unexpected) from the recycled file.
        if !self.format_version.has_log_signing() {
            return 0;
        }
        if self.enable_log_recycle && self.purge_threshold.0 >= self.target_file_size.0 {
            // This is required to squeeze file signature into an u32.
            std::cmp::min(
                (self.purge_threshold.0 / self.target_file_size.0) as usize,
                u32::MAX as usize,
            )
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn remove_dir(dir: &str) -> Result<()> {
        fs::remove_dir_all(dir)?;
        Ok(())
    }

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
            recovery-mode = "tolerate-tail-corruption"
            bytes-per-sync = "2KB"
            target-file-size = "1MB"
            purge-threshold = "3MB"
            format-version = 1
        "#;
        let load: Config = toml::from_str(custom).unwrap();
        assert_eq!(load.dir, "custom_dir");
        assert_eq!(load.recovery_mode, RecoveryMode::TolerateTailCorruption);
        assert_eq!(load.bytes_per_sync, ReadableSize::kb(2));
        assert_eq!(load.target_file_size, ReadableSize::mb(1));
        assert_eq!(load.purge_threshold, ReadableSize::mb(3));
        assert_eq!(load.format_version, Version::V1);
    }

    #[test]
    fn test_invalid() {
        let hard_error = r#"
            dir = "./"
            target-file-size = "5MB"
            purge-threshold = "3MB"
        "#;
        let mut hard_load: Config = toml::from_str(hard_error).unwrap();
        assert!(hard_load.sanitize().is_err());

        let soft_error = r#"
            dir = "./"
            recovery-read-block-size = "1KB"
            recovery-threads = 0
            bytes-per-sync = "0KB"
            target-file-size = "5000MB"
            format-version = 2
            enable-log-recycle = true
        "#;
        let soft_load: Config = toml::from_str(soft_error).unwrap();
        let mut soft_sanitized = soft_load;
        soft_sanitized.sanitize().unwrap();
        assert!(soft_sanitized.recovery_read_block_size.0 >= MIN_RECOVERY_READ_BLOCK_SIZE as u64);
        assert!(soft_sanitized.recovery_threads >= MIN_RECOVERY_THREADS);
        assert_eq!(soft_sanitized.bytes_per_sync.0, u64::MAX);
        assert_eq!(
            soft_sanitized.purge_rewrite_threshold.unwrap(),
            soft_sanitized.target_file_size
        );
        assert_eq!(soft_sanitized.format_version, Version::V2);
        assert!(soft_sanitized.enable_log_recycle);

        let recycle_error = r#"
            dir = "./"
            enable-log-recycle = true
            format-version = 1
        "#;
        let mut cfg_load: Config = toml::from_str(recycle_error).unwrap();
        assert!(cfg_load.sanitize().is_err());
    }

    #[test]
    fn test_backward_compactibility() {
        // Upgrade from older version.
        let old = r#"
            dir = "./"
            recovery-mode = "tolerate-corrupted-tail-records"
        "#;
        let mut load: Config = toml::from_str(old).unwrap();
        load.sanitize().unwrap();
        // Downgrade to older version.
        assert!(toml::to_string(&load)
            .unwrap()
            .contains("tolerate-corrupted-tail-records"));
    }

    #[test]
    fn test_validate_dir_setting() {
        {
            let dir_list = r#""#;
            let mut load: Config = toml::from_str(dir_list).unwrap();
            assert!(load.sanitize().is_err());
        }
        {
            // Set the sub-dir same with main dir
            let dir_list = r#"
                dir = "./test_validate_dir_setting/"
                secondary-dir = "./test_validate_dir_setting/"
            "#;
            let mut load: Config = toml::from_str(dir_list).unwrap();
            load.sanitize().unwrap();
            assert!(load.secondary_dir.is_some());
            assert!(remove_dir(&load.dir).is_ok());
        }
        {
            // Set the sub-dir with `"..."`
            let dir_list = r#"
                dir = "./test_validate_dir_setting"
                secondary-dir = "./test_validate_dir_setting_secondary"
            "#;
            let mut load: Config = toml::from_str(dir_list).unwrap();
            load.sanitize().unwrap();
            assert!(load.secondary_dir.is_some());
            assert!(remove_dir(&load.dir).is_ok());
            assert!(remove_dir(&load.secondary_dir.unwrap()).is_ok());
        }
    }
}
