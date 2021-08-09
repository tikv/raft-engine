// Copyright (c) 2017-present, PingCAP, Inc. Licensed under Apache-2.0.

use serde::{Deserialize, Serialize};

use crate::{util::ReadableSize, Result};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum RecoveryMode {
    TolerateCorruptedTailRecords = 0,
    AbsoluteConsistency = 1,
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
}

impl Default for Config {
    fn default() -> Config {
        Config {
            dir: "".to_owned(),
            recovery_mode: RecoveryMode::TolerateCorruptedTailRecords,
            bytes_per_sync: ReadableSize::kb(256),
            target_file_size: ReadableSize::mb(128),
            purge_threshold: ReadableSize::gb(10),
            batch_compression_threshold: ReadableSize::kb(8),
        }
    }
}

impl Config {
    pub fn new() -> Config {
        Config::default()
    }

    pub fn validate(&self) -> Result<()> {
        if self.purge_threshold.0 < self.target_file_size.0 {
            return Err(box_err!("purge_threshold < target_file_size"));
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
}
