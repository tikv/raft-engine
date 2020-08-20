// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

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

    /// Total size limit to cache log entries.
    ///
    /// FIXME: it doesn't make effect currently.
    pub cache_limit: ReadableSize,

    /// Size limit to cache log entries for every Raft.
    pub cache_limit_per_raft: ReadableSize,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            dir: "".to_owned(),
            recovery_mode: RecoveryMode::TolerateCorruptedTailRecords,
            bytes_per_sync: ReadableSize::kb(256),
            target_file_size: ReadableSize::mb(128),
            purge_threshold: ReadableSize::gb(10),
            cache_limit: ReadableSize::gb(1),
            cache_limit_per_raft: ReadableSize::mb(128),
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
        if self.cache_limit_per_raft.0 > self.cache_limit.0 {
            return Err(box_err!("cache_limit_per_raft > cache_limit"));
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
            cache-limit = "1GB"
            cache-limit-per-raft = "8MB"
        "#;
        let load: Config = toml::from_str(custom).unwrap();
        assert_eq!(load.dir, "custom_dir");
        assert_eq!(load.recovery_mode, RecoveryMode::AbsoluteConsistency);
        assert_eq!(load.bytes_per_sync, ReadableSize::kb(2));
        assert_eq!(load.target_file_size, ReadableSize::mb(1));
        assert_eq!(load.purge_threshold, ReadableSize::mb(3));
        assert_eq!(load.cache_limit, ReadableSize::gb(1));
        assert_eq!(load.cache_limit_per_raft, ReadableSize::mb(8));
    }
}
