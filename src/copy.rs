// Copyright (c) 2023-present, PingCAP, Inc. Licensed under Apache-2.0.

use std::fs::{copy, create_dir_all};
use std::os::unix::fs::symlink;
use std::path::Path;
use std::sync::Arc;

use crate::config::Config;
use crate::env::{FileSystem, OFlag};
use crate::file_pipe_log::{FileNameExt, FilePipeLogBuilder};
use crate::pipe_log::{FileId, LogQueue};

/// Make a copy from `source` to `target`. `source` should exists but `target`
/// shouldn't.
///
/// *minimum* means *symlink* will be used if possbile, otherwise *copy* will be
/// used instead. Generally all inactive log files will be symlinked, but the
/// last active one will be copied.
///
/// After the copy is made both of 2 engines can be started and run at the same
/// time.
pub fn minimum_copy<F, P>(cfg: &Config, fs: Arc<F>, target: P) -> Result<(), String>
where
    F: FileSystem,
    P: AsRef<Path>,
{
    if cfg.enable_log_recycle {
        return Err("enable_log_recycle should be false".to_owned());
    }

    let mut cfg = cfg.clone();
    cfg.sanitize()
        .map_err(|e| format!("sanitize config: {}", e))?;

    create_dir_all(&target)
        .map_err(|e| format!("create_dir_all({}): {}", target.as_ref().display(), e))?;

    let mut builder = FilePipeLogBuilder::new(cfg.clone(), fs, vec![]);
    builder
        .scan_impl(|_, _| -> OFlag { OFlag::O_RDONLY })
        .map_err(|e| format!("scan files: {}", e))?;

    // Iterate all log files and rewrite files.
    for (queue, files) in [
        (LogQueue::Append, builder.get_append_queue_files()),
        (LogQueue::Rewrite, builder.get_rewrite_queue_files()),
    ] {
        let count = files.len();
        for (i, f) in files.iter().enumerate() {
            let file_id = FileId::new(queue, f.seq);
            let src = file_id.build_file_path(&cfg.dir);
            let tgt = file_id.build_file_path(&target);
            let (tag, res) = if i < count - 1 {
                ("symlink", symlink(&src, &tgt))
            } else {
                ("copy", copy(&src, &tgt).map(|_| ()))
            };
            res.map_err(|e| format!("{}({}, {}): {}", tag, src.display(), tgt.display(), e))?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::tests::RaftLogEngine;
    use crate::env::DefaultFileSystem;
    use crate::{LogBatch, ReadableSize};
    use std::path::PathBuf;

    #[test]
    fn test_minimum_copy() {
        let dir = tempfile::Builder::new()
            .prefix("test_minimum_copy")
            .tempdir()
            .unwrap();

        let mut source = PathBuf::from(dir.as_ref());
        source.push("source");
        let mut cfg = Config {
            dir: source.to_str().unwrap().to_owned(),
            target_file_size: ReadableSize::kb(1),
            enable_log_recycle: false,
            ..Default::default()
        };
        let engine = RaftLogEngine::open(cfg.clone()).unwrap();

        let mut log_batch = LogBatch::default();
        log_batch.put(1, vec![b'1'; 16], vec![b'v'; 1024]).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        let mut log_batch = LogBatch::default();
        log_batch.put(1, vec![b'2'; 16], vec![b'v'; 1024]).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        engine.purge_manager().must_rewrite_append_queue(None, None);

        let mut log_batch = LogBatch::default();
        log_batch.put(1, vec![b'3'; 16], vec![b'v'; 1024]).unwrap();
        engine.write(&mut log_batch, false).unwrap();

        let mut log_batch = LogBatch::default();
        log_batch.put(1, vec![b'4'; 16], vec![b'v'; 1024]).unwrap();
        engine.write(&mut log_batch, false).unwrap();
        drop(engine);

        let mut target = PathBuf::from(dir.as_ref());
        target.push("target");
        minimum_copy(&cfg, Arc::new(DefaultFileSystem), &target).unwrap();
        cfg.dir = target.to_str().unwrap().to_owned();
        let engine = RaftLogEngine::open(cfg).unwrap();

        assert!(engine.get(1, vec![b'1'; 16].as_ref()).is_some());
        assert!(engine.get(1, vec![b'2'; 16].as_ref()).is_some());
        assert!(engine.get(1, vec![b'3'; 16].as_ref()).is_some());
        assert!(engine.get(1, vec![b'4'; 16].as_ref()).is_some());
    }
}
