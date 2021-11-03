// use crate::test_util::generate_entries;
// use std::panic;

#[cfg(test)]
#[cfg(feature = "failpoints")]
mod tests {

    use parking_lot::{Mutex, RwLock};
    use raft::eraftpb::Entry;
    use raft_engine::{
        Config, Engine as RaftLogEngine, EventListener, LogBatch, MessageExt, ReadableSize, Result,
    };
    use std::collections::HashMap;
    use std::panic;
    use std::sync::atomic::AtomicU64;
    use std::sync::Arc;

    #[derive(Clone)]
    pub struct MessageExtTyped;

    impl MessageExt for MessageExtTyped {
        type Entry = Entry;

        fn index(e: &Entry) -> u64 {
            e.index
        }
    }

    fn generate_batch(
        region: u64,
        begin_index: u64,
        end_index: u64,
        data: Option<Vec<u8>>,
    ) -> LogBatch {
        let mut batch = LogBatch::default();
        let mut v = vec![Entry::new(); (end_index - begin_index) as usize];
        let mut index = begin_index;
        for e in v.iter_mut() {
            e.set_index(index);
            if let Some(ref data) = data {
                e.set_data(data.clone().into())
            }
            index += 1;
        }
        batch.add_entries::<MessageExtTyped>(region, &v).unwrap();
        batch
    }

    fn write_tmp_engine(
        bytes_per_sync: ReadableSize,
        target_file_size: ReadableSize,
        entry_size: usize,
        entry_count: u64,
        batch_count: u64,
    ) -> Result<usize> {
        let dir = tempfile::Builder::new()
            .prefix("handle_io_error")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            bytes_per_sync,
            target_file_size,
            batch_compression_threshold: ReadableSize(0),
            ..Default::default()
        };
        let engine = RaftLogEngine::open(cfg).unwrap();

        let entry_data = vec![b'x'; entry_size];
        let mut index = 1;
        let mut written = 0;
        for _ in 0..batch_count {
            let mut log_batch =
                generate_batch(1, index, index + entry_count + 1, Some(entry_data.clone()));
            index += entry_count;
            written += engine.write(&mut log_batch, false)?;
        }
        Ok(written)
    }

    struct ConcurrentWriteContext {
        engine: Arc<RaftLogEngine>,
        ths: Vec<std::thread::JoinHandle<()>>,
        res: Arc<Mutex<Vec<Result<usize>>>>,
    }

    impl ConcurrentWriteContext {
        fn new(engine: Arc<RaftLogEngine>) -> Self {
            Self {
                engine,
                ths: Vec::new(),
                res: Arc::new(Mutex::new(Vec::default())),
            }
        }

        fn leader_write(&mut self, mut log_batch: LogBatch, sync: bool) {
            if self.ths.is_empty() {
                fail::cfg("write_barrier::leader_exit", "pause").unwrap();
                let engine_clone = self.engine.clone();
                let res = self.res.clone();
                self.ths.push(
                    std::thread::Builder::new()
                        .spawn(move || {
                            res.lock()
                                .push(engine_clone.write(&mut LogBatch::default(), sync));
                        })
                        .unwrap(),
                );
            }
            let engine_clone = self.engine.clone();
            let res = self.res.clone();
            self.ths.push(
                std::thread::Builder::new()
                    .spawn(move || {
                        res.lock().push(engine_clone.write(&mut log_batch, sync));
                    })
                    .unwrap(),
            );
        }

        fn follower_write(&mut self, mut log_batch: LogBatch, sync: bool) {
            let engine_clone = self.engine.clone();
            let res = self.res.clone();
            self.ths.push(
                std::thread::Builder::new()
                    .spawn(move || {
                        res.lock().push(engine_clone.write(&mut log_batch, sync));
                    })
                    .unwrap(),
            );
        }

        fn join(&mut self) -> Vec<Result<usize>> {
            fail::remove("write_barrier::leader_exit");
            for t in self.ths.drain(..) {
                t.join().unwrap();
            }
            self.res.lock().drain(..).collect()
        }
    }

    #[test]
    fn test_open_error() {
        fail::cfg("log_fd::open::err", "return").unwrap();
        assert!(panic::catch_unwind(|| {
            let dir = tempfile::Builder::new()
                .prefix("handle_io_error")
                .tempdir()
                .unwrap();
            let cfg = Config {
                dir: dir.path().to_str().unwrap().to_owned(),
                bytes_per_sync: ReadableSize::kb(1),
                target_file_size: ReadableSize::kb(4),
                batch_compression_threshold: ReadableSize(0),
                ..Default::default()
            };
            let engine = RaftLogEngine::open(cfg.clone()).unwrap();
            drop(engine);
            let _ = RaftLogEngine::open(cfg).unwrap();
        })
        .is_err());
        fail::cfg("log_fd::open::err", "off").unwrap();
    }

    #[test]
    fn test_rotate_error() {
        // panic when truncate
        fail::cfg("active_file::truncate::force", "return").unwrap();
        fail::cfg("log_fd::truncate::err", "return").unwrap();
        assert!(panic::catch_unwind(|| {
            write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
        })
        .is_err());

        // panic when sync
        fail::cfg("active_file::truncate::force", "off").unwrap();
        fail::cfg("log_fd::truncate::err", "off").unwrap();
        fail::cfg("log_fd::sync::err", "return").unwrap();
        assert!(panic::catch_unwind(|| {
            write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
        })
        .is_err());

        // panic when create file
        fail::cfg("log_fd::sync::err", "off").unwrap();
        fail::cfg("log_fd::create::err", "return").unwrap();
        assert!(panic::catch_unwind(|| {
            write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
        })
        .is_err());

        // panic when write header
        fail::cfg("log_fd::create::err", "off").unwrap();
        fail::cfg("log_fd::write::err", "return").unwrap();
        assert!(panic::catch_unwind(|| {
            write_tmp_engine(ReadableSize::kb(1024), ReadableSize::kb(4), 1024, 1, 4)
        })
        .is_err());
        fail::cfg("log_fd::write::err", "off").unwrap();
    }

    type Action = dyn FnOnce() + Send + Sync;

    #[derive(Default)]
    struct FailpointsHook {
        pre_append_action: Arc<RwLock<HashMap<u64, Box<Action>>>>,
        pre_append_timer: AtomicU64,
    }

    impl FailpointsHook {
        fn new() -> Self {
            Self {
                pre_append_action: Arc::new(RwLock::new(HashMap::default())),
                pre_append_timer: AtomicU64::new(0),
            }
        }

        fn register_pre_append_action(
            &mut self,
            index: u64,
            action: impl FnOnce() + Send + Sync + 'static,
        ) {
            self.pre_append_action
                .write()
                .insert(index, Box::new(action));
        }
    }

    impl EventListener for FailpointsHook {
        fn pre_append(&self) {
            let idx = self
                .pre_append_timer
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if let Some(action) = self.pre_append_action.write().remove(&idx) {
                action();
            }
        }
    }

    #[test]
    fn test_concurrent_write_error() {
        // truncate and sync when write error
        let dir = tempfile::Builder::new()
            .prefix("handle_io_error")
            .tempdir()
            .unwrap();
        let cfg = Config {
            dir: dir.path().to_str().unwrap().to_owned(),
            bytes_per_sync: ReadableSize::kb(1024),
            target_file_size: ReadableSize::kb(1024),
            batch_compression_threshold: ReadableSize(0),
            ..Default::default()
        };

        // b0 (ctx); b1 success; b2 fail, truncate; b3 success
        let mut hook = FailpointsHook::new();
        hook.register_pre_append_action(2, || {
            fail::cfg("log_fd::write::err", "return").unwrap();
        });
        hook.register_pre_append_action(3, || {
            fail::cfg("log_fd::write::err", "off").unwrap();
        });
        let hook = Arc::new(hook);
        let engine = Arc::new(RaftLogEngine::open_with_listeners(cfg, vec![hook]).unwrap());
        let mut ctx = ConcurrentWriteContext::new(engine.clone());

        let content = vec![b'x'; 1024];

        ctx.leader_write(generate_batch(1, 1, 11, Some(content.clone())), false);
        ctx.follower_write(generate_batch(2, 1, 11, Some(content.clone())), false);
        ctx.follower_write(generate_batch(3, 1, 11, Some(content)), false);
        let r = ctx.join();
        assert!(r.len() == 4);
        assert!(r.get(1).unwrap().is_ok());
        assert!(r.get(2).unwrap().is_err());
        assert!(r.get(3).unwrap().is_ok());
        assert_eq!(
            10,
            engine
                .fetch_entries_to::<MessageExtTyped>(1, 1, 11, None, &mut vec![])
                .unwrap()
        );
        assert_eq!(
            0,
            engine
                .fetch_entries_to::<MessageExtTyped>(2, 1, 11, None, &mut vec![])
                .unwrap()
        );
        assert_eq!(
            10,
            engine
                .fetch_entries_to::<MessageExtTyped>(3, 1, 11, None, &mut vec![])
                .unwrap()
        );
    }

    #[test]
    fn test_concurrent_write_truncate_error() {
        assert!(panic::catch_unwind(|| {
            // truncate and sync when write error
            let dir = tempfile::Builder::new()
                .prefix("handle_io_error")
                .tempdir()
                .unwrap();
            let cfg = Config {
                dir: dir.path().to_str().unwrap().to_owned(),
                bytes_per_sync: ReadableSize::kb(1024),
                target_file_size: ReadableSize::kb(1024),
                batch_compression_threshold: ReadableSize(0),
                ..Default::default()
            };

            // b0 (ctx); b1 success; b2 fail, truncate, panic; b3(x)
            let mut hook = FailpointsHook::new();
            hook.register_pre_append_action(2, || {
                fail::cfg("log_fd::write::err", "return").unwrap();
                fail::cfg("log_fd::truncate::err", "return").unwrap();
            });
            hook.register_pre_append_action(3, || {
                fail::cfg("log_fd::write::err", "off").unwrap();
                fail::cfg("log_fd::truncate::err", "off").unwrap();
            });
            let hook = Arc::new(hook);
            let engine = Arc::new(RaftLogEngine::open_with_listeners(cfg, vec![hook]).unwrap());
            let mut ctx = ConcurrentWriteContext::new(engine);

            let content = vec![b'x'; 1024];

            ctx.leader_write(generate_batch(1, 1, 11, Some(content.clone())), false);
            ctx.follower_write(generate_batch(2, 1, 11, Some(content.clone())), false);
            ctx.follower_write(generate_batch(3, 1, 11, Some(content)), false);
            ctx.join();
        })
        .is_err());
    }
}
