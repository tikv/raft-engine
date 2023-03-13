use std::path::Path;
use std::sync::Arc;

use raft_engine::env::DefaultFileSystem;
use raft_engine::Config;
use raft_engine::Engine;

fn main() {
    let mut args = std::env::args();
    let arg0 = args.next().unwrap();
    let prog = Path::new(&arg0)
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap();
    println!("usage: {} {{source}} {{target}}", prog);

    let source = args.next().unwrap();
    let target = args.next().unwrap();

    let cfg = Config {
        dir: source,
        ..Default::default()
    };
    let fs = Arc::new(DefaultFileSystem);
    Engine::<_, _>::fork(&cfg, fs, &target).unwrap();
    println!("success!");
}
