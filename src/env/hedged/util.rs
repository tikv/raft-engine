use std::path::{Path, PathBuf};

pub fn replace_path(path: &Path, from: &Path, to: &Path) -> PathBuf {
    if let Ok(file) = path.strip_prefix(from) {
        to.to_path_buf().join(file)
    } else {
        panic!("Invalid path: {:?}", path);
    }
}
