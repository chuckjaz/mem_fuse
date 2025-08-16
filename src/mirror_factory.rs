use std::sync::Arc;
use crate::mirror::{LocalMirror, Mirror};
use crate::invariant_files_mirror::InvariantFilesMirror;

pub fn create_mirror(mirror_str: &str, option: Option<u64>) -> Arc<dyn Mirror + Send + Sync> {
    if mirror_str.starts_with("file://") {
        let path = mirror_str.strip_prefix("file://").unwrap();
        Arc::new(LocalMirror::new(path.into()))
    } else if mirror_str.starts_with("http://") || mirror_str.starts_with("https://") {
        Arc::new(InvariantFilesMirror::new(mirror_str, option))
    } else {
        // Default to local mirror for plain paths
        Arc::new(LocalMirror::new(mirror_str.into()))
    }
}
