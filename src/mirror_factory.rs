use std::sync::Arc;
use crate::mirror::{LocalMirror, Mirror};
use crate::web_mirror::WebMirror;

pub fn create_mirror(mirror_str: &str) -> Arc<dyn Mirror + Send + Sync> {
    if mirror_str.starts_with("file://") {
        let path = mirror_str.strip_prefix("file://").unwrap();
        Arc::new(LocalMirror::new(path.into()))
    } else if mirror_str.starts_with("http://") || mirror_str.starts_with("https://") {
        Arc::new(WebMirror::new(mirror_str))
    } else {
        // Default to local mirror for plain paths
        Arc::new(LocalMirror::new(mirror_str.into()))
    }
}
