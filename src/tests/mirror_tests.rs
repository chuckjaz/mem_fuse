use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;
use tempfile::tempdir;
use crate::mem_fuse::{MemoryFuse};
use crate::mirror::LocalMirror;
use crate::node::{NodeKind, FileContent};
use std::ffi::OsStr;
use std::sync::Arc;

fn wait_for_path(path: &Path, should_exist: bool) {
    for _ in 0..30 {
        if path.exists() == should_exist {
            return;
        }
        sleep(Duration::from_millis(100));
    }
    panic!(
        "Timeout waiting for path {:?} to {}exist",
        path,
        if should_exist { "" } else { "not " }
    );
}

#[test]
fn test_lru_eviction_dirty() {
    // 1MB cache size, no disk worker
    let mut fuse = MemoryFuse::new(None, 1024 * 1024, 1024 * 1024, false, 1024 * 1024);

    // Create file 1 (0.6 MB)
    let file1_name = OsStr::new("file1.txt");
    let file1_attr = fuse.make_file(1, file1_name, 0o644, 1000, 1000).unwrap();
    let file1_ino = file1_attr.ino;
    let data1 = vec![1u8; 600 * 1024];
    fuse.write_file(file1_ino, 0, &data1).unwrap();

    // Create file 2 (0.6 MB)
    let file2_name = OsStr::new("file2.txt");
    let file2_attr = fuse.make_file(1, file2_name, 0o644, 1000, 1000).unwrap();
    let file2_ino = file2_attr.ino;
    let data2 = vec![2u8; 600 * 1024];
    let result = fuse.write_file(file2_ino, 0, &data2);

    // At this point, the cache is full of dirty files and there's no mirror.
    // The write should fail with ENOSPC.
    assert!(result.is_err());
    assert_eq!(result.unwrap_err(), libc::ENOSPC);
}
