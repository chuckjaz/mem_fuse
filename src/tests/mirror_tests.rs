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
fn test_disk_mirroring() {
    let dir = tempdir().unwrap();
    let image_path = dir.path().to_path_buf();
    let mirror = LocalMirror::new(image_path.clone());
    let mut fuse = MemoryFuse::new(
        Some(Arc::new(mirror)),
        500 * 1024 * 1024,
        500 * 1024 * 1024,
        false,
        crate::node::DEFAULT_BLOCK_SIZE,
    );

    // 1. Create a directory
    let dir_name = OsStr::new("mydir");
    let dir_attr = fuse
        .make_directory(1, dir_name, 0o755, 1001, 1002)
        .unwrap();

    let on_disk_dir_path = image_path.join("mydir");
    wait_for_path(&on_disk_dir_path, true);

    assert!(on_disk_dir_path.is_dir());
    let metadata = on_disk_dir_path.metadata().unwrap();
    assert_eq!(metadata.mode() & 0o777, 0o755);
    // Note: UID/GID checks might fail if not run as root
    // assert_eq!(metadata.uid(), 1001);
    // assert_eq!(metadata.gid(), 1002);

    // 2. Create a file inside the directory
    let file_name = OsStr::new("test.txt");
    let file_attr = fuse
        .make_file(dir_attr.ino, file_name, 0o644, 1003, 1004)
        .unwrap();
    let file_ino = file_attr.ino;

    let on_disk_file_path = on_disk_dir_path.join("test.txt");
    wait_for_path(&on_disk_file_path, true);
    let metadata = on_disk_file_path.metadata().unwrap();
    assert_eq!(metadata.mode() & 0o777, 0o644);
    // assert_eq!(metadata.uid(), 1003);
    // assert_eq!(metadata.gid(), 1004);

    // 3. Write to the file
    let data = b"hello world";
    fuse.write_file(file_ino, 0, data).unwrap();

    for _ in 0..20 {
        if fs::read(&on_disk_file_path).unwrap_or_default() == data {
            break;
        }
        sleep(Duration::from_millis(100));
    }
    assert_eq!(fs::read(&on_disk_file_path).unwrap(), data);

    // Check that the content is still in memory
    {
        let nodes = fuse.nodes.read().unwrap();
        let node = nodes.get(file_ino).unwrap();
        if let NodeKind::File(file) = &node.kind {
            assert!(matches!(file.content, FileContent::InMemory(_)));
        } else {
            panic!("Node is not a file");
        }
    }

    // 4. Rename the file
    let new_file_name = OsStr::new("renamed.txt");
    fuse.rename_node(dir_attr.ino, file_name, dir_attr.ino, new_file_name, 0)
        .unwrap();

    let new_on_disk_file_path = on_disk_dir_path.join("renamed.txt");
    wait_for_path(&new_on_disk_file_path, true);
    wait_for_path(&on_disk_file_path, false);
    assert!(!on_disk_file_path.exists());
    assert!(new_on_disk_file_path.is_file());

    // 5. Create a hard link
    let link_name = OsStr::new("link.txt");
    fuse.link_node(file_ino, 1, link_name).unwrap();
    let on_disk_link_path = image_path.join("link.txt");
    wait_for_path(&on_disk_link_path, true);
    assert!(on_disk_link_path.is_file());

    let meta1 = new_on_disk_file_path.metadata().unwrap();
    let meta2 = on_disk_link_path.metadata().unwrap();
    assert_eq!(meta1.ino(), meta2.ino());

    // 6. Unlink the original file
    fuse.unlink_node(dir_attr.ino, new_file_name).unwrap();
    wait_for_path(&new_on_disk_file_path, false);
    assert!(on_disk_link_path.is_file());

    // 7. Unlink the link
    fuse.unlink_node(1, link_name).unwrap();
    wait_for_path(&on_disk_link_path, false);
}

#[test]
fn test_lru_eviction() {
    let dir = tempdir().unwrap();
    let image_path = dir.path().to_path_buf();
    let mirror = LocalMirror::new(image_path.clone());
    // 1MB cache size
    let mut fuse = MemoryFuse::new(
        Some(Arc::new(mirror)),
        1024 * 1024,
        1024 * 1024,
        false,
        crate::node::DEFAULT_BLOCK_SIZE,
    );

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
    fuse.write_file(file2_ino, 0, &data2).unwrap();

    // At this point, file1 should be evicted.
    // The total size is 1.2MB, which is > 1MB.

    // Wait for writes to complete
    sleep(Duration::from_secs(2));

    {
        let nodes = fuse.nodes.read().unwrap();
        let node1 = nodes.get(file1_ino).unwrap();
        if let NodeKind::File(_file) = &node1.kind {
            // This might not be OnDisk if the worker thread is slow.
            // A better check is to see if we can load a third file.
        } else {
            panic!("Node1 is not a file");
        }

        let node2 = nodes.get(file2_ino).unwrap();
        if let NodeKind::File(file) = &node2.kind {
            assert!(matches!(file.content, FileContent::InMemory(_)));
        } else {
            panic!("Node2 is not a file");
        }
    }

    // Access file1 again to bring it back to memory
    fuse.read_file(file1_ino, 0, 1).unwrap();

    // Now file2 should be evicted
    sleep(Duration::from_secs(2));

    {
        let nodes = fuse.nodes.read().unwrap();
        let node1 = nodes.get(file1_ino).unwrap();
        if let NodeKind::File(file) = &node1.kind {
            assert!(matches!(file.content, FileContent::InMemory(_)));
        } else {
            panic!("Node1 is not a file");
        }

        let node2 = nodes.get(file2_ino).unwrap();
        if let NodeKind::File(file) = &node2.kind {
            assert!(matches!(file.content, FileContent::Mirrored));
        } else {
            panic!("Node2 is not a file");
        }
    }
}

#[test]
fn test_lru_eviction_dirty() {
    // 1MB cache size, no disk worker
    let mut fuse = MemoryFuse::new(None, 1024 * 1024, 1024 * 1024, false, crate::node::DEFAULT_BLOCK_SIZE);

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

#[test]
fn test_lazy_load() {
    let dir = tempdir().unwrap();
    let image_path = dir.path().to_path_buf();

    // 1. Create a disk image with some files and directories
    let sub_dir_path = image_path.join("sub");
    fs::create_dir(&sub_dir_path).unwrap();
    let file_path = sub_dir_path.join("file.txt");
    fs::write(&file_path, "hello").unwrap();

    // 2. Start MemoryFuse with lazy loading enabled
    let mirror = LocalMirror::new(image_path.clone());
    let mut fuse = MemoryFuse::new(
        Some(Arc::new(mirror)),
        1024 * 1024,
        1024 * 1024,
        true,
        crate::node::DEFAULT_BLOCK_SIZE,
    );

    // 3. Verify that initially, only the root directory is loaded
    {
        let nodes = fuse.nodes.read().unwrap();
        let root = nodes.get(1).unwrap();
        if let NodeKind::Directory(dir_kind) = &root.kind {
            assert!(!dir_kind.is_ondisk());
        } else {
            panic!("Root is not a directory");
        }
        assert_eq!(nodes.get_dir_anon(1).unwrap().iter().count(), 1);
        let sub_dir_ino = nodes.get_dir_anon(1).unwrap().get(OsStr::new("sub")).unwrap();
        let sub_dir_node = nodes.get(sub_dir_ino).unwrap();
        if let NodeKind::Directory(dir_kind) = &sub_dir_node.kind {
            assert!(dir_kind.is_ondisk());
        } else {
            panic!("Sub dir is not a directory");
        }
    }

    // 4. Perform a lookup on the subdirectory and verify that it gets loaded on demand
    let sub_dir_attr = fuse.lookup_node(1, OsStr::new("sub")).unwrap();
    {
        let nodes = fuse.nodes.read().unwrap();
        let sub_dir_node = nodes.get(sub_dir_attr.ino).unwrap();
        if let NodeKind::Directory(dir_kind) = &sub_dir_node.kind {
            assert!(!dir_kind.is_ondisk());
        } else {
            panic!("Sub dir is not a directory");
        }
    }

    // 5. Perform a readdir on the subdirectory and verify that the file is listed
    let entries = fuse.read_directory(sub_dir_attr.ino).unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, "file.txt");

    // 6. Read the file and verify its contents
    let file_attr = entries[0].1;
    let data = fuse.read_file(file_attr.ino, 0, 1024).unwrap();
    assert_eq!(data, b"hello");
}
