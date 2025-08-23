fn make_fs(block_size: u64) -> crate::mem_fuse::MemoryFuse {
    let td = tempdir().unwrap();
    let mirror_path = td.path().to_path_buf();
    let mirror = crate::mirror_factory::create_mirror(&format!("file://{}", mirror_path.to_str().unwrap()), None);
    MemoryFuse::new(Some(mirror), 1024 * 1024, 1024 * 1024, false, block_size)
}

#[test]
fn test_write_and_read_small_file() {
    let mut fs = make_fs(1024);
    let parent = 1;
    let name = OsStr::new("test.txt");
    let mode = 0o755;
    let uid = 501;
    let gid = 20;

    let attr = fs.make_file(parent, name, mode, uid, gid).unwrap();
    let ino = attr.ino;

    let data = b"hello world";
    let offset = 0;
    let written = fs.write_file(ino, offset, data).unwrap();
    assert_eq!(written, data.len());

    let read_data = fs.read_file(ino, offset, data.len() as u32).unwrap();
    assert_eq!(read_data, data);
}

#[test]
fn test_write_across_block_boundary() {
    let mut fs = make_fs(1024);
    let parent = 1;
    let name = OsStr::new("test.txt");
    let mode = 0o755;
    let uid = 501;
    let gid = 20;

    let attr = fs.make_file(parent, name, mode, uid, gid).unwrap();
    let ino = attr.ino;

    let mut data = Vec::new();
    data.resize(2048, 1u8);
    let offset = 512;
    let written = fs.write_file(ino, offset, &data).unwrap();
    assert_eq!(written, data.len());

    let read_data = fs.read_file(ino, offset, data.len() as u32).unwrap();
    assert_eq!(read_data, data);
}

#[test]
fn test_file_truncation() {
    let mut fs = make_fs(1024);
    let parent = 1;
    let name = OsStr::new("test.txt");
    let mode = 0o755;
    let uid = 501;
    let gid = 20;

    let attr = fs.make_file(parent, name, mode, uid, gid).unwrap();
    let ino = attr.ino;

    let mut data = Vec::new();
    data.resize(4096, 1u8);
    let offset = 0;
    let written = fs.write_file(ino, offset, &data).unwrap();
    assert_eq!(written, data.len());

    fs.set_attr(ino, None, None, None, Some(2048), None, None, None, None).unwrap();

    let read_data = fs.read_file(ino, 0, 4096).unwrap();
    assert_eq!(read_data.len(), 2048);
}

#[test]
fn test_read_with_offset() {
    let mut fs = make_fs(1024);
    let parent = 1;
    let name = OsStr::new("test.txt");
    let mode = 0o755;
    let uid = 501;
    let gid = 20;

    let attr = fs.make_file(parent, name, mode, uid, gid).unwrap();
    let ino = attr.ino;

    let mut data = Vec::new();
    data.resize(4096, 0u8);
    for i in 0..4096 {
        data[i] = (i % 256) as u8;
    }
    let offset = 0;
    let written = fs.write_file(ino, offset, &data).unwrap();
    assert_eq!(written, data.len());

    let read_data = fs.read_file(ino, 1024, 1024).unwrap();
    assert_eq!(read_data.len(), 1024);
    assert_eq!(read_data, &data[1024..2048]);
}
