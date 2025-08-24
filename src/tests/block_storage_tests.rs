use crate::node::FileBlocks;

#[test]
fn test_write_read_single_block() {
    let mut file_blocks = FileBlocks::new(1024);
    let data = "hello world".as_bytes();
    file_blocks.write(0, data);
    let read_data = file_blocks.read(0, data.len() as u32);
    assert_eq!(data, read_data.as_slice());
}

#[test]
fn test_write_read_multiple_blocks() {
    let mut file_blocks = FileBlocks::new(1024);
    let mut data = Vec::new();
    for i in 0..2048 {
        data.push((i % 256) as u8);
    }
    file_blocks.write(0, &data);
    let read_data = file_blocks.read(0, data.len() as u32);
    assert_eq!(data, read_data);
}

#[test]
fn test_write_read_with_offset() {
    let mut file_blocks = FileBlocks::new(1024);
    let mut data = Vec::new();
    for i in 0..2048 {
        data.push((i % 256) as u8);
    }
    file_blocks.write(512, &data);
    let read_data = file_blocks.read(512, data.len() as u32);
    assert_eq!(data, read_data);
}

#[test]
fn test_overwrite() {
    let mut file_blocks = FileBlocks::new(1024);
    let initial_data = "hello world".as_bytes();
    file_blocks.write(0, initial_data);
    let new_data = "goodbye".as_bytes();
    file_blocks.write(6, new_data);
    let read_data = file_blocks.read(0, "hello goodbye".len() as u32);
    assert_eq!("hello goodbye", String::from_utf8(read_data).unwrap());
}

#[test]
fn test_block_truncate() {
    let mut file_blocks = FileBlocks::new(1024);
    let data = "hello world".as_bytes();
    file_blocks.write(0, data);
    file_blocks.truncate(5);
    let read_data = file_blocks.read(0, 5);
    assert_eq!("hello".as_bytes(), read_data.as_slice());
    assert_eq!(5, file_blocks.length);
}
