use crate::node::FileBlocks;

#[test]
fn test_write_read_single_block() {
    let mut file_blocks = FileBlocks::new(1024);
    let data = "hello world".as_bytes();
    let len = data.len();
    file_blocks.extend_to(len as u64);
    let dest = file_blocks.get_block_mut(0);
    dest[0..len].copy_from_slice(data);
    let read_data = file_blocks.get_block(0);
    assert_eq!(data, read_data.as_slice());
}

#[test]
fn test_write_read_multiple_blocks() {
    let mut file_blocks = FileBlocks::new(1024);
    let mut data = Vec::new();
    file_blocks.extend_to(2048);
    for i in 0..2048 {
        data.push((i % 256) as u8);
    }
    let dest1 = file_blocks.get_block_mut(0);
    dest1[0..1024].copy_from_slice(&data[0..1024]);
    let dest2 = file_blocks.get_block_mut(1);
    dest2[0..1024].copy_from_slice(&data[1024..2048]);
    let read1 = file_blocks.get_block(0);
    assert_eq!(&data[0..1024], &read1[..]);
    let read2 = file_blocks.get_block(1);
    assert_eq!(&data[1024..2048], &read2[..]);
}
#[test]
fn test_block_truncate() {
    let mut file_blocks = FileBlocks::new(1024);
    let data = "hello world".as_bytes();
    file_blocks.extend_to(data.len() as u64);
    let dest = file_blocks.get_block_mut(0);
    dest[..].copy_from_slice(data);
    file_blocks.truncate_to(5);
    let read_data = file_blocks.get_block(0);
    assert_eq!("hello".as_bytes(), read_data.as_slice());
    assert_eq!(5, file_blocks.size);
}
