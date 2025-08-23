use crate::dirty::DirtyBlocks;

#[test]
fn test_add_block() {
    let mut blocks = DirtyBlocks::new();
    blocks.add_block(0);
    blocks.add_block(1);
    assert_eq!(blocks.blocks().len(), 2);
    assert!(blocks.blocks().contains(&0));
    assert!(blocks.blocks().contains(&1));
}

#[test]
fn test_add_block_duplicate() {
    let mut blocks = DirtyBlocks::new();
    blocks.add_block(0);
    blocks.add_block(0);
    assert_eq!(blocks.blocks().len(), 1);
    assert!(blocks.blocks().contains(&0));
}

#[test]
fn test_clear() {
    let mut blocks = DirtyBlocks::new();
    blocks.add_block(0);
    blocks.add_block(1);
    blocks.clear();
    assert!(blocks.blocks().is_empty());
}

#[test]
fn test_truncate() {
    let mut blocks = DirtyBlocks::new();
    blocks.add_block(0);
    blocks.add_block(1);
    blocks.add_block(2);
    blocks.add_block(3);

    blocks.truncate(1024, 2560); // block size = 1024, size = 2560
    assert_eq!(blocks.blocks().len(), 3);
    assert!(blocks.blocks().contains(&0));
    assert!(blocks.blocks().contains(&1));
    assert!(blocks.blocks().contains(&2));

    blocks.truncate(1024, 1024);
    assert_eq!(blocks.blocks().len(), 2);
    assert!(blocks.blocks().contains(&0));
    assert!(blocks.blocks().contains(&1));

    blocks.truncate(1024, 1023);
    assert_eq!(blocks.blocks().len(), 1);
    assert!(blocks.blocks().contains(&0));

    blocks.truncate(1024, 0);
    assert!(blocks.blocks().is_empty());
}
