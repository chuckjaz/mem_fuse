use crate::lru_cache::LruManager;
use crate::block::Block;

#[test]
fn test_put_and_get() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));
    let block = Block::new(vec![1, 2, 3]);
    let put_result = lru.put(1, 0, block.clone(), false, false);
    assert!(put_result.is_ok());
    assert!(put_result.unwrap().is_empty());

    let retrieved_block = lru.get(1, 0).unwrap();
    assert_eq!(*retrieved_block.read().read().unwrap(), *block.read().read().unwrap());
}

#[test]
fn test_lru_manager_eviction() {
    let lru = LruManager::new(10, 10, std::sync::Arc::new(std::sync::Condvar::new()));
    let block1 = Block::new(vec![1; 5]);
    let block2 = Block::new(vec![2; 5]);
    let block3 = Block::new(vec![3; 5]);

    assert!(lru.put(1, 0, block1.clone(), false, false).unwrap().is_empty());
    assert!(lru.put(2, 0, block2.clone(), false, false).unwrap().is_empty());

    // This should evict the first item.
    let evicted = lru.put(3, 0, block3.clone(), false, false).unwrap();
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0].0, (1, 0));

    assert!(lru.get(1, 0).is_none());
    assert!(lru.get(2, 0).is_some());
    assert!(lru.get(3, 0).is_some());
}

#[test]
fn test_remove() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));
    let block = Block::new(vec![1, 2, 3]);
    assert!(lru.put(1, 0, block.clone(), false, false).unwrap().is_empty());

    lru.remove(1);
    assert!(lru.get(1, 0).is_none());
}

#[test]
fn test_mark_as_clean() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));
    let block = Block::new(vec![1, 2, 3]);
    assert!(lru.put(1, 0, block.clone(), false, true).unwrap().is_empty());

    // Dirty size should be the size of the content.
    // We need to access internal state for this, which is not ideal.
    // For this test, we'll infer it from the behavior of put.
    let block2 = Block::new(vec![0; 98]);
    assert_eq!(lru.put(2, 0, block2, false, true).unwrap_err(), libc::ENOSPC);

    lru.mark_as_clean(1, 0);

    let block3 = Block::new(vec![0; 98]);
    assert!(lru.put(3, 0, block3, false, true).is_ok());
}

#[test]
fn test_max_write_size() {
    let lru = LruManager::new(100, 10, std::sync::Arc::new(std::sync::Condvar::new()));
    let block1 = Block::new(vec![1; 5]);
    assert!(lru.put(1, 0, block1.clone(), false, true).unwrap().is_empty());

    let block2 = Block::new(vec![2; 6]);
    assert_eq!(lru.put(2, 0, block2.clone(), false, true).unwrap_err(), libc::ENOSPC);

    // After marking the first one as clean, we should be able to add the second one.
    lru.mark_as_clean(1, 0);
    assert!(lru.put(2, 0, block2.clone(), false, true).is_ok());
}
