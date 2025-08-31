use crate::lru_cache::LruManager;

#[test]
fn test_put() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));

    let put_result = lru.put(1, 1, 10, 1, false, false);
    assert!(put_result.is_ok());
    assert!(put_result.unwrap().is_empty());
}

#[test]
fn test_lru_manager_eviction() {
    let lru = LruManager::new(10, 10, std::sync::Arc::new(std::sync::Condvar::new()));

    assert!(lru.put(1, 0, 5, 1, false, false).unwrap().is_empty());
    assert!(lru.put(2, 0, 5, 1, false, false).unwrap().is_empty());

    // This should evict the first item.
    let evicted = lru.put(3, 0,  5, 1, false, false).unwrap();
    assert_eq!(evicted.len(), 1);
    let block = &evicted[0];
    assert_eq!(block.ino, 1);
    assert_eq!(block.block, 0);
    assert_eq!(block.version, 1);
}

#[test]
fn test_remove() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));

    assert!(lru.put(1, 0, 5, 1, false, false).unwrap().is_empty());

    lru.remove(1, 0);
    assert!(lru.is_empty());
}

#[test]
fn test_mark_as_clean() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));
    assert!(lru.put(1, 0, 100, 1, false, true).unwrap().is_empty());
    assert_eq!(lru.put(2, 0, 100, 1, false, true).unwrap_err(), libc::ENOSPC);

    lru.mark_as_clean(1, 0);

    assert!(lru.put(3, 0, 100, 1, false, true).is_ok());
}

#[test]
fn test_max_write_size() {
    let lru = LruManager::new(100, 10, std::sync::Arc::new(std::sync::Condvar::new()));

    assert!(lru.put(1, 0, 10, 1, false, true).unwrap().is_empty());

    assert_eq!(lru.put(2, 0, 10, 1, false, true).unwrap_err(), libc::ENOSPC);

    // After marking the first one as clean, we should be able to add the second one.
    lru.mark_as_clean(1, 0);
    assert!(lru.put(2, 0, 10, 1, false, true).is_ok());
}
