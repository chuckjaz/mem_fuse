use crate::lru_cache::LruManager;

#[test]
fn test_put_and_get() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));
    let content = std::sync::Arc::new(std::sync::RwLock::new(vec![1, 2, 3]));
    let put_result = lru.put(1, content.clone(), false, false);
    assert!(put_result.is_ok());
    assert!(put_result.unwrap().is_empty());

    let retrieved_content = lru.get(&1).unwrap();
    assert_eq!(*retrieved_content.read().unwrap(), *content.read().unwrap());
}

#[test]
fn test_lru_manager_eviction() {
    let lru = LruManager::new(10, 10, std::sync::Arc::new(std::sync::Condvar::new()));
    let content1 = std::sync::Arc::new(std::sync::RwLock::new(vec![1; 5]));
    let content2 = std::sync::Arc::new(std::sync::RwLock::new(vec![2; 5]));
    let content3 = std::sync::Arc::new(std::sync::RwLock::new(vec![3; 5]));

    assert!(lru.put(1, content1.clone(), false, false).unwrap().is_empty());
    assert!(lru.put(2, content2.clone(), false, false).unwrap().is_empty());

    // This should evict the first item.
    let evicted = lru.put(3, content3.clone(), false, false).unwrap();
    assert_eq!(evicted.len(), 1);
    assert_eq!(evicted[0].0, 1);

    assert!(lru.get(&1).is_none());
    assert!(lru.get(&2).is_some());
    assert!(lru.get(&3).is_some());
}

#[test]
fn test_remove() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));
    let content = std::sync::Arc::new(std::sync::RwLock::new(vec![1, 2, 3]));
    assert!(lru.put(1, content.clone(), false, false).unwrap().is_empty());

    lru.remove(&1);
    assert!(lru.get(&1).is_none());
}

#[test]
fn test_mark_as_clean() {
    let lru = LruManager::new(100, 100, std::sync::Arc::new(std::sync::Condvar::new()));
    let content = std::sync::Arc::new(std::sync::RwLock::new(vec![1, 2, 3]));
    assert!(lru.put(1, content.clone(), false, true).unwrap().is_empty());

    // Dirty size should be the size of the content.
    // We need to access internal state for this, which is not ideal.
    // For this test, we'll infer it from the behavior of put.
    let content2 = std::sync::Arc::new(std::sync::RwLock::new(vec![0; 98]));
    assert_eq!(lru.put(2, content2, false, true).unwrap_err(), libc::ENOSPC);

    lru.mark_as_clean(1);

    let content3 = std::sync::Arc::new(std::sync::RwLock::new(vec![0; 98]));
    assert!(lru.put(3, content3, false, true).is_ok());
}

#[test]
fn test_max_write_size() {
    let lru = LruManager::new(100, 10, std::sync::Arc::new(std::sync::Condvar::new()));
    let content1 = std::sync::Arc::new(std::sync::RwLock::new(vec![1; 5]));
    assert!(lru.put(1, content1.clone(), false, true).unwrap().is_empty());

    let content2 = std::sync::Arc::new(std::sync::RwLock::new(vec![2; 6]));
    assert_eq!(lru.put(2, content2.clone(), false, true).unwrap_err(), libc::ENOSPC);

    // After marking the first one as clean, we should be able to add the second one.
    lru.mark_as_clean(1);
    assert!(lru.put(2, content2.clone(), false, true).is_ok());
}
