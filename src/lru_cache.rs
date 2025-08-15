use std::sync::{Arc, Condvar, RwLock};
use lru::LruCache;

struct CacheData {
    cache: LruCache<u64, (Arc<RwLock<Vec<u8>>>, u64)>,
    current_size: u64,
}

pub struct LruManager {
    data: RwLock<CacheData>,
    max_size: u64,
    cache_cond: Arc<Condvar>,
}

impl LruManager {
    pub fn new(max_size: u64, cache_cond: Arc<Condvar>) -> Self {
        Self {
            data: RwLock::new(CacheData {
                cache: LruCache::unbounded(),
                current_size: 0,
            }),
            max_size,
            cache_cond,
        }
    }

    pub fn put(&self, ino: u64, content: Arc<RwLock<Vec<u8>>>) -> Vec<(u64, Arc<RwLock<Vec<u8>>>)> {
        let mut data = self.data.write().unwrap();
        let content_len = content.read().unwrap().len() as u64;
        let mut evicted = Vec::new();

        if let Some((_old_content, old_len)) = data.cache.put(ino, (content.clone(), content_len)) {
            data.current_size -= old_len;
        }

        data.current_size += content_len;

        while data.current_size > self.max_size {
            if let Some((evicted_ino, (evicted_content, evicted_len))) = data.cache.pop_lru() {
                data.current_size -= evicted_len;
                evicted.push((evicted_ino, evicted_content));
            } else {
                break;
            }
        }
        self.cache_cond.notify_all();
        evicted
    }

    pub fn get(&self, ino: &u64) -> Option<Arc<RwLock<Vec<u8>>>> {
        self.data.write().unwrap().cache.get_mut(ino).map(|(c, _)| c.clone())
    }

    pub fn remove(&self, ino: &u64) {
        let mut data = self.data.write().unwrap();
        if let Some((_content, content_len)) = data.cache.pop(ino) {
            data.current_size -= content_len;
        }
    }

}
