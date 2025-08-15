use std::sync::{Arc, Condvar, RwLock};
use lru::LruCache;

use crate::node::{NodeKind, Nodes};

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

    pub fn put(
        &self,
        ino: u64,
        content: Arc<RwLock<Vec<u8>>>,
        nodes: &Arc<RwLock<Nodes>>,
    ) -> Vec<(u64, Arc<RwLock<Vec<u8>>>)> {
        let mut data = self.data.write().unwrap();
        let content_len = content.read().unwrap().len() as u64;
        let mut evicted = Vec::new();

        if let Some((_old_content, old_len)) = data.cache.put(ino, (content.clone(), content_len)) {
            data.current_size -= old_len;
        }

        data.current_size += content_len;

        while data.current_size > self.max_size {
            let mut evicted_one = false;
            let keys: Vec<u64> = data.cache.iter().map(|(k, _v)| *k).collect();

            for key_to_evict in keys.iter().rev() {
                if *key_to_evict == ino {
                    continue;
                }
                let is_dirty = {
                    let nodes_guard = nodes.read().unwrap();
                    if let Ok(node) = nodes_guard.get(*key_to_evict) {
                        if let NodeKind::File(file) = &node.kind {
                            file.dirty
                        } else {
                            false
                        }
                    } else {
                        false
                    }
                };

                if !is_dirty {
                    if let Some((evicted_content, evicted_len)) = data.cache.pop(key_to_evict) {
                        data.current_size -= evicted_len;
                        evicted.push((*key_to_evict, evicted_content));
                        evicted_one = true;
                        break;
                    }
                }
            }

            if !evicted_one {
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
