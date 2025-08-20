use std::sync::{Arc, Condvar, Mutex, RwLock};
use lru::LruCache;
use libc;

use crate::node::{NodeKind, Nodes};

struct CacheData {
    cache: LruCache<u64, (Arc<RwLock<Vec<u8>>>, u64)>,
    current_size: u64,
}

pub struct LruManager {
    data: Mutex<CacheData>,
    max_size: u64,
    max_write_size: u64,
    cache_cond: Arc<Condvar>,
}

impl LruManager {
    pub fn new(max_size: u64, max_write_size: u64, cache_cond: Arc<Condvar>) -> Self {
        Self {
            data: Mutex::new(CacheData {
                cache: LruCache::unbounded(),
                current_size: 0,
            }),
            max_size,
            max_write_size,
            cache_cond,
        }
    }

    pub fn put(
        &self,
        ino: u64,
        content: Arc<RwLock<Vec<u8>>>,
        nodes: &Arc<RwLock<Nodes>>,
        has_mirror: bool,
    ) -> Result<Vec<(u64, Arc<RwLock<Vec<u8>>>)>, libc::c_int> {
        let content_len = content.read().unwrap().len() as u64;
        let mut data = self.data.lock().unwrap();

        if data.current_size + content_len > self.max_write_size {
            if has_mirror {
                while data.current_size + content_len > self.max_write_size {
                    data = self.cache_cond.wait(data).unwrap();
                    Self::evict_clean(&mut data, self.max_size, ino, nodes);
                }
            } else {
                // Before returning an error, try to evict some clean files to make space.
                // This is a simplified eviction pass. A more thorough one happens below.
                // We only do this if there's no mirror, as a last ditch effort.
                Self::evict_clean(&mut data, self.max_size, ino, nodes);
                if data.current_size + content_len > self.max_write_size {
                    return Err(libc::ENOSPC);
                }
            }
        }

        let mut evicted = Vec::new();

        if let Some((_old_content, old_len)) = data.cache.put(ino, (content.clone(), content_len)) {
            data.current_size -= old_len;
        }
        data.current_size += content_len;

        Self::evict_clean(&mut data, self.max_size, ino, nodes)
            .into_iter()
            .for_each(|item| evicted.push(item));

        self.cache_cond.notify_all();
        Ok(evicted)
    }

    fn evict_clean(data: &mut std::sync::MutexGuard<CacheData>, max_size: u64, current_ino: u64, nodes: &Arc<RwLock<Nodes>>) -> Vec<(u64, Arc<RwLock<Vec<u8>>>)> {
        let mut evicted = Vec::new();
        // Evict files that are not dirty
        // We do this until the cache size is below the max_size
        // Note that we may still be above max_size if the cache is full of dirty files
        // that cannot be evicted.
        while data.current_size > max_size {
            let mut evicted_one = false;
            let keys: Vec<u64> = data.cache.iter().map(|(k, _v)| *k).collect();

            for key_to_evict in keys.iter().rev() {
                if *key_to_evict == current_ino {
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
        evicted
    }


    pub fn get(&self, ino: &u64) -> Option<Arc<RwLock<Vec<u8>>>> {
        self.data.lock().unwrap().cache.get_mut(ino).map(|(c, _)| c.clone())
    }

    pub fn remove(&self, ino: &u64) {
        let mut data = self.data.lock().unwrap();
        if let Some((_content, content_len)) = data.cache.pop(ino) {
            data.current_size -= content_len;
        }
    }

}
