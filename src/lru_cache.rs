use std::sync::{Arc, Condvar, Mutex, RwLock};
use lru::LruCache;
use libc;
use crate::node::FileBlocks;

struct CacheData {
    cache: LruCache<u64, (Arc<RwLock<FileBlocks>>, u64, bool)>, // content, len, is_dirty
    current_size: u64,
    dirty_size: u64,
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
                dirty_size: 0,
            }),
            max_size,
            max_write_size,
            cache_cond,
        }
    }

    pub fn put(
        &self,
        ino: u64,
        content: Arc<RwLock<FileBlocks>>,
        has_mirror: bool,
        dirty: bool,
    ) -> Result<Vec<(u64, Arc<RwLock<FileBlocks>>)>, libc::c_int> {
        let content_len = content.read().unwrap().length;
        let mut data = self.data.lock().unwrap();

        let new_dirty_len = if dirty { content_len } else { 0 };
        if data.dirty_size + new_dirty_len > self.max_write_size {
            if has_mirror {
                while data.dirty_size + new_dirty_len > self.max_write_size {
                    data = self.cache_cond.wait(data).unwrap();
                }
            } else {
                return Err(libc::ENOSPC);
            }
        }

        let mut evicted = Vec::new();
        while data.current_size + content_len > self.max_size {
            let mut evicted_one = false;
            if let Some((lru_ino, ( _, _, lru_dirty))) = data.cache.peek_lru() {
                if *lru_ino == ino {
                    // This can happen if we are updating an existing file that is also the LRU one.
                    // We should not evict it in this case.
                    break;
                }
                if !lru_dirty {
                    let (evicted_ino, (evicted_content, evicted_len, _)) = data.cache.pop_lru().unwrap();
                    data.current_size -= evicted_len;
                    // dirty_size is not affected since it was a clean file
                    evicted.push((evicted_ino, evicted_content));
                    evicted_one = true;
                }
            }

            if !evicted_one {
                break; // No more clean files to evict.
            }
        }

        if data.current_size + content_len > self.max_size {
            return Err(libc::ENOSPC);
        }

        if let Some((_old_content, old_len, old_dirty)) = data.cache.put(ino, (content.clone(), content_len, dirty)) {
            data.current_size -= old_len;
            if old_dirty {
                data.dirty_size -= old_len;
            }
        }
        data.current_size += content_len;
        if dirty {
            data.dirty_size += content_len;
        }

        self.cache_cond.notify_all();
        Ok(evicted)
    }

    pub fn mark_as_clean(&self, ino: u64) {
        let mut data = self.data.lock().unwrap();
        if let Some((_, len, is_dirty)) = data.cache.get_mut(&ino) {
            if *is_dirty {
                *is_dirty = false;
                data.dirty_size -= *len;
            }
        }
    }

    pub fn get(&self, ino: &u64) -> Option<Arc<RwLock<FileBlocks>>> {
        self.data.lock().unwrap().cache.get_mut(ino).map(|(c, _, _)| c.clone())
    }

    pub fn remove(&self, ino: &u64) {
        let mut data = self.data.lock().unwrap();
        if let Some((_content, content_len, is_dirty)) = data.cache.pop(ino) {
            data.current_size -= content_len;
            if is_dirty {
                data.dirty_size -= content_len;
            }
        }
    }
}
