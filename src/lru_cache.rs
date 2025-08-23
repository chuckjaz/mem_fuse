use std::sync::{Arc, Condvar, Mutex};
use lru::LruCache;
use libc;
use crate::block::Block;

struct CacheData {
    cache: LruCache<(u64, u64), (Block, u64, bool)>, // (ino, block_id) -> (block, len, is_dirty)
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
        block_id: u64,
        block: Block,
        has_mirror: bool,
        dirty: bool,
    ) -> Result<Vec<((u64, u64), Block)>, libc::c_int> {
        let block_len = block.read().read().unwrap().len() as u64;
        let mut data = self.data.lock().unwrap();

        let new_dirty_len = if dirty { block_len } else { 0 };
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
        while data.current_size + block_len > self.max_size {
            let mut evicted_one = false;
            if let Some(((lru_ino, _), ( _, _, lru_dirty))) = data.cache.peek_lru() {
                if *lru_ino == ino {
                    break;
                }
                if !lru_dirty {
                    let (evicted_key, (evicted_block, evicted_len, _)) = data.cache.pop_lru().unwrap();
                    data.current_size -= evicted_len;
                    evicted.push((evicted_key, evicted_block));
                    evicted_one = true;
                }
            }

            if !evicted_one {
                break;
            }
        }

        if data.current_size + block_len > self.max_size {
            return Err(libc::ENOSPC);
        }

        if let Some((_old_block, old_len, old_dirty)) = data.cache.put((ino, block_id), (block.clone(), block_len, dirty)) {
            data.current_size -= old_len;
            if old_dirty {
                data.dirty_size -= old_len;
            }
        }
        data.current_size += block_len;
        if dirty {
            data.dirty_size += block_len;
        }

        self.cache_cond.notify_all();
        Ok(evicted)
    }

    pub fn mark_as_clean(&self, ino: u64, block_id: u64) {
        let mut data = self.data.lock().unwrap();
        if let Some((_, len, is_dirty)) = data.cache.get_mut(&(ino, block_id)) {
            if *is_dirty {
                *is_dirty = false;
                data.dirty_size -= *len;
            }
        }
    }

    pub fn get(&self, ino: u64, block_id: u64) -> Option<Block> {
        self.data.lock().unwrap().cache.get_mut(&(ino, block_id)).map(|(c, _, _)| c.clone())
    }

    pub fn remove(&self, ino: u64) {
        let mut data = self.data.lock().unwrap();
        let keys_to_remove: Vec<_> = data.cache.iter().filter(|(k, _)| k.0 == ino).map(|(k, _)| *k).collect();
        for key in keys_to_remove {
            if let Some((_, len, is_dirty)) = data.cache.pop(&key) {
                data.current_size -= len;
                if is_dirty {
                    data.dirty_size -= len;
                }
            }
        }
    }
}
