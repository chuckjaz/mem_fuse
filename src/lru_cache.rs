use std::sync::{Arc, Condvar, Mutex};
use lru::LruCache;
use libc;

struct CacheData {
    cache: LruCache<(u64, usize), (Vec<u8>, bool)>, // (ino, block_index) -> (data, is_dirty)
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
        block_index: usize,
        data: Vec<u8>,
        dirty: bool,
    ) -> Result<Vec<((u64, usize), Vec<u8>)>, libc::c_int> {
        let block_len = data.len() as u64;
        let mut data_lock = self.data.lock().unwrap();

        let new_dirty_len = if dirty { block_len } else { 0 };
        if data_lock.dirty_size + new_dirty_len > self.max_write_size {
            while data_lock.dirty_size + new_dirty_len > self.max_write_size {
                data_lock = self.cache_cond.wait(data_lock).unwrap();
            }
        }

        let mut evicted = Vec::new();
        while data_lock.current_size + block_len > self.max_size {
            let mut evicted_one = false;
            if let Some((lru_key, (_, lru_dirty))) = data_lock.cache.peek_lru() {
                if *lru_key == (ino, block_index) {
                    break;
                }
                if !*lru_dirty {
                    let (evicted_key, (evicted_data, _)) = data_lock.cache.pop_lru().unwrap();
                    data_lock.current_size -= evicted_data.len() as u64;
                    evicted.push((evicted_key, evicted_data));
                    evicted_one = true;
                }
            }

            if !evicted_one {
                break;
            }
        }

        if data_lock.current_size + block_len > self.max_size {
            return Err(libc::ENOSPC);
        }

        if let Some((old_data, old_dirty)) = data_lock.cache.put((ino, block_index), (data, dirty)) {
            data_lock.current_size -= old_data.len() as u64;
            if old_dirty {
                data_lock.dirty_size -= old_data.len() as u64;
            }
        }
        data_lock.current_size += block_len;
        if dirty {
            data_lock.dirty_size += block_len;
        }

        self.cache_cond.notify_all();
        Ok(evicted)
    }

    pub fn get(&self, ino: u64, block_index: usize) -> Option<Vec<u8>> {
        self.data
            .lock()
            .unwrap()
            .cache
            .get_mut(&(ino, block_index))
            .map(|(data, _)| data.clone())
    }

    pub fn remove(&self, ino: u64, block_index: usize) {
        let mut data = self.data.lock().unwrap();
        if let Some((block_data, is_dirty)) = data.cache.pop(&(ino, block_index)) {
            data.current_size -= block_data.len() as u64;
            if is_dirty {
                data.dirty_size -= block_data.len() as u64;
            }
        }
    }

    pub fn remove_file(&self, ino: u64) {
        let mut data = self.data.lock().unwrap();
        let keys_to_remove: Vec<(u64, usize)> = data
            .cache
            .iter()
            .filter(|((entry_ino, _), _)| *entry_ino == ino)
            .map(|(key, _)| *key)
            .collect();

        for key in keys_to_remove {
            if let Some((block_data, is_dirty)) = data.cache.pop(&key) {
                data.current_size -= block_data.len() as u64;
                if is_dirty {
                    data.dirty_size -= block_data.len() as u64;
                }
            }
        }
    }

    pub fn mark_as_clean(&self, ino: u64, block_index: usize) {
        let mut data = self.data.lock().unwrap();
        if let Some((block_data, is_dirty)) = data.cache.get_mut(&(ino, block_index)) {
            if *is_dirty {
                *is_dirty = false;
                data.dirty_size -= block_data.len() as u64;
            }
        }
    }

    pub fn get_dirty_blocks_for_ino(&self, ino: u64) -> Vec<(usize, Vec<u8>)> {
        let mut dirty_blocks = Vec::new();
        let mut data = self.data.lock().unwrap();
        for ((entry_ino, block_index), (block_data, is_dirty)) in data.cache.iter() {
            if *entry_ino == ino && *is_dirty {
                dirty_blocks.push((*block_index, block_data.clone()));
            }
        }
        dirty_blocks
    }
}
