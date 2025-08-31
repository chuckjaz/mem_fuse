use std::sync::{Arc, Condvar, Mutex};
use lru::LruCache;
use libc;

#[derive(Clone, Copy, Debug, Hash, PartialEq, Eq)]
struct BlockAddress {
    ino: u64,
    block: usize,
}

#[derive(Clone, Copy, Debug)]
struct BlockEntry {
    version: usize,
    block_size: usize,
    is_dirty: bool
}

#[derive(Debug)]
pub struct BlockEviction {
    pub ino: u64,
    pub block: usize,
    pub version: usize,
}

struct CacheData {
    cache: LruCache<BlockAddress, BlockEntry>,
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

    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.data.lock().unwrap().cache.is_empty()
    }

    pub fn put(
        &self,
        ino: u64,
        block: usize,
        block_size: usize,
        version: usize,
        has_mirror: bool,
        is_dirty: bool,
    ) -> Result<Vec<BlockEviction>, libc::c_int> {
        let new_dirty_len = if is_dirty { block_size as u64 } else { 0 };
        let mut data = self.data.lock().unwrap();
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
        if data.current_size  + block_size as u64 > self.max_size {
            let mut evicted_size = 0u64;

            for (block_address, block_entry) in data.cache.iter().rev() {
                if !block_entry.is_dirty {
                    evicted.push(BlockEviction {
                        ino: block_address.ino,
                        block: block_address.block,
                        version: block_entry.version,
                    });
                    evicted_size += block_entry.block_size as u64;
                    if (data.current_size - evicted_size) + block_size as u64 <= self.max_size {
                        break
                    }
                }
            }

            // If we still don't have enough room we are out of space.
            if (data.current_size - evicted_size) + block_size as u64 > self.max_size {
                // Restore the previous current size as we are not going to evict the blocks we found.
                return Err(libc::ENOSPC);
            }

            // Remove evicted blocks
            for eviction in &evicted {
                let block_address = BlockAddress { ino: eviction.ino, block:eviction.block };
                data.cache.pop_entry(&block_address);
            }

            data.current_size -= evicted_size;
        }

        // Record the space (maybe update the existing entry)
        let block_address = BlockAddress { ino, block };
        let block_entry = BlockEntry { version, block_size, is_dirty };
        if let Some(old_entry) = data.cache.put(block_address, block_entry) {
            data.current_size -= old_entry.block_size as u64;
            if old_entry.is_dirty {
                data.dirty_size -= old_entry.block_size as u64;
            }
        }
        data.current_size += block_size as u64;
        if is_dirty {
            data.dirty_size += block_size as u64;
        }

        self.cache_cond.notify_all();
        Ok(evicted)
    }

    pub fn update_version(
        &self,
        ino: u64,
        block_index: usize,
        version: usize,
    ) {
        let mut data = self.data.lock().unwrap();
        let block_address = BlockAddress { ino, block: block_index };
        if let Some(current) = data.cache.get_mut(&block_address) {
            current.version = version;
        }
    }

    pub fn mark_as_clean(&self, ino: u64, block: usize) {
        let mut data = self.data.lock().unwrap();
        let block_address = BlockAddress { ino, block };
        if let Some(entry) = data.cache.get_mut(&block_address) {
            if entry.is_dirty {
                entry.is_dirty = false;
                data.dirty_size -= entry.block_size as u64;
            }
        }
        self.cache_cond.notify_all();
    }

    pub fn promote(&self, ino: u64, block: usize) {
        let block_address = BlockAddress { ino, block };
        let mut data = self.data.lock().unwrap();
        data.cache.promote(&block_address);
    }

    pub fn remove(&self, ino: u64, block: usize) {
        let block_address = BlockAddress { ino, block };
        let mut data = self.data.lock().unwrap();
        if let Some(entry) = data.cache.pop(&block_address) {
            data.current_size -= entry.block_size as u64;
            if entry.is_dirty {
                data.dirty_size -= entry.block_size as u64;
            }
            self.cache_cond.notify_all();
        }
    }
}
