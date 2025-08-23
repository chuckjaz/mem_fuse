use std::collections::BTreeSet;

#[derive(Debug, Default, Clone)]
pub struct DirtyBlocks {
    blocks: BTreeSet<u64>,
}

impl DirtyBlocks {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_block(&mut self, block_id: u64) {
        self.blocks.insert(block_id);
    }

    pub fn blocks(&self) -> &BTreeSet<u64> {
        &self.blocks
    }

    pub fn clear(&mut self) {
        self.blocks.clear();
    }

    pub fn truncate(&mut self, block_size: u64, size: u64) {
        let last_block = size / block_size;
        self.blocks.retain(|&block_id| block_id <= last_block);
    }
}
