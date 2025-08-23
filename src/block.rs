use std::sync::{Arc, RwLock};

#[derive(Clone, Debug)]
pub struct Block {
    data: Arc<RwLock<Vec<u8>>>,
}

impl Block {
    pub fn new(data: Vec<u8>) -> Self {
        Self {
            data: Arc::new(RwLock::new(data)),
        }
    }

    pub fn read(&self) -> Arc<RwLock<Vec<u8>>> {
        self.data.clone()
    }
}
