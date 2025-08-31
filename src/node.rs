use std::{
    cmp::min, collections::HashMap, ffi::{OsStr, OsString}, path::{Path, PathBuf}, sync::{Arc, RwLock}, time::SystemTime
};

use fuser::FileAttr;
use indexmap::IndexMap;
use libc::{EEXIST, ENOENT, ENOTDIR};

use crate::{dirty::DirtyRegions, mem_fuse::{OrError, Result}};

#[derive(Clone)]
pub struct Directory {
    entries: IndexMap<OsString, u64>,
}

impl Directory {
    pub fn new() -> Self {
        Self {
            entries: IndexMap::new(),
        }
    }

    pub fn get(&self, name: &OsStr) -> Option<u64> {
        self.entries.get(name).copied()
    }

    pub fn set(&mut self, name: &OsStr, ino: u64) -> Result<Option<u64>> {
        Ok(self.entries.insert(name.into(), ino))
    }

    pub fn insert(&mut self, name: &OsStr, ino: u64) -> Result<()> {
        if self.has(name) {
            Err(EEXIST)
        } else {
            self.entries.insert(name.into(), ino);
            Ok(())
        }
    }

    pub fn remove(&mut self, name: &OsStr) -> Result<u64> {
        self.entries.swap_remove(name).or_error(ENOENT)
    }

    pub fn has(&self, name: &OsStr) -> bool {
        self.entries.contains_key(name)
    }

    pub fn iter(&self) -> indexmap::map::Iter<'_, OsString, u64> {
        self.entries.iter()
    }
}

#[cfg(test)]
pub const DEFAULT_BLOCK_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub struct Block {
    data: Vec<u8>,
    version: usize
}

impl Block {
    fn new(size: usize) -> Self {
        let mut data = Vec::new();
        data.resize(size, 0u8);
        Self { data, version: 1 }
    }

    fn new_version(&mut self) {
        self.version += 1;
    }
}

#[derive(Clone, Debug, PartialEq)]
pub enum FileBlock {
    InMemory(Block),
    Mirrored,
}

impl FileBlock {
    fn new_in_memory(size: usize) -> FileBlock {
        Self::InMemory(Block::new(size))
    }

    fn is_mirrored(&self) -> bool {
        if let FileBlock::Mirrored = self { true } else { false }
    }

    fn unwrap_vec(&self) -> &Vec<u8> {
        if let FileBlock::InMemory(block) = self {
            &block.data
        } else {
            panic!("Unexpected Mirrored block")
        }
    }

    fn unwrap_vec_mut(&mut self) -> &mut Vec<u8> {
        if let FileBlock::InMemory(block) = self {
            block.new_version();
            &mut block.data
        } else {
            panic!("Unexpected Mirrored block")
        }
    }

    fn version(&self) -> usize {
        match self {
            FileBlock::InMemory(block) => block.version,
            FileBlock::Mirrored => 0
        }
    }
}

#[derive(Debug)]
pub struct FileBlocks {
    pub blocks: Vec<FileBlock>,
    pub block_size: usize,
    pub size: u64,
    pub dirty_regions: DirtyRegions,
}

impl FileBlocks {
    pub fn new(block_size: usize) -> Self {
        Self {
            blocks: Vec::new(),
            block_size,
            size: 0,
            dirty_regions: DirtyRegions::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.blocks.len()
    }

    pub fn is_mirrored(&self, block_index: usize) -> bool {
        if block_index >= self.blocks.len() {
            panic!("Indexing past the last block")
        }
        if let FileBlock::Mirrored = self.blocks[block_index] { true } else { false }
    }

    pub fn any_mirrored(&self, start: u64, end: u64) -> bool {
        for (_, block_index, _, _) in block_ranges(start, end, self.block_size) {
            if self.is_mirrored(block_index) {
                return true
            }
        }
        false
    }

    pub fn get_block(&self, block_index: usize) -> &Vec<u8> {
        if block_index >= self.blocks.len() {
            panic!("Indexing past the last block")
        }
        self.blocks[block_index].unwrap_vec()
    }

    pub fn get_block_mut(&mut self, block_index: usize) -> &mut Vec<u8> {
        if block_index >= self.blocks.len() {
            panic!("Indexing past the last block")
        }
        let blocks = &mut self.blocks;
        let blocks_len = blocks.len();
        let block = &mut blocks[block_index];
        if block.is_mirrored() {
            if block_index == blocks_len - 1 && self.size % (self.block_size as u64) != 0 {
                let last_block_size = (self.size % self.block_size as u64) as usize;
                *block = FileBlock::new_in_memory(last_block_size);
            } else {
                *block = FileBlock::new_in_memory(self.block_size);
            }
        }
        block.unwrap_vec_mut()
    }

    pub fn version(&self, block_index: usize) -> usize {
        if block_index >= self.blocks.len() {
            panic!("Indexing past the last block")
        }
        self.blocks[block_index].version()
    }

    pub fn extend_to(&mut self, len: u64) {
        let blocks_needed = (len as f64 / self.block_size as f64).ceil() as usize;
        let last_block_index = (self.size / self.block_size as u64) as usize;
        if self.blocks.len() < blocks_needed {
            if last_block_index < self.blocks.len() && !self.blocks[last_block_index].is_mirrored() {
                self.blocks[last_block_index].unwrap_vec_mut().resize(self.block_size, 0u8)
            }
            self.blocks.resize(blocks_needed, FileBlock::Mirrored);
        } else if self.blocks.len() == blocks_needed && blocks_needed != 0 {
            if !self.blocks[last_block_index].is_mirrored() {
                let new_last_block_size = {
                    let size = (len % self.block_size as u64) as usize;
                    if size == 0usize { self.block_size } else { size }
                };
                self.blocks[last_block_index].unwrap_vec_mut().resize(new_last_block_size, 0u8);
            }
        }
        self.size = len;
    }

    pub fn truncate_to(&mut self, size: u64) -> usize {
        self.size = size;
        let num_blocks = self.required_blocks_for(size);
        self.blocks.truncate(num_blocks);
        let last_block_size = (self.size % self.block_size as u64) as usize;
        if let Some(last_block) = self.blocks.last_mut() {
            let vec = last_block.unwrap_vec_mut();
            vec.truncate(last_block_size);
        }
        last_block_size
    }

    pub fn required_blocks_for(&self, size: u64) -> usize {
        (size as f64 / self.block_size as f64).ceil() as usize
    }

    pub fn evict_version(&mut self, block_index: usize, version: usize) {
        if block_index >= self.blocks.len() {
            // Ignore this as this may be an old request
            return;
        }

        // Only evict if it is still the version we wanted evicted.
        // If not it may have been resurrected by a write so ignore it as the write may still
        // need the buffer.
        if self.version(block_index) == version {
            self.blocks[block_index] = FileBlock::Mirrored
        }
    }

    pub fn size_of_block(&self, block_index: usize) -> usize {
        if block_index == self.len() - 1 {
            (self.size % (self.block_size as u64)) as usize
        } else {
            self.block_size
        }
    }
}

struct BlockIter {
    current: u64,
    end: u64,
    block_size: usize,
}

impl Iterator for BlockIter {
    type Item = (u64, usize, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if self.current >= self.end { return None };
        let current = self.current;
        let block_start = (current % self.block_size as u64) as usize;
        let block_end = min(self.block_size, block_start + (self.end - self.current) as usize);
        let block_index = (current / self.block_size as u64) as usize;
        if block_start > block_end {
            let end = self.end;
            panic!("Something is wrong, current: {current}, end: {end}, block_start: {block_start}, block_end: {block_end}");
        }
        self.current += (block_end - block_start) as u64;
        if block_start == block_end {
            None
        } else {
            Some((current, block_index, block_start, block_end))
        }
    }
}

pub fn block_ranges(start: u64, end: u64, block_size: usize) -> impl Iterator<Item = (u64, usize, usize, usize)> {
    BlockIter { current: start, end, block_size }
}

#[derive(Clone)]
pub struct File {
    pub blocks: Arc<RwLock<FileBlocks>>,
}

impl File {
    pub fn new(block_size: usize, size: u64) -> Self {
        let mut blocks_inner = FileBlocks::new(block_size);
        blocks_inner.extend_to(size);
        Self {
            blocks: Arc::new(RwLock::new(blocks_inner)),
        }
    }
}

#[derive(Clone)]
pub enum DirectoryKind {
    InMemory(Directory),
    Mirrored,
}

impl DirectoryKind {
    pub fn is_ondisk(&self) -> bool {
        matches!(self, DirectoryKind::Mirrored)
    }
}

#[derive(Clone)]
pub enum NodeKind {
    File(File),
    Directory(DirectoryKind),
    SymbolicLink { target: PathBuf },
}

#[derive(Clone)]
pub struct Node {
    pub kind: NodeKind,
    pub attr: FileAttr,
}

impl Node {
    pub fn new_file(attr: FileAttr, block_size: usize) -> Self {
        Self {
            attr,
            kind: NodeKind::File(File::new(block_size, attr.size)),
        }
    }

    pub fn new_directory(attr: FileAttr) -> Self {
        Self {
            attr,
            kind: NodeKind::Directory(DirectoryKind::InMemory(Directory::new())),
        }
    }

    pub fn new_directory_mirrored(attr: FileAttr) -> Self {
        Self {
            attr,
            kind: NodeKind::Directory(DirectoryKind::Mirrored),
        }
    }

    pub fn new_symbolic_link(attr: FileAttr, target: &Path) -> Self {
        Self {
            attr,
            kind: NodeKind::SymbolicLink {
                target: target.to_path_buf(),
            },
        }
    }

    pub fn get_target(&mut self) -> Result<&Path> {
        self.accessed();
        match &self.kind {
            NodeKind::SymbolicLink { target } => Ok(target),
            _ => Err(ENOENT),
        }
    }

    pub fn get_dir(&mut self) -> Result<&Directory> {
        self.accessed();
        match &self.kind {
            NodeKind::Directory(DirectoryKind::InMemory(dir)) => Ok(dir),
            NodeKind::Directory(DirectoryKind::Mirrored) => Err(EEXIST),
            _ => Err(ENOTDIR),
        }
    }

    pub fn get_dir_mut(&mut self) -> Result<&mut Directory> {
        self.written();
        match &mut self.kind {
            NodeKind::Directory(DirectoryKind::InMemory(dir)) => Ok(dir),
            NodeKind::Directory(DirectoryKind::Mirrored) => Err(EEXIST),
            _ => Err(ENOTDIR),
        }
    }

    pub fn add_link(&mut self) {
        self.attr.nlink += 1
    }

    pub fn release(&mut self) -> bool {
        let new_count = self.attr.nlink - 1;
        self.attr.nlink = new_count;
        new_count == 0
    }

    pub fn accessed(&mut self) {
        self.attr.atime = SystemTime::now()
    }

    pub fn written(&mut self) {
        self.accessed();
        self.attr.mtime = SystemTime::now()
    }
}

#[derive(Clone)]
pub struct Nodes {
    nodes: HashMap<u64, Node>,
    parents: HashMap<u64, Vec<(u64, OsString)>>,
}

impl Nodes {
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
            parents: HashMap::new(),
        }
    }

    pub fn add_parent(&mut self, ino: u64, parent: u64, name: &OsStr) {
        self.parents
            .entry(ino)
            .or_default()
            .push((parent, name.to_os_string()));
    }

    pub fn remove_parent(&mut self, ino: u64, parent: u64, name: &OsStr) {
        if let Some(parents) = self.parents.get_mut(&ino) {
            parents.retain(|(p_ino, p_name)| *p_ino != parent || p_name != name);
            if parents.is_empty() {
                self.parents.remove(&ino);
            }
        }
    }

    pub fn get_parents(&self, ino: u64) -> Option<&Vec<(u64, OsString)>> {
        self.parents.get(&ino)
    }

    pub fn insert(&mut self, mut node: Node) -> Result<FileAttr> {
        let ino = node.attr.ino;
        node.add_link();
        let attr = node.attr;
        self.nodes.insert(ino, node);
        Ok(attr)
    }

    pub fn dec_link(&mut self, ino: u64) -> Result<()> {
        let node = self.get_mut(ino)?;
        if node.release() {
            self.nodes.remove(&ino);
        }
        Ok(())
    }

    pub fn get(&self, ino: u64) -> Result<&Node> {
        if let Some(node) = self.nodes.get(&ino) {
            Ok(node)
        } else {
            Err(ENOENT)
        }
    }

    pub fn get_mut(&mut self, ino: u64) -> Result<&mut Node> {
        self.nodes.get_mut(&ino).or_error(ENOENT)
    }

    pub fn get_dir(&mut self, ino: u64) -> Result<&Directory> {
        let node = self.get_mut(ino)?;
        node.get_dir()
    }

    pub fn get_dir_anon(&self, ino: u64) -> Result<&Directory> {
        let node = self.get(ino)?;
        if let NodeKind::Directory(DirectoryKind::InMemory(dir)) = &node.kind {
            Ok(dir)
        } else {
            Err(ENOTDIR)
        }
    }

    pub fn get_dir_mut(&mut self, ino: u64) -> Result<&mut Directory> {
        let node = self.get_mut(ino)?;
        node.get_dir_mut()
    }

    pub fn find(&mut self, parent: u64, name: &OsStr) -> Result<&Node> {
        let dir = self.get_dir(parent)?;
        let ino = dir.get(name).or_error(ENOENT)?;
        self.get(ino)
    }
}
