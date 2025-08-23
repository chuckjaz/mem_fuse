use std::{
    collections::HashMap,
    ffi::{OsStr, OsString},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
    time::SystemTime,
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

pub const DEFAULT_BLOCK_SIZE: usize = 1024 * 1024;

#[derive(Clone, Debug, PartialEq)]
pub struct FileBlocks {
    pub blocks: Vec<Vec<u8>>,
    pub block_size: usize,
    pub length: u64,
}

impl FileBlocks {
    pub fn new(block_size: usize) -> Self {
        Self {
            blocks: Vec::new(),
            block_size,
            length: 0,
        }
    }

    pub fn read(&self, offset: u64, size: u32) -> Vec<u8> {
        let mut data = Vec::with_capacity(size as usize);
        if offset >= self.length {
            return data;
        }

        let mut remaining_size = std::cmp::min(size as u64, self.length - offset) as usize;
        let mut current_offset = offset;

        while remaining_size > 0 {
            let block_index = (current_offset / self.block_size as u64) as usize;
            let offset_in_block = (current_offset % self.block_size as u64) as usize;
            let read_size = std::cmp::min(remaining_size, self.block_size - offset_in_block);

            if block_index < self.blocks.len() {
                let block = &self.blocks[block_index];
                let end = std::cmp::min(offset_in_block + read_size, block.len());
                data.extend_from_slice(&block[offset_in_block..end]);
            } else {
                // Reading from a hole, which is all zeros
                data.extend(std::iter::repeat(0).take(read_size));
            }
            remaining_size -= read_size;
            current_offset += read_size as u64;
        }
        data
    }

    pub fn write(&mut self, offset: u64, new_data: &[u8]) {
        let new_length = offset + new_data.len() as u64;
        if new_length > self.length {
            self.length = new_length;
        }

        let num_blocks = (self.length as f64 / self.block_size as f64).ceil() as usize;
        if num_blocks > self.blocks.len() {
            self.blocks.resize(num_blocks, Vec::new());
        }

        let mut remaining_data = new_data;
        let mut current_offset = offset;

        while !remaining_data.is_empty() {
            let block_index = (current_offset / self.block_size as u64) as usize;
            let offset_in_block = (current_offset % self.block_size as u64) as usize;
            let write_size = std::cmp::min(remaining_data.len(), self.block_size - offset_in_block);

            let block = &mut self.blocks[block_index];
            let required_size = offset_in_block + write_size;
            if block.len() < required_size {
                block.resize(required_size, 0);
            }
            block[offset_in_block..required_size]
                .copy_from_slice(&remaining_data[..write_size]);

            remaining_data = &remaining_data[write_size..];
            current_offset += write_size as u64;
        }
    }

    pub fn truncate(&mut self, size: u64) {
        self.length = size;
        let num_blocks = (self.length as f64 / self.block_size as f64).ceil() as usize;
        self.blocks.truncate(num_blocks);
        if let Some(last_block) = self.blocks.last_mut() {
            let last_block_size = (self.length % self.block_size as u64) as usize;
            last_block.truncate(last_block_size);
        }
    }
}

#[derive(Clone)]
pub enum FileContent {
    InMemory(Arc<RwLock<FileBlocks>>),
    OnDisk,
}

impl FileContent {
    pub fn is_ondisk(&self) -> bool {
        matches!(self, FileContent::OnDisk)
    }
}

#[derive(Clone)]
pub struct File {
    pub content: FileContent,
    pub dirty: bool,
    pub dirty_regions: DirtyRegions,
}

impl File {
    pub fn new(block_size: usize) -> Self {
        Self {
            content: FileContent::InMemory(Arc::new(RwLock::new(FileBlocks::new(block_size)))),
            dirty: false,
            dirty_regions: DirtyRegions::new(),
        }
    }

    pub fn new_on_disk() -> Self {
        Self {
            content: FileContent::OnDisk,
            dirty: false,
            dirty_regions: DirtyRegions::new(),
        }
    }
}

#[derive(Clone)]
pub enum DirectoryKind {
    InMemory(Directory),
    OnDisk,
}

impl DirectoryKind {
    pub fn is_ondisk(&self) -> bool {
        matches!(self, DirectoryKind::OnDisk)
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
            kind: NodeKind::File(File::new(block_size)),
        }
    }

    pub fn new_file_on_disk(attr: FileAttr) -> Self {
        Self {
            attr,
            kind: NodeKind::File(File::new_on_disk()),
        }
    }

    pub fn new_directory(attr: FileAttr) -> Self {
        Self {
            attr,
            kind: NodeKind::Directory(DirectoryKind::InMemory(Directory::new())),
        }
    }

    pub fn new_directory_on_disk(attr: FileAttr) -> Self {
        Self {
            attr,
            kind: NodeKind::Directory(DirectoryKind::OnDisk),
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
            NodeKind::Directory(DirectoryKind::OnDisk) => Err(EEXIST),
            _ => Err(ENOTDIR),
        }
    }

    pub fn get_dir_mut(&mut self) -> Result<&mut Directory> {
        self.written();
        match &mut self.kind {
            NodeKind::Directory(DirectoryKind::InMemory(dir)) => Ok(dir),
            NodeKind::Directory(DirectoryKind::OnDisk) => Err(EEXIST),
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
