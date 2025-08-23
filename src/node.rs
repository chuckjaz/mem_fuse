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

use crate::{block::Block, dirty::DirtyBlocks, mem_fuse::{OrError, Result}};

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

#[derive(Clone)]
pub enum FileContent {
    InMemoryBlocks(Arc<RwLock<HashMap<u64, Block>>>),
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
    pub dirty_blocks: DirtyBlocks,
    pub block_size: u64,
}

impl File {
    pub fn new(block_size: u64) -> Self {
        Self {
            content: FileContent::InMemoryBlocks(Arc::new(RwLock::new(HashMap::new()))),
            dirty: false,
            dirty_blocks: DirtyBlocks::new(),
            block_size,
        }
    }

    pub fn new_on_disk(block_size: u64) -> Self {
        Self {
            content: FileContent::OnDisk,
            dirty: false,
            dirty_blocks: DirtyBlocks::new(),
            block_size,
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
    // TODO: Pass block_size down from mem_fuse
    pub fn new_file(attr: FileAttr, block_size: u64) -> Self {
        Self {
            attr,
            kind: NodeKind::File(File::new(block_size)),
        }
    }

    // TODO: Pass block_size down from mem_fuse
    pub fn new_file_on_disk(attr: FileAttr, block_size: u64) -> Self {
        Self {
            attr,
            kind: NodeKind::File(File::new_on_disk(block_size)),
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
