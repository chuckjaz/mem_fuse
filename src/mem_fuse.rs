use std::{
    collections::HashMap, ffi::{OsStr, OsString}, os::unix::ffi::OsStrExt, path::Path, sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock, Condvar}, time::SystemTime, u32
};

use fuser::{self, FileAttr, Filesystem, TimeOrNow};
use libc::{c_int, EEXIST, ENOENT, ENOSYS};
#[cfg(target_os = "linux")]
use libc::{RENAME_NOREPLACE, RENAME_EXCHANGE};
use log::{debug, info};
use users::{get_current_gid, get_current_uid};

use crate::block::Block;
use crate::lru_cache::LruManager;
use crate::mirror::{Mirror, MirrorDirEntry, MirrorWorker, PathResolver, WriteJob};
use crate::node::{DirectoryKind, FileContent, Node, NodeKind, Nodes};
use std::os::unix::ffi::OsStringExt;
use std::path::PathBuf;

pub(crate) type Result<T> = std::result::Result<T, c_int>;

pub(crate) struct MemoryFuse {
    pub(crate) nodes: Arc<RwLock<Nodes>>,
    next_ino: Arc<AtomicUsize>,
    mirror_worker: Option<Arc<MirrorWorker>>,
    mirror: Option<Arc<dyn Mirror + Send + Sync>>,
    lru_manager: Arc<LruManager>,
    block_size: u64,
}

impl Clone for MemoryFuse {
    fn clone(&self) -> Self {
        Self {
            nodes: self.nodes.clone(),
            next_ino: self.next_ino.clone(),
            mirror_worker: self.mirror_worker.as_ref().map(|w| w.clone()),
            mirror: self.mirror.clone(),
            lru_manager: self.lru_manager.clone(),
            block_size: self.block_size,
        }
    }
}

impl MemoryFuse {
    pub(crate) fn new(
        mirror: Option<Arc<dyn Mirror + Send + Sync>>,
        cache_size: u64,
        cache_max_write_size: u64,
        lazy_load: bool,
        block_size: u64,
    ) -> MemoryFuse {
        let nodes_arc = Arc::new(RwLock::new(Nodes::new()));

        let next_ino_atomic = Arc::new(AtomicUsize::new(2));
        let attr = new_attr(
            1,
            fuser::FileType::Directory,
            0o755,
            get_current_uid(),
            get_current_gid(),
            None,
        );
        let root = Node::new_directory(attr);
        {
            let mut nodes = nodes_arc.write().unwrap();
            let _ = nodes.insert(root);
        }

        if lazy_load {
            if let Some(mirror) = &mirror {
                let new_entries = {
                    let path_resolver = PathResolver::new(&nodes_arc);
                    mirror.read_dir(1, &path_resolver).unwrap()
                };

                let mut fuse = MemoryFuse {
                    nodes: nodes_arc.clone(),
                    next_ino: next_ino_atomic.clone(),
                    mirror_worker: None,
                    mirror: Some(mirror.clone()),
                    lru_manager: Arc::new(LruManager::new(0, 0, Arc::new(Condvar::new()))),
                    block_size,
                };

                let new_dir = fuse.process_directory_entries(
                    mirror,
                    1,
                    &nodes_arc,
                    &fuse.next_ino.clone(),
                    new_entries,
                    block_size,
                ).unwrap();

                {
                    let mut nodes = nodes_arc.write().unwrap();
                    let root_node = nodes.get_mut(1).unwrap();
                    if let NodeKind::Directory(dir_kind) = &mut root_node.kind {
                        *dir_kind = DirectoryKind::InMemory(new_dir);
                    }
                }
            }
        }


        let cache_cond = Arc::new(Condvar::new());
        let lru_manager = Arc::new(LruManager::new(
            cache_size,
            cache_max_write_size,
            cache_cond.clone(),
        ));

        let mirror_worker = mirror.clone().map(|mirror| {
            Arc::new(MirrorWorker::new(mirror, nodes_arc.clone(), lru_manager.clone(), cache_cond.clone()))
        });

        Self {
            nodes: nodes_arc,
            next_ino: next_ino_atomic,
            mirror_worker,
            mirror,
            lru_manager,
            block_size,
        }
    }

    pub(crate) fn create_attr(&mut self, kind: fuser::FileType, perm: u16, uid: u32, gid: u32, size: Option<u64>) -> fuser::FileAttr {
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed) as u64;
        new_attr(ino, kind, perm, uid, gid, size)
    }

    pub(crate) fn lookup_node(&mut self, parent: u64, name: &OsStr) -> Result<FileAttr> {
        self.load_directory(parent)?;
        let attr = {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.find(parent, name)?;
            node.attr
        };
        self.load_directory(attr.ino)?;
        Ok(attr)
    }

    fn get_attr(&self, ino: u64) -> Result<FileAttr> {
        let nodes = self.nodes.read().unwrap();
        let node = nodes.get(ino)?;
        Ok(node.attr)
    }

    fn process_directory_entries(
        &mut self,
        mirror: &Arc<dyn Mirror + Send + Sync>,
        parent: u64,
        nodes: &Arc<RwLock<Nodes>>,
        next_ino: &Arc<AtomicUsize>,
        entries: Vec<MirrorDirEntry>,
        block_size: u64,
    ) -> Result<crate::node::Directory> {
        let mut new_dir = crate::node::Directory::new();
        let mut ino_map = HashMap::new();

        for entry in entries {
            let mut attr = entry.attr;
            let original_ino = attr.ino;

            let ino = next_ino.fetch_add(1, Ordering::Relaxed) as u64;
            attr.ino = ino;
            if original_ino != 0 {
                ino_map.insert(ino, original_ino);
            }

            // Fix the uid/gid if the mirror doesn't provide it.
            if attr.uid == u32::MAX {
                attr.uid = get_current_uid()
            }
            if attr.gid == u32::MAX {
                attr.gid = get_current_gid();
            }

            let new_node = if attr.kind == fuser::FileType::Directory {
                Node::new_directory_on_disk(attr)
            } else if attr.kind == fuser::FileType::RegularFile {
                Node::new_file_on_disk(attr, block_size)
            } else {
                let path_resolver = PathResolver::new(nodes);
                let target = mirror.read_link(original_ino, &path_resolver).unwrap();
                Node::new_symbolic_link(attr, &target)
            };
            {
                let mut nodes = nodes.write().unwrap();
                nodes.add_parent(new_node.attr.ino, parent, &entry.name);
                nodes.insert(new_node).unwrap();
            }
            new_dir.insert(&entry.name, ino)?;
        }
        if !ino_map.is_empty() {
            mirror.set_inode_map(&ino_map).unwrap();
        }

        Ok(new_dir)
    }

    fn load_directory(&mut self, ino: u64) -> Result<()> {
        {
            let nodes = self.nodes.read().unwrap();
            let node = nodes.get(ino)?;
            if let NodeKind::Directory(dir_kind) = &node.kind {
                if !dir_kind.is_ondisk() {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        if let Some(mirror) = self.mirror.clone() {
            let entries = {
                let path_resolver = PathResolver::new(&self.nodes);
                mirror.read_dir(ino, &path_resolver).unwrap()
            };

            let mut fuse = self.clone();
            let new_dir = fuse.process_directory_entries(
                &mirror,
                ino,
                &self.nodes.clone(),
                &self.next_ino,
                entries,
                self.block_size,
            )?;

            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            if let NodeKind::Directory(dir_kind) = &mut node.kind {
                *dir_kind = DirectoryKind::InMemory(new_dir);
            }
        }
        Ok(())
    }

    pub(crate) fn set_attr(
        &mut self,
        ino: u64,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        size: Option<u64>,
        atime: Option<fuser::TimeOrNow>,
        mtime: Option<fuser::TimeOrNow>,
        ctime: Option<SystemTime>,
        flags: Option<u32>,
    ) -> Result<FileAttr> {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;

        if let NodeKind::File(file) = &mut node.kind {
            if let Some(size) = size {
                let old_size = node.attr.size;
                if size < old_size {
                    let last_block_id = size / file.block_size;
                    let new_size_in_last_block = (size % file.block_size) as usize;

                    if let FileContent::InMemoryBlocks(blocks_arc) = &file.content {
                        let mut blocks = blocks_arc.write().unwrap();
                        blocks.retain(|&block_id, _| block_id <= last_block_id);
                        if let Some(last_block) = blocks.get(&last_block_id) {
                            let mut last_block_data = last_block.read();
                            let mut last_block_data = last_block_data.write().unwrap();
                            last_block_data.truncate(new_size_in_last_block);
                        }
                    }
                    file.dirty_blocks.truncate(file.block_size, size);
                }
                node.attr.size = size;
                file.dirty = true;
            }
        } else {
            // Not a file, handle other attributes
            if let Some(mode) = mode { node.attr.perm = mode as u16; }
            if let Some(uid) = uid { node.attr.uid = uid; }
            if let Some(gid) = gid { node.attr.gid = gid; }
        }

        if let Some(atime) = atime { node.attr.atime = atime.to_time(); }
        if let Some(mtime) = mtime { node.attr.mtime = mtime.to_time(); }
        if let Some(ctime) = ctime { node.attr.ctime = ctime; }
        if let Some(flags) = flags { node.attr.flags = flags; }

        if let Some(worker) = &self.mirror_worker {
            worker.sender().send(WriteJob::SetAttr { ino, attr: node.attr }).unwrap();
            if size.is_some() {
                worker.sender().send(WriteJob::Write { ino }).unwrap();
            }
        }
        Ok(node.attr)
    }

    fn read_link(&mut self, ino: u64) -> Result<Vec<u8>> {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        let target = node.get_target()?;
        Ok(target.as_os_str().as_bytes().to_vec())
    }

    pub(crate) fn make_file(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr> {
        self.load_directory(parent)?;
        let attr = self.create_attr(
            fuser::FileType::RegularFile,
            mode as u16,
            uid,
            gid,
            None,
        );
        let mut nodes = self.nodes.write().unwrap();
        let dir = nodes.get_dir_mut(parent)?;
        dir.insert(name, attr.ino)?;
        nodes.add_parent(attr.ino, parent, name);
        if let Some(worker) = &self.mirror_worker {
            worker
                .sender()
                .send(WriteJob::CreateFile { ino: attr.ino, parent, name: name.to_owned(), attr })
                .unwrap();
        }
        let node = Node::new_file(attr, self.block_size);
        nodes.insert(node)
    }

    pub(crate) fn make_directory(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr> {
        self.load_directory(parent)?;
        let attr = self.create_attr(
            fuser::FileType::Directory,
            mode as u16,
            uid,
            gid,
            None,
        );
        let mut nodes = self.nodes.write().unwrap();
        let dir = nodes.get_dir_mut(parent)?;
        dir.insert(name, attr.ino)?;
        nodes.add_parent(attr.ino, parent, name);
        if let Some(worker) = &self.mirror_worker {
            worker
                .sender()
                .send(WriteJob::CreateDir { ino: attr.ino, parent, name: name.to_owned(), attr })
                .unwrap();
        }
        let node = Node::new_directory(attr);
        nodes.insert(node)
    }

    fn make_symbolic_link(
        &mut self,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr> {
        self.load_directory(parent)?;
        let attr = self.create_attr(
            fuser::FileType::Symlink,
            0o777,
            uid,
            gid,
            None,
        );
        let mut nodes = self.nodes.write().unwrap();
        let dir = nodes.get_dir_mut(parent)?;
        dir.insert(link_name, attr.ino)?;
        nodes.add_parent(attr.ino, parent, link_name);
        if let Some(worker) = &self.mirror_worker {
            worker
                .sender()
                .send(WriteJob::CreateSymlink {
                    ino: attr.ino,
                    parent,
                    name: link_name.to_owned(),
                    target: target.to_path_buf(),
                    attr,
                })
                .unwrap();
        }
        let node = Node::new_symbolic_link(attr, target);
        nodes.insert(node)
    }

    pub(crate) fn link_node(
        &mut self,
        ino: u64,
        new_parent: u64,
        new_name: &std::ffi::OsStr,
    ) -> Result<FileAttr> {
        self.load_directory(new_parent)?;
        let mut nodes = self.nodes.write().unwrap();
        let attr = {
            let node = nodes.get(ino)?;
            node.attr
        };
        {
            let dir = nodes.get_dir_mut(new_parent)?;
            dir.insert(new_name, ino)?;
            nodes.add_parent(ino, new_parent, new_name);
            if let Some(worker) = &self.mirror_worker {
                worker
                    .sender()
                    .send(WriteJob::Link {
                        ino,
                        new_parent,
                        new_name: new_name.to_owned(),
                    })
                    .unwrap();
            }
        }
        {
            let node = nodes.get_mut(ino)?;
            node.add_link();
        }
        Ok(attr)
    }

    pub(crate) fn unlink_node(&mut self, parent: u64, name: &OsStr) -> Result<()> {
        self.load_directory(parent)?;
        let (ino, nlink) = {
            let mut nodes = self.nodes.write().unwrap();
            let ino = {
                let dir = nodes.get_dir_mut(parent)?;
                dir.remove(name)?
            };
            let nlink = {
                let node = nodes.get(ino)?;
                node.attr.nlink
            };
            (ino, nlink)
        };

        if nlink == 1 {
            self.lru_manager.remove(ino);
        }

        let mut nodes = self.nodes.write().unwrap();
        nodes.remove_parent(ino, parent, name);
        if let Some(worker) = &self.mirror_worker {
            worker
                .sender()
                .send(WriteJob::Delete {
                    ino,
                    parent,
                    name: name.to_owned(),
                })
                .unwrap();
        }
        nodes.dec_link(ino)
    }

     pub(crate) fn rename_node(
        &mut self,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
    ) -> Result<()> {
        self.load_directory(parent)?;
        self.load_directory(newparent)?;
        let allow_overwrite = {
            #[cfg(target_os = "linux")]
            {
                (flags & RENAME_NOREPLACE) == 0
            }
            #[cfg(not(target_os = "linux"))]
            {
                flags == 0
            }
        };
        let exchange = {
            #[cfg(target_os = "linux")]
            {
                (flags & RENAME_EXCHANGE) != 0
            }
            #[cfg(not(target_os = "linux"))]
            {
                false
            }
        };

        let mut nodes = self.nodes.write().unwrap();

        let old_ino = match nodes.get_dir(parent)?.get(name) {
            Some(ino) => ino,
            None => return Err(ENOENT),
        };

        if exchange {
            let new_ino = match nodes.get_dir(newparent)?.get(newname) {
                Some(ino) => ino,
                None => return Err(ENOENT),
            };

            // Atomically swap entries
            nodes.get_dir_mut(parent).unwrap().set(name, new_ino).unwrap();
            nodes.remove_parent(old_ino, parent, name);
            nodes.add_parent(new_ino, parent, name);

            nodes.get_dir_mut(newparent).unwrap().set(newname, old_ino).unwrap();
            nodes.remove_parent(new_ino, newparent, newname);
            nodes.add_parent(old_ino, newparent, newname);

            return Ok(());
        }

        // Not an exchange, regular rename
        if let Some(existing_ino) = nodes.get_dir(newparent)?.get(newname) {
            if !allow_overwrite {
                return Err(EEXIST);
            }
            // Unlink the existing destination
            nodes.get_dir_mut(newparent)?.remove(newname)?;
            nodes.remove_parent(existing_ino, newparent, newname);
            nodes.dec_link(existing_ino)?;
        }

        // Move the source to the new destination
        nodes.get_dir_mut(parent)?.remove(name)?;
        nodes.remove_parent(old_ino, parent, name);
        nodes.get_dir_mut(newparent)?.insert(newname, old_ino)?;
        nodes.add_parent(old_ino, newparent, newname);

        if let Some(worker) = &self.mirror_worker {
            worker
                .sender()
                .send(WriteJob::Rename {
                    ino: old_ino,
                    parent,
                    name: name.to_owned(),
                    new_parent: newparent,
                    new_name: newname.to_owned(),
                })
                .unwrap();
        }

        Ok(())
    }

    fn get_or_fetch_block(&mut self, ino: u64, block_id: u64) -> Result<Block> {
        let (block_size, blocks_arc) = {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            if let NodeKind::File(file) = &mut node.kind {
                if let FileContent::InMemoryBlocks(blocks) = &file.content {
                    if let Some(block) = blocks.read().unwrap().get(&block_id) {
                        return Ok(block.clone());
                    }
                }
                (file.block_size, match &mut file.content {
                    FileContent::InMemoryBlocks(blocks) => Some(blocks.clone()),
                    FileContent::OnDisk => None,
                })
            } else {
                return Err(ENOENT);
            }
        };

        let blocks = if let Some(blocks_arc) = blocks_arc {
            blocks_arc
        } else {
            let new_blocks = Arc::new(RwLock::new(HashMap::new()));
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            if let NodeKind::File(file) = &mut node.kind {
                file.content = FileContent::InMemoryBlocks(new_blocks.clone());
            }
            new_blocks
        };

        let path_resolver = PathResolver::new(&self.nodes);
        let new_data = self
            .mirror
            .as_ref()
            .unwrap()
            .read_block(ino, block_id, &path_resolver)
            .unwrap_or_default();
        let new_block = Block::new(new_data);
        let evicted = self.lru_manager.put(
            ino,
            block_id,
            new_block.clone(),
            self.mirror.is_some(),
            false,
        )?;
        blocks.write().unwrap().insert(block_id, new_block.clone());
        Ok(new_block)
    }

    pub(crate) fn read_file(&mut self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>> {
        let file_size = self.get_attr(ino)?.size;
        let effective_offset = if offset < 0 {
            max(0, (file_size as i64 + offset)) as u64
        } else {
            min(offset as u64, file_size)
        };
        let effective_end = min(effective_offset + size as u64, file_size);
        if effective_offset >= effective_end {
            return Ok(Vec::new());
        }
        let read_size = (effective_end - effective_offset) as usize;
        let mut result = vec![0; read_size];
        let block_size = self.block_size;

        let start_block = effective_offset / block_size;
        let end_block = (effective_end - 1) / block_size;
        let mut written_bytes = 0;

        for block_id in start_block..=end_block {
            let block = self.get_or_fetch_block(ino, block_id)?;
            let block_data = block.read();
            let block_data = block_data.read().unwrap();

            let block_start_offset = block_id * block_size;
            let read_start_in_block = if effective_offset > block_start_offset {
                (effective_offset - block_start_offset) as usize
            } else {
                0
            };

            let read_end_in_block = if effective_end < block_start_offset + block_data.len() as u64 {
                (effective_end - block_start_offset) as usize
            } else {
                block_data.len()
            };

            if read_start_in_block >= read_end_in_block {
                continue;
            }

            let copy_len = read_end_in_block - read_start_in_block;
            let result_start = written_bytes;
            let result_end = result_start + copy_len;
            result[result_start..result_end]
                .copy_from_slice(&block_data[read_start_in_block..read_end_in_block]);
            written_bytes += copy_len;
        }

        Ok(result)
    }

    pub(crate) fn write_file(&mut self, ino: u64, offset: i64, new_data: &[u8]) -> Result<usize> {
        let file_size = self.get_attr(ino)?.size;
        let effective_offset = if offset < 0 {
            max(0, file_size as i64 + offset) as u64
        } else {
            offset as u64
        };
        let new_size = max(file_size as i64, (effective_offset + new_data.len() as u64) as i64) as u64;

        let block_size = self.block_size;
        let start_block = effective_offset / block_size;
        let end_block = (effective_offset + new_data.len() as u64 - 1) / block_size;
        let mut read_bytes = 0;

        for block_id in start_block..=end_block {
            let block = self.get_or_fetch_block(ino, block_id)?;
            let mut block_data_arc = block.read();
            let mut block_data = block_data_arc.write().unwrap();

            let block_start_offset = block_id * block_size;
            let write_start_in_block = if effective_offset > block_start_offset {
                (effective_offset - block_start_offset) as usize
            } else {
                0
            };

            let write_end_in_block = min(
                block_size,
                effective_offset + new_data.len() as u64 - block_start_offset,
            ) as usize;

            let copy_len = write_end_in_block - write_start_in_block;
            if block_data.len() < write_end_in_block {
                block_data.resize(write_end_in_block, 0);
            }
            block_data[write_start_in_block..write_end_in_block]
                .copy_from_slice(&new_data[read_bytes..read_bytes + copy_len]);
            read_bytes += copy_len;

            let evicted = self.lru_manager.put(
                ino,
                block_id,
                block.clone(),
                self.mirror.is_some(),
                true,
            )?;

            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            if let NodeKind::File(file) = &mut node.kind {
                file.dirty = true;
                file.dirty_blocks.add_block(block_id);
                if let FileContent::InMemoryBlocks(blocks) = &mut file.content {
                    for ((_evicted_ino, evicted_block_id), evicted_block) in evicted {
                        if let Some(b) = blocks.read().unwrap().get(&evicted_block_id) {
                            if Arc::ptr_eq(&b.read(), &evicted_block.read()) {
                                blocks.write().unwrap().remove(&evicted_block_id);
                            }
                        }
                    }
                }
            }
        }

        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        node.attr.size = new_size;
        if let Some(worker) = &self.mirror_worker {
            worker.sender().send(WriteJob::Write { ino }).unwrap();
        }

        Ok(new_data.len())
    }

    pub(crate) fn read_directory(&mut self, ino: u64) -> Result<Vec<(OsString, FileAttr)>> {
        self.load_directory(ino)?;
        let nodes = self.nodes.read().unwrap();
        let dir = nodes.get_dir_anon(ino)?;
        let mut entries = Vec::new();
        for (name, ino) in dir.iter() {
            if let Ok(node) = nodes.get(*ino) {
                entries.push((name.clone(), node.attr));
            }
        }
        Ok(entries)
    }
}

impl Filesystem for MemoryFuse {
    fn init(&mut self, _req: &fuser::Request<'_>, _config: &mut fuser::KernelConfig) -> std::result::Result<(), c_int> {
        Ok(())
    }

    fn destroy(&mut self) {
        info!("destroying");
        if let Some(worker) = self.mirror_worker.take() {
            if let Ok(worker) = Arc::try_unwrap(worker) {
                worker.stop();
            }
        }
    }


    fn lookup(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEntry) {
        debug!("lookup(parent: {parent}, name {name:?})");
        match self.lookup_node(parent, name) {
            Ok(attr) => reply.entry(&MEM_TTL, &attr, 1),
            Err(err) => { reply.error(log_err("lookup", err)); },
        }
    }

    fn forget(&mut self, _req: &fuser::Request<'_>, _ino: u64, _nlookup: u64) {}

    fn getattr(&mut self, _req: &fuser::Request<'_>, ino: u64, _fh: Option<u64>, reply: fuser::ReplyAttr) {
        match self.get_attr(ino) {
            Ok(attr) => reply.attr(&MEM_TTL, &attr),
            Err(err) => { reply.error(log_err("getattr", err)) },
        }
    }

    fn setattr(
            &mut self,
            _req: &fuser::Request<'_>,
            ino: u64,
            mode: Option<u32>,
            uid: Option<u32>,
            gid: Option<u32>,
            size: Option<u64>,
            atime: Option<fuser::TimeOrNow>,
            mtime: Option<fuser::TimeOrNow>,
            ctime: Option<SystemTime>,
            _fh: Option<u64>,
            _crtime: Option<SystemTime>,
            _chgtime: Option<SystemTime>,
            _bkuptime: Option<SystemTime>,
            flags: Option<u32>,
            reply: fuser::ReplyAttr,
        ) {
            match self.set_attr(ino, mode, uid, gid, size, atime, mtime, ctime, flags) {
                Ok(attr) => reply.attr(&MEM_TTL, &attr),
                Err(err) => reply.error(log_err("setattr", err)),
            }
        }


    fn readlink(&mut self, _req: &fuser::Request<'_>, ino: u64, reply: fuser::ReplyData) {
        match self.read_link(ino) {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(log_err("readlink", err)),
        }
    }

    fn mknod(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        rdev: u32,
        reply: fuser::ReplyEntry,
    ) {
        debug!("mknod(parent: {parent}, name: {name:?}, mode: {mode}, umask: {umask}, rdev: {rdev}");
        match self.make_file(parent, name, mode, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(&MEM_TTL, &attr, 1),
            Err(err) => reply.error(log_err("mknod", err)),
        }
    }

    fn mkdir(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &std::ffi::OsStr,
        mode: u32,
        umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        debug!("mkdir(parent: {parent}, name: {name:?}, mode: {mode}, umask: {umask}");
        match self.make_directory(parent, name, mode, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(&MEM_TTL, &attr, 1),
            Err(err) => reply.error(log_err("mkdir", err)),
        }
    }

    fn symlink(
            &mut self,
            req: &fuser::Request<'_>,
            parent: u64,
            link_name: &OsStr,
            target: &Path,
            reply: fuser::ReplyEntry,
        ) {
        debug!("symlink(parent: {parent}, link_name: {link_name:?}, target: {target:?}");
        match self.make_symbolic_link(parent, link_name, target, req.uid(), req.gid()) {
            Ok(attr) => reply.entry(&MEM_TTL, &attr, 1),
            Err(err) => reply.error(log_err("symlink", err)),
        }
    }

    fn create(
        &mut self,
        req: &fuser::Request<'_>,
        parent: u64,
        name: &OsStr,
        mode: u32,
        umask: u32,
        flags: i32,
        reply: fuser::ReplyCreate,
    ) {
        debug!("create(parent: {parent}, name: {name:?}, mode: {mode}, umask: {umask}");
        match self.make_file(parent, name, mode, req.uid(), req.gid()) {
            Ok(attr) => reply.created(&MEM_TTL, &attr, 1, 0, flags as u32),
            Err(err) => reply.error(log_err("create", err)),
        }
    }

    fn link(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        new_parent: u64,
        new_name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        debug!("unlink(ino: {ino}, new_parent: {new_parent}, new_name: {new_name:?})");
        match self.link_node(ino, new_parent, new_name) {
            Ok(attr) => reply.entry(&MEM_TTL, &attr, 1),
            Err(err) => reply.error(log_err("link", err)),
        }
    }

     fn unlink(&mut self, _req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        debug!("unlink(parent: {parent}, name: {name:?})");
        match self.unlink_node(parent, name) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(log_err("unlink", err)),
        }
     }

     fn rmdir(&mut self, req: &fuser::Request<'_>, parent: u64, name: &OsStr, reply: fuser::ReplyEmpty) {
        debug!("rmdir(parent: {parent}, name: {name:?})");
        self.unlink(req, parent, name, reply)
     }

     fn rename(
             &mut self,
             _req: &fuser::Request<'_>,
             parent: u64,
             name: &OsStr,
             newparent: u64,
             newname: &OsStr,
             flags: u32,
             reply: fuser::ReplyEmpty,
    ) {
        debug!("rename(parent: {parent}, name: {name:?}, newparent: {newparent}, newname: {newname:?}, flags: {flags}");
        match self.rename_node(parent, name, newparent, newname, flags) {
            Ok(()) => reply.ok(),
            Err(err) => reply.error(log_err("rename", err)),
        }
    }

    fn open(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn read(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        size: u32,
        flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyData,
    ) {
        debug!("read(ino: {ino}, fh: {fh}, offset: {offset}, size: {size}, flags: {flags}, lock_owner: {lock_owner:?})");
        match self.read_file(ino, offset, size) {
            Ok(data) => reply.data(&data),
            Err(err) => reply.error(log_err("read", err)),
        }
    }

     fn write(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        new_data: &[u8],
        write_flags: u32,
        _flags: i32,
        lock_owner: Option<u64>,
        reply: fuser::ReplyWrite,
    ) {
        debug!("write(ino: {ino}, fh: {fh}, offset: {offset}, write_flags: {write_flags}, lock_owner: {lock_owner:?})");
        match self.write_file(ino, offset, new_data) {
            Ok(len) => reply.written(len as u32),
            Err(err) => reply.error(log_err("write", err)),
        }
    }

    fn flush(&mut self, _req: &fuser::Request<'_>, ino: u64, fh: u64, lock_owner: u64, reply: fuser::ReplyEmpty) {
        debug!("flush(ino: {ino}, fh: {fh}, lock_owner: {lock_owner:?})");
        reply.ok();
    }

    fn release(
            &mut self,
            _req: &fuser::Request<'_>,
            ino: u64,
            fh: u64,
            flags: i32,
            lock_owner: Option<u64>,
            flush: bool,
            reply: fuser::ReplyEmpty,
        ) {
        debug!("release(ino: {ino}, fh: {fh}, flags: {flags}, lock_owner: {lock_owner:?}), flush: {flush}");
        reply.ok();
    }


    fn opendir(&mut self, _req: &fuser::Request<'_>, _ino: u64, _flags: i32, reply: fuser::ReplyOpen) {
        reply.opened(0, 0);
    }

    fn readdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        mut reply: fuser::ReplyDirectory,
    ) {
        debug!("readdir(ino: {ino}, fh: {fh}, offset: {offset})");
        match self.read_directory(ino) {
            Ok(entries) => {
                for (i, (name, attr)) in entries.into_iter().skip(offset as usize).enumerate() {
                    let entry_offset = offset + i as i64 + 1;
                    if reply.add(attr.ino, entry_offset, attr.kind, &name) {
                        break;
                    }
                }
                reply.ok();
            },
            Err(err) => { reply.error(log_err("readdir", err)) },
        }
    }

   fn readdirplus(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        offset: i64,
        reply: fuser::ReplyDirectoryPlus,
    ) {
        debug!(
            "[Not Implemented] readdirplus(ino: {:#x?}, fh: {}, offset: {})",
            ino, fh, offset
        );
        reply.error(ENOSYS);
    }

    fn releasedir(
        &mut self,
        _req: &fuser::Request<'_>,
        _ino: u64,
        _fh: u64,
        _flags: i32,
        reply: fuser::ReplyEmpty,
    ) {
        reply.ok();
    }

    fn fsyncdir(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        fh: u64,
        datasync: bool,
        reply: fuser::ReplyEmpty,
    ) {
        debug!("fsyncdir(ino: {:#x?}, fh: {}, datasync: {})", ino, fh, datasync);
        reply.ok();
    }

    fn statfs(&mut self, _req: &fuser::Request<'_>, _ino: u64, reply: fuser::ReplyStatfs) {
        reply.statfs(0, 0, 0, 0, 0, 512, 255, 0);
    }

    fn setxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        _value: &[u8],
        flags: i32,
        position: u32,
        reply: fuser::ReplyEmpty,
    ) {
        debug!(
            "[Not Implemented] setxattr(ino: {:#x?}, name: {:?}, flags: {:#x?}, position: {})",
            ino, name, flags, position
        );
        reply.error(ENOSYS);
    }

    fn getxattr(
        &mut self,
        _req: &fuser::Request<'_>,
        ino: u64,
        name: &std::ffi::OsStr,
        size: u32,
        reply: fuser::ReplyXattr,
    ) {
        debug!(
            "[Not Implemented] getxattr(ino: {:#x?}, name: {:?}, size: {})",
            ino, name, size
        );
        reply.error(ENOSYS);
    }

    fn listxattr(&mut self, _req: &fuser::Request<'_>, ino: u64, size: u32, reply: fuser::ReplyXattr) {
        debug!(
            "[Not Implemented] listxattr(ino: {:#x?}, size: {})",
            ino, size
        );
        reply.error(ENOSYS);
    }

    fn removexattr(&mut self, _req: &fuser::Request<'_>, ino: u64, name: &std::ffi::OsStr, reply: fuser::ReplyEmpty) {
        debug!(
            "[Not Implemented] removexattr(ino: {:#x?}, name: {:?})",
            ino, name
        );
        reply.error(ENOSYS);
    }

    fn access(&mut self, _req: &fuser::Request<'_>, ino: u64, mask: i32, reply: fuser::ReplyEmpty) {
        debug!("[Not Implemented] access(ino: {:#x?}, mask: {})", ino, mask);
        reply.error(ENOSYS);
    }

}

fn log_err(msg: &str, err: c_int) -> c_int {
    debug!("{msg} failed -> err: {err}");
    err
}

fn new_attr(
        ino: u64,
        kind: fuser::FileType,
        perm: u16,
        uid: u32,
        gid: u32,
        size: Option<u64>,
) -> fuser::FileAttr {
    let now = SystemTime::now();
    fuser::FileAttr {
        ino,
        size: size.unwrap_or(0),
        blocks: 1u64,
        atime: now,
        mtime: now,
        ctime: now,
        crtime: now,
        kind,
        perm,
        nlink: 1,
        uid,
        gid,
        rdev: 0,
        blksize: u32::MAX,
        flags: 0,
    }
}

trait ToSystemTime {
    fn to_time(self) -> SystemTime;
}

impl ToSystemTime for TimeOrNow {
    fn to_time(self) -> SystemTime {
        match self {
            Self::Now => SystemTime::now(),
            Self::SpecificTime(time) => time
        }
    }
}


const MEM_TTL: std::time::Duration = std::time::Duration::from_secs(30);

fn max(a: i64, b: i64) -> i64 {
    if a > b { a } else { b }
}

fn min(a: u64, b: u64) -> u64 {
    if a < b { a } else { b }
}

pub(crate) trait OrError<R> {
    fn or_error(self, err: c_int) -> Result<R>;
}

impl<R> OrError<R> for Option<R> {
    fn or_error(self, err: c_int) -> Result<R> {
        match self {
            Some(r) => Ok(r),
            None => Err(err),
        }
    }
}
