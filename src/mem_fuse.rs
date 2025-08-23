use std::{
    collections::HashMap, ffi::{OsStr, OsString}, os::unix::ffi::OsStrExt, path::Path, sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock, Condvar}, time::SystemTime, u32
};

use fuser::{self, FileAttr, Filesystem, TimeOrNow};
use libc::{c_int, EEXIST, ENOENT, ENOSYS};
#[cfg(target_os = "linux")]
use libc::{RENAME_NOREPLACE, RENAME_EXCHANGE};
use log::{debug, info};
use users::{get_current_gid, get_current_uid};

use crate::lru_cache::LruManager;
use crate::mirror::{Mirror, MirrorDirEntry, MirrorWorker, PathResolver, WriteJob};
use crate::node::{DirectoryKind, FileContent, Node, NodeKind, Nodes};

pub(crate) type Result<T> = std::result::Result<T, c_int>;

pub(crate) struct MemoryFuse {
    pub(crate) nodes: Arc<RwLock<Nodes>>,
    next_ino: AtomicUsize,
    mirror_worker: Option<MirrorWorker>,
    mirror: Option<Arc<dyn Mirror + Send + Sync>>,
    lru_manager: Arc<LruManager>,
}

impl MemoryFuse {
    pub(crate) fn new(
        mirror: Option<Arc<dyn Mirror + Send + Sync>>,
        cache_size: u64,
        cache_max_write_size: u64,
        lazy_load: bool,
    ) -> MemoryFuse {
        let nodes_arc = Arc::new(RwLock::new(Nodes::new()));

        let next_ino_atomic = AtomicUsize::new(2);
        if lazy_load {
            if let Some(mirror) = &mirror {
                let root_attr = new_attr(
                    1,
                    fuser::FileType::Directory,
                    0o755,
                    get_current_uid(),
                    get_current_gid(),
                    None,
                );
                let root = Node::new_directory(root_attr);
                {
                    let mut nodes = nodes_arc.write().unwrap();
                    let _ = nodes.insert(root);
                }
                let new_entries = {
                    let path_resolver = PathResolver::new(&nodes_arc);
                    mirror.read_dir(1, &path_resolver).unwrap()
                };

                let new_dir = Self::process_directory_entries(
                    mirror,
                    1,
                    &nodes_arc,
                    &next_ino_atomic,
                    new_entries,
                ).unwrap();

                {
                    let mut nodes = nodes_arc.write().unwrap();
                    let root_node = nodes.get_mut(1).unwrap();
                    if let NodeKind::Directory(dir_kind) = &mut root_node.kind {
                        *dir_kind = DirectoryKind::InMemory(new_dir);
                    }
                }
            }
        } else {
            let attr = new_attr(
                1,
                fuser::FileType::Directory,
                0o755,
                get_current_uid(),
                get_current_gid(),
                None,
            );
            let root = Node::new_directory(attr);
            let mut nodes = nodes_arc.write().unwrap();
            let _ = nodes.insert(root);
        }


        let cache_cond = Arc::new(Condvar::new());
        let lru_manager = Arc::new(LruManager::new(
            cache_size,
            cache_max_write_size,
            cache_cond.clone(),
        ));

        let mirror_worker = mirror.clone().map(|mirror| {
            MirrorWorker::new(mirror, nodes_arc.clone(), lru_manager.clone(), cache_cond.clone())
        });

        Self {
            nodes: nodes_arc,
            next_ino: next_ino_atomic,
            mirror_worker,
            mirror,
            lru_manager,
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
        mirror: &Arc<dyn Mirror + Send + Sync>,
        parent: u64,
        nodes: &Arc<RwLock<Nodes>>,
        next_ino: &AtomicUsize,
        entries: Vec<MirrorDirEntry>,
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
                Node::new_file_on_disk(attr)
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

            let new_dir = Self::process_directory_entries(
                &mirror,
                ino,
                &self.nodes,
                &self.next_ino,
                entries,
            )?;

            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            if let NodeKind::Directory(dir_kind) = &mut node.kind {
                *dir_kind = DirectoryKind::InMemory(new_dir);
            }
        }
        Ok(())
    }

    fn set_attr(
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
        let (data_arc, mut attr) = {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            let mut attr_clone = node.attr;
            if let NodeKind::File(file) = &mut node.kind {
                let d = match &mut file.content {
                    FileContent::InMemory(data) => {
                        self.lru_manager.get(&ino);
                        Some(data.clone())
                    }
                    FileContent::OnDisk => None,
                };
                (d, attr_clone)
            } else {
                // Not a file, handle other attributes
                if let Some(mode) = mode { attr_clone.perm = mode as u16; }
                if let Some(uid) = uid { attr_clone.uid = uid; }
                if let Some(gid) = gid { attr_clone.gid = gid; }
                if let Some(atime) = atime { attr_clone.atime = atime.to_time(); }
                if let Some(mtime) = mtime { attr_clone.mtime = mtime.to_time(); }
                if let Some(ctime) = ctime { attr_clone.ctime = ctime; }
                if let Some(flags) = flags { attr_clone.flags = flags; }
                node.attr = attr_clone;
                if let Some(worker) = &self.mirror_worker {
                    worker.sender().send(WriteJob::SetAttr { ino, attr: attr_clone }).unwrap();
                }
                return Ok(attr_clone);
            }
        };

        let data = if let Some(data_arc) = data_arc {
            data_arc
        } else {
            let path_resolver = PathResolver::new(&self.nodes);
            let new_data = self
                .mirror
                .as_ref()
                .unwrap()
                .read_file(ino, &path_resolver)
                .unwrap_or_default();
            Arc::new(RwLock::new(new_data))
        };

        if let Some(mode) = mode { attr.perm = mode as u16; }
        if let Some(uid) = uid { attr.uid = uid; }
        if let Some(gid) = gid { attr.gid = gid; }
        if let Some(atime) = atime { attr.atime = atime.to_time(); }
        if let Some(mtime) = mtime { attr.mtime = mtime.to_time(); }
        if let Some(ctime) = ctime { attr.ctime = ctime; }
        if let Some(flags) = flags { attr.flags = flags; }

        let evicted = if let Some(size) = size {
            data.write().unwrap().resize(size as usize, 0u8);
            attr.size = size;
            self.lru_manager
                .put(ino, data.clone(), self.mirror.is_some(), true)?
        } else {
            Vec::new()
        };

        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        let old_size = node.attr.size;
        node.attr = attr;
        if let NodeKind::File(file) = &mut node.kind {
            if file.content.is_ondisk() {
                file.content = FileContent::InMemory(data.clone());
            }
            if let Some(size) = size {
                if size < old_size {
                    file.dirty_regions.truncate(size);
                } else {
                    file.dirty_regions.add_region(old_size, size);
                }
                file.dirty = true;
            }
        }
        for (evicted_ino, evicted_content) in evicted {
            if let Ok(node) = nodes.get_mut(evicted_ino) {
                if let NodeKind::File(file) = &mut node.kind {
                    if let FileContent::InMemory(existing_content) = &file.content {
                        if Arc::ptr_eq(&evicted_content, existing_content) {
                            file.content = FileContent::OnDisk;
                        }
                    }
                }
            }
        }

        if let Some(worker) = &self.mirror_worker {
            worker.sender().send(WriteJob::SetAttr { ino, attr }).unwrap();
            if size.is_some() {
                worker.sender().send(WriteJob::Write { ino }).unwrap();
            }
        }
        Ok(attr)
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
        let node = Node::new_file(attr);
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
            self.lru_manager.remove(&ino);
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

    pub(crate) fn read_file(&mut self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>> {
        let content = {
            let nodes = self.nodes.read().unwrap();
            let node = nodes.get(ino)?;
            if let NodeKind::File(file) = &node.kind {
                match &file.content {
                    FileContent::InMemory(data) => {
                        self.lru_manager.get(&ino);
                        Some(data.clone())
                    }
                    FileContent::OnDisk => None,
                }
            } else {
                return Err(ENOENT);
            }
        };

        let data_arc = if let Some(content) = content {
            content
        } else {
            let path_resolver = PathResolver::new(&self.nodes);
            let new_data = self
                .mirror
                .as_ref()
                .unwrap()
                .read_file(ino, &path_resolver)
                .unwrap_or_default();
            let new_data_arc = Arc::new(RwLock::new(new_data));

            let evicted = self.lru_manager.put(
                ino,
                new_data_arc.clone(),
                self.mirror.is_some(),
                false,
            )?;
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            if let NodeKind::File(file) = &mut node.kind {
                file.content = FileContent::InMemory(new_data_arc.clone());
            }
            for (evicted_ino, evicted_content) in evicted {
                if let Ok(node) = nodes.get_mut(evicted_ino) {
                    if let NodeKind::File(file) = &mut node.kind {
                        if let FileContent::InMemory(existing_content) = &file.content {
                            if Arc::ptr_eq(&evicted_content, existing_content) {
                                file.content = FileContent::OnDisk;
                            }
                        }
                    }
                }
            }
            new_data_arc
        };

        let data = data_arc.read().unwrap();
        let effective_offset = if offset < 0 {
            max(0, (data.len() as i64 + offset) as usize)
        } else {
            min(offset as usize, data.len())
        };
        let effective_end = min(effective_offset + size as usize, data.len());
        Ok(data[effective_offset..effective_end].to_vec())
    }

    pub(crate) fn write_file(&mut self, ino: u64, offset: i64, new_data: &[u8]) -> Result<usize> {
        let data_arc = {
            let mut nodes = self.nodes.write().unwrap();
            let node = nodes.get_mut(ino)?;
            if let NodeKind::File(file) = &mut node.kind {
                match &mut file.content {
                    FileContent::InMemory(data) => {
                        self.lru_manager.get(&ino);
                        Some(data.clone())
                    }
                    FileContent::OnDisk => None,
                }
            } else {
                return Err(ENOENT);
            }
        };

        let data = if let Some(data_arc) = data_arc {
            data_arc
        } else {
            let path_resolver = PathResolver::new(&self.nodes);
            let new_data = self
                .mirror
                .as_ref()
                .unwrap()
                .read_file(ino, &path_resolver)
                .unwrap_or_default();
            Arc::new(RwLock::new(new_data))
        };

        let new_size;
        let effective_offset;
        {
            let mut data_w = data.write().unwrap();
            effective_offset = if offset < 0 {
                (data_w.len() as i64 + offset) as usize
            } else {
                offset as usize
            };
            let effective_size = effective_offset + new_data.len();
            new_size = if effective_size > data_w.len() {
                effective_size
            } else {
                data_w.len()
            };
            if new_size > data_w.len() {
                data_w.resize(new_size, 0u8);
            }
            data_w[effective_offset..effective_size].copy_from_slice(new_data);
        }

        let evicted = self.lru_manager.put(
            ino,
            data.clone(),
            self.mirror.is_some(),
            true,
        )?;

        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        node.attr.size = new_size as u64;
        if let NodeKind::File(file) = &mut node.kind {
            if file.content.is_ondisk() {
                file.content = FileContent::InMemory(data.clone());
            }
            file.dirty = true;
            file.dirty_regions
                .add_region(effective_offset as u64, (effective_offset + new_data.len()) as u64);
            for (evicted_ino, evicted_content) in evicted {
                if let Ok(node) = nodes.get_mut(evicted_ino) {
                    if let NodeKind::File(file) = &mut node.kind {
                        if let FileContent::InMemory(existing_content) = &file.content {
                            if Arc::ptr_eq(&evicted_content, existing_content) {
                                file.content = FileContent::OnDisk;
                            }
                        }
                    }
                }
            }
            if let Some(worker) = &self.mirror_worker {
                worker.sender().send(WriteJob::Write { ino }).unwrap();
            }
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
            worker.stop();
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

fn max(a: usize, b: usize) -> usize {
    if a > b { a } else { b }
}

fn min(a: usize, b: usize) -> usize {
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
