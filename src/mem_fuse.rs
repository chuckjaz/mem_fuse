use std::{
    collections::HashMap, ffi::{OsStr, OsString}, os::unix::ffi::OsStrExt, path::{Path, PathBuf}, sync::atomic::{AtomicUsize, Ordering}, time::SystemTime, u32
};

use indexmap::IndexMap;
use fuser::{self, FileAttr, Filesystem, TimeOrNow};
use libc::{c_int, EEXIST, ENOENT, ENOTDIR, ENOSYS};
#[cfg(target_os = "linux")]
use libc::{RENAME_NOREPLACE, RENAME_EXCHANGE};
use log::debug;
use users::{get_current_gid, get_current_uid};

type Result<T> = std::result::Result<T, c_int>;

struct Directory {
    entries: IndexMap<OsString, u64>
}

impl Directory {
    fn new() -> Self {
        Self { entries: IndexMap::new()}
    }

    fn get(&self, name: &OsStr) -> Option<u64> {
        self.entries.get(name).copied()
    }

    fn set(&mut self, name: &OsStr, ino: u64) -> Result<Option<u64>> {
        Ok(self.entries.insert(name.into(), ino))
    }

    fn insert(&mut self, name: &OsStr, ino: u64) -> Result<()> {
        if self.has(name) {
            Err(EEXIST)
        } else {
            self.entries.insert(name.into(), ino);
            Ok(())
        }
    }

    fn remove(&mut self, name: &OsStr) -> Result<u64> {
        self.entries.swap_remove(name).or_error(ENOENT)
    }

    fn has(&self, name: &OsStr) -> bool {
        self.entries.contains_key(name)
    }

    fn iter(&self) -> indexmap::map::Iter<'_, OsString, u64> {
        self.entries.iter()
    }
}

enum NodeKind {
    File { data: Vec<u8> },
    Directory { dir: Directory },
    SymbolicLink { target: PathBuf },
}

struct Node {
    kind: NodeKind,
    attr: FileAttr,
}

impl Node {
    fn new_file(attr: FileAttr) -> Self {
        Self { attr, kind: NodeKind::File { data: Vec::new() } }
    }

    fn new_directory(attr: FileAttr) -> Self {
        Self { attr, kind: NodeKind::Directory { dir: Directory::new() } }
    }

    fn new_symbolic_link(attr: FileAttr, target: &Path) -> Self {
        Self { attr, kind: NodeKind::SymbolicLink { target: target.to_path_buf() } }
    }

    fn get_data(&mut self) -> Result<&[u8]> {
        self.accessed();
        match &self.kind {
            NodeKind::File { data } => {
                Ok(data)
            },
            _ => Err(ENOENT)
        }
    }

    fn get_data_mut(&mut self) -> Result<&mut Vec<u8>> {
        self.written();
        match &mut self.kind {
            NodeKind::File { data } => {
                Ok(data)
            },
            _ => Err(ENOENT)
        }
    }

    fn get_target(&mut self) -> Result<&Path> {
        self.accessed();
        match &self.kind {
            NodeKind::SymbolicLink { target } => {
                Ok(target)
            }
            _ => Err(ENOENT)
        }
    }

    fn get_dir(&mut self) -> Result<&Directory> {
        self.accessed();
        match &self.kind {
            NodeKind::Directory { dir  } => {
                Ok(dir)
            },
            _ => Err(ENOTDIR)
        }
    }

    fn get_dir_mut(&mut self) -> Result<&mut Directory> {
        self.written();
        match &mut self.kind {
            NodeKind::Directory { dir } => {
                Ok(dir)
            },
            _ => Err(ENOTDIR)
        }
    }

    fn add_link(&mut self) {
        self.attr.nlink += 1
    }

    fn release(&mut self) -> bool {
        let new_count = self.attr.nlink - 1;
        self.attr.nlink = new_count;
        new_count == 0
    }

    fn accessed(&mut self) {
        self.attr.atime = SystemTime::now()
    }

    fn written(&mut self) {
        self.accessed();
        self.attr.mtime = SystemTime::now()
    }
}

struct Nodes {
    nodes: HashMap<u64, Node>
}

impl Nodes {
    fn new() -> Self {
        Self { nodes: HashMap::new() }
    }

    fn insert(&mut self, mut node: Node) -> Result<FileAttr> {
        let ino = node.attr.ino;
        node.add_link();
        let attr = node.attr;
        self.nodes.insert(ino, node);
        Ok(attr)
    }

    fn dec_link(&mut self, ino: u64) -> Result<()> {
        let node = self.get_mut(ino)?;
        if node.release() {
            self.nodes.remove(&ino);
        }
        Ok(())
    }

    fn get(&self, ino: u64) -> Result<&Node> {
        if let Some(node) = self.nodes.get(&ino) {
            Ok(node)
        } else {
            Err(ENOENT)
        }
    }

    fn get_mut(&mut self, ino: u64) -> Result<&mut Node> {
        self.nodes.get_mut(&ino).or_error(ENOENT)
    }

    fn get_dir(&mut self, ino: u64) -> Result<&Directory> {
        let node = self.get_mut(ino)?;
        node.get_dir()

    }

    fn get_dir_anon(&self, ino: u64) -> Result<&Directory> {
        let node = self.get(ino)?;
        if let NodeKind::Directory { dir } = &node.kind {
            Ok(dir)
        } else {
            Err(ENOTDIR)
        }
    }

    fn get_dir_mut(&mut self, ino: u64) -> Result<&mut Directory> {
        let node = self.get_mut(ino)?;
        node.get_dir_mut()

    }

    fn has_dir_with(&mut self, ino: u64, name: &OsStr) -> Result<bool> {
        let dir = self.get_dir(ino)?;
        Ok(dir.has(name))
    }

    fn find(&mut self, parent: u64, name: &OsStr) -> Result<&Node> {
        let dir = self.get_dir(parent)?;
        let ino = dir.get(name).or_error(ENOENT)?;
        self.get(ino)
    }
}

pub struct MemoryFuse {
    nodes: Nodes,
    next_ino: AtomicUsize,
}

impl MemoryFuse {
    pub fn new() -> MemoryFuse {
        let attr = new_attr(1, fuser::FileType::Directory,0o755, get_current_uid(), get_current_gid());
        let mut nodes = Nodes::new();
        let root = Node::new_directory(attr);
        let _ = nodes.insert(root);
        Self {
            nodes,
            next_ino: AtomicUsize::new(2),
        }
    }

    pub fn create_attr(&mut self, kind: fuser::FileType, perm: u16, uid: u32, gid: u32) -> fuser:: FileAttr {
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed) as u64;
        new_attr(ino, kind, perm, uid, gid)
    }

    fn lookup_node(&mut self, parent: u64, name: &OsStr) -> Result<FileAttr> {
        let node = self.nodes.find(parent, name)?;
        Ok(node.attr)
    }

    fn get_attr(&self, ino: u64) -> Result<FileAttr> {
        let node = self.nodes.get(ino)?;
        Ok(node.attr)
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
        flags: Option<u32>
    ) -> Result<FileAttr> {
        let node = self.nodes.get_mut(ino)?;
        let mut attr = node.attr;
        if let Some(mode) = mode { attr.perm = mode as u16; }
        if let Some(uid) = uid { attr.uid = uid; }
        if let Some(gid) = gid { attr.gid = gid; }
        if let Some(atime) = atime { attr.atime = atime.to_time(); }
        if let Some(mtime) = mtime { attr.mtime = mtime.to_time(); }
        if let Some(ctime) = ctime { attr.ctime = ctime; }
        if let Some(flags) = flags { attr.flags = flags; }
        if let Some(size) = size {
            let data = node.get_data_mut()?;
            data.resize(size as usize, 0u8);
            attr.size = size;
        }
        node.attr = attr;
        Ok(attr)
    }

    fn read_link(&mut self, ino: u64) -> Result<&[u8]> {
        let node = self.nodes.get_mut(ino)?;
        let target = node.get_target()?;
        Ok(target.as_os_str().as_bytes())
    }

    fn make_file(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr> {
        let attr = self.create_attr(
            fuser::FileType::RegularFile,
            mode as u16,
            uid,
            gid
        );
        let dir = self.nodes.get_dir_mut(parent)?;
        dir.insert(name, attr.ino)?;
        let node = Node::new_file(attr);
        self.nodes.insert(node)
    }

    fn make_directory(
        &mut self,
        parent: u64,
        name: &OsStr,
        mode: u32,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr> {
        let attr = self.create_attr(
            fuser::FileType::Directory,
            mode as u16,
            uid,
            gid
        );
        let dir = self.nodes.get_dir_mut(parent)?;
        dir.insert(name, attr.ino)?;
        let node = Node::new_directory(attr);
        self.nodes.insert(node)
    }

    fn make_symbolic_link(
        &mut self,
        parent: u64,
        link_name: &OsStr,
        target: &Path,
        uid: u32,
        gid: u32,
    ) -> Result<FileAttr> {
        let attr = self.create_attr(
            fuser::FileType::Symlink,
            0o777,
            uid,
            gid
        );
        let dir = self.nodes.get_dir_mut(parent)?;
        dir.insert(link_name, attr.ino)?;
        let node = Node::new_symbolic_link(attr, target);
        self.nodes.insert(node)
    }

    fn link_node(
        &mut self,
        ino: u64,
        new_parent: u64,
        new_name: &std::ffi::OsStr,
    ) -> Result<FileAttr> {
        let attr = {
            let node = self.nodes.get(ino)?;
            node.attr
        };
        {
            let dir = self.nodes.get_dir_mut(new_parent)?;
            dir.insert(new_name, ino)?;
        }
        {
            let node = self.nodes.get_mut(ino)?;
            node.add_link();
        }
        Ok(attr)
    }

    fn unlink_node(&mut self, parent: u64, name: &OsStr) -> Result<()> {
        let dir = self.nodes.get_dir_mut(parent)?;
        let ino = dir.remove(name)?;
        self.nodes.dec_link(ino)
    }

     fn rename_node(
        &mut self,
        parent: u64,
        name: &OsStr,
        newparent: u64,
        newname: &OsStr,
        flags: u32,
    ) -> Result<()> {
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

        if self.nodes.has_dir_with(newparent, newname)? {
            if !allow_overwrite {
                // The newname must not exist
                return Err(EEXIST);
            }
            self.unlink_node(newparent, newname)?;
        } else {
            // The target must exist
            return Err(ENOENT)?
        }

        if exchange {
            let name_ino = self.nodes.find(parent, name)?.attr.ino;
            let newname_ino = self.nodes.find(newparent, newname)?.attr.ino;

            // The next to operations cannot fail as they have been validated by the previous
            // two calls. However, if they do we should panic as it will result in an inconsistent
            // file system.
            {
                self.nodes.get_dir_mut(parent).unwrap().set(name, newname_ino).unwrap();
            }
            {
                self.nodes.get_dir_mut(newparent).unwrap().set(newname, name_ino).unwrap();
            }
            Ok(())
        } else {
            let ino = self.nodes.get_dir_mut(parent)?.remove(name)?;
            self.nodes.get_dir_mut(newparent)?.insert(newname, ino)
        }
    }

    fn read_file(&mut self, ino: u64, offset: i64, size: u32) -> Result<&[u8]>{
        let node = self.nodes.get_mut(ino)?;
        let data = node.get_data()?;
        let effective_offset = if offset < 0 {
            max(0, (data.len() as i64 + offset) as usize)
        } else {
            min(offset as usize, data.len())
        };
        let effective_end = min(effective_offset + size as usize, data.len());
        Ok(&data[effective_offset..effective_end])
    }

    fn write_file(&mut self, ino: u64, offset: i64, new_data: &[u8]) -> Result<usize> {
        let node = self.nodes.get_mut(ino)?;
        let new_size = {
            let data = node.get_data_mut()?;
            let effective_offset = if offset < 0 {
                (data.len() as i64 + offset) as usize
            } else {
                offset as usize
            };
            let effective_size = effective_offset + new_data.len();
            let new_size = if effective_size > data.len() {
                effective_size
            } else {
                data.len()
            };
            if new_size > data.len() {
                data.resize(new_size, 0u8);
            }
            data[effective_offset..effective_size].copy_from_slice(new_data);
            new_size
        };
        node.attr.size = new_size as u64;
        Ok(new_data.len())
    }

    fn read_directory(&self, ino: u64) -> Result<impl Iterator<Item = (&OsString, Result<&FileAttr>)>> {
        let dir = self.nodes.get_dir_anon(ino)?;
        Ok(dir.iter().map(|(name, ino)| {
            (name, self.nodes.get(*ino).and_then(|node| { Ok(&node.attr) }))
        }))
    }
}

impl Filesystem for MemoryFuse {
    fn init(&mut self, _req: &fuser::Request<'_>, _config: &mut fuser::KernelConfig) -> std::result::Result<(), c_int> {
        Ok(())
    }

    fn destroy(&mut self) { }


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
            Ok(data) => reply.data(data),
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
            Ok(data) => reply.data(data),
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
            Ok(iter) => {
                let mut current = 0;
                for (name, attr_result) in iter {
                    if attr_result.is_err() {
                        continue
                    }
                    current += 1;
                    if current >= offset {
                        let attr = attr_result.unwrap();
                        if reply.add(attr.ino, current + 1, attr.kind, name) {
                            break
                        }
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
) -> fuser::FileAttr {
    let now = SystemTime::now();
    fuser::FileAttr {
        ino,
        size: 0u64,
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

trait OrError<R> {
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