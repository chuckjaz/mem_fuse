use std::{
    ffi::{OsStr, OsString}, os::unix::ffi::OsStrExt, path::{Path, PathBuf}, sync::{atomic::{AtomicUsize, Ordering}, Arc, RwLock}, time::SystemTime, u32
};

use fuser::{self, FileAttr, Filesystem, TimeOrNow};
use libc::{c_int, EEXIST, ENOENT, ENOSYS};
#[cfg(target_os = "linux")]
use libc::{RENAME_NOREPLACE, RENAME_EXCHANGE};
use log::{debug, info};
use users::{get_current_gid, get_current_uid};

use crate::disk_image::{DiskImageWorker, WriteJob};
use crate::node::{FileContent, Node, NodeKind, Nodes};

pub(crate) type Result<T> = std::result::Result<T, c_int>;

pub struct MemoryFuse {
    nodes: Arc<RwLock<Nodes>>,
    next_ino: AtomicUsize,
    disk_worker: Option<DiskImageWorker>,
}

impl MemoryFuse {
    pub fn new(disk_image_path: Option<PathBuf>) -> MemoryFuse {
        let attr = new_attr(
            1,
            fuser::FileType::Directory,
            0o755,
            get_current_uid(),
            get_current_gid(),
        );
        let mut nodes = Nodes::new();
        let root = Node::new_directory(attr);
        let _ = nodes.insert(root);
        let nodes = Arc::new(RwLock::new(nodes));

        let disk_worker =
            disk_image_path.map(|path| DiskImageWorker::new(path, Arc::clone(&nodes)));

        Self {
            nodes,
            next_ino: AtomicUsize::new(2),
            disk_worker,
        }
    }

    pub fn create_attr(&mut self, kind: fuser::FileType, perm: u16, uid: u32, gid: u32) -> fuser:: FileAttr {
        let ino = self.next_ino.fetch_add(1, Ordering::Relaxed) as u64;
        new_attr(ino, kind, perm, uid, gid)
    }

    fn lookup_node(&mut self, parent: u64, name: &OsStr) -> Result<FileAttr> {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.find(parent, name)?;
        Ok(node.attr)
    }

    fn get_attr(&self, ino: u64) -> Result<FileAttr> {
        let nodes = self.nodes.read().unwrap();
        let node = nodes.get(ino)?;
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
        flags: Option<u32>,
    ) -> Result<FileAttr> {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        let mut attr = node.attr;
        if let Some(mode) = mode {
            attr.perm = mode as u16;
        }
        if let Some(uid) = uid {
            attr.uid = uid;
        }
        if let Some(gid) = gid {
            attr.gid = gid;
        }
        if let Some(atime) = atime {
            attr.atime = atime.to_time();
        }
        if let Some(mtime) = mtime {
            attr.mtime = mtime.to_time();
        }
        if let Some(ctime) = ctime {
            attr.ctime = ctime;
        }
        if let Some(flags) = flags {
            attr.flags = flags;
        }
        if let Some(size) = size {
            if let NodeKind::File { content } = &mut node.kind {
                let data = match content {
                    FileContent::Dirty(data) | FileContent::InMemory(data) => data,
                    FileContent::OnDisk => {
                        let new_data = self
                            .disk_worker
                            .as_ref()
                            .unwrap()
                            .image()
                            .read(ino)
                            .unwrap_or_default();
                        *content = FileContent::InMemory(new_data);
                        if let FileContent::InMemory(data) = content {
                            data
                        } else {
                            unreachable!()
                        }
                    }
                };
                data.resize(size as usize, 0u8);
                attr.size = size;
            }
        }
        node.attr = attr;
        Ok(attr)
    }

    fn read_link(&mut self, ino: u64) -> Result<Vec<u8>> {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        let target = node.get_target()?;
        Ok(target.as_os_str().as_bytes().to_vec())
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
        let mut nodes = self.nodes.write().unwrap();
        let dir = nodes.get_dir_mut(parent)?;
        dir.insert(name, attr.ino)?;
        let node = Node::new_file(attr);
        nodes.insert(node)
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
        let mut nodes = self.nodes.write().unwrap();
        let dir = nodes.get_dir_mut(parent)?;
        dir.insert(name, attr.ino)?;
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
        let attr = self.create_attr(
            fuser::FileType::Symlink,
            0o777,
            uid,
            gid
        );
        let mut nodes = self.nodes.write().unwrap();
        let dir = nodes.get_dir_mut(parent)?;
        dir.insert(link_name, attr.ino)?;
        let node = Node::new_symbolic_link(attr, target);
        nodes.insert(node)
    }

    fn link_node(
        &mut self,
        ino: u64,
        new_parent: u64,
        new_name: &std::ffi::OsStr,
    ) -> Result<FileAttr> {
        let mut nodes = self.nodes.write().unwrap();
        let attr = {
            let node = nodes.get(ino)?;
            node.attr
        };
        {
            let dir = nodes.get_dir_mut(new_parent)?;
            dir.insert(new_name, ino)?;
        }
        {
            let node = nodes.get_mut(ino)?;
            node.add_link();
        }
        Ok(attr)
    }

    fn unlink_node(&mut self, parent: u64, name: &OsStr) -> Result<()> {
        let mut nodes = self.nodes.write().unwrap();
        let ino = {
            let dir = nodes.get_dir_mut(parent)?;
            dir.remove(name)?
        };
        if let Some(worker) = &self.disk_worker {
            worker.sender().send(WriteJob::Delete { ino }).unwrap();
        }
        nodes.dec_link(ino)
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

        let mut nodes = self.nodes.write().unwrap();

        if nodes.has_dir_with(newparent, newname)? {
            if !allow_overwrite {
                // The newname must not exist
                return Err(EEXIST);
            }
            let ino = {
                let dir = nodes.get_dir_mut(newparent)?;
                dir.remove(newname)?
            };
            nodes.dec_link(ino)?;
        } else {
            // The target must exist
            return Err(ENOENT)?
        }

        if exchange {
            let name_ino = nodes.find(parent, name)?.attr.ino;
            let newname_ino = nodes.find(newparent, newname)?.attr.ino;

            // The next to operations cannot fail as they have been validated by the previous
            // two calls. However, if they do we should panic as it will result in an inconsistent
            // file system.
            {
                nodes.get_dir_mut(parent).unwrap().set(name, newname_ino).unwrap();
            }
            {
                nodes.get_dir_mut(newparent).unwrap().set(newname, name_ino).unwrap();
            }
            Ok(())
        } else {
            let ino = nodes.get_dir_mut(parent)?.remove(name)?;
            nodes.get_dir_mut(newparent)?.insert(newname, ino)
        }
    }

    fn read_file(&mut self, ino: u64, offset: i64, size: u32) -> Result<Vec<u8>> {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        if let NodeKind::File { content } = &mut node.kind {
            let data = match content {
                FileContent::Dirty(data) | FileContent::InMemory(data) => data,
                FileContent::OnDisk => {
                    let new_data = self
                        .disk_worker
                        .as_ref()
                        .unwrap()
                        .image()
                        .read(ino)
                        .unwrap_or_default();
                    *content = FileContent::InMemory(new_data);
                    if let FileContent::InMemory(data) = content {
                        data
                    } else {
                        unreachable!()
                    }
                }
            };
            let effective_offset = if offset < 0 {
                max(0, (data.len() as i64 + offset) as usize)
            } else {
                min(offset as usize, data.len())
            };
            let effective_end = min(effective_offset + size as usize, data.len());
            Ok(data[effective_offset..effective_end].to_vec())
        } else {
            Err(ENOENT)
        }
    }

    fn write_file(&mut self, ino: u64, offset: i64, new_data: &[u8]) -> Result<usize> {
        let mut nodes = self.nodes.write().unwrap();
        let node = nodes.get_mut(ino)?;
        let new_size = {
            if let NodeKind::File { content } = &mut node.kind {
                let data = match content {
                    FileContent::Dirty(data) | FileContent::InMemory(data) => data,
                    FileContent::OnDisk => {
                        let new_data = self
                            .disk_worker
                            .as_ref()
                            .unwrap()
                            .image()
                            .read(ino)
                            .unwrap_or_default();
                        *content = FileContent::InMemory(new_data);
                        if let FileContent::InMemory(data) = content {
                            data
                        } else {
                            unreachable!()
                        }
                    }
                };
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
                if let Some(worker) = &self.disk_worker {
                    let new_content = FileContent::Dirty(data.clone());
                    worker
                        .sender()
                        .send(WriteJob::Write {
                            ino,
                            data: data.clone(),
                        })
                        .unwrap();
                    *content = new_content;
                }
                new_size
            } else {
                return Err(ENOENT);
            }
        };
        node.attr.size = new_size as u64;
        Ok(new_data.len())
    }

    fn read_directory(&self, ino: u64) -> Result<Vec<(OsString, FileAttr)>> {
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
        if let Some(mut worker) = self.disk_worker.take() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::time::Duration;
    use std::thread::sleep;
    use tempfile::tempdir;

    #[test]
    fn test_write_behind_cache() {
        let dir = tempdir().unwrap();
        let cache_path = dir.path().to_path_buf();

        let mut fuse = MemoryFuse::new(Some(cache_path.clone()));

        let parent_ino = 1;
        let file_name = OsStr::new("test.txt");
        let file_attr = fuse.make_file(parent_ino, file_name, 0o644, 1000, 1000).unwrap();
        let file_ino = file_attr.ino;

        let data = b"hello world";
        fuse.write_file(file_ino, 0, data).unwrap();

        let on_disk_path = cache_path.join(file_ino.to_string());
        let mut found = false;
        for _ in 0..20 {
            if on_disk_path.exists() {
                found = true;
                break;
            }
            sleep(Duration::from_millis(100));
        }
        assert!(found, "File was not written to disk in time");

        let on_disk_data = fs::read(&on_disk_path).unwrap();
        assert_eq!(on_disk_data, data);

        let mut is_on_disk = false;
        for _ in 0..20 {
            let nodes = fuse.nodes.read().unwrap();
            let node = nodes.get(file_ino).unwrap();
            if let NodeKind::File { content } = &node.kind {
                if let FileContent::OnDisk = content {
                    is_on_disk = true;
                    break;
                }
            }
            drop(nodes);
            sleep(Duration::from_millis(100));
        }
        assert!(is_on_disk, "File did not transition to OnDisk state");

        let read_data = fuse.read_file(file_ino, 0, data.len() as u32).unwrap();
        assert_eq!(read_data, data);

        fuse.unlink_node(parent_ino, file_name).unwrap();

        let mut deleted = false;
        for _ in 0..20 {
            if !on_disk_path.exists() {
                deleted = true;
                break;
            }
            sleep(Duration::from_millis(100));
        }
        assert!(deleted, "File was not deleted from disk in time");
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