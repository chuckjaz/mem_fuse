use std::{
    collections::HashMap,
    ffi::OsString,
    fmt::Debug,
    fs,
    os::unix::fs::{chown, symlink, PermissionsExt, FileExt},
    path::{Path, PathBuf},
    sync::{
        mpsc::{self, Sender},
        Arc, Condvar, RwLock,
    },
    thread::{self, JoinHandle},
};

use std::os::unix::fs::MetadataExt;
use std::time::SystemTime;

use fuser::{FileAttr, FileType};
use libc::{c_int, ENOENT};
use log::{debug, info};
use crate::{lru_cache::LruManager, node::{FileContent, NodeKind, Nodes}};

pub(crate) type Result<T> = std::result::Result<T, c_int>;

pub fn build_path(nodes: &Nodes, ino: u64) -> Result<PathBuf> {
    if ino == 1 {
        // Root directory
        return Ok(PathBuf::new());
    }

    if let Some(parents) = nodes.get_parents(ino) {
        if let Some((parent_ino, name)) = parents.first() {
            let parent_path = build_path(nodes, *parent_ino)?;
            return Ok(parent_path.join(name));
        }
    }

    Err(ENOENT)
}

fn to_io_error<T>(res: Result<T>) -> std::io::Result<T> {
    res.map_err(|e| std::io::Error::from_raw_os_error(e))
}

pub struct PathResolver<'a> {
    nodes: &'a Arc<RwLock<Nodes>>,
}

impl<'a> PathResolver<'a> {
    pub fn new(nodes: &'a Arc<RwLock<Nodes>>) -> Self {
        Self { nodes }
    }

    pub fn resolve(&self, ino: u64) -> std::io::Result<PathBuf> {
        let nodes = self.nodes.read().unwrap();
        to_io_error(build_path(&nodes, ino))
    }

    pub fn resolve_parent(&self, parent: u64, name: &OsString) -> std::io::Result<PathBuf> {
        let parent_path = self.resolve(parent)?;
        Ok(parent_path.join(name))
    }
}

#[derive(Debug)]
pub enum WriteJob {
    CreateFile {
        ino: u64,
        parent: u64,
        name: OsString,
        attr: FileAttr,
    },
    CreateDir {
        ino: u64,
        parent: u64,
        name: OsString,
        attr: FileAttr,
    },
    CreateSymlink {
        ino: u64,
        parent: u64,
        name: OsString,
        target: PathBuf,
        attr: FileAttr,
    },
    Write {
        ino: u64,
    },
    WriteBlock {
        ino: u64,
        block_id: u64,
    },
    SetAttr {
        ino: u64,
        attr: FileAttr,
    },
    Delete {
        ino: u64,
        parent: u64,
        name: OsString,
    },
    Rename {
        ino: u64,
        parent: u64,
        name: OsString,
        new_parent: u64,
        new_name: OsString,
    },
    Link {
        ino: u64,
        new_parent: u64,
        new_name: OsString,
    },
    Shutdown,
}

pub struct MirrorWorker {
    work_queue: Sender<WriteJob>,
    worker_thread: Option<JoinHandle<()>>,
}

impl MirrorWorker {
    pub fn new(
        mirror: Arc<dyn Mirror + Send + Sync>,
        nodes: Arc<RwLock<Nodes>>,
        lru_manager: Arc<LruManager>,
        cache_cond: Arc<Condvar>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<WriteJob>();

        let worker_mirror = mirror;
        let worker_thread = thread::spawn(move || {
            for job in rx {
                if matches!(job, WriteJob::Shutdown) {
                    break;
                }
                if let Err(e) = Self::handle_job(&worker_mirror, &nodes, &lru_manager, job, &cache_cond) {
                    debug!("Failed to execute write job: {e:?}");
                }
            }
        });


        Self {
            work_queue: tx,
            worker_thread: Some(worker_thread),
        }
    }

    fn handle_job(
        mirror: &Arc<dyn Mirror + Send + Sync>,
        nodes: &Arc<RwLock<Nodes>>,
        lru_manager: &Arc<LruManager>,
        job: WriteJob,
        cache_cond: &Arc<Condvar>,
    ) -> std::io::Result<()> {
        debug!("Executing job: {job:?}");
        let path_resolver = PathResolver::new(nodes);
        match job {
            WriteJob::CreateFile { ino, parent, name, attr } => {
                mirror.create_file(ino, parent, &name, &attr, &path_resolver)?;
            }
            WriteJob::CreateDir { ino, parent, name, attr } => {
                mirror.create_dir(ino, parent, &name, &attr, &path_resolver)?;
            }
            WriteJob::CreateSymlink {
                ino,
                parent,
                name,
                target,
                attr,
            } => {
                mirror.create_symlink(ino, parent, &name, &target, &attr, &path_resolver)?;
            }
            WriteJob::Write { ino } => {
                let (blocks_to_write, dirty_blocks) = {
                    let mut nodes = nodes.write().unwrap();
                    let node = match nodes.get_mut(ino) {
                        Ok(n) => n,
                        Err(_) => return Ok(()),
                    };

                    if let NodeKind::File(file) = &mut node.kind {
                        if !file.dirty {
                            return Ok(());
                        }
                        file.dirty = false;
                        let dirty_blocks = file.dirty_blocks.blocks().clone();
                        file.dirty_blocks.clear();
                        if let FileContent::InMemoryBlocks(blocks) = &file.content {
                            (Some(blocks.clone()), dirty_blocks)
                        } else {
                            (None, dirty_blocks)
                        }
                    } else {
                        return Ok(());
                    }
                };

                if let Some(blocks) = blocks_to_write {
                    for block_id in dirty_blocks {
                        let block_to_write = {
                            let blocks = blocks.read().unwrap();
                            blocks.get(&block_id).cloned()
                        };
                        if let Some(block) = block_to_write {
                            let block_data = block.read();
                            let block_data = block_data.read().unwrap();
                            mirror.write_block(ino, block_id, &block_data, &path_resolver)?;
                        }
                        lru_manager.mark_as_clean(ino, block_id);
                    }
                }
                cache_cond.notify_all();
            }
            WriteJob::WriteBlock { ino, block_id } => {
                // This is a placeholder. The actual implementation will be more complex.
            }
            WriteJob::SetAttr { ino, attr } => {
                mirror.set_attr(ino, &attr, Some(attr.size), &path_resolver)?;
            }
            WriteJob::Delete { ino, parent, name } => {
                mirror.delete(ino, parent, &name, &path_resolver)?;
            }
            WriteJob::Rename {
                ino,
                parent,
                name,
                new_parent,
                new_name,
            } => {
                mirror.rename(ino, parent, &name, new_parent, &new_name, &path_resolver)?;
            }
            WriteJob::Link {
                ino,
                new_parent,
                new_name,
            } => {
                mirror.link(ino, new_parent, &new_name, &path_resolver)?;
            }
            WriteJob::Shutdown => {}
        }
        Ok(())
    }


    pub fn sender(&self) -> Sender<WriteJob> {
        self.work_queue.clone()
    }

    pub fn stop(self) {
        self.work_queue.send(WriteJob::Shutdown).unwrap();
        if let Some(worker_thread) = self.worker_thread {
            worker_thread.join().unwrap();
        }
    }
}

#[derive(Debug)]
pub struct MirrorDirEntry {
    pub name: OsString,
    pub attr: FileAttr,
}

pub trait Mirror: Debug {
    fn read_dir<'a>(
        &self,
        ino: u64,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<Vec<MirrorDirEntry>>;
    fn read_link<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<PathBuf>;

    fn read_block<'a>(
        &self,
        ino: u64,
        block_id: u64,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<Vec<u8>>;
    fn create_file<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn create_dir<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn create_symlink<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        target: &Path,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn write_block<'a>(
        &self,
        ino: u64,
        block_id: u64,
        data: &[u8],
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn set_attr<'a>(
        &self,
        ino: u64,
        attr: &FileAttr,
        size: Option<u64>,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn delete<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn rename<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        new_parent: u64,
        new_name: &OsString,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn link<'a>(
        &self,
        ino: u64,
        new_parent: u64,
        new_name: &OsString,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;

    fn set_inode_map(&self, _ino_map: &HashMap<u64, u64>) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct LocalMirror {
    base_path: PathBuf,
}

impl LocalMirror {
    pub fn new(base_path: PathBuf) -> Self {
        if !base_path.exists() {
            info!("Creating disk image path at {:?}", base_path);
            fs::create_dir_all(&base_path).unwrap();
        }
        Self { base_path }
    }

    pub fn get_fs_path(&self, path: &Path) -> PathBuf {
        let relative_path = if path.has_root() {
            path.strip_prefix("/").unwrap()
        } else {
            path
        };
        self.base_path.join(relative_path)
    }
}

impl Mirror for LocalMirror {
    fn read_dir<'a>(
        &self,
        ino: u64,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<Vec<MirrorDirEntry>> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        let mut entries = Vec::new();
        for entry in fs::read_dir(fs_path)? {
            let entry = entry?;
            let metadata = entry.metadata()?;
            let file_type = metadata.file_type();
            let kind = if file_type.is_dir() {
                FileType::Directory
            } else if file_type.is_file() {
                FileType::RegularFile
            } else {
                FileType::Symlink
            };

            let attr = FileAttr {
                ino: metadata.ino(),
                size: metadata.len(),
                blocks: metadata.blocks(),
                atime: metadata.accessed().unwrap_or(SystemTime::now()),
                mtime: metadata.modified().unwrap_or(SystemTime::now()),
                ctime: metadata.created().unwrap_or(SystemTime::now()),
                crtime: metadata.created().unwrap_or(SystemTime::now()),
                kind,
                perm: metadata.permissions().mode() as u16,
                nlink: metadata.nlink() as u32,
                uid: metadata.uid(),
                gid: metadata.gid(),
                rdev: metadata.rdev() as u32,
                blksize: metadata.blksize() as u32,
                flags: 0,
            };

            entries.push(MirrorDirEntry {
                name: entry.file_name(),
                attr,
            });
        }
        Ok(entries)
    }

    fn read_link<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<PathBuf> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        fs::read_link(fs_path)
    }

    fn read_block<'a>(
        &self,
        ino: u64,
        block_id: u64,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<Vec<u8>> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        if !fs_path.exists() {
            return Ok(Vec::new());
        }
        let file = fs::File::open(fs_path)?;
        // TODO: Get block size from somewhere
        let block_size = 1024 * 1024;
        let mut buffer = vec![0; block_size];
        let bytes_read = file.read_at(&mut buffer, block_id * block_size as u64)?;
        buffer.truncate(bytes_read);
        Ok(buffer)
    }

    fn create_file<'a>(
        &self,
        _ino: u64,
        parent: u64,
        name: &OsString,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let path = path_resolver.resolve_parent(parent, name)?;
        let fs_path = self.get_fs_path(&path);
        if let Some(parent) = fs_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::File::create(&fs_path)?;
        let perm = fs::Permissions::from_mode(attr.perm as u32);
        fs::set_permissions(&fs_path, perm)?;
        chown(&fs_path, Some(attr.uid), Some(attr.gid))
    }

    fn create_dir<'a>(
        &self,
        _ino: u64,
        parent: u64,
        name: &OsString,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let path = path_resolver.resolve_parent(parent, name)?;
        let fs_path = self.get_fs_path(&path);
        fs::create_dir_all(&fs_path)?;
        let perm = fs::Permissions::from_mode(attr.perm as u32);
        fs::set_permissions(&fs_path, perm)?;
        chown(&fs_path, Some(attr.uid), Some(attr.gid))
    }

    fn create_symlink<'a>(
        &self,
        _ino: u64,
        parent: u64,
        name: &OsString,
        target: &Path,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let link_path = path_resolver.resolve_parent(parent, name)?;
        let fs_path = self.get_fs_path(&link_path);
        if let Some(parent) = fs_path.parent() {
            fs::create_dir_all(parent)?;
        }
        symlink(target, &fs_path)?;
        chown(&fs_path, Some(attr.uid), Some(attr.gid))
    }

    fn write_block<'a>(
        &self,
        ino: u64,
        block_id: u64,
        data: &[u8],
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        let file = fs::OpenOptions::new().write(true).create(true).open(&fs_path)?;
        // TODO: Get block size from somewhere
        let block_size = 1024 * 1024;
        file.write_all_at(data, block_id * block_size as u64)
    }

    fn set_attr<'a>(
        &self,
        ino: u64,
        attr: &FileAttr,
        size: Option<u64>,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        let perm = fs::Permissions::from_mode(attr.perm as u32);
        fs::set_permissions(&fs_path, perm)?;
        chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
        if let Some(size) = size {
            if let Ok(file) = fs::OpenOptions::new().write(true).open(&fs_path) {
                file.set_len(size)?;
            }
        }
        Ok(())
    }

    fn delete<'a>(
        &self,
        _ino: u64,
        parent: u64,
        name: &OsString,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let path = path_resolver.resolve_parent(parent, name)?;
        let fs_path = self.get_fs_path(&path);
        if fs_path.is_dir() {
            fs::remove_dir(fs_path)
        } else {
            fs::remove_file(fs_path)
        }
    }

    fn rename<'a>(
        &self,
        _ino: u64,
        parent: u64,
        name: &OsString,
        new_parent: u64,
        new_name: &OsString,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let old_path = path_resolver.resolve_parent(parent, name)?;
        let new_path = path_resolver.resolve_parent(new_parent, new_name)?;
        let old_fs_path = self.get_fs_path(&old_path);
        let new_fs_path = self.get_fs_path(&new_path);
        fs::rename(old_fs_path, new_fs_path)
    }

    fn link<'a>(
        &self,
        ino: u64,
        new_parent: u64,
        new_name: &OsString,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let source_path = path_resolver.resolve(ino)?;
        let link_path = path_resolver.resolve_parent(new_parent, new_name)?;
        let source_fs_path = self.get_fs_path(&source_path);
        let link_fs_path = self.get_fs_path(&link_path);
        if let Some(parent) = link_fs_path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::hard_link(source_fs_path, link_fs_path)
    }
}
