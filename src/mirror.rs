use std::{
    ffi::OsString,
    fmt::Debug,
    fs,
    os::unix::fs::{chown, symlink, PermissionsExt, FileExt},
    path::{Path, PathBuf},
    sync::{
        mpsc::{self, Sender},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

use fuser::FileAttr;
use libc::{c_int, ENOENT};
use log::{debug, info};
use crate::node::{FileContent, NodeKind, Nodes};

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
        parent: u64,
        name: OsString,
        attr: FileAttr,
    },
    CreateDir {
        parent: u64,
        name: OsString,
        attr: FileAttr,
    },
    CreateSymlink {
        parent: u64,
        name: OsString,
        target: PathBuf,
        attr: FileAttr,
    },
    Write {
        ino: u64,
    },
    SetAttr {
        ino: u64,
        attr: FileAttr,
    },
    Delete {
        parent: u64,
        name: OsString,
    },
    Rename {
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
}

pub struct MirrorWorker {
    work_queue: Sender<WriteJob>,
    worker_thread: Option<JoinHandle<()>>,
}

impl MirrorWorker {
    pub fn new(
        mirror: Arc<dyn Mirror + Send + Sync>,
        nodes: Arc<RwLock<Nodes>>,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<WriteJob>();

        let worker_mirror = mirror;
        let worker_thread = thread::spawn(move || {
            for job in rx {
                if let Err(e) = Self::handle_job(&worker_mirror, &nodes, job) {
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
        job: WriteJob,
    ) -> std::io::Result<()> {
        debug!("Executing job: {job:?}");
        let path_resolver = PathResolver::new(nodes);
        match job {
            WriteJob::CreateFile { parent, name, attr } => {
                mirror.create_file(parent, &name, &attr, &path_resolver)?;
            }
            WriteJob::CreateDir { parent, name, attr } => {
                mirror.create_dir(parent, &name, &attr, &path_resolver)?;
            }
            WriteJob::CreateSymlink {
                parent,
                name,
                target,
                attr,
            } => {
                mirror.create_symlink(parent, &name, &target, &attr, &path_resolver)?;
            }
            WriteJob::Write { ino } => {
                let (data_to_write, regions) = {
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
                        let regions = file.dirty_regions.regions().clone();
                        file.dirty_regions.clear();
                        if let FileContent::InMemory(data) = &file.content {
                            (Some(data.clone()), regions)
                        } else {
                            (None, regions)
                        }
                    } else {
                        return Ok(());
                    }
                };

                if let Some(data) = data_to_write {
                    let data = data.read().unwrap();
                    for (start, end) in regions {
                        let start = start as usize;
                        let end = end as usize;
                        if end > data.len() {
                            continue;
                        }
                        mirror.write(ino, &data[start..end], start as u64, &path_resolver)?;
                    }
                }
            }
            WriteJob::SetAttr { ino, attr } => {
                mirror.set_attr(ino, &attr, Some(attr.size), &path_resolver)?;
            }
            WriteJob::Delete { parent, name } => {
                mirror.delete(parent, &name, &path_resolver)?;
            }
            WriteJob::Rename {
                parent,
                name,
                new_parent,
                new_name,
            } => {
                mirror.rename(parent, &name, new_parent, &new_name, &path_resolver)?;
            }
            WriteJob::Link {
                ino,
                new_parent,
                new_name,
            } => {
                mirror.link(ino, new_parent, &new_name, &path_resolver)?;
            }
        }
        Ok(())
    }


    pub fn sender(&self) -> Sender<WriteJob> {
        self.work_queue.clone()
    }

    pub fn stop(mut self) {
        // By taking ownership of self, the work_queue sender is dropped when
        // this function returns, which will cause the worker thread to exit.
        drop(self.work_queue);
        if let Some(worker_thread) = self.worker_thread.take() {
            worker_thread.join().unwrap();
        }
    }
}

pub trait Mirror: Debug {
    fn read_dir<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<Vec<fs::DirEntry>>;
    fn read_link<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<PathBuf>;
    fn read_file<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<Vec<u8>>;
    fn create_file<'a>(
        &self,
        parent: u64,
        name: &OsString,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn create_dir<'a>(
        &self,
        parent: u64,
        name: &OsString,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn create_symlink<'a>(
        &self,
        parent: u64,
        name: &OsString,
        target: &Path,
        attr: &FileAttr,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn write<'a>(
        &self,
        ino: u64,
        data: &[u8],
        offset: u64,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn set_attr<'a>(
        &self,
        ino: u64,
        attr: &FileAttr,
        size: Option<u64>,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()>;
    fn delete<'a>(&self, parent: u64, name: &OsString, path_resolver: &PathResolver<'a>) -> std::io::Result<()>;
    fn rename<'a>(
        &self,
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
    fn read_dir<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<Vec<fs::DirEntry>> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        let mut entries = Vec::new();
        for entry in fs::read_dir(fs_path)? {
            entries.push(entry?);
        }
        Ok(entries)
    }

    fn read_link<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<PathBuf> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        fs::read_link(fs_path)
    }

    fn read_file<'a>(&self, ino: u64, path_resolver: &PathResolver<'a>) -> std::io::Result<Vec<u8>> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        fs::read(fs_path)
    }

    fn create_file<'a>(
        &self,
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

    fn write<'a>(
        &self,
        ino: u64,
        data: &[u8],
        offset: u64,
        path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let path = path_resolver.resolve(ino)?;
        let fs_path = self.get_fs_path(&path);
        let file = fs::OpenOptions::new().write(true).open(&fs_path)?;
        file.write_all_at(data, offset)
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

    fn delete<'a>(&self, parent: u64, name: &OsString, path_resolver: &PathResolver<'a>) -> std::io::Result<()> {
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
