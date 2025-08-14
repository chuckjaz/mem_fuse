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

#[derive(Debug, Clone)]
pub struct DiskImage {
    base_path: PathBuf,
}

impl DiskImage {
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

pub struct DiskImageWorker {
    disk_image: DiskImage,
    work_queue: Sender<WriteJob>,
    worker_thread: Option<JoinHandle<()>>,
}

impl DiskImageWorker {
    pub fn new(
        base_path: PathBuf,
        nodes: Arc<RwLock<Nodes>>,
    ) -> Self {
        let disk_image = DiskImage::new(base_path);
        let (tx, rx) = mpsc::channel::<WriteJob>();

        let worker_disk_image = disk_image.clone();
        let worker_thread = thread::spawn(move || {
            for job in rx {
                if let Err(e) = Self::handle_job(&worker_disk_image, &nodes, job) {
                    debug!("Failed to execute write job: {e:?}");
                }
            }
        });

        Self {
            disk_image,
            work_queue: tx,
            worker_thread: Some(worker_thread),
        }
    }

    fn handle_job(
        disk_image: &DiskImage,
        nodes: &Arc<RwLock<Nodes>>,
        job: WriteJob,
    ) -> std::io::Result<()> {
        debug!("Executing job: {job:?}");
        match job {
            WriteJob::CreateFile { parent, name, attr } => {
                let path = {
                    let nodes = nodes.read().unwrap();
                    let parent_path = build_path(&nodes, parent).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    parent_path.join(name)
                };
                let fs_path = disk_image.get_fs_path(&path);
                if let Some(parent) = fs_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::File::create(&fs_path)?;
                let perm = fs::Permissions::from_mode(attr.perm as u32);
                fs::set_permissions(&fs_path, perm)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
            }
            WriteJob::CreateDir { parent, name, attr } => {
                let path = {
                    let nodes = nodes.read().unwrap();
                    let parent_path = build_path(&nodes, parent).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    parent_path.join(name)
                };
                let fs_path = disk_image.get_fs_path(&path);
                fs::create_dir_all(&fs_path)?;
                let perm = fs::Permissions::from_mode(attr.perm as u32);
                fs::set_permissions(&fs_path, perm)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
            }
            WriteJob::CreateSymlink { parent, name, target, attr } => {
                let path = {
                    let nodes = nodes.read().unwrap();
                    let parent_path = build_path(&nodes, parent).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    parent_path.join(name)
                };
                let fs_path = disk_image.get_fs_path(&path);
                if let Some(parent) = fs_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                symlink(&target, &fs_path)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
            }
            WriteJob::Write { ino } => {
                let (path, data_to_write, regions) = {
                    let path = {
                        let nodes = nodes.read().unwrap();
                        match build_path(&nodes, ino) {
                            Ok(p) => p,
                            Err(_) => return Ok(()), // Node not found, nothing to do
                        }
                    };

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
                            (path, Some(data.clone()), regions)
                        } else {
                            (path, None, regions)
                        }
                    } else {
                        return Ok(());
                    }
                };

                if let Some(data) = data_to_write {
                    let fs_path = disk_image.get_fs_path(&path);
                    let file = fs::OpenOptions::new().write(true).open(&fs_path)?;
                    let data = data.read().unwrap();
                    for (start, end) in regions {
                        let start = start as usize;
                        let end = end as usize;
                        if end > data.len() {
                            continue;
                        }
                        file.write_all_at(&data[start..end], start as u64)?;
                    }

                    // Now re-acquire lock to update state
                    let mut nodes = nodes.write().unwrap();
                    if let Ok(node) = nodes.get_mut(ino) {
                        if let NodeKind::File(file) = &mut node.kind {
                            if !file.dirty {
                                file.content = FileContent::OnDisk;
                            }
                        }
                    }
                }
            }
            WriteJob::SetAttr { ino, attr } => {
                let path = {
                    let nodes = nodes.read().unwrap();
                    build_path(&nodes, ino).map_err(|e| std::io::Error::from_raw_os_error(e))?
                };
                let fs_path = disk_image.get_fs_path(&path);
                let perm = fs::Permissions::from_mode(attr.perm as u32);
                fs::set_permissions(&fs_path, perm)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
                if let Ok(file) = fs::OpenOptions::new().write(true).open(&fs_path) {
                    file.set_len(attr.size)?;
                }
            }
            WriteJob::Delete { parent, name } => {
                let path = {
                    let nodes = nodes.read().unwrap();
                    let parent_path = build_path(&nodes, parent).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    parent_path.join(name)
                };
                let fs_path = disk_image.get_fs_path(&path);
                if fs_path.is_dir() {
                    fs::remove_dir(fs_path)?;
                } else {
                    fs::remove_file(fs_path)?;
                }
            }
            WriteJob::Rename { parent, name, new_parent, new_name } => {
                let (old_path, new_path) = {
                    let nodes = nodes.read().unwrap();
                    let old_parent_path = build_path(&nodes, parent).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    let old_path = old_parent_path.join(name);
                    let new_parent_path = build_path(&nodes, new_parent).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    let new_path = new_parent_path.join(new_name);
                    (old_path, new_path)
                };
                let old_fs_path = disk_image.get_fs_path(&old_path);
                let new_fs_path = disk_image.get_fs_path(&new_path);
                fs::rename(old_fs_path, new_fs_path)?;
            }
            WriteJob::Link { ino, new_parent, new_name } => {
                let (source_path, link_path) = {
                    let nodes = nodes.read().unwrap();
                    let source_path = build_path(&nodes, ino).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    let new_parent_path = build_path(&nodes, new_parent).map_err(|e| std::io::Error::from_raw_os_error(e))?;
                    let link_path = new_parent_path.join(new_name);
                    (source_path, link_path)
                };
                let source_fs_path = disk_image.get_fs_path(&source_path);
                let link_fs_path = disk_image.get_fs_path(&link_path);
                if let Some(parent) = link_fs_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::hard_link(source_fs_path, link_fs_path)?;
            }
        }
        Ok(())
    }


    pub fn sender(&self) -> Sender<WriteJob> {
        self.work_queue.clone()
    }

    pub fn stop(&mut self) {
        drop(self.work_queue.clone());
        if let Some(worker_thread) = self.worker_thread.take() {
            worker_thread.join().unwrap();
        }
    }

    pub fn image(&self) -> &DiskImage {
        &self.disk_image
    }
}
