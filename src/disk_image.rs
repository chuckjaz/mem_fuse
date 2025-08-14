use std::{
    fs,
    os::unix::fs::{chown, symlink, PermissionsExt},
    path::{Path, PathBuf},
    sync::{
        mpsc::{self, Sender},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

use fuser::FileAttr;
use log::{debug, info};

use crate::node::{FileContent, NodeKind, Nodes};

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
        path: PathBuf,
        attr: FileAttr,
    },
    CreateDir {
        path: PathBuf,
        attr: FileAttr,
    },
    CreateSymlink {
        path: PathBuf,
        target: PathBuf,
        attr: FileAttr,
    },
    Write {
        ino: u64,
        path: PathBuf,
        data: Vec<u8>,
    },
    SetAttr {
        path: PathBuf,
        attr: FileAttr,
    },
    Delete {
        path: PathBuf,
    },
    Rename {
        old_path: PathBuf,
        new_path: PathBuf,
    },
    Link {
        source_path: PathBuf,
        link_path: PathBuf,
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
            WriteJob::CreateFile { path, attr } => {
                let fs_path = disk_image.get_fs_path(&path);
                if let Some(parent) = fs_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::File::create(&fs_path)?;
                let perm = fs::Permissions::from_mode(attr.perm as u32);
                fs::set_permissions(&fs_path, perm)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
            }
            WriteJob::CreateDir { path, attr } => {
                let fs_path = disk_image.get_fs_path(&path);
                fs::create_dir_all(&fs_path)?;
                let perm = fs::Permissions::from_mode(attr.perm as u32);
                fs::set_permissions(&fs_path, perm)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
            }
            WriteJob::CreateSymlink { path, target, attr } => {
                let fs_path = disk_image.get_fs_path(&path);
                if let Some(parent) = fs_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                symlink(&target, &fs_path)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
            }
            WriteJob::Write { ino, path, data } => {
                let fs_path = disk_image.get_fs_path(&path);
                fs::write(&fs_path, &data)?;

                let mut nodes = nodes.write().unwrap();
                if let Ok(node) = nodes.get_mut(ino) {
                    if let NodeKind::File { content } = &mut node.kind {
                        if let FileContent::Dirty(mem_data) = content {
                            if mem_data.as_slice() == data.as_slice() {
                                *content = FileContent::OnDisk;
                            }
                        }
                    }
                }
            }
            WriteJob::SetAttr { path, attr } => {
                let fs_path = disk_image.get_fs_path(&path);
                let perm = fs::Permissions::from_mode(attr.perm as u32);
                fs::set_permissions(&fs_path, perm)?;
                chown(&fs_path, Some(attr.uid), Some(attr.gid))?;
                if let Ok(file) = fs::OpenOptions::new().write(true).open(&fs_path) {
                    file.set_len(attr.size)?;
                }
            }
            WriteJob::Delete { path } => {
                let fs_path = disk_image.get_fs_path(&path);
                if fs_path.is_dir() {
                    fs::remove_dir(fs_path)?;
                } else {
                    fs::remove_file(fs_path)?;
                }
            }
            WriteJob::Rename { old_path, new_path } => {
                let old_fs_path = disk_image.get_fs_path(&old_path);
                let new_fs_path = disk_image.get_fs_path(&new_path);
                fs::rename(old_fs_path, new_fs_path)?;
            }
            WriteJob::Link {
                source_path,
                link_path,
            } => {
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
