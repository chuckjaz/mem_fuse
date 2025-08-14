use std::{
    fs,
    path::PathBuf,
    sync::{
        mpsc::{self, Sender},
        Arc, RwLock,
    },
    thread::{self, JoinHandle},
};

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

    pub fn path_for_ino(&self, ino: u64) -> PathBuf {
        self.base_path.join(ino.to_string())
    }

    pub fn write(&self, ino: u64, data: &[u8]) -> std::io::Result<()> {
        info!("Writing to disk ino: {ino}");
        fs::write(self.path_for_ino(ino), data)
    }

    pub fn read(&self, ino: u64) -> std::io::Result<Vec<u8>> {
        info!("Reading from disk ino: {ino}");
        fs::read(self.path_for_ino(ino))
    }

    pub fn remove(&self, ino: u64) -> std::io::Result<()> {
        info!("Removing from disk ino: {ino}");
        fs::remove_file(self.path_for_ino(ino))
    }
}

pub enum WriteJob {
    Write { ino: u64, data: Vec<u8> },
    Delete { ino: u64 },
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
                match job {
                    WriteJob::Write { ino, data } => {
                        if let Err(e) = worker_disk_image.write(ino, &data) {
                            debug!("Failed to write {ino} to disk: {e:?}");
                            continue;
                        }
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
                    WriteJob::Delete { ino } => {
                        if let Err(e) = worker_disk_image.remove(ino) {
                            debug!("Failed to remove {ino} from disk: {e:?}");
                        }
                    }
                }
            }
        });

        Self {
            disk_image,
            work_queue: tx,
            worker_thread: Some(worker_thread),
        }
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
