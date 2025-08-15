use std::{
    fs::{self},
    path::{Path, PathBuf},
};

use fuser::FileAttr;

use crate::mirror::Mirror;

#[derive(Debug)]
pub struct WebMirror {
    // To be implemented later
}

impl WebMirror {
    pub fn new(_base_url: &str) -> Self {
        // To be implemented later
        Self {}
    }
}

impl Mirror for WebMirror {
    fn read_dir(&self, _path: &Path) -> std::io::Result<Vec<fs::DirEntry>> {
        unimplemented!()
    }

    fn read_link(&self, _path: &Path) -> std::io::Result<PathBuf> {
        unimplemented!()
    }

    fn read_file(&self, _path: &Path) -> std::io::Result<Vec<u8>> {
        unimplemented!()
    }

    fn create_file(&self, _path: &Path, _attr: &FileAttr) -> std::io::Result<()> {
        unimplemented!()
    }

    fn create_dir(&self, _path: &Path, _attr: &FileAttr) -> std::io::Result<()> {
        unimplemented!()
    }

    fn create_symlink(
        &self,
        _target: &Path,
        _link_path: &Path,
        _attr: &FileAttr,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn write(&self, _path: &Path, _data: &[u8], _offset: u64) -> std::io::Result<()> {
        unimplemented!()
    }

    fn set_attr(&self, _path: &Path, _attr: &FileAttr, _size: Option<u64>) -> std::io::Result<()> {
        unimplemented!()
    }

    fn delete(&self, _path: &Path) -> std::io::Result<()> {
        unimplemented!()
    }

    fn rename(&self, _old_path: &Path, _new_path: &Path) -> std::io::Result<()> {
        unimplemented!()
    }

    fn link(&self, _source_path: &Path, _link_path: &Path) -> std::io::Result<()> {
        unimplemented!()
    }
}
