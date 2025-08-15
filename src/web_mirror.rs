use std::{
    ffi::OsString,
    fs::{self},
    path::{Path, PathBuf},
};

use fuser::FileAttr;

use crate::mirror::{Mirror, PathResolver};

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
    fn read_dir<'a>(&self, _ino: u64, _path_resolver: &PathResolver<'a>) -> std::io::Result<Vec<fs::DirEntry>> {
        unimplemented!()
    }

    fn read_link<'a>(&self, _ino: u64, _path_resolver: &PathResolver<'a>) -> std::io::Result<PathBuf> {
        unimplemented!()
    }

    fn read_file<'a>(&self, _ino: u64, _path_resolver: &PathResolver<'a>) -> std::io::Result<Vec<u8>> {
        unimplemented!()
    }

    fn create_file<'a>(
        &self,
        _ino: u64,
        _parent: u64,
        _name: &OsString,
        _attr: &FileAttr,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn create_dir<'a>(
        &self,
        _ino: u64,
        _parent: u64,
        _name: &OsString,
        _attr: &FileAttr,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn create_symlink<'a>(
        &self,
        _ino: u64,
        _parent: u64,
        _name: &OsString,
        _target: &Path,
        _attr: &FileAttr,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn write<'a>(
        &self,
        _ino: u64,
        _data: &[u8],
        _offset: u64,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn set_attr<'a>(
        &self,
        _ino: u64,
        _attr: &FileAttr,
        _size: Option<u64>,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn delete<'a>(&self, _parent: u64, _name: &OsString, _path_resolver: &PathResolver<'a>) -> std::io::Result<()> {
        unimplemented!()
    }

    fn rename<'a>(
        &self,
        _parent: u64,
        _name: &OsString,
        _new_parent: u64,
        _new_name: &OsString,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        unimplemented!()
    }

    fn link<'a>(
        &self,
        _ino: u64,
        _new_parent: u64,
        _new_name: &OsString,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        unimplemented!()
    }
}
