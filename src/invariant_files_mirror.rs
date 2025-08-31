use std::{
    ffi::OsString, io::{self, ErrorKind}, path::{Path, PathBuf}, time::UNIX_EPOCH, u32
};

use std::collections::HashMap;
use std::sync::RwLock;

use fuser::{FileAttr, FileType};
use log::error;
use reqwest::{blocking::Client};
use serde::{Deserialize, Serialize};
use url::{form_urlencoded, Url};

use crate::mirror::{Mirror, MirrorDirEntry, PathResolver};

fn to_io_error(err: reqwest::Error) -> io::Error {
    error!("Web mirror error: {err}");
    io::Error::new(io::ErrorKind::Other, err)
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(unused)]
pub struct FileContentInformation {
    pub node: u64,
    pub modify_time: u64,
    pub create_time: u64,
    pub executable: bool,
    pub writable: bool,
    pub etag: String,
    pub size: u64,
    #[serde(rename = "type")]
    pub type_: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(unused)]
pub struct DirectoryContentInformation {
    pub node: u64,
    pub modify_time: u64,
    pub create_time: u64,
    pub executable: bool,
    pub writable: bool,
    pub etag: String,
    pub size: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
#[allow(unused)]
pub struct SymbolicLinkContentInformation {
    pub node: u64,
    pub modify_time: u64,
    pub create_time: u64,
    pub executable: bool,
    pub writable: bool,
    pub etag: String,
    pub target: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "PascalCase")]
pub enum ContentInformation {
    File(FileContentInformation),
    Directory(DirectoryContentInformation),
    SymbolicLink(SymbolicLinkContentInformation),
}

impl ContentInformation {
    pub fn to_file_attr(&self) -> FileAttr {
        let (kind, perms, size, node, mtime, ctime) = match self {
            ContentInformation::File(info) => {
                let mut perms = 0o444;
                if info.writable {
                    perms |= 0o200;
                }
                if info.executable {
                    perms |= 0o111;
                }
                (
                    FileType::RegularFile,
                    perms,
                    info.size,
                    info.node,
                    info.modify_time,
                    info.create_time,
                )
            }
            ContentInformation::Directory(info) => {
                let mut perms = 0o555;
                if info.writable {
                    perms |= 0o200;
                }
                if info.executable {
                    perms |= 0o111;
                }
                (
                    FileType::Directory,
                    perms,
                    info.size,
                    info.node,
                    info.modify_time,
                    info.create_time,
                )
            }
            ContentInformation::SymbolicLink(info) => {
                let perms = 0o777;
                (
                    FileType::Symlink,
                    perms,
                    info.target.len() as u64,
                    info.node,
                    info.modify_time,
                    info.create_time,
                )
            }
        };

        FileAttr {
            ino: node,
            size,
            blocks: (size + 511) / 512,
            atime: UNIX_EPOCH + std::time::Duration::from_millis(mtime),
            mtime: UNIX_EPOCH + std::time::Duration::from_millis(mtime),
            ctime: UNIX_EPOCH + std::time::Duration::from_millis(ctime),
            crtime: UNIX_EPOCH + std::time::Duration::from_millis(ctime),
            kind,
            perm: perms,
            nlink: 1,
            uid: u32::MAX, // Not provided by server
            gid: u32::MAX, // Not provided by server
            rdev: 0,
            flags: 0,
            blksize: 512,
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FileDirectoryEntry {
    pub name: String,
    pub info: ContentInformation,
}

#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct EntryAttributes {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub executable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writable: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modify_time: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_time: Option<u128>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<u64>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub type_: Option<Option<String>>, // string | null
}

#[derive(Debug)]
pub struct InvariantFilesMirror {
    base_url: Url,
    client: Client,
    ino_map: RwLock<HashMap<u64, u64>>, // local -> remote
}

impl InvariantFilesMirror {
    pub fn new(base_url: &str, root: Option<u64>) -> Self {
        let mut ino_map = HashMap::new();
        ino_map.insert(1, root.unwrap_or(1));
        Self {
            base_url: Url::parse(base_url).unwrap(),
            client: Client::new(),
            ino_map: RwLock::new(ino_map),
        }
    }

    fn get_url(&self, path: &str) -> Url {
        self.base_url.join(path).unwrap()
    }

    fn get_remote_ino(&self, local_ino: u64) -> u64 {
        *self.ino_map.read().unwrap().get(&local_ino).unwrap_or(&local_ino)
    }
}

impl Mirror for InvariantFilesMirror {
    fn read_dir<'a>(
        &self,
        ino: u64,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<Vec<MirrorDirEntry>> {
        let remote_ino = self.get_remote_ino(ino);
        let url = self.get_url(&format!("files/directory/{remote_ino}"));
        let response = self.client.get(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to read directory: {}", response.status()),
            ));
        }
        let text = response.text().map_err(to_io_error)?;
        let entries: Vec<FileDirectoryEntry> =
            serde_json::from_str(&text).map_err(|e| io::Error::new(ErrorKind::Other, e))?;

        let mirror_entries = entries
            .into_iter()
            .map(|entry| MirrorDirEntry {
                name: entry.name.into(),
                attr: entry.info.to_file_attr(),
            })
            .collect();
        Ok(mirror_entries)
    }

    fn read_link<'a>(&self, ino: u64, _path_resolver: &PathResolver<'a>) -> std::io::Result<PathBuf> {
        let remote_ino = self.get_remote_ino(ino);
        let url = self.get_url(&format!("files/info/{remote_ino}"));
        let response = self.client.get(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to read link: {}", response.status()),
            ));
        }
        let info: ContentInformation = response.json().map_err(to_io_error)?;
        if let ContentInformation::SymbolicLink(info) = info {
            Ok(PathBuf::from(info.target))
        } else {
            Err(io::Error::new(
                ErrorKind::InvalidInput,
                "Not a symbolic link",
            ))
        }
    }

    fn read<'a>(
        &self,
        ino: u64,
        data: &mut [u8],
        offset: u64,
        _path_resolver: &PathResolver<'a>
    ) -> std::io::Result<usize> {
        let remote_ino = self.get_remote_ino(ino);
        let url = self.get_url(&format!("files/{remote_ino}?offset={offset}&length={}", data.len()));
        let response = self.client.get(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to read file: {}", response.status())
            ));
        };
        let bytes = response.bytes().map_err(to_io_error)?;
        let result = bytes.len();
        data.copy_from_slice(&bytes[0..result]);
        Ok(result)
    }

    fn create_file<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        _attr: &FileAttr,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_parent = self.get_remote_ino(parent);
        let url = self.get_url(&format!(
            "files/{}/{}?kind=File",
            remote_parent,
            name.to_str().unwrap()
        ));
        let response = self.client.put(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to create file: {}", response.status()),
            ));
        }
        let info: ContentInformation = response.json().map_err(to_io_error)?;
        let remote_ino = match info {
            ContentInformation::File(f) => f.node,
            _ => {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    "Create file did not return a file",
                ))
            }
        };
        self.ino_map.write().unwrap().insert(ino, remote_ino);
        Ok(())
    }

    fn create_dir<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        _attr: &FileAttr,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_parent = self.get_remote_ino(parent);
        let url = self.get_url(&format!(
            "files/{}/{}?kind=Directory",
            remote_parent,
            name.to_str().unwrap()
        ));
        let response = self.client.put(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to create directory: {}", response.status()),
            ));
        }
        let info: ContentInformation = response.json().map_err(to_io_error)?;
        let remote_ino = match info {
            ContentInformation::Directory(d) => d.node,
            _ => {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    "Create directory did not return a directory",
                ))
            }
        };
        self.ino_map.write().unwrap().insert(ino, remote_ino);
        Ok(())
    }

    fn create_symlink<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        target: &Path,
        _attr: &FileAttr,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_parent = self.get_remote_ino(parent);
        let encoded_target =
            form_urlencoded::byte_serialize(target.to_str().unwrap().as_bytes()).collect::<String>();
        let url = self.get_url(&format!(
            "files/{}/{}?kind=SymbolicLink&target={}",
            remote_parent,
            name.to_str().unwrap(),
            encoded_target
        ));
        let response = self.client.put(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to create symlink: {}", response.status()),
            ));
        }
        let info: ContentInformation = response.json().map_err(to_io_error)?;
        let remote_ino = match info {
            ContentInformation::SymbolicLink(s) => s.node,
            _ => {
                return Err(io::Error::new(
                    ErrorKind::Other,
                    "Create symlink did not return a symlink",
                ))
            }
        };
        self.ino_map.write().unwrap().insert(ino, remote_ino);
        Ok(())
    }

    fn write<'a>(
        &self,
        ino: u64,
        data: &[u8],
        offset: u64,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_ino = self.get_remote_ino(ino);
        let url = self.get_url(&format!("files/{remote_ino}?offset={offset}"));
        let response = self
            .client
            .post(url)
            .body(data.to_vec())
            .send()
            .map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to write to file: {}", response.status()),
            ));
        }
        Ok(())
    }

    fn set_attr<'a>(
        &self,
        ino: u64,
        attr: &FileAttr,
        size: Option<u64>,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_ino = self.get_remote_ino(ino);
        let url = self.get_url(&format!("files/attributes/{remote_ino}"));
        let mut entry_attributes = EntryAttributes::default();
        entry_attributes.executable = Some((attr.perm & 0o100) != 0);
        entry_attributes.writable = Some((attr.perm & 0o200) != 0);
        entry_attributes.size = size;
        entry_attributes.modify_time =
            Some(attr.mtime.duration_since(UNIX_EPOCH).unwrap().as_millis());
        entry_attributes.create_time =
            Some(attr.crtime.duration_since(UNIX_EPOCH).unwrap().as_millis());

        let response = self
            .client
            .post(url)
            .json(&entry_attributes)
            .send()
            .map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to set attributes: {}", response.status()),
            ));
        }
        Ok(())
    }

    fn delete<'a>(
        &self,
        ino: u64,
        parent: u64,
        name: &OsString,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_parent = self.get_remote_ino(parent);
        let url = self.get_url(&format!(
            "files/remove/{}/{}",
            remote_parent,
            name.to_str().unwrap()
        ));
        let response = self.client.post(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to delete entry: {}", response.status()),
            ));
        }
        self.ino_map.write().unwrap().remove(&ino);
        Ok(())
    }

    fn rename<'a>(
        &self,
        ino: u64,
        _parent: u64,
        _name: &OsString,
        new_parent: u64,
        new_name: &OsString,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_ino = self.get_remote_ino(ino);
        let remote_new_parent = self.get_remote_ino(new_parent);
        let url = self.get_url(&format!(
            "files/rename/{}?newParent={}&newName={}",
            remote_ino,
            remote_new_parent,
            new_name.to_str().unwrap()
        ));
        let response = self.client.put(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to rename entry: {}", response.status()),
            ));
        }
        Ok(())
    }

    fn link<'a>(
        &self,
        ino: u64,
        new_parent: u64,
        new_name: &OsString,
        _path_resolver: &PathResolver<'a>,
    ) -> std::io::Result<()> {
        let remote_ino = self.get_remote_ino(ino);
        let remote_new_parent = self.get_remote_ino(new_parent);
        let url = self.get_url(&format!(
            "files/link/{}/{}?node={}",
            remote_new_parent,
            new_name.to_str().unwrap(),
            remote_ino
        ));
        let response = self.client.put(url).send().map_err(to_io_error)?;
        if !response.status().is_success() {
            return Err(io::Error::new(
                ErrorKind::Other,
                format!("Failed to link entry: {}", response.status()),
            ));
        }
        Ok(())
    }

    fn set_inode_map(&self, ino_map: &HashMap<u64, u64>) -> std::io::Result<()> {
        let mut own_map = self.ino_map.write().unwrap();
        for (local_ino, remote_ino) in ino_map {
            own_map.insert(*local_ino, *remote_ino);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::invariant_files_mirror::{ContentInformation, FileDirectoryEntry};

    #[test]
    fn test_entries_deserialization() -> std::io::Result<()> {
        let data = "[{\"name\":\".gitignore\",\"info\":{\"node\":2,\"kind\":\"File\",\"modifyTime\":1721419791909,\"createTime\":1721419791909,\"executable\":false,\"writable\":false,\"etag\":\"aae8e9997fb040fb78109337af8a9ee31d6649d7280a9f7fa61e3bcb7854709f\",\"size\":34}}]";
        let entries:  Vec<FileDirectoryEntry>  = serde_json::from_str(data)?;
        let entry = &entries[0];
        assert_eq!(entry.name, ".gitignore");
        if let ContentInformation::File(info) = &entry.info {
            assert_eq!(info.size, 34);
        }
        Ok(())
    }
}