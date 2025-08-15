use clap::Parser;
use env_logger::Env;
use log::info;
use std::error::Error;
use std::{ffi::OsString, path::{Path, PathBuf}};
use std::result::Result;
mod disk_image;
mod lru_cache;
mod mem_fuse;
mod node;
mod dirty;

#[derive(Debug, Parser)]
#[command(name = "inv-fuse")]
#[command(author = "Chuck Jazdzewski (chuckjaz@gmail.com)")]
#[command(version = "0.1.0")]
#[command(about = "An in-memory FUSE")]
pub struct FuseCommand {
    /// The location to mount the directory
    path: OsString,
    /// The location to save the image to
    #[arg(short, long)]
    disk_image_path: Option<PathBuf>,
    /// The size of the lru cache in Mb
    #[arg(long, default_value_t = 500)]
    cache_size: u64,
    /// Load files from the disk image lazily
    #[arg(long, default_value_t = false)]
    lazy_load: bool,
}

fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let env = Env::new().filter_or("MEM_LOG", "info");
    env_logger::try_init_from_env(env)?;
    let config = FuseCommand::parse();
    let path_text = config.path;

    info!("Starting FUSE: path: {path_text:?}");
    let path = Path::new(&path_text);
    start_fuse(
        path,
        config.disk_image_path,
        config.cache_size * 1024 * 1024,
        config.lazy_load,
    )?;

    Ok(())
}

fn start_fuse(
    path: &Path,
    disk_image_path: Option<PathBuf>,
    cache_size: u64,
    lazy_load: bool,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let filesystem = mem_fuse::MemoryFuse::new(disk_image_path, cache_size, lazy_load);
    fuser::mount2(filesystem, path, &[])?;
    Ok(())
}
