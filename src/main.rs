use clap::Parser;
use env_logger::Env;
use log::info;
use std::error::Error;
use std::sync::Arc;
use std::{ffi::OsString, path::Path};
use std::result::Result;
use mem_fuse::MemoryFuse;
mod lru_cache;
mod mem_fuse;
mod node;
mod dirty;
mod mirror;
mod mirror_factory;
mod invariant_files_mirror;

#[derive(Debug, Parser)]
#[command(name = "mem_fuse")]
#[command(author = "Chuck Jazdzewski (chuckjaz@gmail.com)")]
#[command(version = "0.1.0")]
#[command(about = "An in-memory FUSE")]
pub struct FuseCommand {
    /// The location to mount the directory
    path: OsString,
    /// The location of the mirror
    mirror: Option<String>,
    /// The size of the lru cache in Mb
    #[arg(long, default_value_t = 500)]
    cache_size: u64,
    /// Load files from the mirror lazily
    #[arg(long, default_value_t = true)]
    lazy_load: bool,
    #[arg(short, long)]
    /// The root node of the invariant files server, ignored otherwise
    root: Option<u64>,
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
        config.mirror,
        config.cache_size * 1024 * 1024,
        config.lazy_load,
        config.root,
    )?;

    Ok(())
}

use crate::mirror::Mirror;
use crate::mirror_factory::create_mirror;

fn start_fuse(
    path: &Path,
    mirror: Option<String>,
    cache_size: u64,
    lazy_load: bool,
    root: Option<u64>,
) -> Result<(), Box<dyn Error + Sync + Send>> {
    let mirror: Option<Arc<dyn Mirror + Send + Sync>> =
        mirror.map(|mirror_str| create_mirror(&mirror_str, root));
    let filesystem = MemoryFuse::new(mirror, cache_size, lazy_load);
    fuser::mount2(filesystem, path, &[])?;
    Ok(())
}
