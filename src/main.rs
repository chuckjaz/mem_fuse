use clap::Parser;
use env_logger::Env;
use log::info;
use std::error::Error;
use std::{ffi::OsString, path::{Path, PathBuf}};
use std::result::Result;
mod disk_image;
mod mem_fuse;
mod node;

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
}

fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let env = Env::new().filter_or("MEM_LOG", "info");
    env_logger::try_init_from_env(env)?;
    let config = FuseCommand::parse();
    let path_text = config.path;

    info!("Starting FUSE: path: {path_text:?}");
    let path = Path::new(&path_text);
    start_fuse(path, config.disk_image_path)?;

    Ok(())
}

fn start_fuse(path: &Path, disk_image_path: Option<PathBuf>) -> Result<(), Box<dyn Error + Sync + Send>>{
    let filesystem = mem_fuse::MemoryFuse::new(disk_image_path);
    fuser::mount2(filesystem, path, &[])?;
    Ok(())
}
