use clap::Parser;
use env_logger::Env;
use std::error::Error;
use std::{ffi::OsString, path::Path};
use std::result::Result;
mod mem_fuse;

#[derive(Debug, Parser)]
#[command(name = "inv-fuse")]
#[command(author = "Chuck Jazdzewski (chuckjaz@gmail.com)")]
#[command(version = "0.1.0")]
#[command(about = "An in-memory FUSE")]
pub struct FuseCommand {
    /// The location to mount the directory
    path: OsString,
}

fn main() -> Result<(), Box<dyn Error + Sync + Send>> {
    let env = Env::new().filter("MEM_LOG");
    env_logger::try_init_from_env(env)?;
    let config = FuseCommand::parse();
    let path_text = config.path;

    println!("Starting FUSE: path: {path_text:?}");
    let path = Path::new(&path_text);
    start_fuse(path)?;

    Ok(())
}

fn start_fuse(path: &Path) -> Result<(), Box<dyn Error + Sync + Send>>{
    let filesystem = mem_fuse::MemoryFuse::new();
    fuser::mount2(filesystem, path, &[])?;
    Ok(())
}
