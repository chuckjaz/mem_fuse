# mem-fuse

An in-memory FUSE filesystem written in Rust.

## Description

`mem-fuse` is a simple in-memory filesystem that implements a subset of the FUSE API. It is intended as a base-line implementation using the `fuser` crate to implement a FUSE filesystem to determine overhead of FUSE and Rust in building a FUSE.

The filesystem supports creating files, directories, and symbolic links. It also supports reading and writing to files, as well as reading directory contents.

## Building and Running

To build the project, run:

```bash
cargo build
```

To run the filesystem, you need to specify a mount point. For example:

```bash
cargo run -- /tmp/mem-fuse
```

This will mount the in-memory filesystem at `/tmp/mem-fuse`. You can then interact with it like any other filesystem.

## Modules

*   `main.rs`: This is the main entry point for the application. It parses command-line arguments and starts the FUSE filesystem.
*   `mem_fuse.rs`: This module contains the implementation of the `fuser::Filesystem` trait. It implements the logic for handling FUSE operations such as `lookup`, `getattr`, `mknod`, `mkdir`, `read`, `write`, etc.
