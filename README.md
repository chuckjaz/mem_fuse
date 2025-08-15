# mem-fuse

An in-memory FUSE filesystem written in Rust.

## Description

`mem-fuse` is a simple in-memory filesystem that implements a subset of the FUSE API. It is intended as a base-line implementation using the `fuser` crate to implement a FUSE filesystem to determine overhead of FUSE and Rust in building a FUSE.

The filesystem supports creating files, directories, and symbolic links. It also supports reading and writing to files, as well as reading directory contents.

## Features

*   **Disk Image Persistence**: The filesystem can persist its state to a disk image. This is controlled by the `--disk-image-path` command-line argument. A background worker thread handles writing changes to the disk to avoid blocking FUSE operations.
*   **LRU Cache**: The filesystem uses an LRU cache to manage file content in memory. This is controlled by the `--cache-size` command-line argument, which specifies the cache size in megabytes. The cache evicts the least recently used files when the cache is full, but it avoids evicting files with unsaved changes.

## Building and Running

To build the project, run:

```bash
cargo build
```

To run the filesystem, you need to specify a mount point. For example, to mount the filesystem at `/tmp/mem-fuse` with a 1GB cache and a disk image at `/tmp/mem-fuse.img`:

```bash
cargo run -- /tmp/mem-fuse --disk-image-path /tmp/mem-fuse.img --cache-size 1024
```

This will mount the in-memory filesystem at `/tmp/mem-fuse`. You can then interact with it like any other filesystem.

## Modules

*   `main.rs`: This is the main entry point for the application. It parses command-line arguments and starts the FUSE filesystem.
*   `mem_fuse.rs`: This module contains the implementation of the `fuser::Filesystem` trait. It implements the logic for handling FUSE operations such as `lookup`, `getattr`, `mknod`, `mkdir`, `read`, `write`, etc.
*   `node.rs`: This module defines the `Node` struct, which represents a file or directory in the filesystem.
*   `disk_image.rs`: This module implements the disk image persistence feature.
*   `lru_cache.rs`: This module implements the LRU cache for file content.
*   `dirty.rs`: This module tracks dirty regions in files.
