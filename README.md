# mem-fuse

An in-memory FUSE filesystem written in Rust.

## Description

`mem-fuse` is an in-memory filesystem that implements a subset of the FUSE API that can act as a in-memory cache of a different file system. These other file systems are mirrored, by mirroring all changes in the in-memory file system to the mirrored filesystem.

The filesystem can be backed by a "mirror". A mirror is a secondary storage layer that `mem-fuse` will keep in sync with its in-memory state. There are two types of mirrors available:

*   **LocalMirror**: This mirror persists the filesystem to a local directory on disk. This is useful for saving the state of the filesystem across mounts.
*   **InvariantFilesMirror**: This mirror connects to a remote file server over HTTP, allowing `mem-fuse` to act as a local cache for a remote filesystem.

## Features

*   **Mirroring**: The filesystem can persist its state to a mirror. This is controlled by the `--mirror` command-line argument. A background worker thread handles writing changes to the mirror to avoid blocking FUSE operations.
    *   `--mirror file:///path/to/mirror` or `--mirror /path/to/mirror`: Use a `LocalMirror` to persist to a local directory.
    *   `--mirror http://<server-address>`: Use an `InvariantFilesMirror` to connect to a remote file server.
*   **LRU Cache**: The filesystem uses an LRU cache to manage file content in memory.
    *   `--cache-size <size_in_mb>`: Specifies the total size of the cache in megabytes.
    *   `--cache-max-write-size <size_in_mb>`: Specifies the maximum amount of memory that can be occupied by "dirty" (modified) files. This prevents the cache from being filled with modified data that cannot be evicted, which could lead to deadlocks. When the dirty file size exceeds this limit, write operations will block until some data is written to the mirror and marked as "clean".
*   **Lazy Loading**: The `--lazy-load` flag controls how files are loaded from the mirror. When enabled (the default), file content is loaded on-demand when the file is first accessed.

## Building and Running

To build the project, run:

```bash
cargo build --release
```

To run the filesystem, you need to specify a mount point.

**Example with `LocalMirror`:**

To mount the filesystem at `/tmp/mem-fuse` with a 1GB cache, a 512MB write cache, and a local mirror at `/tmp/mem-fuse-mirror`:

```bash
cargo run -- /tmp/mem-fuse --mirror /tmp/mem-fuse-mirror --cache-size 1024 --cache-max-write-size 512
```

**Example with `InvariantFilesMirror`:**

To mount the filesystem at `/tmp/mem-fuse` and connect to a remote server:

```bash
cargo run -- /tmp/mem-fuse --mirror http://127.0.0.1:8080 --cache-size 1024 --cache-max-write-size 512
```

This will mount the in-memory filesystem at `/tmp/mem-fuse`. You can then interact with it like any other filesystem.

## Modules

*   `main.rs`: This is the main entry point for the application. It parses command-line arguments and starts the FUSE filesystem.
*   `mem_fuse.rs`: This module contains the implementation of the `fuser::Filesystem` trait. It implements the logic for handling FUSE operations such as `lookup`, `getattr`, `mknod`, `mkdir`, `read`, `write`, etc.
*   `node.rs`: This module defines the `Node` struct, which represents a file or directory in the filesystem.
*   `lru_cache.rs`: This module implements the LRU cache for file content. It manages the cache size and eviction policy.
*   `dirty.rs`: This module tracks dirty regions in files, allowing the filesystem to write only the modified parts of a file to the mirror.
*   `mirror.rs`: Defines the `Mirror` trait, which abstracts the backend storage, and includes the `LocalMirror` implementation.
*   `mirror_factory.rs`: A factory for creating `Mirror` instances based on the command-line arguments.
*   `invariant_files_mirror.rs`: Implements the `Mirror` trait for a remote file server, acting as an HTTP client.
