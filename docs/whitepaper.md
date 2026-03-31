# Biu: A Low-Resource, Transparent Backup Architecture

## Abstract

Modern backup solutions often prioritize absolute storage efficiency through complex block-level deduplication and custom storage formats. This approach, while space-efficient, demands significant CPU and RAM overhead and locks the user's data behind proprietary software. This architecture outlines a "Time Machine-style" backup system designed specifically for low-powered hardware. It prioritizes transparent restoration, strictly bounded memory usage, robust data integrity, and complete freedom from vendor lock-in.

---

## 1. Core Design Goals

The architecture is driven by five foundational constraints:

*   **Hardware Accessibility:** The software must operate efficiently on low-powered machines. Memory usage is strictly bound to O(K), where K is the maximum number of files in any single directory, completely avoiding the need to load full filesystem trees or global hash tables into RAM.
*   **Transparent Restoration:** A backup is only as useful as its accessibility during a crisis. Backups must be natively browsable via standard file explorers. Restoring a file must require zero specialized software—only standard operating system copy commands.
*   **Verifiable Data Integrity:** The system must actively detect silent data corruption (bit rot) on the backup media without relying on advanced filesystem features like [ZFS or Btrfs checksumming](https://en.wikipedia.org/wiki/ZFS#Data_integrity).
*   **Human-Readable Metadata:** The backup log and any associated metadata must be trivial to read and parse by humans and standard command-line tools (e.g., `cat`, `grep`, `awk`) without requiring the backup software itself. 
*   **Zero Vendor Lock-In:** It must be trivial to stop using the backup tool at any time. Because data is stored in standard directory structures and metadata is kept in plain text, no data is ever trapped in proprietary archives or specialized formats.

---

## 2. Architectural Decisions

### Time Machine-Style Hard Linking

To achieve transparent restoration and zero vendor lock-in, the system leverages standard [POSIX hard links](https://pubs.opengroup.org/onlinepubs/9699919799/functions/link.html). Each backup epoch presents as a complete, independent directory tree. Unmodified files are hard-linked to the physical inode of the previous epoch. This shifts the deduplication logic to the file-level, sacrificing intra-file efficiency (e.g., for appended logs or databases) to maintain a zero-friction restore process perfectly suited for WORM (Write Once, Read Many) data like photo and video collections.

### The Streaming Traversal Model

To maintain the O(K) memory constraint, the architecture rejects a central, in-memory database of hashes. Instead, it utilizes a synchronous, streaming comparison. By reading a path-sorted log from the previous epoch in lockstep with a Depth-First Search (DFS) of the live filesystem, the system can identify new, modified, and deleted files sequentially.

---

## 3. The Backup Log Specification

The backup log serves as the system's memory, manifest, and integrity ledger. It is stored directly alongside each backup epoch.

### Data Structure

*   **Hashing Algorithm:** The system utilizes [xxHash3 (xxh3)](https://github.com/Cyan4973/xxHash) to generate file fingerprints. xxh3 provides memory-bandwidth-saturating speeds on low-power CPUs, minimizing the bottleneck of integrity hashing.
*   **File Format:** In standard environments, the log is a semi-colon separated CSV file containing the file's `path`, `mtime`, `size`, and `xxh3 hash`. This fulfills the requirement for human-readable metadata.
*   **Arbitrary Byte Support:** Because standard Linux filesystems treat filenames as arbitrary byte sequences (excluding `\0` and `/`), the log natively stores the byte length of the path. This guarantees support for non-UTF8 filenames and paths containing semicolons without requiring complex escaping logic.

### The Case for Path-Sorting

A standard approach to deduplication would sort the log by hash to enable fast O(1) lookups for identical files. However, this architecture deliberately sorts the log strictly by file path to enable the streaming traversal model.

This design choice provides two critical advantages:

1.  **Single-Pass Output:** A path-sorted log naturally aligns with a recursive directory traversal. The software can continuously stream the log to disk as it processes the filesystem, keeping memory usage flat.
2.  **Zero-Stat Incremental Backups:** By keeping both the live filesystem traversal and the previous backup log sorted alphabetically, the software can execute a lockstep comparison. If the live path matches the log path, and the `mtime` and `size` match, the system skips reading the live file and entirely avoids executing expensive `stat()` calls against the backup drive's filesystem.
