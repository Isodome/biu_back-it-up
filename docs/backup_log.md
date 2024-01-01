# Backup Log

The backup log lists all files in the backup along with their sizes, mtimes and xxh3 hashes. It has multiple use cases.
1. Skip stat'ing the backup for incremental backups.
2. Allow bit rot detection via files hashes.
3. Allow for fast deduplication without scanning the file tree and computing hashes.
4. Document which files were modified, added and deleted for regular backup use cases.

Linux file names are arbitrary bytes sequences. The backup log is designed such that it supports all possible file names by storing the byte length of the path. In the (most common) case that all file names are valid UTF8 the backup log will be a regular csv file with semicolon as separator. Semicolons in the file path are not escaped.

The backup log entries are sorted by path. One could find good arguments for sorting by hash instead.

* Makes deduplication fast and memory extremley efficient.

Nevertheless I decided to keep sorting by path:
* It's easy to output the log sorted by path in a single pass. 
* It allows for efficient incremental backups without the need to read the backup file tree itself.



