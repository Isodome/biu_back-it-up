use super::{BackupLogPath, BackupStats};
use crate::utils::{HybridFileParser, PeekableFile};
use std::{
    fs::File,
    io::{self, empty, BufWriter, Write},
    path::{Path, PathBuf},
};

#[derive(Debug, PartialEq, Clone, Hash)]
pub struct BackupFileStats {
    pub path: BackupLogPath,
    pub xxh3: u64,
    pub mtime: i64,
    pub size: u64,
}

#[derive(Debug, PartialEq)]
pub struct DeleteData {
    pub path: BackupLogPath,
    pub size: u64,
}

#[derive(Debug, PartialEq)]
pub enum LogEntry {
    Write(BackupFileStats),
    Link(BackupFileStats),
    Delete(DeleteData),
}

const DELIMITER: u8 = b';';
//
// * w: the file was changed and written
// * h: the file was not changed and we wrote a hardlink
// * s: the file is a symlink and was

#[derive(Debug)]
pub struct BackupLog {
    backup_dir: PathBuf,
}

impl BackupLog {
    pub fn create(backup_dir: &Path) -> BackupLog {
        return BackupLog {
            backup_dir: backup_dir.to_path_buf(),
        };
    }

    pub fn iter(&self) -> Result<BackupLogIterator, String> {
        let file = File::open(&self.backup_dir.join("backup.log"))
            .map_err(|e| format!("Failed to open backup log: {}", e.to_string()))?;

        return Ok(BackupLogIterator::new(file));
    }
}

pub struct BackupLogIterator {
    reader: HybridFileParser<Box<dyn PeekableFile>>,
    /// The path where the backup log file is located.
    lines_read: i32,
}

impl BackupLogIterator {
    fn new(file: File) -> BackupLogIterator {
        return BackupLogIterator {
            reader: HybridFileParser::new(Box::from(file)),
            lines_read: 0,
        };
    }
    pub fn empty() -> BackupLogIterator {
        return BackupLogIterator {
            reader: HybridFileParser::new(Box::from(empty())),
            lines_read: 0,
        };
    }

    fn parse_item(&mut self) -> io::Result<LogEntry> {
        self.lines_read += 1;
        let op = self.reader.read_string(10, DELIMITER)?;
        let hash = self.reader.read_hex_u64(DELIMITER)?;
        let mtime = self.reader.read_i64(DELIMITER)?;
        let size = self.reader.read_u64(DELIMITER)?;
        let path_length = self.reader.read_u64(DELIMITER)?;
        let path = self.reader.read_path(path_length as usize)?;

        self.reader.skip_bytes(1)?; // Skipping the newline. We ignore errors here.
        return match op.as_str() {
            "w" => Ok(LogEntry::Write(BackupFileStats {
                path: path.into(),
                xxh3: hash,
                mtime: mtime,
                size: size,
            })),
            "l" => Ok(LogEntry::Link(BackupFileStats {
                path: path.into(),
                xxh3: hash,
                mtime: mtime,
                size: size,
            })),
            "d" => Ok(LogEntry::Delete(DeleteData {
                path: path.into(),
                size: size,
            })),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "Backup log is unreadable.",
            )),
        };
    }
}

impl Iterator for BackupLogIterator {
    type Item = io::Result<LogEntry>;
    fn next(&mut self) -> Option<Self::Item> {
        if self.reader.is_depleted() {
            return None;
        }
        return Some(self.parse_item());
    }
}

pub struct AllFilesLogIterator {
    inner: BackupLogIterator,
}
impl AllFilesLogIterator {
    pub fn new(inner: BackupLogIterator) -> AllFilesLogIterator {
        return AllFilesLogIterator { inner };
    }
}

/// A log iterator that will only read those log entries that point to file that exists in a backup.
impl Iterator for AllFilesLogIterator {
    type Item = io::Result<BackupFileStats>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let log_entry = match self.inner.next() {
                Some(e) => e,
                None => return None,
            };
            let entry = match log_entry {
                Ok(entry) => entry,
                Err(e) => return Some(Err(e)),
            };
            match entry {
                LogEntry::Write(wd) => return Some(Ok(wd)),
                LogEntry::Link(wd) => return Some(Ok(wd)),
                _ => (),
            }
        }
    }
}

pub struct NewFilesLogIterator {
    inner: BackupLogIterator,
}
impl From<BackupLogIterator> for NewFilesLogIterator {
    fn from(inner: BackupLogIterator) -> Self {
        return NewFilesLogIterator { inner };
    }
}

/// A log iterator that will only read those log entries that point to file that exists in a backup.
impl Iterator for NewFilesLogIterator {
    type Item = io::Result<BackupFileStats>;
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let log_entry = match self.inner.next() {
                Some(e) => e,
                None => return None,
            };
            let entry = match log_entry {
                Ok(entry) => entry,
                Err(e) => return Some(Err(e)),
            };
            match entry {
                LogEntry::Write(wd) => return Some(Ok(wd)),
                _ => (),
            }
        }
    }
}

pub struct BackupLogWriter {
    writer: BufWriter<File>,
    stats: BackupStats,
}

impl BackupLogWriter {
    pub fn new(log_path: &Path) -> io::Result<BackupLogWriter> {
        return Ok(BackupLogWriter {
            writer: BufWriter::new(File::create(log_path)?),
            stats: BackupStats::new(),
        });
    }

    pub fn finalize(mut self) -> BackupStats {
        self.stats.report_done();
        return self.stats;
    }

    pub fn writeline(
        &mut self,
        operation: &str,
        path: &BackupLogPath,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        write!(
            self.writer,
            "{};{:x};{};{};{};",
            operation,
            hash,
            mtime,
            size,
            path.bytes_len()
        )?;
        self.writer.write_all(path.as_bytes())?;

        self.writer.write_all(&[b'\n'])?;
        Ok(())
    }

    pub fn report_write(
        &mut self,
        path: &BackupLogPath,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        self.stats.report_write(size, mtime);
        self.writeline("w", path, hash, mtime, size)
    }

    pub fn report_hardlink(
        &mut self,
        path: &BackupLogPath,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        self.stats.report_link(mtime);
        self.writeline("l", path, hash, mtime, size)
    }

    pub fn report_delete(&mut self, path: &BackupLogPath, size: u64) -> io::Result<()> {
        self.stats.report_delete(size);
        self.writeline("d", path, 0, 0, 0)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{ffi::OsString, io::Seek, os::unix::ffi::OsStringExt, str::FromStr};

    fn write_test_file(bytes: &[u8]) -> io::Result<File> {
        let mut f = tempfile::tempfile().unwrap();
        f.write(&bytes)?;
        f.flush()?;
        f.seek(io::SeekFrom::Start(0)).unwrap();
        return Ok(f);
    }

    #[test]
    fn parseable_lines() -> io::Result<()> {
        let file = write_test_file(
            [
                "w;0394b8fafef76701;1234;56788;15;Downloads/1.mp3",
                "d;0;0;0;15;Downloads/2.mp3",
            ]
            .join("\n")
            .as_bytes(),
        )?;
        let mut it = BackupLogIterator::new(file);
        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Write(BackupFileStats {
                size: 56788,
                xxh3: 258034466825922305,
                mtime: 1234,
                path: PathBuf::from_str("Downloads/1.mp3").unwrap().into(),
            })
        );

        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Delete(DeleteData {
                path: PathBuf::from_str("Downloads/2.mp3").unwrap().into(),
                size: 0
            })
        );
        Ok(())
    }

    #[test]
    fn logwriter() -> io::Result<()> {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let mut w = BackupLogWriter::new(tmpfile.path()).unwrap();

        assert!(w
            .report_write(
                &BackupLogPath::from(PathBuf::from("Documents/foo.txt")),
                123,
                456,
                789
            )
            .is_ok());
        assert!(w
            .report_hardlink(
                &BackupLogPath::from(PathBuf::from("Documents/foo2.txt")),
                234,
                567,
                890
            )
            .is_ok());
        assert!(w
            .report_delete(
                &BackupLogPath::from(PathBuf::from("Documents/foo3.txt")),
                10
            )
            .is_ok());

        drop(w);
        let mut it = BackupLogIterator::new(tmpfile.reopen().unwrap());

        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Write(BackupFileStats {
                size: 789,
                xxh3: 123,
                mtime: 456,
                path: PathBuf::from_str("Documents/foo.txt").unwrap().into(),
            })
        );

        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Write(BackupFileStats {
                size: 890,
                xxh3: 234,
                mtime: 567,
                path: PathBuf::from_str("Documents/foo2.txt").unwrap().into(),
            })
        );

        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Delete(DeleteData {
                path: PathBuf::from_str("Documents/foo3.txt").unwrap().into(),
                size: 10
            })
        );
        assert!(it.next().is_none());
        Ok(())
    }

    #[test]
    fn path_not_utf8() -> io::Result<()> {
        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let mut w = BackupLogWriter::new(tmpfile.path()).unwrap();

        // "foo<BEL>"
        let non_utf8_path = PathBuf::from(OsString::from_vec(vec![102, 111, 111, 7]));
        assert!(w
            .report_write(&BackupLogPath::from(non_utf8_path.clone()), 123, 456, 789)
            .is_ok());

        // Path with separator char
        assert!(w
            .report_write(
                &&BackupLogPath::from(PathBuf::from("Documents/@;54;.foo")),
                123,
                456,
                789
            )
            .is_ok());
        drop(w);

        let mut it = BackupLogIterator::new(tmpfile.reopen().unwrap());
        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Write(BackupFileStats {
                size: 789,
                xxh3: 123,
                mtime: 456,
                path: non_utf8_path.into(),
            })
        );

        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Write(BackupFileStats {
                size: 789,
                xxh3: 123,
                mtime: 456,
                path: PathBuf::from_str("Documents/@;54;.foo").unwrap().into(),
            })
        );

        assert!(it.next().is_none());
        Ok(())
    }
}
