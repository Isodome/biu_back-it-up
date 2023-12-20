use super::BackupLogPath;
use crate::utils::PeekableReader;
use std::{
    fs::File,
    io::{self, empty, BufRead, BufReader, BufWriter, Read, Write},
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
};

#[derive(Debug, PartialEq, Clone)]
pub struct BackupFileStats {
    pub path: BackupLogPath,
    pub xxh3: u64,
    pub mtime: i64,
    pub size: u64,
}

#[derive(Debug, PartialEq)]
pub struct DeleteData {
    pub path: BackupLogPath,
}

#[derive(Debug, PartialEq)]
pub enum LogEntry {
    Write(BackupFileStats),
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
    reader: PeekableReader<Box<dyn Read>>,
    /// The path where the backup log file is located.
    lines_read: i32,
}

impl BackupLogIterator {
    fn new(file: File) -> BackupLogIterator {
        return BackupLogIterator {
            reader: PeekableReader::new(Box::from(file)),
            lines_read: 0,
        };
    }
    pub fn empty() -> BackupLogIterator {
        return BackupLogIterator {
            reader: PeekableReader::new(Box::from(empty())),
            lines_read: 0,
        };
    }

    fn skip_a_byte(&mut self) {
        let mut dummy = [0u8; 1];
        let _ = self.reader.read_exact(dummy.as_mut_slice());
    }

    fn parse_item(&mut self) -> io::Result<LogEntry> {
        self.lines_read += 1;
        let op = self.reader.read_string(10, DELIMITER)?;
        let hash = self.reader.read_hex_u64(DELIMITER)?;
        let mtime = self.reader.read_i64(DELIMITER)?;
        let size = self.reader.read_u64(DELIMITER)?;
        let path_length = self.reader.read_u64(DELIMITER)?;
        let path = self.reader.read_path(path_length as usize)?;

        self.skip_a_byte(); // Skipping the newline. We ignore errors here.
        return match op.as_str() {
            "w" | "l" => Ok(LogEntry::Write(BackupFileStats {
                path: path.into(),
                xxh3: hash,
                mtime: mtime,
                size: size,
            })),
            "d" => Ok(LogEntry::Delete(DeleteData { path: path.into() })),
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

pub struct BackupFilesLogIterator {
    inner: BackupLogIterator,
}
impl BackupFilesLogIterator {
    pub fn new(inner: BackupLogIterator) -> BackupFilesLogIterator {
        return BackupFilesLogIterator { inner };
    }
}

/// A log iterator that will only read those log entries that point to file that exists in a backup.
impl Iterator for BackupFilesLogIterator {
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

    // Stats
    num_writes: i32,
    num_hardlinks: i32,
    num_deletes: i32,
    bytes_written: u64,
}

impl BackupLogWriter {
    pub fn new(path: &Path) -> io::Result<BackupLogWriter> {
        return Ok(BackupLogWriter {
            writer: BufWriter::new(File::create(path)?),
            num_writes: 0,
            num_hardlinks: 0,
            num_deletes: 0,
            bytes_written: 0,
        });
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

        // Since a linux path can contain any bytes except a null byte we use that to end the line.
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
        self.num_writes += 1;
        self.bytes_written += size;
        self.writeline("w", path, hash, mtime, size)
    }

    pub fn report_hardlink(
        &mut self,
        path: &BackupLogPath,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        self.num_hardlinks += 1;
        self.writeline("l", path, hash, mtime, size)
    }

    pub fn report_delete(&mut self, path: &BackupLogPath) -> io::Result<()> {
        self.num_deletes += 1;
        self.writeline("d", path, 0, 0, 0)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::{io::Seek, str::FromStr, ffi::OsString};

    fn write_test_file(bytes: &[u8]) -> io::Result<File> {
        let mut f = tempfile::tempfile().unwrap();
        f.write(&bytes)?;
        f.flush()?;
        f.seek(io::SeekFrom::Start(0)).unwrap();
        return Ok(f);
    }

    #[test]
    fn unparseable_missing_simicolon() -> io::Result<()> {
        // let file = write_test_file(b"some random word")?;
        // let mut it = BackupLogIterator::new(file);
        // assert_eq!(
        //     it.parse_item()?,
        //     LogEntry::Unparseable {
        //         0: "Didn't find expected delimiter.".to_owned()
        //     }
        // );
        // assert_eq!(
        //     parse_row("-;;my/path".to_owned()),
        //     LogEntry::Unparseable {
        //         0: "-;;my/path".to_owned()
        //     }
        // );
        // assert_eq!(
        //     parse_row("-;;my/path".to_owned()),
        //     LogEntry::Unparseable {
        //         0: "-;;my/path".to_owned()
        //     }
        // );
        Ok(())
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
                123,
                456,
                789
            )
            .is_ok());
        assert!(w
            .report_delete(&BackupLogPath::from(PathBuf::from("Documents/foo3.txt")))
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
                size: 789,
                xxh3: 123,
                mtime: 456,
                path: PathBuf::from_str("Documents/foo2.txt").unwrap().into(),
            })
        );

        assert_eq!(
            it.next().unwrap()?,
            LogEntry::Delete(DeleteData {
                path: PathBuf::from_str("Documents/foo3.txt").unwrap().into(),
            })
        );
        assert!(it.next().is_none());
        Ok(())
    }

    #[test]
    fn path_not_utf8() -> io::Result<()> {
        // "foo<BEL>"

        let tmpfile = tempfile::NamedTempFile::new().unwrap();
        let mut w = BackupLogWriter::new(tmpfile.path()).unwrap();

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
