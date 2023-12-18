use std::{
    ffi::OsString,
    fs::File,
    io::{self, empty, BufRead, BufReader, BufWriter, Read, Write},
    os::unix::ffi::OsStringExt,
    path::{Path, PathBuf},
};

use super::BackupLogPath;

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
// Backup log first letter
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
    reader: BufReader<Box<dyn Read>>,
    /// The path where the backup log file is located.
    lines_read: i32,
}

impl BackupLogIterator {
    fn new(file: File) -> BackupLogIterator {
        return BackupLogIterator {
            reader: BufReader::new(Box::from(file)),
            lines_read: 0,
        };
    }
    pub fn empty() -> BackupLogIterator {
        return BackupLogIterator {
            reader: BufReader::new(Box::from(empty())),
            lines_read: 0,
        };
    }

    fn read_until_limited(&mut self, limit: usize) -> io::Result<Vec<u8>> {
        let mut res = vec![];
        loop {
            let buf = self.reader.fill_buf()?;
            if buf.is_empty() {
                break;
            }
            let bytes_read = buf.len();
            for (i, next_byte) in buf.iter().enumerate() {
                if *next_byte == DELIMITER {
                    self.reader.consume(i + 1);
                    return Ok(res);
                }

                res.push(*next_byte);
                if res.len() >= limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "Didn't find expected delimiter in line {}.",
                            self.lines_read
                        ),
                    ));
                }
            }
            self.reader.consume(bytes_read);
        }

        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "Reached end of file instead of delimiter.",
        ));
    }

    fn read_path(&mut self, num_bytes: usize) -> io::Result<PathBuf> {
        let mut buf: Vec<u8> = vec![0u8; num_bytes];
        self.reader.read_exact(buf.as_mut_slice())?;
        let path = PathBuf::from(OsString::from_vec(buf));
        return Ok(path);
    }

    fn read_string(&mut self, limit: usize) -> io::Result<String> {
        let bytes = self.read_until_limited(limit)?;
        return String::from_utf8(bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected UTF-8 data {}", e),
            )
        });
    }
    fn read_u64(&mut self) -> io::Result<u64> {
        return self.read_string(30)?.trim().parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
        });
    }
    fn read_i64(&mut self) -> io::Result<i64> {
        return self.read_string(30)?.trim().parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
        });
    }

    fn read_hex_u64(&mut self) -> io::Result<u64> {
        const LIMIT: usize = 30; // 64/16+1
        let hex_string = self.read_string(LIMIT)?;
        return u64::from_str_radix(&hex_string.trim(), 16).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected hex string, error: {}", e),
            )
        });
    }
    fn skip_a_byte(&mut self) {
        let mut dummy = [0u8; 1];
        let _ = self.reader.read_exact(dummy.as_mut_slice());
    }

    fn parse_item(&mut self) -> io::Result<LogEntry> {
        self.lines_read += 1;
        let op = self.read_string(10)?;
        let hash = self.read_hex_u64()?;
        let mtime = self.read_i64()?;
        let size = self.read_u64()?;
        let path_length = self.read_u64()?;
        let path = self.read_path(path_length as usize)?;

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
        let buf = self.reader.fill_buf();
        if buf.is_err() || buf.unwrap().is_empty() {
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
    use std::{io::Seek, str::FromStr};

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

    // #[test]
    // fn semicolon_in_path() {
    //     assert_eq!(
    //         parse_row("+;0394b8fafef76701;2020/07/19-12:24:58;Downloads;1.mp3".to_owned()),
    //         LogEntry::Write(WriteData {
    //             xxh3: "0394b8fafef76701".to_owned(),
    //             mtime: "2020/07/19-12:24:58".to_owned(),
    //             path: "Downloads;1.mp3".to_owned(),
    //         })
    //     );

    //     assert_eq!(
    //         parse_row("-;;;Downloads;1.mp3".to_owned()),
    //         LogEntry::Delete(DeleteData {
    //             path: "Downloads;1.mp3".to_owned(),
    //         })
    //     );
    // }
}
