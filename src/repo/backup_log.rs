use std::{
    ffi::OsString,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Read, Write},
    os::unix::ffi::{OsStrExt, OsStringExt},
    path::{Path, PathBuf},
};

#[derive(Debug, PartialEq)]
pub struct WriteData {
    pub path: PathBuf,
    pub xxh3: u64,
    pub mtime: i64,
    pub size: i64,
}
#[derive(Debug, PartialEq)]
pub struct DeleteData {
    pub path: String,
}

#[derive(Debug, PartialEq)]
pub enum LogEntry {
    Unparseable(String),
    Write(WriteData),
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
    pub path: PathBuf,
}

impl BackupLog {
    pub fn create(path: &Path) -> BackupLog {
        return BackupLog {
            path: path.to_path_buf(),
        };
    }
    pub fn iter(&self) -> Result<BackupLogIterator, String> {
        let file = File::open(&self.path)
            .map_err(|e| format!("Failed to open backup log: {}", e.to_string()))?;

        return Ok(BackupLogIterator::new(file));
    }
}

pub struct BackupLogIterator {
    reader: BufReader<File>,
    // path: &'a Path,
    lines_read: i32,
}

impl BackupLogIterator {
    fn new(file: File) -> BackupLogIterator {
        return BackupLogIterator {
            reader: BufReader::new(file),
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
            for (i, next_byte) in buf.iter().enumerate() {
                if *next_byte == DELIMITER {
                    self.reader.consume(i + 1);
                    return Ok(res);
                }

                res.push(*next_byte);
                if res.len() >= limit {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Didn't find expected delimiter.",
                    ));
                }
            }
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
        return self.read_string(20)?.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
        });
    }
    fn read_i64(&mut self) -> io::Result<i64> {
        return self.read_string(20)?.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
        });
    }

    fn read_hex_u64(&mut self) -> io::Result<u64> {
        const LIMIT: usize = 17; // 64/16+1
        let hex_string = self.read_string(LIMIT)?;
        return u64::from_str_radix(&hex_string, 16).map_err(|e| {
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
        let op = self.read_string(5)?;
        println!("{:?}", op);
        let hash = self.read_hex_u64()?;
        let mtime = self.read_i64()?;
        let size = self.read_i64()?;
        let path_length = self.read_u64()?;
        let path = self.read_path(path_length as usize)?;

        self.skip_a_byte(); // Skipping the newline. We ignore errors here.
        return match op.as_str() {
            "w" | "h" | "s" => Ok(LogEntry::Write(WriteData {
                path: path,
                xxh3: hash,
                mtime: mtime,
                size: size,
            })),
            _ => Ok(LogEntry::Unparseable(format!(
                "Unable to read line '{}' of the backup log",
                self.lines_read
            ))),
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

pub struct BackupLogWriter {
    writer: BufWriter<File>,
    path_prefix_bytes: usize,
}

impl BackupLogWriter {
    pub fn new(path: &Path) -> io::Result<BackupLogWriter> {
        let path_prefix_bytes = path
            .parent()
            .ok_or(io::Error::new(io::ErrorKind::NotFound, ""))?
            .as_os_str()
            .as_bytes()
            .len()
            + 1;
        return Ok(BackupLogWriter {
            writer: BufWriter::new(File::create(path)?),
            path_prefix_bytes,
        });
    }
    pub fn writeline(
        &mut self,
        operation: &str,
        path: &Path,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        let path_as_bytes = &path.as_os_str().as_bytes()[self.path_prefix_bytes..];
        write!(
            self.writer,
            "{};{:x};{};{};{};",
            operation,
            hash,
            mtime,
            size,
            path_as_bytes.len()
        )?;
        self.writer.write_all(path_as_bytes)?;

        // Since a linux path can contain any bytes except a null byte we use that to end the line.
        self.writer.write_all(&[b'\n'])?;
        Ok(())
    }

    pub fn report_write(
        &mut self,
        path: &Path,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        self.writeline("w", path, hash, mtime, size)
    }

    pub fn report_symlink(
        &mut self,
        path: &Path,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        self.writeline("s", path, hash, mtime, size)
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
        let file = write_test_file(b"some random word")?;
        let mut it = BackupLogIterator::new(file);
        assert_eq!(
            it.parse_item()?,
            LogEntry::Unparseable {
                0: "Didn't find expected delimiter.".to_owned()
            }
        );
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
        let file = write_test_file(b"w;0394b8fafef76701;1234;56788;15;Downloads/1.mp3")?;
        let mut it = BackupLogIterator::new(file);
        assert_eq!(
            it.parse_item()?,
            LogEntry::Write(WriteData {
                size: 56788,
                xxh3: 258034466825922305,
                mtime: 1234,
                path: PathBuf::from_str("Downloads/1.mp3").unwrap(),
            })
        );

        //     assert_eq!(
        //         parse_row("-;;;Downloads/1.mp3".to_owned()),
        //         LogEntry::Delete(DeleteData {
        //             path: "Downloads/1.mp3".to_owned(),
        //         })
        //     );
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
