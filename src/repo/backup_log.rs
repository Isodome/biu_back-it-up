use core::slice;
use std::{
    error::Error,
    ffi::OsString,
    fs::File,
    io::{self, BufRead, BufReader, BufWriter, Lines, Read, Write},
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

        return Ok(BackupLogIterator {
            reader: BufReader::new(file),
            path: &self.path,
            lines_read: 0,
            depleted: false,
        });
    }
}

pub struct BackupLogIterator<'a> {
    reader: BufReader<File>,
    path: &'a Path,
    lines_read: i32,
    depleted: bool,
}

impl BackupLogIterator<'_> {
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

    fn read_path(&self, num_bytes: usize) -> io::Result<PathBuf> {
        let mut buf: Vec<u8> = vec![0u8; num_bytes];
        self.reader.read_exact(buf.as_mut_slice())?;
        let path = PathBuf::from(OsString::from_vec(buf));
        return Ok(path);
    }

    fn read_string(&self, limit: usize) -> io::Result<String> {
        let bytes = self.read_until_limited(limit)?;
        return String::from_utf8(bytes).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected UTF-8 data {}", e),
            )
        });
    }
    fn read_u64(&self) -> io::Result<u64> {
        return self.read_string(20)?.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
        });
    }
    fn read_i64(&self) -> io::Result<i64> {
        return self.read_string(20)?.parse().map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, format!("Expected int: {}", e))
        });
    }

    fn read_hex_u64(&self) -> io::Result<u64> {
        const LIMIT: usize = 17; // 64/16+1
        let hex_string = self.read_string(LIMIT)?;
        return u64::from_str_radix(&hex_string, 16).map_err(|e| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Expected hex string, error: {}", e),
            )
        });
    }
    fn skip_a_byte(&self) {
        let mut dummy = [0u8; 1];
        self.reader.read_exact(dummy.as_mut_slice());
    }

    fn parse_item(&self) -> io::Result<LogEntry> {
        let op = self.read_string(5)?;
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
                "Unable to read line {} of the backup log at {}",
                self.lines_read,
                self.path.display()
            ))),
        };
    }
}

impl Iterator for BackupLogIterator<'_> {
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
}

impl BackupLogWriter {
    pub fn new(path: &Path) -> io::Result<BackupLogWriter> {
        return Ok(BackupLogWriter {
            writer: BufWriter::new(File::create(path)?),
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
        let path_as_bytes = path.as_os_str().as_bytes();
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
        self.writeline("wf", path, hash, mtime, size)
    }

    pub fn report_symlink(
        &mut self,
        path: &Path,
        hash: u64,
        mtime: i64,
        size: u64,
    ) -> io::Result<()> {
        self.writeline("ws", path, hash, mtime, size)
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn unparseable_missing_simicolon() {
        assert_eq!(
            parse_row("-;;my/path".to_owned()),
            LogEntry::Unparseable {
                0: "-;;my/path".to_owned()
            }
        );
        assert_eq!(
            parse_row("-;;my/path".to_owned()),
            LogEntry::Unparseable {
                0: "-;;my/path".to_owned()
            }
        );
        assert_eq!(
            parse_row("-;;my/path".to_owned()),
            LogEntry::Unparseable {
                0: "-;;my/path".to_owned()
            }
        );
    }

    #[test]
    fn parseable_lines() {
        assert_eq!(
            parse_row("+;0394b8fafef76701;2020/07/19-12:24:58;Downloads/1.mp3".to_owned()),
            LogEntry::Write(WriteData {
                xxh3: "0394b8fafef76701".to_owned(),
                mtime: "2020/07/19-12:24:58".to_owned(),
                path: "Downloads/1.mp3".to_owned(),
            })
        );

        assert_eq!(
            parse_row("-;;;Downloads/1.mp3".to_owned()),
            LogEntry::Delete(DeleteData {
                path: "Downloads/1.mp3".to_owned(),
            })
        );
    }

    #[test]
    fn semicolon_in_path() {
        assert_eq!(
            parse_row("+;0394b8fafef76701;2020/07/19-12:24:58;Downloads;1.mp3".to_owned()),
            LogEntry::Write(WriteData {
                xxh3: "0394b8fafef76701".to_owned(),
                mtime: "2020/07/19-12:24:58".to_owned(),
                path: "Downloads;1.mp3".to_owned(),
            })
        );

        assert_eq!(
            parse_row("-;;;Downloads;1.mp3".to_owned()),
            LogEntry::Delete(DeleteData {
                path: "Downloads;1.mp3".to_owned(),
            })
        );
    }
}
