use std::{
    fs::File,
    io,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub struct BackupLog {
    pub path: PathBuf,
    pub file: File,
}

#[derive(Debug, PartialEq)]
pub struct WriteData {
    pub path: String,
    pub xxh3: String,
    pub mtime: String,
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

/// Parses a log entry.
pub fn parse_row(row: String) -> LogEntry {
    let split: Vec<_> = row.splitn(4, ';').collect();
    if let [op, xxh3, mtime, path] = split[..] {
        return match op {
            "+" => LogEntry::Write(WriteData {
                path: path.to_owned(),
                xxh3: xxh3.to_owned(),
                mtime: mtime.to_owned(),
            }),
            "-" => LogEntry::Delete(DeleteData {
                path: path.to_owned(),
            }),
            _ => LogEntry::Unparseable(row),
        };
    }
    return LogEntry::Unparseable(row);
}

impl BackupLog {
    pub fn create(&self, path: &Path) -> io::Result<BackupLog> {
        return Ok(BackupLog {
            path: path.to_path_buf(),
            file: File::create(path)?,
        });
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
