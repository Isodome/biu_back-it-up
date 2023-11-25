use std::{
    fs::File,
    io::{ BufRead, BufReader, Lines},
    path::{Path, PathBuf},
};

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
            lines: BufReader::new(file).lines(),
        });
    }
}

pub struct BackupLogIterator {
    lines: Lines<BufReader<File>>,
}

impl Iterator for BackupLogIterator {
    type Item = Result<LogEntry, String>;
    fn next(&mut self) -> Option<Self::Item> {
        return match self.lines.next()? {
            Err(e) => Some(Err(format!(
                "FATAL: Failed to read backup log: {}",
                e.to_string()
            ))),
            Ok(text) => Some(Ok(parse_row(text))),
        };
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
