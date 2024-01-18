use chrono::{DateTime, NaiveDateTime, TimeZone};
use std::{
    io::Write,
    os::unix::ffi::OsStrExt,
    path::{Path, PathBuf},
};

use super::{BackupLog, BackupLogWriter, BackupStats};

// A path as it appears in the backup log
#[derive(PartialEq, Eq, PartialOrd, Ord, Debug, Clone, Hash)]
pub struct BackupLogPath(PathBuf);

impl BackupLogPath {
    /// Length in bytes.
    pub fn bytes_len(&self) -> usize {
        return self.as_bytes().len();
    }

    pub fn as_bytes(&self) -> &[u8] {
        return self.0.as_os_str().as_bytes();
    }

    /// Returnst the path of this file in a given backup.
    pub fn path_in_backup(&self, backup: &Backup) -> PathBuf {
        return backup.path().join(&self.0);
    }

    pub fn join<T>(&self, path: T) -> BackupLogPath
    where
        T: AsRef<Path>,
    {
        return BackupLogPath(self.0.join(path));
    }
}

impl From<PathBuf> for BackupLogPath {
    fn from(value: PathBuf) -> Self {
        return BackupLogPath(value);
    }
}
impl From<&Path> for BackupLogPath {
    fn from(value: &Path) -> Self {
        return BackupLogPath(value.into());
    }
}

#[derive(Debug)]
pub struct Backup {
    pub path: PathBuf,
    creation_time: DateTime<chrono::Local>,
}

impl Backup {
    pub fn path(&self) -> &Path {
        return self.path.as_path();
    }
    pub fn log(&self) -> BackupLog {
        return BackupLog::create(&self.path);
    }

    pub fn from_existing<P: Into<PathBuf>>(path: P) -> Option<Self> {
        let pathbuf: PathBuf = path.into();
        if !pathbuf.is_dir() {
            return None;
        }
        let dir_name = pathbuf.file_name()?.to_str()?;
        let time_from_dir = NaiveDateTime::parse_from_str(&dir_name[..16], "%Y-%m-%d_%H-%M").ok()?;

        return Some(Backup {
            path: pathbuf,
            creation_time: chrono::Local.from_local_datetime(&time_from_dir).unwrap(),
        });
    }

    pub fn new_backup_now(repo_path: &Path) -> Self {
        let now = chrono::Local::now();
        let now_string = now.format("%Y-%m-%d_%H-%M").to_string();
        let mut new_path = repo_path.join(&now_string);
        let mut i = 1;
        loop {
            if !new_path.exists() {
                break;
            }
            new_path = repo_path.join(format!("{}_{}", now_string, i));
            i += 1;
        }
        Self {
            path: new_path,
            creation_time: now,
        }
    }

    pub fn creation_time(&self) -> DateTime<chrono::Local> {
        return self.creation_time;
    }

    pub fn log_writer(&self) -> std::io::Result<BackupLogWriter> {
        return BackupLogWriter::new(&self.log_path());
    }
    pub fn write_stats(&self, stats: &BackupStats) -> std::io::Result<()> {
        let mut stats_file = std::fs::File::create(self.stats_path())?;
        stats_file.write_all(stats.as_toml().as_bytes())
    }

    fn log_path(&self) -> PathBuf {
        return self.path.join("backup.log");
    }
    fn stats_path(&self) -> PathBuf {
        return self.path.join("backup.stats");
    }
    pub fn read_stats(&self) -> std::io::Result<BackupStats> {
        return Ok(BackupStats::from_toml(&std::fs::read_to_string(
            self.stats_path(),
        )?));
    }
}
