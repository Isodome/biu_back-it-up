use chrono::NaiveDateTime;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Backup {
    pub path: PathBuf,
    time: chrono::NaiveDateTime,
}

impl Backup {
    pub fn path(&self) -> &Path {
        return self.path.as_path();
    }

    pub fn from_existing<P: Into<PathBuf>>(path: P) -> Option<Self> {
        let pathbuf: PathBuf = path.into();
        if !pathbuf.is_dir() {
            return None;
        }
        let dir_name = pathbuf.file_name()?.to_str()?;
        let time_from_dir = NaiveDateTime::parse_from_str(dir_name, "%Y-%m-%d_%H-%M").ok()?;

        return Some(Backup {
            path: pathbuf,
            time: time_from_dir,
        });
    }

    pub fn new_backup_now(repo_path: &Path) -> Self {
        let now = chrono::Local::now().naive_local();
        let mut new_path = PathBuf::from(repo_path);
        new_path.push(now.format("%Y-%m-%d_%H-%M").to_string());
        Self {
            path: new_path,
            time: now,
        }
    }
}
