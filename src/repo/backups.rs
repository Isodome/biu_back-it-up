use chrono::NaiveDateTime;
use std::path::{Path, PathBuf};

#[derive(Debug)]
pub struct Backup {
    path: PathBuf,
    time: chrono::NaiveDateTime,
}

impl Backup {
    pub fn from_existing(path: &PathBuf) -> Option<Self> {
        if !path.is_dir() {
            return None;
        }
        let dir_name = path.file_name()?.to_str()?;

        return Some(Backup {
            path: path.to_path_buf(),
            time: NaiveDateTime::parse_from_str(dir_name, "%Y-%m-%d_%H-%M").ok()?,
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
