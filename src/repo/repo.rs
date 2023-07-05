use super::Backup;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub struct Repo {
    path: PathBuf,
    backups: Vec<Backup>,
}

pub enum OpenRepoError {
    RepoNotInitializedError,
    IoError(io::Error),
}

impl Repo {
    pub fn initialize(path: &Path) -> io::Result<Repo> {
        if path.exists() {
            return Err(io::Error::from(io::ErrorKind::AlreadyExists));
        }
        fs::create_dir_all(path)?;
        Ok(Repo {
            path: PathBuf::from(path),
            backups: Vec::new(),
        })
    }

    pub fn existing(path: PathBuf) -> Result<Repo, OpenRepoError> {
        if !path.is_dir() {
            return Err(OpenRepoError::RepoNotInitializedError);
        }
        let backups = list_dirs(path.as_path())?
            .iter()
            .map(Backup::from_existing)
            .filter(Option::is_some)
            .flatten() // Option<Backup> -> Backup
            .collect();

        Ok(Repo {
            path: PathBuf::from(path),
            backups,
        })
    }
}

fn list_dirs(path: &Path) -> Result<Vec<PathBuf>, OpenRepoError> {
    if !path.is_dir() {
        return Err(OpenRepoError::RepoNotInitializedError);
    }

    let directories = fs::read_dir(path)
        .map_err(OpenRepoError::IoError)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let metadata = entry.metadata().ok()?;
            if metadata.is_dir() {
                return Some(entry.path());
            } else {
                return None;
            }
        })
        .collect();

    Ok(directories)
}
