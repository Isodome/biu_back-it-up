use super::Backup;
use std::{
    fs, io,
    path::{Path, PathBuf},
};

#[derive(Debug)]
pub enum OpenRepoError {
    RepoNotInitializedError,
    IoError(io::Error),
}

pub struct Repo {
    path: PathBuf,
    backups: Vec<Backup>,
}

impl Repo {
    pub fn num_backups(&self) -> i32 {
        return self.backups.len() as i32;
    }

    pub fn path(&self) -> &Path {
        return self.path.as_path();
    }

    pub fn backups(&self) -> &[Backup] {
        return &self.backups;
    }

    pub fn from(path: &Path, initialize: bool) -> Result<Self, String> {
        if initialize {
            Repo::initialize(path)
        } else {
            Repo::existing(path)
        }
    }

    pub fn initialize(path: &Path) -> Result<Self, String> {
        if path.exists() {
            return Err(format!(
                "Unable to initialize a repository at {}. The directory does already exist.",
                path.display()
            )
            .into());
        }
        fs::create_dir_all(path).map_err(|e| {
            format!(
                "Unable to create a directory at {:?}. :{}",
                path,
                e.to_string()
            )
        })?;

        Ok(Repo {
            path: PathBuf::from(path),
            backups: Vec::new(),
        })
    }

    pub fn existing(path: &Path) -> Result<Repo, String> {
        if !path.is_dir() {
            return Err("The provided backup {path} path does not exist. Please provide a valid path or use --initialize to create a new directory.".into());
        }
        let backups = list_dirs(path)
            .map_err(|e| format!("Unable to list backups at {:?}. :{:?}", path, e))?
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

    pub fn latest_backup(&self) -> Option<&Backup> {
        self.backups.last()
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
