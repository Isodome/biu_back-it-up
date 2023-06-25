use std::{
    fs, io,
    path::{Path, PathBuf},
};

pub struct RepoNotInitializedError;

pub struct Repo {
    path: PathBuf,
}

impl Repo {
    pub fn initialize(path: &Path) -> io::Result<Repo> {
        if path.exists() {
            return io::Error::from(io::ErrorKind::AlreadyExists);
        }
        fs::create_dir_all(path)?;
        Ok(Repo {
            path: PathBuf::from(path),
        })
    }

    pub fn existing(path: &Path) -> Result<Repo, RepoNotInitializedError> {
        if !path.is_dir() {
            return RepoNotInitializedError;
        }
        Ok(Repo {
            path: PathBuf::from(path),
        })
    }
}
