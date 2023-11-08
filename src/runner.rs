use std::{
    io,
    path::{Path, PathBuf},
    process::Command,
};

pub struct Runner {}



impl Runner {
    pub fn remove(&self, path: &Path) -> Result<(), io::Error> {
        return std::fs::remove_file(path);
    }

    pub fn copy_as_hardlinks(&self, source: &Path, dest: &Path) -> Result<(), io::Error> {
        Command::new("cp")
            .arg("-al")
            .arg(source)
            .arg(dest)
            .output()?;
        Ok(())
    }

    pub fn make_dir(&self, path: &Path) -> Result<(), io::Error> {
        std::fs::create_dir(path)
    }

    pub fn rsync(&self, flags: &[&str], source: &[PathBuf], dest: &Path) -> Result<(), io::Error> {

        Command::new("rsync")
            .args(flags)
            .args(source)
            .arg(dest)
            .output()?;
        Ok(())
    }
}
