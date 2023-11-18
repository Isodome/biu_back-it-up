use std::{
    fs::File,
    path::{Path, PathBuf},
    process::Command,
};

pub struct Runner {
    pub verbose: bool,
}

impl Runner {
    pub fn verbose<S: AsRef<str>>(&self, s: S) {
        if self.verbose {
            self.commentln(s);
        }
    }

    pub fn sed(&self, flags: &[&str]) -> Result<(), String> {
        Command::new("sed")
            .args(flags)
            .status()
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    pub fn remove_file(&self, path: &Path) -> Result<(), String> {
        return std::fs::remove_file(path).map_err(|err| err.to_string());
    }

    pub fn copy_as_hardlinks(&self, source: &Path, dest: &Path) -> Result<(), String> {
        Command::new("cp")
            .arg("-al")
            .arg(source)
            .arg(dest)
            .output()
            .map_err(|err| err.to_string())?;
        Ok(())
    }

    pub fn make_dir(&self, path: &Path) -> Result<(), String> {
        std::fs::create_dir(path).map_err(|err| err.to_string())
    }

    pub fn rsync(
        &self,
        flags: &[&str],
        source: &[PathBuf],
        dest: &Path,
        log_file: &Path,
    ) -> Result<(), String> {
        let log_file = File::create(log_file).map_err(|err| err.to_string())?;
        let status = Command::new("rsync")
            .args(flags)
            .args(source)
            .arg(dest)
            .stdout(log_file)
            .status()
            .map_err(|err| err.to_string())?;
        match status.success() {
            true => Ok(()),
            false => Err("Rsync terminated with status {status.exit_code()}".to_owned()),
        }
    }

    pub fn commentln<S: AsRef<str>>(&self, arg: S) {
        println!("{}", arg.as_ref());
    }

    pub fn remove_path(&self, path: &Path) -> Result<(), String> {
        println!("rm {:?}", path);
        Ok(())
        // let status = Command::new("rm")
        //     .arg("-rf")
        //     .arg(path)
        //     .status()
        //     .map_err(|err| err.to_string())?;
        // match status.success() {
        //     true => Ok(()),
        //     false => Err("Unable to delete path {path}}".into()),
        // }
    }
}
