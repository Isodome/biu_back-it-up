use crate::repo::Backup;
use crate::repo::Repo;
use std::path::Path;
use std::path::PathBuf;

use super::filesystem::make_backup;
use super::filesystem::BackupContext;

#[derive()]
pub struct BackupOptions<'a> {
    pub source_paths: &'a [PathBuf],
    pub backup_path: &'a Path,
    pub archive_mode: bool,
}

pub fn run_backup_flow(repo: &Repo, opts: &BackupOptions) -> Result<(), String> {
    let target_backup = Backup::new_backup_now(&repo.path());
    if target_backup.path().is_dir() {
        return Err(String::from("Backup path already exists"));
    }

    let backup_context = BackupContext {
        new_backup: &target_backup,
        prev_backup: repo.latest_backup(),
        repo: &repo,
    };

    return make_backup(opts.source_paths, &backup_context);
}
