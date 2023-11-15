use crate::repo::Backup;
use crate::repo::Repo;
use crate::retention_plan::RetentionPlan;
use crate::Runner;
use std::io;
use std::path::Path;
use std::path::PathBuf;

// derive[Debug]
pub struct CleanupOptions {
    pub backup_path: PathBuf,
    pub retentian_plan: RetentionPlan,
    pub force_delete: bool,
}

#[derive(Debug)]
enum BackupFlowErr {}

pub fn run_cleanup_flow(repo: &Repo, opts: &CleanupOptions, runner: &Runner) -> Result<(), String> {
    Ok(())
}
