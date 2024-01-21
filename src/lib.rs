mod flows;
mod repo;
mod utils;

use flows::DeduplicationOptions;
use repo::Repo;
use std::path::PathBuf;
use utils::Runner;

pub struct BackupFlowOptions {
    pub initialize: bool,
    pub source_paths: Vec<PathBuf>,
    pub backup_path: PathBuf,
    
    pub follow_symlinks: bool,

    pub deep_compare: bool,
    pub preserve_mtime: bool,
    pub min_bytes_for_dedup: u64,
}

pub fn run_backup_flow(opts: BackupFlowOptions) -> Result<(), String> {
    let runner = Runner { verbose: true };

    let backup_opts = flows::BackupOptions {
        source_paths: &opts.source_paths,
        backup_path: &opts.backup_path,
        follow_symlinks: false,
    };
    let repo = Repo::from(&backup_opts.backup_path, opts.initialize)?;

    flows::run_backup_flow_internal(&repo, &backup_opts)?;
    return flows::run_deduplication_flow(
        &Repo::existing(&backup_opts.backup_path)?,
        &DeduplicationOptions {
            deep_compare: opts.deep_compare,
            preserve_mtime: opts.preserve_mtime,
            min_bytes_for_dedup: opts.min_bytes_for_dedup,
        },
        &runner,
    );
}

pub type RetentionPlan = repo::RetentionPlan;
pub type CleanupFlowOptions<'a> = flows::CleanupOptions<'a>;

pub fn run_cleanup_flow(opts: CleanupFlowOptions) -> Result<(), String> {
    let runner = Runner { verbose: true };
    let repo = Repo::existing(&opts.backup_path)?;

    return flows::run_cleanup_flow_int(repo, opts, &runner);
}
