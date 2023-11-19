// derive[Debug]
pub struct DeduplicationOptions {
    pub backup_path: PathBuf,
}

pub fn run_deduplication_flow(
    repo: &Repo,
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> Result<(), String> {
}
