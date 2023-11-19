use crate::repo::Repo;
use crate::runner::Runner;
use std::collections::HashMap;

#[derive()]
pub struct DeduplicationOptions {}

pub fn run_deduplication_flow(
    repo: &Repo,
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> Result<(), String> {
    if repo.has_no_backups() {
        return Ok(());
    }

    Ok(())
}

struct FileBatch {
    min_hash: String,
    max_hash: String,
    hash_to_path: HashMap<String, String>,
}
