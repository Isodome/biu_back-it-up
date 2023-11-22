use crate::repo::{LogEntry, Repo, WriteData};
use crate::runner::Runner;
use std::collections::HashMap;
use std::fs::{File, Metadata};
use std::io::{BufReader, Read};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

#[derive()]
pub struct DeduplicationOptions {
    pub deep_compare: bool,
    pub preserve_mtime: bool,
}

pub fn run_deduplication_flow(
    repo: &Repo,
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> Result<(), String> {
    if repo.has_no_backups() {
        return Ok(());
    }

    let backups = repo.backups();

    let mut last_file = String::new();
    let mut last_hash = String::new();

    for log_entry in backups[0].log().iter()? {
        match log_entry? {
            LogEntry::Write(data) => {
                if data.xxh3 == last_hash {
                    if let Err(e) = maybe_dedup_files(
                        &backups[0].abs_path(&last_file),
                        &backups[0].abs_path(&data.path),
                        opts,
                    ) {
                        runner.commentln(format!(
                            "Failure while trying to eliminate duplicates: {} and {}: {}",
                            &last_file, &data.path, e
                        ));
                    }
                }
                last_file = data.path;
                last_hash = data.xxh3;
            }
            LogEntry::Unparseable(_) => todo!(),
            _ => {}
        }
    }

    Ok(())
}

fn maybe_dedup_files(
    original: &Path,
    duplicate: &Path,
    opts: &DeduplicationOptions,
) -> std::io::Result<()> {
    let stat_original = std::fs::symlink_metadata(original)?;
    let stat_duplicate = std::fs::symlink_metadata(duplicate)?;

    if stat_original.size() != stat_duplicate.size() {
        return Ok(());
    }
    if opts.preserve_mtime && stat_original.mtime() != stat_duplicate.mtime() {
        return Ok(());
    }
    if opts.deep_compare && !file_content_is_identical(original, duplicate)? {
        return Ok(());
    }
    println!("{:?} and {:?} are dups.", original, duplicate);
    return Ok(());
}

fn file_content_is_identical(lhs: &Path, rhs: &Path) -> std::io::Result<bool> {
    let rhs_buf = BufReader::new(File::open(rhs)?);
    let lhs_buf = BufReader::new(File::open(lhs)?);

    for (byte1, byte2) in rhs_buf.bytes().zip(lhs_buf.bytes()) {
        if byte1? != byte2? {
            return Ok(false);
        }
    }
    return Ok(true);
}

struct FileBatch {
    min_hash: String,
    max_hash: String,
    hash_to_path: HashMap<String, String>,
}
