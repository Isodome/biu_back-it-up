use crate::repo::{LogEntry, Repo, WriteData};
use crate::runner::Runner;
use std::collections::HashMap;
use std::fs::{File, Metadata};
use std::io::{self, BufReader, Bytes, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive()]
pub struct DeduplicationOptions {
    pub deep_compare: bool,
    pub preserve_mtime: bool,
}

fn log_entry_to_hash(entry: Option<&Result<LogEntry, String>>) -> Option<&str> {
    let log_entry = entry?.as_ref().ok()?;
    return match log_entry {
        LogEntry::Write(data) => Some(data.xxh3),
        _ => None,
    };
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

    let mut batch = FileBatch::new();

    let mut log_entries = backups[0].log().iter()?.peekable();
    for log_entry in log_entries {
        match log_entry? {
            LogEntry::Write(data) => {
                batch.hash_to_paths[&data.xxh3].push(data.path);
            }
            LogEntry::Unparseable(_) => todo!(),
            _ => {}
        }
        if batch.hash_to_paths.len() > 1000 {
            if let Some(next_hash) = log_entry_to_hash(log_entries.peek()) {
                if batch.hash_to_paths.contains_key(&next_hash) {
                    // The batch is full, however there are more entries with the same hash.
                    continue;
                }
            }
            for (_, paths) in batch.hash_to_paths.iter() {
                let absolute_paths = paths
                    .iter()
                    .map(|path| backups[0].abs_path(&path))
                    .collect::<Vec<PathBuf>>();
                let status = maybe_dedup_files(absolute_paths, opts, runner);
                if let Err(e) = status {
                    runner.commentln(format!(
                        "Failure while trying to eliminate duplicates: {:?}: {}",
                        paths, e
                    ));
                }
            }
        }
    }

    Ok(())
}

fn as_path_refs(pathbufs: &Vec<PathBuf>) -> Vec<&Path> {
    return pathbufs.iter().map(|pathbuf| pathbuf.as_path()).collect();
}

fn maybe_dedup_files(
    duplicates: Vec<PathBuf>,
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> io::Result<()> {
    if duplicates.len() < 2 {
        return Ok(());
    }

    let mut duplicate_candidates = as_path_refs(&duplicates);
    while duplicate_candidates.len() > 1 {
        let true_dups =
            find_all_real_dups(&duplicate_candidates[0], &duplicate_candidates[1..], opts)?;
        for true_dup in &true_dups {
            runner.replace_file_with_link(&duplicate_candidates[0], true_dup)
        }
        if true_dups.len() == duplicate_candidates.len() - 1 {
            // In case all dups by hash are true dups (so almost always) we can take this shortcut.
            return Ok(());
        }
        duplicate_candidates.remove(0);
        duplicate_candidates.retain(|path| !true_dups.contains(&path.as_ref()));
    }
    return Ok(());
}

fn find_all_real_dups<'b>(
    original: &Path,
    duplicates: &[&'b Path],
    opts: &DeduplicationOptions,
) -> std::io::Result<Vec<&'b Path>> {
    let mut true_dups = Vec::from(duplicates);
    let stat_original = std::fs::symlink_metadata(original)?;

    true_dups.retain(|path| {
        let stat_duplicate = std::fs::symlink_metadata(path).expect("");
        return stat_duplicate.size() == stat_duplicate.size()
            && (!opts.preserve_mtime || stat_original.mtime() != stat_duplicate.mtime());
    });

    if opts.deep_compare {
        return find_all_dups_by_content(original, &true_dups);
    }
    return Ok(true_dups);
}

///
fn find_all_dups_by_content<'a>(
    original: &Path,
    files: &[&'a Path],
) -> std::io::Result<Vec<&'a Path>> {
    struct FileAndPath<'a> {
        bytes: Bytes<BufReader<File>>,
        path: &'a Path,
    }
    let buffer_orig = BufReader::new(File::open(original)?);
    let mut files = files
        .iter()
        .map(|path| -> io::Result<FileAndPath> {
            let file = File::open(path)?;
            let reader = BufReader::new(file);
            return Ok(FileAndPath {
                bytes: reader.bytes(),
                path: path,
            });
        })
        .collect::<io::Result<Vec<FileAndPath>>>()?;

    for original_byte in buffer_orig.bytes() {
        let b = original_byte?;
        files.retain_mut(|file| {
            file.bytes
                .next()
                .expect("Trying to compare files with different lenghts.")
                .expect("Error reading file")
                == b
        });
    }

    return Ok(files.iter().map(|file| file.path).collect());
}

struct FileBatch {
    min_hash: String,
    max_hash: String,
    hash_to_paths: HashMap<String, Vec<String>>,
}
impl FileBatch {
    fn new() -> FileBatch {
        return FileBatch {
            min_hash: "////////////////".into(),
            max_hash: "AAAAAAAAAAAAAAAA".into(),
            hash_to_paths: HashMap::new(),
        };
    }
}
