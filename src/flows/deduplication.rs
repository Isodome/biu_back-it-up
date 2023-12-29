extern crate cuckoofilter;

use cuckoofilter::CuckooFilter;

use crate::repo::{
    AllFilesLogIterator, Backup, BackupLogPath, LogEntry, NewFilesLogIterator, Repo,
};
use crate::runner::Runner;
use crate::utils::Interval;
use core::num;
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs::{File, Metadata};
use std::io::{self, BufReader, Bytes, Read};
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};

#[derive()]
pub struct DeduplicationOptions {
    pub deep_compare: bool,
    pub preserve_mtime: bool,

    // The minimum amount of bytest that we must have written in a backup in order to
    // trigger the dedup flow.
    pub min_bytes_for_dedup: u64,
}

fn log_entry_to_hash(entry: Option<&Result<LogEntry, String>>) -> Option<&str> {
    let log_entry = entry?.as_ref().ok()?;
    return match log_entry {
        LogEntry::Write(data) => Some(data.xxh3),
        _ => None,
    };
}

fn existance_filter_of_written_files(
    backup: &Backup,
    num_writes: i32,
) -> io::Result<CuckooFilter<DefaultHasher>> {
    let mut cf = CuckooFilter::with_capacity(num_writes as usize);
    for new_file in NewFilesLogIterator::from(backup.log().iter()?) {
        cf.add(&new_file?.xxh3);
    }
    return Ok(cf);
}

fn push_all_matching_files(
    backup: &Backup,
    filter: &CuckooFilter<DefaultHasher>,
    candidates: &mut Vec<DedupLogEntry>,
    wants_dedup: bool,
) -> io::Result<()> {
    for file in AllFilesLogIterator::from(backup.log().iter()?) {
        if filter.contains(&file?.xxh3) {
            candidates.push(DedupLogEntry {
                key: CompareKey {
                    mtime: file?.mtime,
                    size: file?.size,
                    hash: file?.xxh3,
                },
                wants_dedup,
                path: file?.path,
            });
        }
    }
    Ok(())
}

fn reduce_set_of_backups<'a>(
    backups: &[&'a Backup],
    relevant_mtimes: &Interval<i64>,
) -> Vec<&'a Backup> {
    backups
        .iter()
        .filter(|&&backup| {
            match backup.read_stats() {
                Ok(s) => return s.mtimes().overlaps(relevant_mtimes),
                Err(_) => return true,
            };
        })
        .cloned()
        .collect()
}

fn written_mtimes(candidates: &[DedupLogEntry]) -> Interval<i64> {
    let mut interval = Interval { hi: 0, lo: -1 };
    for candidate in candidates {
        if candidate.wants_dedup {
            interval.expand(candidate.key.mtime);
        }
    }
    return interval;
}

pub fn run_deduplication_flow(
    repo: &Repo,
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> Result<(), String> {
    let (latest_backup, prev_backups) = match repo.backups().split_last() {
        Some(bs) => bs,
        None => return Ok(()),
    };

    let latest_stats = latest_backup
        .read_stats()
        .map_err(|e| "Could not read backup stats file.")?;

    if latest_stats.num_writes == 0 || latest_stats.bytes_written < opts.min_bytes_for_dedup {
        return Ok(());
    }

    // We only look at backups that contain files in the mtime range that we need.

    let existence_filter = existance_filter_of_written_files(&latest_backup, latest_stats.num_writes).map_err(|e|"Unable to read backup log during deduplication. The backup should be complete but not deduplicated.")?;
    let mut candidates = Vec::with_capacity(latest_stats.num_writes as usize);

    // First we read this backup and the last backups files.
    push_all_matching_files(latest_backup, &existence_filter, &mut candidates, true);

    let mut remaining_backups: Vec<&Backup> = prev_backups.iter().collect();
    let mut remaining_backups: Vec<&Backup> =
        reduce_set_of_backups(&remaining_backups, &latest_stats.mtimes_written());
    while !candidates.is_empty() {
        let split = remaining_backups.split_last();
        if let Some((this_backup, _)) = split {
            push_all_matching_files(this_backup, &existence_filter, &mut candidates, false);
        }

        candidates.sort_unstable_by(|a, b| {
            a.key
                .cmp(&b.key)
                .then_with(|| a.wants_dedup.cmp(&b.wants_dedup))
        });

        // dedup and delete
        candidates = dedup_and_delete(candidates);

        // compute mtimes interval
        let mtimes = written_mtimes(&candidates);
        match split {
            Some((_, remainder)) => {
                remaining_backups = reduce_set_of_backups(remainder, &latest_stats.mtimes_written())
            }
            None => break,
        }
    }
    Ok(())
}

fn dedup_and_delete (sorted_candidates: Vec<DedupLogEntry>) -> Vec<DedupLogEntry> {
    for log_entry in sorted_candidates {
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

#[derive(PartialEq, PartialOrd, Eq, Ord)]
struct CompareKey {
    mtime: i64,
    size: u64,
    hash: u64,
}
struct DedupLogEntry {
    key: CompareKey, // Reduce

    wants_dedup: bool,
    path: BackupLogPath,
}
