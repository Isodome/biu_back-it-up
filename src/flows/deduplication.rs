extern crate cuckoofilter;

use cuckoofilter::CuckooFilter;

use crate::repo::{AllFilesLogIterator, Backup, NewFilesLogIterator, Repo};
use crate::runner::Runner;
use crate::utils::Interval;

use std::collections::hash_map::DefaultHasher;
use std::fs::File;
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
        let file = &file?;
        if filter.contains(&file.xxh3) {
            candidates.push(DedupLogEntry {
                key: CompareKey {
                    mtime: file.mtime,
                    size: file.size,
                    hash: file.xxh3,
                },
                wants_dedup,
                abs_path: file.path.path_in_backup(backup),
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
        candidates = dedup_and_delete(candidates, opts, runner);

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

fn dedup_and_delete(
    mut sorted_candidates: Vec<DedupLogEntry>,
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> Vec<DedupLogEntry> {
    let mut group: Vec<DedupLogEntry> = Vec::new();
    let mut remaining_candidates: Vec<DedupLogEntry> = Vec::new();
    for log_entry in sorted_candidates.into_iter() {
        if group.is_empty() || log_entry.key == group[0].key {
            group.push(log_entry);
            continue;
        }

        let first = group.first().unwrap();
        let last = group.last().unwrap();
        if !first.wants_dedup && !last.wants_dedup {
            // We have a group of old files from previous backups. These are probably false positives from the existence filter. We can safely discard them.
            continue;
        } else if first.wants_dedup && last.wants_dedup {
            // We have a group of new files that but nothing to dedup against.
            remaining_candidates.append(group);
        } else {
            // Lets dedup
            assert!(!first.wants_dedup, "This is a bug and should never happen");
            let absolute_paths: Vec<&Path> = group[1..]
                .iter()
                .filter(|entry| entry.wants_dedup)
                .map(|entry| entry.abs_path.as_path())
                .collect();
            let status = maybe_dedup_files(&first.abs_path, &absolute_paths, opts, runner);
            if let Err(e) = status {
                runner.commentln(format!(
                    "Failure while trying to eliminate duplicates: {:?}: {}",
                    &absolute_paths, e
                ));
            }
        }
        group.clear();
    }
    remaining_candidates
}

fn maybe_dedup_files(
    original: &Path,
    duplicate_candidates: &[&Path],
    opts: &DeduplicationOptions,
    runner: &Runner,
) -> io::Result<()> {
    if duplicate_candidates.is_empty() {
        return Ok(());
    }

    // Note: By default we assume everythin with equal mtime, size and hash is a dupe. The
    // use may chose to do a deep compare. In case we'll indeed encounter a false positive
    // (same hash, different content), we simply won't dedup the remaining files here.
    let true_dups = find_all_real_dups(original, duplicate_candidates, opts)?;
    for true_dup in &true_dups {
        runner.replace_file_with_link(duplicate_candidates[0], true_dup)
    }
    if true_dups.len() != duplicate_candidates.len() {
        runner.commentln(format!(
            "We found dups in the backup logs but the underlying files either weren't readable or the content didn't match.
            Original file: {:?} 
            Duplicates: {:?}
            Candidates: {:?}",
            original.display(),
            true_dups,
            duplicate_candidates
        ));
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
        let stat_duplicate = match std::fs::symlink_metadata(path) {
            Ok(s) => s,
            Err(_) => return false,
        };
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
    abs_path: PathBuf,
}
