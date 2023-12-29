use crate::repo::AllFilesLogIterator;
use crate::repo::Backup;
use crate::repo::BackupLogIterator;
use crate::repo::BackupLogPath;
use crate::repo::BackupLogWriter;
use crate::repo::Repo;
use std::cmp::Ordering;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Read;
use std::io::Write;
use std::iter::Peekable;
use std::os::unix::ffi::OsStrExt;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::path::PathBuf;

#[derive()]
pub struct BackupOptions<'a> {
    pub source_paths: &'a [PathBuf],
    pub backup_path: &'a Path,
    pub archive_mode: bool,
}

struct BackupSource {
    abs_path_to_backup: PathBuf,
    backup_target: BackupLogPath,
}

fn get_top_level_backup_dir(path: &Path) -> Result<BackupLogPath, String> {
    let root_path = PathBuf::from("/");

    if path == root_path {
        return Ok(PathBuf::from("root").into());
    }
    match path.file_name() {
        Some(n) => return Ok(PathBuf::from(n).into()),
        None => {
            return Err(format!(
                "The path {} is not valid since it does not point to a valid directory",
                path.display()
            ))
        }
    };
}

fn prepare_sources(sources: &[PathBuf]) -> Result<Vec<BackupSource>, String> {
    let mut backup_sources = vec![];
    for source in sources {
        let canonical = std::fs::canonicalize(source).map_err(|e| {
            format!(
                "Failed to canonicalize path {}. Error: {}",
                source.display(),
                e
            )
        })?;

        let file_name = get_top_level_backup_dir(&canonical)?;

        backup_sources.push(BackupSource {
            abs_path_to_backup: canonical.into(),
            backup_target: file_name,
        });
    }
    backup_sources.sort_by(|l, r| l.backup_target.cmp(&r.backup_target));

    for (lhs, rhs) in backup_sources.iter().zip(backup_sources[1..].iter()) {
        if lhs.backup_target == rhs.backup_target {
            return Err(format!("The name each directoties that's are backup up must be unique. Found {} and {} have a conflict", lhs.abs_path_to_backup.display(), rhs.abs_path_to_backup.display()));
        }
    }

    Ok(backup_sources)
}

/// Make a non-deduped backup.
pub fn run_backup_flow(repo: &Repo, opts: &BackupOptions) -> Result<(), String> {
    let target_backup = Backup::new_backup_now(&repo.path());
    if target_backup.path().is_dir() {
        return Err(String::from("Backup path already exists"));
    }

    let sorted_sources = prepare_sources(opts.source_paths)?;

    let context = BackupContext {
        new_backup: &target_backup,
        prev_backup: repo.latest_backup(),
        repo: &repo,
    };

    // Create dated top level backup directory.
    std::fs::create_dir(context.new_backup.path()).map_err(|e| {
        format!(
            "Could not create backup directory at {}: {}",
            context.new_backup.path().display(),
            e
        )
    })?;

    let mut backup_log_writer = target_backup
        .log_writer()
        .map_err(|e| format!("Can't create backup log: {}", e))?;
    let mut backup_log_reader = AllFilesLogIterator::from(match repo.latest_backup() {
        Some(backup) => backup
            .log()
            .iter()
            .map_err(|e| "Unable to open previous backup log.")?,
        None => BackupLogIterator::empty(),
    })
    .peekable();

    for source in sorted_sources {
        copy_directory_incremental_recursive(
            &source.abs_path_to_backup,
            &source.backup_target,
            &mut backup_log_reader,
            &mut backup_log_writer,
            &context,
        )
        .map_err(|e| {
            format!(
                "Unable to backup {}: {:?}",
                source.abs_path_to_backup.display(),
                e
            )
        })?;
    }
    // We silently ignore errors here since failure to report missing files is not a fatal error.
    let _ = report_remaining_deletes(&mut backup_log_reader, &mut backup_log_writer);

    let stats = backup_log_writer.finalize();
    let _ = target_backup.write_stats(&stats);

    Ok(())
}

pub struct BackupContext<'a> {
    pub repo: &'a Repo,
    pub prev_backup: Option<&'a Backup>,
    pub new_backup: &'a Backup,
}

fn copy_file(from: &Path, to: &Path) -> io::Result<u64> {
    let mut from_file = File::open(from)?;

    let mut buffer = [0u8; 4096];
    let mut to_file = File::create(to)?;

    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    loop {
        let bytes_read = from_file.read(buffer.as_mut_slice())?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[0..bytes_read]);
        to_file.write_all(&buffer[0..bytes_read])?;
    }

    return Ok(hasher.digest());
}

fn make_symlink(target: &Path, to: &Path) -> io::Result<u64> {
    std::os::unix::fs::symlink(target, to)?;
    return Ok(xxhash_rust::xxh3::xxh3_64(target.as_os_str().as_bytes()));
}

fn report_remaining_deletes(
    log_iter: &mut Peekable<AllFilesLogIterator>,
    backup_log_writer: &mut BackupLogWriter,
) -> io::Result<()> {
    while let Some(log_entry) = log_iter.next() {
        let log_entry = &log_entry?;
        backup_log_writer.report_delete(&log_entry.path, log_entry.size)?;
    }
    Ok(())
}

fn report_deletes_until_file(
    path: &BackupLogPath,
    prev_backup: &mut Peekable<AllFilesLogIterator>,
    backup_log_writer: &mut BackupLogWriter,
) -> io::Result<bool> {
    while let Some(data) = prev_backup.peek() {
        let write_data = match data {
            Ok(f) => f,
            Err(e) => return Err(io::Error::new(e.kind(), e.to_string())),
        };

        let ord = write_data.path.cmp(path);
        if ord == Ordering::Equal {
            return Ok(true); // Iterator matched.
        } else if ord == Ordering::Greater {
            return Ok(false); // The file seems to be missing
        }
        backup_log_writer.report_delete(&write_data.path, write_data.size)?;
        prev_backup.next();
    }
    return Ok(false);
}

fn copy_directory_incremental_recursive(
    source_dir: &Path,
    to_dir: &BackupLogPath,
    prev_backup: &mut Peekable<AllFilesLogIterator>,
    backup_log_writer: &mut BackupLogWriter,
    context: &BackupContext,
) -> io::Result<()> {
    let mut dir_contents: Vec<PathBuf> = fs::read_dir(source_dir)?
        .map(|entry| entry.map(|e| PathBuf::from(e.file_name())))
        .collect::<io::Result<Vec<PathBuf>>>()?;
    dir_contents.sort();

    fs::create_dir(to_dir.path_in_backup(context.new_backup))?;

    for file_name in dir_contents {
        let to_copy_abs_path = source_dir.join(&file_name);
        let to_copy_meta = std::fs::symlink_metadata(&to_copy_abs_path)?;

        let dest_path = to_dir.join(&file_name);
        if to_copy_meta.file_type().is_dir() {
            copy_directory_incremental_recursive(
                &to_copy_abs_path,
                &dest_path,
                prev_backup,
                backup_log_writer,
                context,
            )?;
            continue;
        }

        let next_matches = report_deletes_until_file(&dest_path, prev_backup, backup_log_writer)?;

        if next_matches {
            let file_in_previous = prev_backup.next().unwrap()?;
            if file_in_previous.mtime == to_copy_meta.mtime()
                && file_in_previous.size == to_copy_meta.size()
            {
                std::fs::hard_link(
                    &dest_path.path_in_backup(context.prev_backup.unwrap()),
                    &dest_path.path_in_backup(context.new_backup),
                )?;
                backup_log_writer.report_hardlink(
                    &dest_path,
                    file_in_previous.xxh3,
                    file_in_previous.mtime,
                    file_in_previous.size,
                )?;
                // File was found, we linked it. Yay!
                continue;
            }
            prev_backup.next();
        }

        let dest_file_abs_path = dest_path.path_in_backup(context.new_backup);
        if to_copy_meta.file_type().is_file() {
            let xxh3 = copy_file(&to_copy_abs_path, &dest_file_abs_path)?;
            backup_log_writer.report_write(
                &dest_path,
                xxh3,
                to_copy_meta.mtime(),
                to_copy_meta.size(),
            )?;
        } else if to_copy_meta.file_type().is_symlink() {
            // We don't follow symlinks but copy them as is
            let xxh3 = make_symlink(&fs::read_link(&to_copy_abs_path)?, &dest_file_abs_path)?;
            backup_log_writer.report_write(
                &dest_path,
                xxh3,
                to_copy_meta.mtime(),
                to_copy_meta.size(),
            )?;
        }
        // We silently ignore sockets, fifos and block devices.
    }
    Ok(())
}
