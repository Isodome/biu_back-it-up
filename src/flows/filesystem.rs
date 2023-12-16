use std::{
    cmp::Ordering,
    fs::{self, File},
    io::{self, Read, Write},
    iter::Peekable,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Path, PathBuf},
};

use crate::repo::{
    Backup, BackupLogIterator, BackupLogPath, BackupLogWriter, LogEntry, Repo, WriteData,
};

struct BackupSource {
    path: PathBuf,
    backup_path: BackupLogPath,
}

fn sort_sources(sources: &[PathBuf]) -> Result<Vec<BackupSource>, String> {
    let mut backup_sources = vec![];
    for source in sources {
        let file_name = source.file_name();
        if file_name.is_none() {
            return Err(format!(
                "The path {} is not valid since it does not point to a valid directory",
                source.display()
            ));
        }
        backup_sources.push(BackupSource {
            path: source.into(),
            backup_path: PathBuf::from(file_name.unwrap()).into(),
        });
    }
    backup_sources.sort_by(|l, r| l.backup_path.cmp(&r.backup_path));
    backup_sources.dedup_by(|l, r| l.backup_path == r.backup_path);
    if backup_sources.len() < sources.len() {
        return Err("The names of directories must be unique, but we found duplicates.".into());
    }
    Ok(backup_sources)
}

pub struct BackupContext<'a> {
    pub repo: &'a Repo,
    pub prev_backup: Option<&'a Backup>,
    pub new_backup: &'a Backup,
}

pub fn make_backup(sources: &[PathBuf], context: &BackupContext) -> Result<(), String> {
    let sorted_sources = sort_sources(sources)?;

    // Create dated top level backup directory.
    std::fs::create_dir(context.new_backup.path()).map_err(|e| {
        format!(
            "Could not create backup directory at {}: {}",
            context.new_backup.path().display(),
            e
        )
    })?;

    let mut backup_log_writer = context
        .new_backup
        .log_writer()
        .map_err(|e| format!("Can't create backup log: {}", e))?;
    let mut backup_log_reader = match context.prev_backup {
        Some(backup) => backup.log().iter()?,
        None => BackupLogIterator::empty(),
    }
    .peekable();

    for source in sorted_sources {
        copy_directory_incremental_recursive(
            &source.path,
            &source.backup_path,
            &mut backup_log_reader,
            &mut backup_log_writer,
            context,
        )
        .map_err(|e| format!("Unable to backup {}: {:?}", source.path.display(), e))?;
    }
    Ok(())
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

fn skip_over_deleted_files(
    path: &BackupLogPath,
    prev_backup: &mut Peekable<BackupLogIterator>,
    backup_log_writer: &mut BackupLogWriter,
) -> io::Result<Option<WriteData>> {
    while let Some(data) = prev_backup.peek() {
        let prev_file_log_entry = match data {
            Ok(f) => f,
            Err(e) => return Err(io::Error::new(e.kind(), e.to_string())),
        };

        if let LogEntry::Write(wd) = prev_file_log_entry {
            let ord = wd.path.cmp(path);
            if ord == Ordering::Equal {
                return Ok(Some(wd.clone())); // Iterator matched.
            } else if ord == Ordering::Greater {
                return Ok(None); // The file seems to be missing
            }
            backup_log_writer.report_delete(&wd.path)?;
        }
        prev_backup.next();
    }
    return Ok(None);
}

fn copy_directory_incremental_recursive(
    source_dir: &Path,
    to_dir: &BackupLogPath,
    prev_backup: &mut Peekable<BackupLogIterator>,
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

        let file_in_previous_backup =
            skip_over_deleted_files(&dest_path, prev_backup, backup_log_writer)?;
        let dest_file_abs_path = dest_path.path_in_backup(context.new_backup);
        if let Some(file_in_previous) = file_in_previous_backup {
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
        }

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
