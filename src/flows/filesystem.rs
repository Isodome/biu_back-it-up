use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, Read, Write},
    iter::Peekable,
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::{Path, PathBuf},
};

use crate::repo::{Backup, BackupLogIterator, BackupLogWriter};

struct BackupSource {
    path: PathBuf,
    file_name: PathBuf,
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
            file_name: file_name.unwrap().into(),
        });
    }
    backup_sources.sort_by(|l, r| l.file_name.cmp(&r.file_name));
    backup_sources.dedup_by(|l, r| l.file_name == r.file_name);
    if backup_sources.len() < sources.len() {
        return Err("The names of directories must be unique, but we found duplicates.".into());
    }
    Ok(backup_sources)
}

pub fn make_backup(
    sources: &[PathBuf],
    to: &Backup,
    prev_backup: Option<&Backup>,
) -> Result<(), String> {
    let sorted_sources = sort_sources(sources)?;

    // Create dated top level backup directory.
    std::fs::create_dir(to.path()).map_err(|e| {
        format!(
            "Could not create backup directory at {}: {}",
            to.path().display(),
            e
        )
    })?;

    let mut backup_log_writer = to
        .log_writer()
        .map_err(|e| format!("Can't create backup log: {}", e))?;

    for source in sorted_sources {
        if let Some(prev_backup) = prev_backup {
            copy_directory_incremental_recursive(
                &source.path,
                &&to.abs_path(source.file_name.as_ref()),
                &mut prev_backup.log().iter()?.peekable(),
                &mut backup_log_writer,
            )
            .map_err(|e| format!("Unable to backup {}: {:?}", source.path.display(), e))?;
        } else {
            copy_directory_recursive(
                &source.path,
                &&to.abs_path(source.file_name.as_ref()),
                &mut backup_log_writer,
            )
            .map_err(|e| format!("Unable to backup {}: {:?}", source.path.display(), e))?;
        }
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

fn make_hardlink(target: &Path, to: &Path) -> io::Result<u64> {
    std::os::unix::fs::symlink(target, to)?;
    return Ok(xxhash_rust::xxh3::xxh3_64(target.as_os_str().as_bytes()));
}

fn copy_directory_recursive(
    source: &Path,
    to: &Path,
    backup_log_writer: &mut BackupLogWriter,
) -> io::Result<()> {
    let mut dir_contents: Vec<PathBuf> = fs::read_dir(source)?
        .map(|entry| entry.map(|e| PathBuf::from(e.file_name())))
        .collect::<io::Result<Vec<PathBuf>>>()?;
    dir_contents.sort();

    fs::create_dir(&to)?;

    for file_name in dir_contents {
        let full_path = source.join(&file_name);
        let source_meta = std::fs::symlink_metadata(&full_path)?;

        let dest_path = to.join(&file_name);
        if source_meta.file_type().is_dir() {
            copy_directory_recursive(&full_path, &dest_path, backup_log_writer)?;
        } else if source_meta.file_type().is_file() {
            let xxh3 = copy_file(&full_path, &dest_path)?;
            backup_log_writer.report_write(
                &dest_path,
                xxh3,
                source_meta.mtime(),
                source_meta.size(),
            )?;
        } else if source_meta.file_type().is_symlink() {
            // We don't follow symlinks but copy them as is
            let xxh3 = make_symlink(&fs::read_link(&full_path)?, &dest_path)?;
            backup_log_writer.report_symlink(
                &dest_path,
                xxh3,
                source_meta.mtime(),
                source_meta.size(),
            )?;
        }
        // We silently ignore sockets, fifos and block devices.
    }
    Ok(())
}

fn copy_directory_incremental_recursive(
    source: &Path,
    to: &Path,
    prev_backup: &mut Peekable<BackupLogIterator>,
    backup_log_writer: &mut BackupLogWriter,
) -> io::Result<()> {
    if !prev_backup.peek().is_none() {
        return copy_directory_recursive(source, to, backup_log_writer);
    }
    let mut dir_contents: Vec<PathBuf> = fs::read_dir(source)?
        .map(|entry| entry.map(|e| PathBuf::from(e.file_name())))
        .collect::<io::Result<Vec<PathBuf>>>()?;
    dir_contents.sort();

    for file_name in dir_contents {
        let full_path = source.join(&file_name);
        let source_meta = std::fs::symlink_metadata(&full_path)?;

        let dest_path = to.join(&file_name);
        if source_meta.file_type().is_dir() {
            copy_directory_incremental_recursive(
                &full_path,
                &dest_path,
                prev_backup,
                backup_log_writer,
            )?;
        } else if source_meta.file_type().is_file() {
            let prev_file = prev_backup.peek().unwrap()?;
            let xxh3 = copy_file(&full_path, &dest_path)?;
            backup_log_writer.report_write(
                &dest_path,
                xxh3,
                source_meta.mtime(),
                source_meta.size(),
            )?;
        } else if source_meta.file_type().is_symlink() {
            // We don't follow symlinks but copy them as is
            let xxh3 = make_symlink(&fs::read_link(&full_path)?, &dest_path)?;
            backup_log_writer.report_symlink(
                &dest_path,
                xxh3,
                source_meta.mtime(),
                source_meta.size(),
            )?;
        }
        // We silently ignore sockets, fifos and block devices.
    }
    Ok(())
}
