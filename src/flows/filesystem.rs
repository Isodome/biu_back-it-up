use std::{
    fs::{self, File},
    io::{self, BufRead, BufReader, Write},
    os::unix::{ffi::OsStrExt, fs::MetadataExt},
    path::Path,
};

use crate::repo::{Backup, BackupLogWriter};

pub fn make_backup(
    sources: &[&Path],
    to: &Backup,
    prev_backup: Option<&Backup>,
) -> Result<(), String> {
    sources.sort();

    let backup_log_writer = to.log_writer().map_err(|e| "Can't create backup log.")?;

    for source in sources {
        let source_name = source.file_name().ok_or(format!(
            "The path '{}' does not end in a usable name.",
            source.display()
        ))?;
        copy_directory_recursive(
            source,
            &&to.abs_path(source_name.as_ref()),
            &backup_log_writer,
        )
        .map_err(|e| format!("Unable to backup {}: {:?}", source.display(), e))?;
    }
    Ok(())
}

fn copy_file(from: &Path, to: &Path) -> io::Result<u64> {
    let mut from_file = BufReader::new(File::open(from)?);
    let mut to_file = File::create(to)?;

    let mut hasher = xxhash_rust::xxh3::Xxh3::new();

    loop {
        let bytes = from_file.fill_buf()?;
        let bytes_read = bytes.len();
        if bytes_read == 0 {
            break;
        }
        hasher.update(bytes);
        to_file.write_all(bytes)?;
    }

    return Ok(hasher.digest());
}

fn make_symlink(target: &Path, to: &Path) -> io::Result<u64> {
    std::os::unix::fs::symlink(target, to)?;
    return Ok(xxhash_rust::xxh3::xxh3_64(target.as_os_str().as_bytes()));
}

fn copy_directory_recursive(
    source: &Path,
    to: &Path,
    backup_log_writer: &BackupLogWriter,
) -> io::Result<()> {
    let source_items = fs::read_dir(source)?;

    for item in source_items {
        let file_name = &item?.file_name();
        let source_meta = &item?.metadata()?;

        let file_type = &item?.file_type()?;
        let dest_path = to.join(file_name);
        if file_type.is_dir() {
            fs::create_dir(dest_path)?;
            copy_directory_recursive(&item?.path(), &dest_path, backup_log_writer)?;
        } else if file_type.is_file() {
            let xxh3 = copy_file(&item?.path(), &dest_path)?;
            backup_log_writer.report_write(
                &dest_path,
                xxh3,
                source_meta.mtime(),
                source_meta.size(),
            );
        } else if file_type.is_symlink() {
            // We don't follow symlinks but copy them as is
            let xxh3 = make_symlink(&fs::read_link(item?.path())?, &dest_path)?;
            backup_log_writer.report_symlink(
                &dest_path,
                xxh3,
                source_meta.mtime(),
                source_meta.size(),
            );
        }
        // We silently ignore sockets, fifos and block devies.
    }

    Ok(())
}

fn copy_directory_incremental_recursive(
    source: &Path,
    to: &Path,
    prev_backup: &Path,
    backup_log_writer: &BackupLogWriter,
) -> io::Result<()> {
    if !prev_backup.exists() {
        return copy_directory_recursive(source, to, backup_log_writer);
    }

    // for item in fs::read_dir(source)? {
    //     let file_name = &item?.file_name();
    //     let file_type = &item?.file_type()?;

    //     let dest_path = to.join(file_name);
    //     let prev_path = prev_backup.join(file_name);

    //     let previous = fs::metadata(prev_backup.unwrap().join(file_name));
    //     if file_type.is_dir() {
    //         fs::create_dir(dest_path)
    //         copy_directory_incremental_recursive(&item?.path() ,&dest_path, &prev_path)?;
    //     } else if file_type.is_file()
    // }

    Ok(())
}
