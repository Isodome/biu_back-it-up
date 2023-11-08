use crate::repo::Backup;
use crate::repo::Repo;
use crate::Runner;
use std::io;
use std::path::Path;
use std::path::PathBuf;

// derive[Debug]
pub struct BackupOptions<'a> {
    pub source_paths: &'a [PathBuf],
    pub backup_path: &'a Path,
    pub archive_mode: bool,
}

#[derive(Debug)]
enum BackupFlowErr {}

pub fn run_backup_flow(
    repo: &Repo,
    opts: &BackupOptions,
    runner: &Runner,
) -> Result<(), io::Error> {
    let target_backup = Backup::new_backup_now(&repo.path());
    if target_backup.path().is_dir() {
        return Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "Backup path already exists",
        ));
    }

    let mut rsync_flags = vec![
        // Propagate deletions
        "--delete",
        // No rsync deltas for local backups
        "--whole-file",
        // We want a list of all the changed files.
        // Doc: https://linux.die.net/man/5/rsyncd.conf under "log format"
        // We keep:
        // * %o: The operation (Send or Del.)
        // * %C: The checksum
        // * $M: The mtime of the file
        // * %n: the name/path of the file.
        "--out-format='o;%C;%M;%n'",
        // The default algorithm outputs 128 bits. We"re happy usin xxh3"s 64 bits.
        "--checksum-choice=xxh3",
    ];

    if opts.archive_mode {
        rsync_flags.push("--archive")
    } else {
        // We're not using the archive mode by default since preserving permissions is
        // not what we need. Rsync's archive is equivalent to -rlptgoD. We don't want to
        // preserve permissions(p), owner(o) nor group(g).
        // We want to follow symlinks, not copy them(l).
        // We don't want to copy devices or special files (we don't even want to allow
        // them in the source)
        rsync_flags.extend([
            "--recursive",
            "--links",
            "--hard-links",
            "--times",
            "--xattrs",
        ]);
    }
    if let Some(last_backup) = repo.backups().last() {
        runner.copy_as_hardlinks(&last_backup.path(), target_backup.path())?;
        // runner.remove(target_backup.path.as_path())?;
    } else {
        runner.make_dir(target_backup.path())?;
    }
    runner.rsync(&rsync_flags, opts.source_paths, target_backup.path())
}
