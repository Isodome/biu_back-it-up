use crate::repo;
use crate::Runner;
use std::path::Path;
use std::path::PathBuf;
use std::time::SystemTime;

// derive[Debug]
pub struct BackupOptions {
    snapshot_date_pattern: String,
    source_paths: &[Path],
    backup_path: Path,
    archive_mode: bool,
}

fn make_backup_path(path: &Path) -> Path {
    let now = SystemTime::now();
    let mut new_path = PathBuf::from(path);
    new_path.push(now.format("%Y-%m-%d_%H-%M").to_string());
    new_path
}

#[derive(Debug)]
enum BackupFlowErr {
    RepoNotInitializedError(repo::RepoNotInitializedError),
}

pub fn run_backup_flow(opts: &BackupOptions, runner: &Runner) -> Result<BackupFlowErr, String> {
    let backups = backups::list_backups(opts.backup_path())?;

    let backup_target = make_backup_path(&opts.backup_path);
    if (backup_target.is_dir()) {
        return BackupFlowErr::RepoNotInitializedError(());
    }

    backup_command = vec![
        "rsync",
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

    if (opts.archive_mode) {
        backup_command.push("--archive")
    } else {
        // We're not using the archive mode by default since preserving permissions is
        // not what we need. Rsync's archive is equivalent to -rlptgoD. We don't want to
        // preserve permissions(p), owner(o) nor group(g).
        // We want to follow symlinks, not copy them(l).
        // We don't want to copy devices or special files (we don't even want to allow
        // them in the source)
        backup_command.extend([
            "--recursive",
            "--links",
            "--hard-links",
            "--times",
            "--xattrs",
        ]);
    }
    if (!backups.is_empty()) {
        runner.run(["cp", "-al", backups.last().directory, new_backup.directory]);
        runner.remove(new_backup.backup_log_path())
    } else {
        runner.run(["mkdir", new_backup.directory])
    }
}
