#!/usr/bin/env python3

# biu - back it up!
# Copyright (C) 2023  Dominic Rausch

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

import uuid
import sys
import pathlib
from datetime import datetime
from os import path
from dataclasses import dataclass, field

from backitup.backups.backup import list_backups, Backup


@dataclass
class BackupOptions:
    snapshot_date_pattern: str = '%Y-%m-%d_%H-%M'
    source_paths: list[pathlib.Path] = field(default_factory=list)
    backup_path: pathlib.Path | None = None
    archive_mode: bool = False


def exists(path):
    return path and path.isDir()


def backup_command(opts, runner):
    backups = list_backups(opts.backup_path)

    # Some paths
    backup_time = datetime.now()
    backup_target = (opts.backup_path /
                     backup_time.strftime(opts.snapshot_date_pattern))
    if path.isdir(backup_target):
        sys.exit(
            f'The backup target directory "{backup_target}" already exists.')

    new_backup = Backup(backup_target, backup_time)
    rsync_log_tmp = backup_target / f'.{uuid.uuid4().hex}'

    backup_command = ['rsync',
                      # Propagate deletions
                      '--delete',
                      # No rsync deltas for local backups
                      '--whole-file',
                      # We want a list of all the changed files.
                      # Doc: https://linux.die.net/man/5/rsyncd.conf under "log format"
                      # We keep:
                      # * %o: The operation (Send or Del.)
                      # * %C: The checksum
                      # * $M: The mtime of the file
                      # * %n: the name/path of the file.
                      '--out-format', '%o;%C;%M;%n',
                      # The default algorithm outputs 128 bits. We're happy usin xxh3's 64 bits.
                      '--checksum-choice=xxh3',
                      ]

    if opts.archive_mode:
        # Some users may want to apply archive mode.
        backup_command.append('--archive')
    else:
        # We're not using the archive mode by default since preserving permissions is
        # not what we need. Rsync's archive is equivalent to -rlptgoD. We don't want to
        # preserve permissions(p), owner(o) nor group(g).
        # We want to follow symlinks, not copy them(l).
        # We don't want to copy devices or special files (we don't even want to allow
        # them in the source)
        backup_command.extend([
            '--recursive', '--links', '--hard-links', '--times', '--xattrs'
        ])

    if len(backups) > 0:
        runner.run(
            ['cp', '-al', backups[-1].directory, new_backup.directory])
        runner.remove(new_backup.backup_log_path())
    else:
        runner.run(['mkdir', new_backup.directory])

    backup_command.extend((str(p) for p in opts.source_paths))
    backup_command.append(str(new_backup.directory))

    runner.run(backup_command, stdout_to_file=rsync_log_tmp)

    # Modify the backup log a bit to save storage space (we don't want to zip)
    runner.run(['sed', '-i',
                '-e', r'/\/$/d',  # Delete lines ending in / (=folders)
                '-e', r's/^send/+/',  # Replace send with +
                '-e', r's/^del./-/',  # Replace del. with -
                rsync_log_tmp])
    runner.run(['sort', rsync_log_tmp, '-s', '-o',  rsync_log_tmp])

    runner.replace(rsync_log_tmp, new_backup.backup_log_path())
    rsync_log_tmp.unlink(missing_ok=True)
