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

from commands.common import list_backups,  Backup


@dataclass
class BackupOptions:
    snapshot_date_pattern: str = '%Y-%m-%d_%H-%M'
    source_paths: list[pathlib.Path] = field(default_factory=list)
    backup_path: pathlib.Path = None
    archive_mode: bool = False


def exists(path):
    return path and path.isDir()


def backup_command(opts, runner):
    backups = list_backups(opts.backup_path)

    # Some paths
    backup_time = datetime.now()
    backup_target = path.join(
        opts.backup_path, backup_time.strftime(opts.snapshot_date_pattern))
    if path.isdir(backup_target):
        sys.exit(
            f'The backup target directory "{backup_target}" already exists.')

    new_backup = Backup(backup_target, backup_time)
    rsync_log_tmp = path.join(backup_target, uuid.uuid4().hex())

    backup_command = ['rsync',
                      # Propagate deletions
                      '--delete',
                      # No rsync deltas for local backups
                      '--whole-file',
                      # We want to list all the change files.
                      '--out-format', '%o %C %M %n',
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
            '--recursive', '--copy-links', '--times', '--xattrs'
        ])

    if len(backups) > 0:
        runner.run(
            ['cp', '-al', backups[-1].directory.path, new_backup.directory])
        runner.run(['rm', '-f', new_backup.backup_log_path,
                   new_backup.backup_completed_path])
    else:
        runner.run(['mkdir', new_backup.directory])

    backup_command.extend((str(p) for p in opts.source_paths))
    backup_command.append(new_backup.directory)

    runner.run(backup_command, stdout_to_file=rsync_log_tmp)
    # Delete lines ending in /
    runner.run(['sed', '-i', r'/\/$/d', rsync_log_tmp])
    runner.run(['sort', rsync_log_tmp, '-o', rsync_log_tmp])

    runner.run(['xz', '-z', '-f', '--block-size=5120', rsync_log_tmp])
    runner.run(['mv', rsync_log_tmp, new_backup.backup_log_path])
    runner.run(['echo', 'The existence of this file means that the backup completed successfully',],
               stdout_to_file=new_backup.backup_completed_path)
