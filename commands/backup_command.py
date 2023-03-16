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

from commands.common import list_backups


@dataclass
class BackupOptions:
    snapshot_date_pattern: str = '%Y-%m-%d_%H-%M'
    source_paths: list[pathlib.Path] = field(default_factory=list)
    temp_path: pathlib.Path = None
    backup_path: pathlib.Path = None
    archive_mode: bool = False


def exists(path):
    return path and path.isDir()


def backup_command(opts, runner):
    backups = list_backups(opts.backup_path)

    backup_target = path.join(
        opts.backup_path, datetime.now().strftime(opts.snapshot_date_pattern))
    if path.isdir(backup_target):
        sys.exit(
            f'The backup target directory "{backup_target}" already exists.')

    tmp_dir: str = None
    if opts.temp_path is not None:
        if not opts.temp_path.is_dir():
            sys.exit("FATAL: Temporary directory doesn't exist.")
        tmp_dir = path.join(opts.temp_path, uuid.uuid4().hex)
        runner.run(['mkdir', tmp_dir])

    backup_command = ['rsync',
                      # Propagate deletions
                      '--delete',
                      # Since we're creating this backup in a tmp directory this is safe.
                      '--inplace',
                      # No rsync deltas for local backups
                      '--whole-file',
                      ]
    #TODO: --stats --info=progress2, --out-format

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
            '--recursive', ' --copy-links', '--times'
        ])

    if len(backups) > 0:
        backup_command.extend(['--link_dest', backups[0].directory.path])

    backup_command.extend((str(p) for p in opts.source_paths))
    backup_command.append(tmp_dir or backup_target)
    runner.run(backup_command)

    if tmp_dir:
        runner.run(['mv', tmp_dir, backup_target])
