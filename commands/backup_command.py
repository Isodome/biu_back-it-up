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
from datetime import datetime
from os import path
from dataclasses import dataclass

from commands.common import list_backups


@dataclass
class BackupOptions:
    temp_path = None
    backup_path = None
    source_paths = []
    snapshot_date_pattern = '%Y-%m-%d_%H-%M'


def exists(path):
    return path and path.isDir()


def backup_command(opts, runner):
    backups = list_backups(opts.backup_path)

    backup_target = path.join(
        opts.backup_path, datetime.now.strftime(opts.snapshot_date_pattern))
    if path.isdir(backup_target):
        sys.exit(f'The backup target {backup_target} already exists')

    tmp_dir = path.join(opts.temp_path, uuid.uuid4().hex)

    runner.run(['mkdir', tmp_dir])

    backup_command = ['rsync',
                      # Archive mode and propagate deletions
                      '-a', '--delete',
                      # Since we're creating this backup in a tmp directory this is safe.
                      '--inplace',
                      # No rsync deltas for local backups
                      '--whole-file',
                      ]

    if len(backups) > 0:
        backup_command.extend(['--link_dest', backups[0].directory.path])

    backup_command.extend(opts.source_paths)
    backup_command.append(tmp_dir)
    runner.run(backup_command)

    runner.run(['mv', 'tmp_dir', os])
