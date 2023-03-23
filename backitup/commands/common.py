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

import gzip
import re
import os
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class Backup:
    def __init__(self, directory, creation_time):
        self.creation_time = creation_time
        self.directory = directory

    directory = None
    creation_time = None
    should_keep = False


def parse_datetime(datetime_str):
    pattern = r'(\d{4})[-_]?(\d{2})[-_]?(\d{2})([-_Tt](\d{2})[_-]?(\d{2})([_-]?(\d{2}))?)?'
    match = re.fullmatch(pattern, datetime_str)
    if match:
        groups = match.groups()
        year = int(groups[0])
        month = int(groups[1])
        day = int(groups[2])
        hour = int(groups[4]) if groups[4] is not None else 0
        minute = int(groups[5]) if groups[5] is not None else 0
        second = int(groups[7]) if groups[7] is not None else 0
        try:
            return datetime(year, month, day, hour, minute, second)
        except ValueError:
            pass
    else:
        try:
            return datetime.fromisoformat(datetime_str)
        except ValueError:
            return None


def backup_log(backup_dir, zipped=False):
    return os.path.join(backup_dir, 'backup.log.gz' if zipped else 'backup.log')


def list_backups(path):
    dirs = [e for e in os.scandir(
        path=path) if e.is_dir() and e.name[0] != '.']

    backups = []
    for dir in dirs:
        backup_time = parse_datetime(dir.name)
        if backup_time:
            backups.append(Backup(directory=dir, creation_time=backup_time))
        else:
            print(
                f'Ignoring "{dir.name}" since the name can\'t be parsed to date/time.')

    # Sort backups by time

    def by_creation_time(b):
        return b.creation_time
    backups.sort(key=by_creation_time)
    return backups


@dataclass
class BackupLog:
    class Operation(Enum):
        WRITE = 1
        DELETE = 2
    op: Operation = None
    hash = None
    mtime: int = None
    path: str = None


def read_backup_log(gz_file_path):
    with gzip.open(gz_file_path, 'rb') as f:
        for line in f:
            if f.startswith("send "):
                hash, mtime, path = f[5:].strip().split(' ', 3)
                yield BackupLog(op=BackupLog.Operation.WRITE, hash=int(hash, 16), mtime=parse_datetime(mtime), path=path)
            elif f.startswith('del. '):
                mtime, path = f[5:].strip().split(' ', 2)
                yield BackupLog(op=BackupLog.Operation.DELETE, hash=0, mtime=parse_datetime(mtime), path=path)
