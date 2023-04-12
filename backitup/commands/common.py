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
import pathlib
import re
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from enum import Enum


class FileOperation(Enum):
    WRITE = 1
    DELETE = 2


class BackupLogOrder(Enum):
    BY_PATH = 1
    BY_HASH = 2


@dataclass
class BackupLogEntry:
    op: FileOperation = None
    hash: int = None
    mtime: int = None
    path: pathlib.Path = None


class Backup:
    def __init__(self, directory, creation_time):
        self.creation_time = creation_time
        self.directory = directory

    directory: pathlib.Path = None
    creation_time = None
    should_keep = False
    _size_bytes = None
    sorted_by_hash_tmp_file = None

    def backup_log_path(self):
        return os.path.join(self.directory, 'backup.log')

    def backup_completed_path(self):
        return os.path.join(self.directory, 'backup_completed.txt')

    def read_backup_log(self, order: BackupLogOrder, filter: FileOperation = None):
        if not order:
            sys.exit("")
        return BackupLogIter(self, self.backup_log_path(), filter)


class BackupLogIter:

    backup: Backup = None
    log_file: str = None
    file_handle = None

    cache: BackupLogEntry = None
    filter = None
    seek_position: int = 0

    def __init__(self, backup, log_file, filter):
        if not os.path.isfile(log_file):
            print(f"WARNING: {backup.directory} has no backup log.")
            return
        self.backup = backup
        self.filter = filter
        self.log_file = log_file

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def suspend(self):
        if not self.file_handle.closed:
            self.seek_position = self.file_handle.tell()
            self.close()

    def resume(self):
        self.file_handle = open(self.log_file, 'r')
        self.file_handle.seek(self.seek_position)

    def close(self):
        if self.file_handle and not self.file_handle.closed:
            self.file_handle.close()

    def peek(self):
        if not self.cache:
            try:
                self.cache = next(self)
            except StopIteration:
                self.close()
                return None
        return self.cache

    def __next__(self):
        if not self.file_handle:
            self.resume()
        if self.cache:
            tmp = self.cache
            self.cache = None
            return tmp
        while True:
            line = self.file_handle.readline()
            if not line:
                raise StopIteration
            if self.filter == FileOperation.WRITE and line.startswith("send"):
                hash_hex, mtime, path = line[5:].strip().split(';', 2)
                hash_int = int(hash_hex, 16)
                if hash_int <= 0:
                    continue
                return BackupLogEntry(op=FileOperation.WRITE, hash=hash_int, mtime=parse_datetime(mtime), path=self.backup.directory.joinpath(path))
            elif self.filter == FileOperation.DELETE and line.startswith('del.'):
                mtime, path = line[5:].strip().split(';', 1)
                return BackupLogEntry(op=FileOperation.DELETE, hash=0, mtime=parse_datetime(mtime), path=self.backup.directory.joinpath(path))


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


def list_backups(path: pathlib.Path):
    dirs = [e for e in path.iterdir() if e.is_dir() and e.name[0] != '.']

    backups = []
    for dir in dirs:
        backup_time = parse_datetime(dir.name)
        if backup_time:
            backups.append(Backup(directory=dir, creation_time=backup_time))
        else:
            print(
                f'Found directory "{dir.name}" in the backup path. It will be ignored since the name can\'t be parsed to date/time.')

    # Sort backups by time

    def by_creation_time(b):
        return b.creation_time
    backups.sort(key=by_creation_time)
    return backups
