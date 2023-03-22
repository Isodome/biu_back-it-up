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

import re
import os
from datetime import datetime


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
        try:
            backup_time = parse_datetime(dir.name)
            backups.append(Backup(directory=dir, creation_time=backup_time))
        except ValueError:
            print(
                f'Ignoring "{dir.name}" since the name can\'t be parsed to date/time.')
            continue

    # Sort backups by time
    def by_creation_time(b):
        return b.creation_time
    backups.sort(key=by_creation_time)
    return backups
