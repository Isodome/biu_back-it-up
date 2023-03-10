#!/usr/bin/env python3

# backup-janitor
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


import os
import argparse
import re

from datetime import datetime, timedelta
from collections import namedtuple




class Options(object):
    follow_symlinks = False
    plan = {}

class Backup:
    def __init__(self, path, creation_time):
        self.creation_time = creation_time
        self.path = path
 
    path = None
    creation_time = None
    should_keep = False


def parse_datetime(datetime_str):
    pattern = r'(\d{4})[-_]?(\d{2})[-_]?(\d{2})([-_Tt](\d{2})[_-](\d{2}))?'
    match = re.fullmatch(pattern, datetime_str)
    if match:
        groups = match.groups()
        year = int(groups[0])
        month = int(groups[1])
        day = int(groups[2])
        hour = int(groups[4]) if groups[3] is not None else 0
        minute = int(groups[5]) if groups[4] is not None else 0
        second = 0
        datetime_obj = datetime(year, month, day, hour, minute, second)
        return datetime_obj
    else:
        raise ValueError("Invalid datetime format")

def list_backups(path):
    dirs = [e for e in os.scandir(path = path) if e.is_dir() and len(e.name)>=8 and e.name[0] != '.']

    backups = []
    for dir in dirs:
        try:
            backup_time =  parse_datetime(dir.name)
            backups.append(Backup(path = dir.name, creation_time=backup_time))
        except ValueError:
            print(f'Ignoring "{dir.name}" since the name can\'t be parsed to date/time.')
            continue
    def by_creation_time(b):
        return b.creation_time
    backups.sort(key=by_creation_time)
    return backups


def mark_backups_to_keep(backup_plan, actual_backups):
    now = datetime.now()

    # Create the list of timestamps that we'd ideally like to see according to the backup plan.
    desired_timestamps = []
    for (interval, iterations) in backup_plan:
        desired_timestamps.extend((now-i*interval for i in range(iterations+1)))
    desired_timestamps.sort(reverse=True)
    
    actual_backups[-1].should_keep = True # Always keep the newest backup
    backup_idx = 0
    for desired_timestamp in desired_timestamps:
        for backup in actual_backups:
            if desired_timestamp < backup.creation_time:
                backup.should_keep = True
                break


def cleanup_command(backup_plan, path):
    backups = list_backups(path)

    mark_backups_to_keep(backup_plan, backups)

    for b in backups:
        op = "# keep " if b.should_keep else "rm -rf "
        print(f'{op} {b.path}')