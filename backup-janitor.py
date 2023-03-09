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


def parse_backup_plan(arg):
    plan = {}
    intervals = arg.split(',')

    def parse_interval(interval):
        (k,v) = interval.split(":")
        return (timedelta(hours = int(k)), int(v))

    plan = {parse_interval(k) for k in intervals}
    return plan if len(plan)>0 else None



def parse_args():
    parser = argparse.ArgumentParser(
                        prog = 'backup-janitor',
                        description = 'What the program does',
                        epilog = 'Text at the bottom of help')
    parser.add_argument('path')  
    parser.add_argument('-p', '--plan', required = True, type=parse_backup_plan)
    return parser.parse_args()


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
            backups.append(Backup(path = dir, creation_time=backup_time))
        except ValueError:
            print(f'Ignoring "{dir.name}" since the name can\'t be parsed to date/time.')
            continue
    def by_creation_time(b):
        return b.creation_time
    backups.sort(key=by_creation_time)
    return backups


def mark_save(interval, iterations, backups):
    now = datetime.now()
    for i in range(iterations+1):
        backup_to_keep = now - i * interval
        best_match = min(enumerate(backups), key=lambda b:abs((b[1].creation_time - backup_to_keep).total_seconds()))
        backups[best_match[0]].should_keep = True

def main():
    args = parse_args()
    print (args.plan)
    backups = list_backups(args.path)

    for (interval, iterations) in args.plan:
        mark_save(interval, iterations, backups)

    for b in backups:
        op = "keep" if b.should_keep else "rm  "
        print(f'{op} {b.path}')

if __name__ == '__main__':
    main()