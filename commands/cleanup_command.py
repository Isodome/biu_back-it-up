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
from common import list_backups
from collections import namedtuple




def mark_backups_to_keep(backup_plan, actual_backups):
    now = datetime.now()

    # Create the list of timestamps that we'd ideally like to see according to the backup plan.
    desired_timestamps = []
    for (interval, iterations) in backup_plan:
        desired_timestamps.extend((now-i*interval for i in range(iterations+1)))
    desired_timestamps.sort(reverse=True)
    
    actual_backups[-1].should_keep = True # Always keep the newest backup

    # For each desired timestamp, we look for the youngest backup that's older than it. 
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