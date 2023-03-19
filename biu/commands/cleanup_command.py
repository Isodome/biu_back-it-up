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


import os
import argparse
import re

from datetime import datetime, timedelta
import sys
from commands.common import list_backups
import commands.cmd as cmd
from collections import namedtuple

CleanupOptions = namedtuple(
    "CleanupOptions", 'retention_plan force_delete path')


def _num_backups_to_keep(backups):
    return len(backup for backup in backups if backup.should_keep)


def determine_backups_to_keep(opts, backups):
    now = datetime.now()

    # The number of backups we are allowed to keep. We can keep all of them if --force_delete is 0.
    backup_budget = len(backups) - opts.force_delete

    # Create the list of timestamps that we'd ideally like to see according to the backup plan.
    backups[-1].should_keep = True  # Always keep the newest backup

    # We go through the various intervals in the user-specified order and "save" backups according to the plans from new to old.
    for (interval, iterations) in opts.retention_plan:
        desired_timestamps = (now-i*interval for i in range(iterations+1))
        for desired_timestamp in desired_timestamps:

            num_backups_to_keep = _num_backups_to_keep(backups)
            if num_backups_to_keep == len(backups):
                return
            if num_backups_to_keep >= backup_budget:
                print(
                    f"# WARNING --force_delete={opts.force_delete} requires us to delete backups that are still within the retention plan.")
                return
            # Walk through the backups and keep the youngest backup that's older then the desired time.
            for backup in backups:
                if desired_timestamp < backup.creation_time:
                    backup.should_keep = True
                    break


def cleanup_command(opts, runner):
    backups = list_backups(opts.path)

    if len(backups) < 2:
        return

    if opts.force_delete >= len(backups):
        sys.exit(f'User requested to delete all backups. Exiting.', 1)

    determine_backups_to_keep(opts, backups)

    for b in backups:
        if b.should_keep:
            runner.comment(f'Keep {b.directory.path}')
        else:
            runner.run(["rm", '-r', b.directory.path])
