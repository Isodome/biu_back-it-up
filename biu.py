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

import argparse
import sys
from commands.cleanup_command import cleanup_command, CleanupOptions
from commands.backup_command import backup_command, BackupOptions
from datetime import timedelta
from commands.cmd import Runner
from pathlib import Path


def parse_duration(dur):
    if len(dur) < 2:
        raise argparse.ArgumentTypeError("Malformed backup plan.")
    if not re.fullmatch(r'[0-9]+', dur[:-1]):
        raise argparse.ArgumentTypeError("Malformed backup plan.")

    unit = dur[-1]
    if unit == 'm':
        return timedelta(minutes=int(dur[:-1]))
    if unit == 'h':
        return timedelta(hours=int(dur[:-1]))
    if unit == 'd':
        return timedelta(days=int(dur[:-1]))
    if unit == 'w':
        return timedelta(weeks=int(dur[:-1]))
    raise argparse.ArgumentTypeError("Malformed backup plan.")


def parse_backup_plan(arg):
    def parse_interval(interval):
        (k, v) = interval.split(":")
        return (parse_duration(k), int(v))

    plan = [parse_interval(k) for k in arg.split(',')]
    return plan if len(plan) > 0 else None


def positive_int(arg):
    i = int(arg)
    if i <= 0:
        raise argparse.ArgumentTypeError(f'Illegal argument: {arg}')
    return i


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='biu',
        description='a backup program')
    parser.add_argument('-n', '--dry_run',
                        action=argparse.BooleanOptionalAction, default=True)

    subparsers = parser.add_subparsers(dest='command')

    cleanup = subparsers.add_parser(
        'cleanup', help='Removes obsolete backups')
    cleanup.add_argument('-p', '--retention_plan',
                         required=True, type=parse_backup_plan)
    cleanup.add_argument('-f', '--force_delete',
                         type=positive_int, default=0)
    cleanup.add_argument('-b', '--backup_path', type=Path)


    backup = subparsers.add_parser('backup', help='Produces a new backup')
    backup.add_argument('-t', '--temp_path', type=Path)
    backup.add_argument('-s', '--source', type=Path,
                        action='append', required=True)
    backup.add_argument('-a', '--archive', type=bool)
    backup.add_argument('-b', '--backup_path', type=Path)


    return parser.parse_args()


def main():
    args = parse_arguments()

    if not args.backup_path.exists():
        sys.exit(f'Backup path does not exist: {args.path}')

    runner = Runner(dry_run=args.dry_run)
    if args.command == "cleanup":
        opts = CleanupOptions(
            retention_plan=args.retention_plan, force_delete=args.force_delete, path=args.backup_path)
        cleanup_command(opts, runner)
    elif args.command == 'backup':
        opts = BackupOptions(backup_path=args.backup_path,
                             temp_path=args.temp_path, source_paths=args.source, archive_mode=args.archive)
        backup_command(opts, runner)


if __name__ == '__main__':
    main()
