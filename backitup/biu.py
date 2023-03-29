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
from datetime import timedelta
from pathlib import Path

from commands.cleanup_command import cleanup_command, CleanupOptions
from commands.backup_command import backup_command, BackupOptions
from commands.dedup_command import dedup_command, DedupOptions
from commands.cmd import Runner


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


def parse_retention_plan(arg):
    if not arg:
        return None

    def parse_interval(interval):
        (k, v) = interval.split(":")
        return (parse_duration(k), int(v))

    plan = [parse_interval(k) for k in arg.split(',')]
    return plan if len(plan) > 0 else None


def positive_int(arg):
    if not arg.isdigit():
        return None
    i = int(arg)
    if i < 0:
        return None
    return i


def parse_arguments():
    parser = argparse.ArgumentParser(
        prog='biu',
        description='a backup program')

    parser.add_argument(
        'command', type=str, help='Bla', choices=['backup', 'cleanup', 'dedup'])
    parser.add_argument('-n', '--dry_run',
                        action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument('-b', '--backup_path', type=Path)
    parser.add_argument('-p', '--retention_plan',
                        default="1d:2,1w:4,1d:14,1w:8,1m:60", type=str)
    parser.add_argument('-f', '--force_delete', type=str)
    parser.add_argument('-s', '--source', type=Path, action='append')
    parser.add_argument('-a', '--archive_mode', type=bool)

    if len(sys.argv) == 1:
        parser.print_help(sys.stderr)
        sys.exit(1)
    return parser.parse_args()


def backup_path_from_args(args):
    if not args.backup_path:
        sys.exit("Required argument --backup_path was not provided.")
    if not args.backup_path.exists():
        sys.exit("The provided --backup_path does not exist.")
    if not args.backup_path.is_dir():
        sys.exit("The provided --backup_path is not a directory.")
    return args.backup_path


def source_from_args(args):
    print('foo')
    if not args.source:
        sys.exit("Required argument --source was not provided.")
    for source in args.source:
        if not source.is_dir():
            sys.exit(f"The provided --source={source} does not exist.")
    return args.source


def force_delete_from_args(args):
    if args.force_delete and not positive_int(args.force_delete):
        fd = positive_int(args.force_delete)
        if fd is None or fd < 0:
            sys.exit(
                f'--force_delete={args.force_delete} not allowed. It must be a positive integer.')
    return fd


def retention_plan_from_args(args, optional=False):
    if not args.retention_plan:
        if optional:
            return None
        else:
            sys.exit(f'Required argument --retention_plan was not provided.')
    if not parse_retention_plan(args.retention_plan):
        sys.exit(
            f'--retention_plan={args.retention_plan} not allowed. Please read the docs to learn about retention plans.')
    return parse_retention_plan(args.retention_plan)


def dedup_options_from_args(args):
    return DedupOptions(backup_path=backup_path_from_args(args))


def cleanup_options_from_args(args):
    return CleanupOptions(
        retention_plan=retention_plan_from_args(args),
        force_delete=force_delete_from_args(args),
        path=backup_path_from_args(args))


def backup_options_from_args(args):
    return BackupOptions(backup_path=backup_path_from_args(args),
                         source_paths=source_from_args(args),
                         archive_mode=args.archive or False)


def main():
    args = parse_arguments()

    runner = Runner(dry_run=args.dry_run)
    if args.command == "cleanup":
        cleanup_command(cleanup_options_from_args(args), runner)
    elif args.command == 'backup':
        backup_command(backup_options_from_args(args), runner)
    elif args.command == 'dedup':
        dedup_command(dedup_options_from_args(args), runner)


if __name__ == '__main__':
    main()
