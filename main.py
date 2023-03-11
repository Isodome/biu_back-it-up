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
import re

import argparse
from commands.cleanup_command import cleanup_command, CleanupOptions
from datetime import timedelta


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
        prog='backup-janitor',
        description='a backup program')
    subparsers = parser.add_subparsers(dest='command')

    cleanup = subparsers.add_parser(
        'cleanup', help='Removes obsolete backups')
    cleanup.add_argument('-p', '--retention_plan',
                         required=True, type=parse_backup_plan)
    cleanup.add_argument('-f', '--force_delete',
                         type=positive_int, default=0)
    cleanup = subparsers.add_parser(
        'backup', help='Creates a backup')

    parser.add_argument('path')

    return parser.parse_args()


def main():
    args = parse_arguments()

    if args.command == "cleanup":
        opts = CleanupOptions(
            retention_plan=args.retention_plan, force_delete=args.force_delete, path=args.path)
        cleanup_command(opts)


if __name__ == '__main__':
    main()
