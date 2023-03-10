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

import argparse
from commands.cleanup_command import cleanup_command
from datetime import  timedelta

def parse_duration(dur):
    if len(dur) < 2:
        raise argparse.ArgumentTypeError("Illegal value for duration.")
    unit = dur[-1]
    if unit == 'h':
        return timedelta(hours = int(dur[:-1]))
    if unit == 'd':
        return timedelta(days = int(dur[:-1]))
    if unit == 'w':
        return timedelta(weeks = int(dur[:-1]))
    raise argparse.ArgumentTypeError("Illegal value for duration.")
    

def parse_backup_plan(arg):
    plan = {}
    intervals = arg.split(',')

    def parse_interval(interval):
        (k,v) = interval.split(":")
        return (parse_duration(k), int(v))

    plan = {parse_interval(k) for k in intervals}
    return plan if len(plan)>0 else None



def parse_args():
    parser = argparse.ArgumentParser(
                        prog = 'backup-janitor',
                        description = 'What the program does',
                        epilog = 'Text at the bottom of help')
    parser.add_argument('command')  
    parser.add_argument('-p', '--plan', required = True, type=parse_backup_plan)
    parser.add_argument('path')  

    return parser.parse_args()


def main():
    args = parse_args()
    
    if args.command == "cleanup":
        cleanup_command(args.plan, args.path)



if __name__ == '__main__':
    main()