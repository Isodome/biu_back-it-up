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

import subprocess


class BaseCommand:
    def get_command():
        pass

    def setup_args(subparser):
        pass


class Runner:

    dry_run = False

    def __init__(self, dry_run):
        self.dry_run = dry_run

    def for_shell(self, s):
        if ' ' in s:
            s = s.replace('"', r'\"')
            return f'"{s}"'
        return s

    def print_command(self, args, stdout_to_file):
        command = " ".join((self.for_shell(a) for a in args))
        if stdout_to_file:
            command += f' > "{stdout_to_file}"'
        print(command)

    def comment(self, comment):
        if self.dry_run:
            print(f"# {comment}")
        else:
            print(comment)

    def run(self,  args, stdout_to_file=None):
        self.print_command(args, stdout_to_file)
        if self.dry_run:
            return
        try:
            outfile = open(stdout_to_file, 'w') if stdout_to_file else None
            result = subprocess.run(
                args, check=True, shell=False, stdout=outfile)
            print(result)
        except subprocess.CalledProcessError as e:
            print(e.output, e)
