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
import pathlib
import subprocess
import sys

from uuid import uuid4


class Runner:

    dry_run = False

    def __init__(self, dry_run):
        self.dry_run = dry_run

    def for_shell(self, s):
        s = str(s)
        special = [' ', '$', '?', '*', '!']
        if any(c in s for c in special):
            s = s.replace('"', r'\"')
            return f'"{s}"'
        return s

    def print_command(self, args, stdout_to_file=None):
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
        str_args = [str(arg) for arg in args]
        self.print_command(str_args, stdout_to_file)
        if self.dry_run:
            return
        try:
            outfile = open(stdout_to_file, 'w') if stdout_to_file else None
            result = subprocess.run(
                str_args, check=True, shell=False, stdout=outfile)
            print(result)
        except subprocess.CalledProcessError as e:
            print(e.output, e)

    def replace(self, src, dst):
        if self.dry_run:
            self.print_command(
                ['mv', '-f', str(src), str(dst)])
        else:
            os.replace(src, dst)

    def link(self, target, link: pathlib.Path):
        if self.dry_run:
            self.print_command(
                ['ln', '-f', str(target), str(link)])
        else:
            tmp = link.with_name(uuid4().hex)
            try:
                os.link(target, tmp)
            except os.error as e:
                os.remove(tmp)
                sys.exit('Failed to create hardlink.')
            os.replace(tmp, link)

    def remove(self, *files):
        if self.dry_run:
            self.print_command(
                ['rm', '-f', files])
        else:
            for file in files:
                os.remove(file)
