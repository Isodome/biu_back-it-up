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


class Runner:

    dry_run = False

    def __init__(self, dry_run):
        self.dry_run = dry_run

    def print_command(self, args):
        print(" ".join(args))

    def comment(self, comment):
        if self.dry_run:
            print(f"# {comment}")
        else:
            print(comment)

    def run(self,  args):
        self.print_command(args)
        if self.dry_run:
            return
        try:
            # result = subprocess.run(
            #     args, check=True, shell=True, capture_output=True)
            result = subprocess.check_output(
                args)
            print(result)
        except subprocess.CalledProcessError as e:
            print(e.output, e)
