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

import pathlib
import stat
from collections import namedtuple
from typing import List

from backitup.backups.backup import BackupLogEntry

BUFFER_SIZE = 10*1024


def read_whole_file(path):
    with path.open('rb') as f:
        return f.read()


def file_contents_identical(lhs, rhs, lhs_bytes=None):
    with lhs.open('rb') as f_lhs, rhs.open('rb') as f_rhs:
        while True:
            buf_lhs = f_lhs.read(BUFFER_SIZE)
            buf_rhs = f_rhs.read(BUFFER_SIZE)
            if not buf_lhs:
                return not buf_rhs
            if buf_lhs != buf_rhs:
                return False


def file_bytes_are(bytes, path):
    with path.open('rb') as f:
        return bytes == f.read(BUFFER_SIZE) and not f.read(1)


DedupResult = namedtuple("DedupResult", "dups no_dups")


def find_content_duplicates_of(hero_file: pathlib.Path, candidate_dups):
    if not (hero_file and candidate_dups):
        return DedupResult(dups=[], no_dups=candidate_dups)

    stat_ex = hero_file.stat()
    if not stat.S_ISREG(stat_ex.st_mode):
        return DedupResult(dups=[], no_dups=candidate_dups)

    buffer = None
    if len(candidate_dups) > 1 and stat_ex.st_size <= BUFFER_SIZE:
        buffer = read_whole_file(hero_file)

    no_dups = []
    dups = []
    for candidate_dup in candidate_dups:
        identical = file_bytes_are(buffer, candidate_dup) if buffer else file_contents_identical(
            hero_file, candidate_dup)
        if identical:
            dups.append(candidate_dup)
        else:
            no_dups.append(candidate_dup)
    return DedupResult(dups=dups, no_dups=no_dups)


def group_duplicates(candidate_dups: List[BackupLogEntry]):
    """Finds all duplicates in a list of files.

    Args:
        candidate_dups (List[BackupLogEntry]): a list of files

    Returns:
        List[List[BackupLogEntry]]: A list of lists. Each list contains a set of dups.
    """
    if not candidate_dups:
        return []
    result = []
    no_dups = candidate_dups

    while no_dups:
        cand = no_dups[0]
        dups, no_dups = find_content_duplicates_of(
            cand.path, [f.path for f in no_dups[1:]])
        dups.append(cand.path)
        result.append(dups)
    return result
