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
import os
import filecmp

from dataclasses import dataclass
from commands.common import list_backups, FileOperation
from contextlib import ExitStack


@dataclass
class DedupOptions:
    backup_path: pathlib.Path = None


def group_and_dedup(candidate_dups, runner):
    if not candidate_dups:
        return

    unique_contents = []
    for candidate_dup in candidate_dups:
        dup = next((f for f in unique_contents if filecmp.cmp(
            candidate_dup.path, f.path, shallow=False)), None)
        if dup:
            runner.run(
                ['ln', '-f', dup.path, candidate_dup.path])
        else:
            unique_contents.append(candidate_dup)


def deduplicate_against_existing_file(candidate_dups, existing_file, runner):
    if not (existing_file and candidate_dups):
        return candidate_dups

    # Case 1: we found an existing file with the same hash.

    no_dups = []
    for candidate_dup in candidate_dups:
        if filecmp.cmp(existing_file.path, candidate_dup.path, shallow=False):
            runner.run(
                ['ln', '-f', existing_file.path, candidate_dup.path])
        else:
            no_dups.append(candidate_dup)
    # We found files that shared the hash with `existing_file` but contents didn't match.
    return no_dups


def batched_as_dict(backup_log, n: int):
    if not backup_log.peek():
        return

    current_hash = backup_log.peek().hash
    min_hash = current_hash
    max_hash = current_hash
    potential_dups = [next(backup_log)]
    result = dict()
    batch_counter = 0

    for log_entry in backup_log:
        if batch_counter >= n and backup_log.peek().hash != current_hash:
            yield (result, min_hash, max_hash)
        batch_counter += 1

        if log_entry.hash == current_hash:
            potential_dups.append(log_entry)
        else:
            result[current_hash] = potential_dups
            current_hash = log_entry.hash
            max_hash = current_hash
            potential_dups = [log_entry]

    result[current_hash] = potential_dups
    yield (result, min_hash, max_hash)


def dedup_command(opts: DedupOptions, runner):
    backups = list_backups(opts.backup_path)[:-3]
    if len(backups) == 0:
        return

    new_backup = backups[-1]
    old_backups = backups[:-1]

    runner.comment(
        f'Deduping {new_backup.directory.name} against {len(old_backups)} previous backup(s).')

    # Since we don't want to use too much memory at once, we'll read a batch of the new files and go through all the previous backup logs to look for dups.
    with ExitStack() as stack:
        new_backup_log = stack.enter_context(new_backup.read_backup_log(
            filter=FileOperation.WRITE))
        old_backup_logs = [stack.enter_context(b.read_backup_log(
            filter=FileOperation.WRITE)) for b in old_backups]

        for (entries_dict, min_hash, max_hash) in batched_as_dict(new_backup_log, 5000):

            for old_backup_log in old_backup_logs:
                old_backup_log.resume()

                while old_backup_log.peek():
                    old_log_entry = old_backup_logs.peek()
                    old_file_hash = old_log_entry.hash
                    if old_file_hash > max_hash:
                        break
                    if old_file_hash >= min_hash:
                        dups = entries_dict.pop(old_file_hash, None)
                        if dups:
                            dups = deduplicate_against_existing_file(
                                dups, old_log_entry.path, runner)
                        # If the same-hash files don't have the same content, we need to put them back in our list.
                        # This will happen extremley rarely - maybe never :)
                        if dups:
                            entries_dict[old_file_hash] = dups

                    next(old_backup_logs)

                old_backup_log.suspend()

            # At this point we checked all the old backups. Entries that remain in the map are new files never seen before.
            for dups in entries_dict.values():
                if len(dups) > 1:
                    group_and_dedup(dups, runner)
