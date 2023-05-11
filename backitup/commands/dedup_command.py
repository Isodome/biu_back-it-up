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
from dataclasses import dataclass
from typing import List
from contextlib import ExitStack

from backitup.backups.backup import (
    list_backups,
    FileOperation,
    BackupLogOrder,
    BackupLogEntry,
    BackupLogIter)
from backitup.commands.cmd import Runner
from backitup.commands.compare_files import find_content_duplicates_of, group_duplicates


@dataclass
class DedupOptions:
    backup_path: pathlib.Path
    batch_size: int


def dedup_key(log_entry: BackupLogEntry) -> str:
    # This works ok since the log entries are already sorted by hash.
    # Collisions are extremly unlikely at this point.
    return log_entry.hash + log_entry.mtime


def batched_as_dict(backup_log: BackupLogIter, n: int):
    if not backup_log.peek():
        return

    prev_key = backup_log.peek().hash
    min_hash = max_hash = backup_log.peek().hash
    potential_dups = list[BackupLogEntry]()
    result = dict()

    for log_entry in backup_log:
        if not log_entry:
            continue
        key = dedup_key(log_entry)
        if key == prev_key:
            potential_dups.append(log_entry)
        else:
            result[prev_key] = potential_dups
            if len(result) >= n:
                yield (result, min_hash, max_hash)
                min_hash = log_entry.hash
                result = dict()
            prev_key = key
            max_hash = log_entry.hash
            potential_dups = [log_entry]

    result[prev_key] = potential_dups
    yield (result, min_hash, max_hash)


def replace_files_with_links(target, files, runner):
    for dup in files:
        runner.link(target, dup)


def dedup_against_old_backups(new_backup: BackupLogIter, old_backups: List[BackupLogIter], opts: DedupOptions, runner):

    # Since we don't want to use too much memory at once, we'll read a batch
    # of the new files and go through all the previous backup logs to look for
    # dups.
    for (batch, min_hash, max_hash) in batched_as_dict(new_backup, opts.batch_size):
        for old_backup_log in old_backups:
            while True:
                prev_log_entry = old_backup_log.peek()
                if not prev_log_entry:
                    break
                old_file_hash = prev_log_entry.hash
                if old_file_hash > max_hash:
                    break
                if old_file_hash >= min_hash:
                    dups = batch.pop(dedup_key(prev_log_entry), None)
                    if dups:
                        res = find_content_duplicates_of(
                            prev_log_entry.path, [dup.path for dup in dups])
                        # If the same-hash files don't have the same content, we need to put them back in our list.
                        # This will happen extremley rarely - maybe never :)
                        if res.no_dups:
                            batch[old_file_hash] = res.no_dups

                        replace_files_with_links(
                            prev_log_entry.path, res.dups, runner)

                next(old_backup_log)

            old_backup_log.suspend()

        # At this point we checked all the old backups. Entries that remain in the map are new files
        # that we never saw at an ealier date. We still may dedup them against each other.
        for p_dups in (pdups for pdups in batch.values() if len(pdups) > 1):
            for group_of_dupes in group_duplicates(p_dups):
                if len(group_of_dupes) > 1:
                    replace_files_with_links(
                        group_of_dupes[0], group_of_dupes[1:], runner)


def dedup_command(opts: DedupOptions, runner):
    backups = list_backups(opts.backup_path)
    if len(backups) == 0:
        return

    new_backup = backups[-1]
    old_backups = backups[:-1]

    runner.comment(
        f'Deduping {new_backup.directory.name}'
        f' against {len(old_backups)} previous backup(s).')

    with ExitStack() as stack:
        new_backup_log = stack.enter_context(
            new_backup.read_backup_log(
                BackupLogOrder.BY_HASH,
                filter=FileOperation.WRITE))
        old_backup_logs = [
            stack.enter_context(b.read_backup_log(BackupLogOrder.BY_HASH,
                                                  filter=FileOperation.WRITE))
            for b in old_backups]

        dedup_against_old_backups(
            new_backup_log, old_backup_logs, opts, runner)
