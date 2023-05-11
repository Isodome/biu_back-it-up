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

from pathlib import Path
from freezegun import freeze_time
from backitup.commands.backup_command import BackupOptions, backup_command
from backitup.commands.cmd import Runner

from backitup.commands.dedup_command import DedupOptions, dedup_command
from tests.repo import Repo


def test_dedup_single_file(tmp_path):
    repo = Repo(tmp_path)
    runner = Runner(dry_run=False)
    opts = DedupOptions(backup_path=repo.backup_path(), batch_size=10)
    bopts = BackupOptions(
        source_paths=[repo.data_path()], backup_path=repo.backup_path())

    repo.write_data({Path('test_file.txt'): "test content"})

    with freeze_time("2023-05-04 04:00:00"):
        backup_command(bopts, runner)
        dedup_command(opts, runner)

    assert repo.backup_files() == {
        Path('2023-05-04_04-00/source/test_file.txt'): "test content"}


def test_move_file(tmp_path):
    repo = Repo(tmp_path)
    runner = Runner(dry_run=False)
    opts = DedupOptions(backup_path=repo.backup_path(), batch_size=10)
    bopts = BackupOptions(
        source_paths=[repo.data_path()], backup_path=repo.backup_path())

    repo.write_data({Path('test_file.txt'): "test content"})

    with freeze_time("2023-05-04 04:00:00"):
        backup_command(bopts, runner)
        dedup_command(opts, runner)

    repo.data_path(file_name='test_file.txt').rename(
        repo.data_path(file_name='actual_name.txt'))

    with freeze_time("2023-05-05 04:00:00"):
        backup_command(bopts, runner)
        dedup_command(opts, runner)

    assert len(repo.backup_files()) == 2
    assert repo.are_same_inode(
        [Path('2023-05-04_04-00/source/test_file.txt'),
         Path('2023-05-05_04-00/source/actual_name.txt')])


def test_sharded_dedup(tmp_path):
    repo = Repo(tmp_path)
    runner = Runner(dry_run=False)
    opts = DedupOptions(backup_path=repo.backup_path(), batch_size=2)
    bopts = BackupOptions(
        source_paths=[repo.data_path()], backup_path=repo.backup_path())

    with freeze_time("2023-05-04 04:00:00"):
        repo.write_data(
            {Path(f'test_file_{i}.txt'): f"test content_{i%2}" for i in range(100)})
        backup_command(bopts, runner)
        dedup_command(opts, runner)

    with freeze_time("2023-05-05 04:00:00"):
        repo.write_data(
            {Path(f'test_file_{i}.txt'): f"test content_{i%4}" for i in range(100, 200)})
        backup_command(bopts, runner)
        dedup_command(opts, runner)

    with freeze_time("2023-05-06 04:00:00"):
        repo.write_data(
            {Path(f'test_file_{i}.txt'): f"test content_{i%8}" for i in range(200, 300)})
        backup_command(bopts, runner)
        dedup_command(opts, runner)

    assert repo.are_same_inode(
        [Path(f'2023-05-04_04-00/source/test_file_{i}.txt') for i in range(100) if i % 2 == 0])
    assert repo.are_same_inode(
        [Path(f'2023-05-04_04-00/source/test_file_{i}.txt') for i in range(100) if i % 2 == 1])
    assert repo.are_same_inode(
        [Path(f'2023-05-05_04-00/source/test_file_{i}.txt') for i in range(200) if i % 4 == 0])
    assert repo.are_same_inode(
        [Path(f'2023-05-05_04-00/source/test_file_{i}.txt') for i in range(200) if i % 4 == 1])
