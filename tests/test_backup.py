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

from backitup.commands.backup_command import backup_command, BackupOptions
from backitup.commands.cmd import Runner
from tests.repo import Repo


def test_backup_simple(tmp_path):
    repo = Repo(tmp_path)

    repo.write_data({Path('test_file.txt'): "test content"})

    opts = BackupOptions(
        source_paths=[repo.data_path()],
        backup_path=repo.backup_path())
    with freeze_time("2023-05-04 03:00:00"):
        backup_command(opts, Runner(dry_run=False))
    with freeze_time("2023-05-04 03:01:00"):
        backup_command(opts, Runner(dry_run=False))

    assert repo.backup_files() == {
        Path('2023-05-04_03-00/source/test_file.txt'): "test content",
        Path('2023-05-04_03-01/source/test_file.txt'): "test content"}

    assert repo.are_same_inode(
        [Path('2023-05-04_03-00/source/test_file.txt'),
         Path('2023-05-04_03-01/source/test_file.txt')])


def test_backup_links(tmp_path):
    repo = Repo(tmp_path)

    source = repo.data_path()
    repo.write_data({Path('test_file.txt'): "test content"})
    source.joinpath('sym_link.txt').symlink_to(
        source.joinpath('test_file.txt'))
    source.joinpath('hard_link.txt').hardlink_to(
        source.joinpath('test_file.txt'))

    opts = BackupOptions(
        source_paths=[repo.data_path()],
        backup_path=repo.backup_path())
    with freeze_time("2023-05-04 03:00:00"):
        backup_command(opts, Runner(dry_run=False))
    with freeze_time("2023-05-04 03:01:00"):
        backup_command(opts, Runner(dry_run=False))

    assert repo.backup_files() == {
        Path('2023-05-04_03-00/source/test_file.txt'): "test content",
        Path('2023-05-04_03-00/source/hard_link.txt'): 'test content',
        Path('2023-05-04_03-00/source/sym_link.txt'): 'test content',
        Path('2023-05-04_03-01/source/test_file.txt'): "test content",
        Path('2023-05-04_03-01/source/sym_link.txt'): "test content",
        Path('2023-05-04_03-01/source/hard_link.txt'): 'test content'}

    assert repo.are_same_inode(
        [Path('2023-05-04_03-00/source/test_file.txt'),
         Path('2023-05-04_03-00/source/hard_link.txt'),
         Path('2023-05-04_03-01/source/test_file.txt'),
         Path('2023-05-04_03-01/source/hard_link.txt')])


def test_backup_log(tmp_path):
    repo = Repo(tmp_path)
    opts = BackupOptions(
        source_paths=[repo.data_path()],
        backup_path=repo.backup_path())

    with freeze_time("2023-05-03 00:00"):
        repo.write_data({Path('test_file.txt'): "test content",
                        Path('tmp.txt'): "I'll be deleted",
                        Path('pics/large_image.jpg'): "much blob of large"})

    with freeze_time("2023-05-04 03:00:00"):
        backup_command(opts, Runner(dry_run=False))

    with freeze_time("2023-05-04 17:00:00"):
        repo.write_data({Path('test_file.txt'): "updated content"})
        repo.delete_file(Path('tmp.txt'))

    with freeze_time("2023-05-04 03:01:00"):
        backup_command(opts, Runner(dry_run=False))

    assert repo.backup_files(include_data=False, include_internal=True) == {
        Path('2023-05-04_03-00/backup.log'):
        '+;122566cfb6aea24f;2023/05/03-02:00:00;source/test_file.txt\n'
        '+;2bc7d17f89674713;2023/05/03-02:00:00;source/tmp.txt\n'
        '+;a45ed86e585760b4;2023/05/03-02:00:00;source/pics/large_image.jpg\n',
        Path('2023-05-04_03-01/backup.log'):
        '-;                ;1970/01/01-01:00:00;source/tmp.txt\n'
        '+;4e052c0e120dbf96;2023/05/04-19:00:00;source/test_file.txt\n'}


def test_multiple_sources(tmp_path):
    repo = Repo(tmp_path)

    repo.write_data({Path('test_file.txt'): "test content"},
                    data_dir='source1')
    repo.write_data({Path('test_file.txt'): "test content"},
                    data_dir='source2')

    opts = BackupOptions(
        source_paths=[repo.data_path('source1'), repo.data_path('source2')],
        backup_path=repo.backup_path())

    with freeze_time("2023-05-04 03:00:00"):
        backup_command(opts, Runner(dry_run=False))
    with freeze_time("2023-05-04 03:01:00"):
        backup_command(opts, Runner(dry_run=False))

    assert repo.backup_files() == {
        Path('2023-05-04_03-00/source1/test_file.txt'): "test content",
        Path('2023-05-04_03-00/source2/test_file.txt'): "test content",
        Path('2023-05-04_03-01/source1/test_file.txt'): "test content",
        Path('2023-05-04_03-01/source2/test_file.txt'): "test content"}

    assert repo.are_same_inode(
        [Path('2023-05-04_03-00/source1/test_file.txt'),
         Path('2023-05-04_03-01/source1/test_file.txt')])
    assert repo.are_same_inode(
        [Path('2023-05-04_03-00/source2/test_file.txt'),
         Path('2023-05-04_03-01/source2/test_file.txt')])
