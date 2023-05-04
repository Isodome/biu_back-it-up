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
        Path('2023-05-04_03-00/data/test_file.txt'): "test content",
        Path('2023-05-04_03-01/data/test_file.txt'): "test content"}
