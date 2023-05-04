

from pathlib import Path
from typing import Dict

DEFAULT_DATA_DIR = 'data'


class Repo:
    def __init__(self, tmp_path: Path):
        self.tmp_path = tmp_path
        self.backup_path().mkdir(parents=True, exist_ok=True)

    def data_path(self, name=DEFAULT_DATA_DIR):
        return self.tmp_path.joinpath(name)

    def backup_path(self):
        return self.tmp_path.joinpath('backups')

    def backup_files(self, include_internal: bool = False, include_data: bool = True):
        _INTERNAL_FILES = {'backup.log', 'backup_completed.txt'}
        backup_dir = self.backup_path()

        def filter(x): return x.is_file() and ((
            include_internal and x.name in _INTERNAL_FILES) or (
                include_data and x.name not in _INTERNAL_FILES))

        return {path.relative_to(backup_dir): path.read_text() for path in backup_dir.rglob('*') if filter(path)}

    def write_data(self, files: Dict[Path, str], data_dir=DEFAULT_DATA_DIR):
        for k, v in files.items():
            test_file = self.data_path(name=data_dir).joinpath(k)
            test_file.parent.mkdir(parents=True, exist_ok=True)
            test_file.write_text(v)
