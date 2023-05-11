

from datetime import datetime
import os
from pathlib import Path
from typing import Dict

DEFAULT_DATA_DIR = 'source'


class Repo:
    def __init__(self, tmp_path: Path):
        self.tmp_path = tmp_path
        self.backup_path().mkdir(parents=True, exist_ok=True)

    def data_path(self, data_dir=DEFAULT_DATA_DIR, file_name=None):
        if file_name is None:
            return self.tmp_path.joinpath(data_dir)
        return self.tmp_path.joinpath(data_dir).joinpath(file_name)

    def delete_file(self, path, data_dir=DEFAULT_DATA_DIR):
        path = self.data_path(data_dir).joinpath(path)
        path.unlink()

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
        epoch = datetime.now().timestamp()
        for k, v in files.items():
            test_file = self.data_path(data_dir).joinpath(k)
            test_file.parent.mkdir(parents=True, exist_ok=True)
            test_file.write_text(v)
            os.utime(test_file, (epoch, epoch))

    def are_same_inode(self, paths):
        if len(paths) < 2:
            return True
        full_paths = [self.backup_path().joinpath(p) for p in paths]

        ino = full_paths[0].stat().st_ino
        for path in full_paths[1:]:
            if path.stat().st_ino != ino:
                print(f"Found inode missmatch: {paths[0]} != {path}")
                return False

        return True
