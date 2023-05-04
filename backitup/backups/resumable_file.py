
import io


class ResumableFile:
    path: str
    file_handle: io.TextIOBase
    seek_position: int = 0
    peek_cache: str = ''

    def __init__(self, path, line_start_filter):
        self.path = path
        self.line_start_filter = line_start_filter

    def __iter__(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def suspend(self):
        if not self.file_handle.closed:
            self.seek_position = self.file_handle.tell()
            self.close()

    def peek(self):
        if not self.peek_cache:
            try:
                self.peek_cache = next(self)
            except StopIteration:
                self.close()
                return None
        return self.peek_cache

    def close(self):
        if self.file_handle and not self.file_handle.closed:
            self.file_handle.close()

    def __next__(self):
        if not self.file_handle:
            self.file_handle = open(self.path, 'r')
            self.file_handle.seek(self.seek_position)
        if self.peek_cache:
            tmp = self.peek_cache
            self.peek_cache = ''
            return tmp

        while True:
            line = self.file_handle.readline()
            if not line:
                raise StopIteration
            if line.startswith(self.line_start_filter):
                return line
