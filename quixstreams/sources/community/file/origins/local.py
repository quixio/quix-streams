from io import BytesIO
from pathlib import Path
from typing import Generator

from .base import Origin

__all__ = ("LocalOrigin",)


class LocalOrigin(Origin):
    def __init__(
        self,
    ):
        self._client = None
        self._credentials = {}
        self.root_location = "/"

    @property
    def client(self):
        return

    def file_collector(self, filepath: Path) -> Generator[Path, None, None]:
        if filepath.is_dir():
            for i in sorted(filepath.iterdir(), key=lambda x: x.name):
                yield from self.file_collector(i)
        else:
            yield filepath

    def get_folder_count(self, folder: Path) -> int:
        return len([f for f in folder.iterdir()])

    def get_raw_file_stream(self, filepath: Path) -> BytesIO:
        return BytesIO(filepath.read_bytes())
