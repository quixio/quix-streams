from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

from .base import FileOrigin

__all__ = ("LocalFileOrigin",)


class LocalFileOrigin(FileOrigin):
    def __init__(
        self,
    ):
        self._client = None
        self._credentials = {}
        self.root_location = "/"

    @property
    def client(self):
        return

    def file_collector(
        self, filepath: Optional[Path] = None
    ) -> Generator[Path, None, None]:
        if filepath.is_dir():
            for i in sorted(filepath.iterdir(), key=lambda x: x.name):
                yield from self.file_collector(i)
        else:
            yield filepath

    def get_root_folder_count(self, folder: Path) -> int:
        return len([f for f in folder.iterdir()])

    def get_raw_file_stream(self, filepath: Path) -> BytesIO:
        with open(filepath, "rb") as f:
            return BytesIO(f.read())
