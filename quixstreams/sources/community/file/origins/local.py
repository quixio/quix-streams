from io import BytesIO
from pathlib import Path
from typing import Generator

from typing_extensions import Self

from .base import Origin

__all__ = ("LocalOrigin",)


class LocalOrigin(Origin):
    def file_collector(self, filepath: Path) -> Generator[Path, None, None]:
        if filepath.is_dir():
            for i in sorted(filepath.iterdir(), key=lambda x: x.name):
                yield from self.file_collector(i)
        else:
            yield filepath

    def get_folder_count(self, directory: Path) -> int:
        return len([f for f in directory.iterdir()])

    def get_raw_file_stream(self, filepath: Path) -> BytesIO:
        return BytesIO(filepath.read_bytes())

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
