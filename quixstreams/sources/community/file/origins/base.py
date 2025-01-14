from abc import ABC, abstractmethod
from pathlib import Path
from typing import BinaryIO, Iterable

from typing_extensions import Self

__all__ = ("Origin",)


class Origin(ABC):
    """
    An interface for interacting with a file-based client.

    Provides methods for navigating folders and retrieving/opening raw files.
    """

    @abstractmethod
    def file_collector(self, filepath: Path) -> Iterable[Path]: ...

    """
    Find all blobs starting from a root folder.
    
    Each item in the iterable should be a filepath resolvable by `get_raw_file_stream`.
    """

    @abstractmethod
    def get_folder_count(self, directory: Path) -> int: ...

    """Counts the number of folders at directory to assume partition counts."""

    @abstractmethod
    def get_raw_file_stream(self, filepath: Path) -> BinaryIO: ...

    """
    Obtain a file and return it as an (open) filestream.
    
    Result should be ready for deserialization (and/or decompression).
    """

    @abstractmethod
    def __enter__(self) -> Self: ...

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb): ...
