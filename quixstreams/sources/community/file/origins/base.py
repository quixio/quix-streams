from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Union

__all__ = (
    "Origin",
    "ExternalOrigin",
)


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
    def get_folder_count(self, folder: Path) -> int: ...

    """Counts the number of folders at filepath to assume partition counts."""

    @abstractmethod
    def get_raw_file_stream(self, filepath: Path) -> BinaryIO: ...

    """
    Obtain a file and return it as an (open) filestream.
    
    Result should be ready for deserialization (and/or decompression).
    """


@dataclass
class ExternalOrigin(Origin, ABC):
    """An interface for interacting with an external file-based client"""

    _client: Any
    root_location: Union[str, Path]
