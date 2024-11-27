from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, BinaryIO, Iterable, Union

__all__ = ("FileOrigin",)


@dataclass
class FileOrigin:
    """
    An interface for interacting with a file-based client.

    Provides methods for navigating folders and retrieving/opening raw files.
    """

    _client: Any
    _credentials: Union[dict, str]
    root_location: Union[str, Path]

    @property
    @abstractmethod
    def client(self): ...

    """
    Set _client here.
    
    Circumvents pickling issues with multiprocessing by init-ing the client in a 
    later step (when Application.run() is called).
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
