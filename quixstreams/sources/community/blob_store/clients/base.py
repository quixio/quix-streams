from abc import abstractmethod
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import Any, Iterable, Union

__all__ = ("BlobClient",)


@dataclass
class BlobClient:
    _client: Any
    _credentials: Union[dict, str]
    location: Union[str, Path]

    @property
    @abstractmethod
    def client(self): ...

    """
    Set _client here.
    
    Circumvents pickling issues with multiprocessing by init'ing the client in a 
    later step (when Application.run() is called).
    """

    @abstractmethod
    def blob_collector(self, folder: Path) -> Iterable[Path]: ...

    """
    Find all blobs starting from a root folder.
    
    Each item in the iterable should be a filepath resolvable by `get_raw_blob_stream`.
    """

    @abstractmethod
    def get_root_folder_count(self, filepath: Path) -> int: ...

    """Counts the number of folders at filepath to assume partition counts."""

    @abstractmethod
    def get_raw_blob_stream(self, blob_path: Path) -> BytesIO: ...

    """
    Obtain a specific blob in its raw form.
    
    Result should be ready for deserialization (and/or decompression).
    """
