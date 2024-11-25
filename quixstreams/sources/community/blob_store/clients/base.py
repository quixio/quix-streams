from abc import abstractmethod
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Iterable, Union

__all__ = ("BlobClient",)


@dataclass
class BlobClient:
    _client: Any
    _credentials: Union[dict, str]
    location: str

    @abstractmethod
    @property
    def client(self): ...

    """
    Set _client here.
    
    Circumvents pickling issues with multiprocessing by init'ing the client in a 
    later step (when Application.run() is called).
    """

    @abstractmethod
    def blob_finder(self, folder: str) -> Iterable[str]: ...

    """
    Find all blobs starting from a root folder.
    
    Each item in the iterable should be a filepath resolvable by `get_raw_blob_stream`.
    """

    @abstractmethod
    def get_raw_blob_stream(self, blob_path: str) -> BytesIO: ...

    """
    Obtain a specific blob in its raw form.
    
    Result should be ready for deserialization (and/or decompression).
    """
