from abc import abstractmethod
from dataclasses import dataclass
from io import BytesIO
from typing import Any, Iterable

__all__ = ("BlobClient",)


@dataclass
class BlobClient:
    client: Any
    location: str

    @abstractmethod
    def blob_finder(self, folder: str) -> Iterable[str]: ...

    """
    Find all blobs starting from a root folder.
    
    Each item in the iterable should be a filepath resolvable by `get_raw_blob`.
    """

    @abstractmethod
    def get_raw_blob(self, blob_path: str) -> BytesIO: ...

    """
    Obtain a specific blob in its raw form.
    
    Result should be ready for deserialization (and/or decompression).
    """
