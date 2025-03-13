import logging
from io import BytesIO
from pathlib import Path
from typing import BinaryIO, Callable, Iterable, Optional, Union

from quixstreams.sources import (
    ClientConnectFailureCallback,
    ClientConnectSuccessCallback,
)

from .base import FileSource
from .compressions import CompressionName
from .formats import Format, FormatName

__all__ = ("LocalFileSource",)

logger = logging.getLogger(__name__)


class LocalFileSource(FileSource):
    """
    An interface for interacting with a file-based client.

    Provides methods for navigating folders and retrieving/opening raw files.
    """

    def __init__(
        self,
        directory: Union[str, Path],
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        super().__init__(
            directory=directory,
            key_setter=key_setter,
            value_setter=value_setter,
            timestamp_setter=timestamp_setter,
            file_format=file_format,
            compression=compression,
            replay_speed=replay_speed,
            name=name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

    def get_file_list(self, filepath: Path) -> Iterable[Path]:
        if filepath.is_dir():
            for i in sorted(filepath.iterdir(), key=lambda x: x.name):
                yield from self.get_file_list(i)
        else:
            yield filepath

    def read_file(self, filepath: Path) -> BinaryIO:
        return BytesIO(filepath.read_bytes())
