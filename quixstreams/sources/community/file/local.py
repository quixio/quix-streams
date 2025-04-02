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
    A source for extracting records stored within files in a local filesystem.

    It recursively iterates from the provided path (file or folder) and
    processes all found files by parsing and producing the given records contained
    in each file as individual messages to a kafka topic (same topic for all).
    """

    def __init__(
        self,
        filepath: Union[str, Path],
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        has_partition_folders: bool = False,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[ClientConnectFailureCallback] = None,
    ):
        """
        :param filepath: folder to recursively iterate from (a file will be used directly).
        :param key_setter: sets the kafka message key for a record in the file.
        :param value_setter: sets the kafka message value for a record in the file.
        :param timestamp_setter: sets the kafka message timestamp for a record in the file.
        :param file_format: what format the files are stored as (ex: "json").
        :param compression: what compression was used on the files, if any (ex. "gzip").
        :param has_partition_folders: whether files are nested within partition folders.
            If True, FileSource will match the output topic partition count with it.
            Set this flag to True if Quix Streams FileSink was used to dump data.
            Note: messages will only align with these partitions if original key is used.
            Example structure - a 2 partition topic (0, 1):
            [/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
        :param replay_speed: Produce messages with this speed multiplier, which
            roughly reflects the time "delay" between the original message producing.
            Use any float >= 0, where 0 is no delay, and 1 is the original speed.
            NOTE: Time delay will only be accurate per partition, NOT overall.
        :param name: The name of the Source application (Default: last folder name).
        :param shutdown_timeout: Time in seconds the application waits for the source
            to gracefully shut down.
        :param on_client_connect_success: An optional callback made after successful
            client authentication, primarily for additional logging.
        :param on_client_connect_failure: An optional callback made after failed
            client authentication (which should raise an Exception).
            Callback should accept the raised Exception as an argument.
            Callback must resolve (or propagate/re-raise) the Exception.
        """
        super().__init__(
            filepath=filepath,
            key_setter=key_setter,
            value_setter=value_setter,
            timestamp_setter=timestamp_setter,
            file_format=file_format,
            compression=compression,
            has_partition_folders=has_partition_folders,
            replay_speed=replay_speed,
            name=name,
            shutdown_timeout=shutdown_timeout,
            on_client_connect_success=on_client_connect_success,
            on_client_connect_failure=on_client_connect_failure,
        )

    def get_file_list(self, filepath: Path) -> Iterable[Path]:
        if filepath.is_dir():
            for fp in sorted(filepath.iterdir()):
                yield from self.get_file_list(fp)
        else:
            yield filepath

    def read_file(self, filepath: Path) -> BinaryIO:
        return BytesIO(filepath.read_bytes())

    def file_partition_counter(self) -> int:
        return len(list(self._filepath.iterdir()))
