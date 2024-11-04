import logging
from typing import BinaryIO, Callable, Generator, Optional

from ..compressions import CompressionName
from .base import Format

__all__ = ["JSONFormat"]

logger = logging.getLogger(__name__)


class JSONFormat(Format):
    def __init__(
        self,
        compression: Optional[CompressionName],
        loads: Optional[Callable[[str], dict]] = None,
    ):
        """
        Read a JSON-formatted file (along with decompressing it).

        :param compression: the compression type used on the file
        :param loads: A custom function to deserialize objects to the expected dict
            with {_key: str, _value: dict, _timestamp: int}.
        """
        import jsonlines

        self._reader = jsonlines.Reader
        super().__init__(compression)

        self._reader_arguments = {}
        if loads is not None:
            self._reader_arguments["loads"] = loads

    def deserialize(self, filestream: BinaryIO) -> Generator[dict, None, None]:
        reader = self._reader(filestream, **self._reader_arguments)
        for _dict in reader.iter(type=dict, skip_invalid=False):
            yield _dict
