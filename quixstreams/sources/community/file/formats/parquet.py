import logging
from io import BytesIO
from pathlib import Path
from typing import Generator, Optional

import pyarrow.parquet as pq

from ..compressions import CompressionName
from .base import Format

__all__ = ["ParquetFormat"]

logger = logging.getLogger(__name__)


class ParquetFormat(Format):
    def __init__(
        self,
        compression: Optional[CompressionName] = None,
    ) -> None:
        if compression:
            self._decompressor = self._get_decompressor(compression)
        else:
            self._decompressor = None

    def deserialize(self, filepath: Path) -> Generator[dict, None, None]:
        file = None
        try:
            logger.debug(f"Opening file at {filepath}...")
            if self._decompressor:
                file = BytesIO(self._decompressor.decompress(filepath))
            else:
                file = open(filepath, "rb")
            for _dict in pq.read_table(source=file).to_pylist():
                yield {
                    "_key": _dict["_key"],
                    "_timestamp": _dict["_timestamp"],
                    "_value": {
                        k: v
                        for k, v in _dict.items()
                        if k not in ["_key", "_timestamp"]
                    },
                }
        finally:
            if file:
                logger.debug(f"closing file at {filepath}...")
                file.close()
