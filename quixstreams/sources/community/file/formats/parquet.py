import logging
from typing import BinaryIO, Generator

from .base import Format

__all__ = ["ParquetFormat"]

logger = logging.getLogger(__name__)


class ParquetFormat(Format):
    def __init__(
        self,
        compression=None,
    ) -> None:
        if compression:
            logger.info(
                "Parquet inherently knows the compression type of its files; "
                f"the provided type '{compression}' will be ignored."
            )
        try:
            import pyarrow.parquet as pq
        except ImportError as exc:
            raise ImportError(
                f"Package {exc.name} is missing: "
                'run "pip install quixstreams[parquet]" to use the Parquet file format '
                '(Note: options can be installed together i.e. "quixstreams[s3,parquet]")'
            ) from exc

        self._reader = pq.read_table
        super().__init__(None)

    def deserialize(self, filestream: BinaryIO) -> Generator[dict, None, None]:
        for _dict in self._reader(source=filestream).to_pylist():
            yield {
                "_key": _dict["_key"],
                "_timestamp": _dict["_timestamp"],
                "_value": {
                    k: v for k, v in _dict.items() if k not in ["_key", "_timestamp"]
                },
            }
