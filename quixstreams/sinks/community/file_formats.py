import abc
import json
from gzip import compress as gzip_compress
from quixstreams.models.messages import KafkaMessage
from io import BytesIO
from typing import Optional, Callable, Any, List

from jsonlines import Writer

__all__ = ("BatchFormat", "JSONFormat", "BytesFormat", "ParquetFormat")


# TODO: Document the compatible topic formats for each formatter
# TODO: Check the types of the values before serializing


class BatchFormat:
    """
    Base class to format batches for S3 Sink
    """

    @property
    @abc.abstractmethod
    def file_extension(self) -> str:
        ...

    #TODO: This probably needs to move to sources.
    @abc.abstractmethod
    def deserialize_value(self, value: bytes) -> List[KafkaMessage]:
        ...

    @abc.abstractmethod
    def serialize_batch_values(self, values: List[Any]) -> bytes:
        ...

