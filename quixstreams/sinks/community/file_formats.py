import abc
from typing import Any, List

from quixstreams.models.messages import KafkaMessage

__all__ = ["BatchFormat"]


# TODO: Document the compatible topic formats for each formatter
# TODO: Check the types of the values before serializing


class BatchFormat:
    """
    Base class to format batches for S3 Sink
    """

    @property
    @abc.abstractmethod
    def file_extension(self) -> str: ...

    # TODO: This probably needs to move to sources.
    @abc.abstractmethod
    def deserialize_value(self, value: bytes) -> List[KafkaMessage]: ...

    @abc.abstractmethod
    def serialize_batch_values(self, values: List[Any]) -> bytes: ...
