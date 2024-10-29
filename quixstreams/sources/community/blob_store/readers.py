import abc
from gzip import decompress as gzip_decompress
from typing import Optional, Any, Literal, Iterable
from quixstreams.models.serializers.avro import AvroDeserializer
from quixstreams.models.serializers.json import JSONDeserializer


__all__ = ("BlobFormatReader", "AvroBlobReader", "COMPRESSIONS")


class BaseDecompressor:
    @abc.abstractmethod
    def decompress(self, value: bytes): ...

    @property
    @abc.abstractmethod
    def extension(self) -> str: ...


class GZipDecompressor(BaseDecompressor):
    def __init__(self, extension=".gz"):
        self._extension = extension

    def extension(self) -> str:
        return self._extension

    def decompress(self, value):
        return gzip_decompress(value)


COMPRESSIONS = {"gz": GZipDecompressor, "gzip": GZipDecompressor}


class BlobFormatReader:
    """
    Base class to format batches for Azure Blob Sink
    """

    @abc.abstractmethod
    def deserialize_blob(self, value: bytes) -> Iterable[Any]: ...


class AvroBlobReader(BlobFormatReader):
    """
    A format for deserializing batches of values to/from Avro format
    """

    def __init__(
        self,
        compression: Optional[Literal["gzip"]] = None,
    ):
        if compression:
            self._decompressor = COMPRESSIONS[compression]

        self._deserializer = AvroDeserializer(schema=self._get_default_schema())

    def deserialize_blob(self, value: bytes) -> Iterable[Any]:
        if self._decompressor:
            value = self._decompressor.decompress(value)
        return self._deserializer(value)

    def _get_default_schema(self) -> dict:
        return {
            "type": "record",
            "name": "KafkaMessage",
            "fields": [
                {"name": "_timestamp", "type": "long"},
                {"name": "_key", "type": ["null", "bytes"], "default": None},
                {"name": "_headers", "type": {"type": "map", "values": "bytes"}},
                {"name": "_value", "type": "bytes"},
            ],
        }


class JSONBlobReader(BlobFormatReader):
    """
    A format for deserializing batches of values from JSON.
    """

    def __init__(
        self,
        compression: Optional[Literal["gzip"]] = None,
    ):
        if compression:
            self._decompressor = COMPRESSIONS[compression]
        else:
            self._decompressor = None

        self._deserializer = JSONDeserializer()

    def deserialize_blob(self, value: bytes) -> Iterable[Any]:
        if self._decompressor:
            value = self._decompressor.decompress(value)
        return self._deserializer(value)
