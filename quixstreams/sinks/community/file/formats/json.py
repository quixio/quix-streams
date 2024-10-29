import json
from gzip import compress as gzip_compress
from io import BytesIO
from typing import Any, Callable, Optional

from jsonlines import Writer

from quixstreams.models.messages import KafkaMessage

from .base import BatchFormat


class JSONFormat(BatchFormat):
    # TODO: Docs
    def __init__(
        self,
        dumps: Optional[Callable[[Any], bytes]] = None,
        loads: Optional[Callable[[bytes], Any]] = None,
        file_extension: str = ".json",
        compress: bool = False,
    ):
        self._dumps = dumps or json.dumps
        self._loads = loads or json.loads
        self._compress = compress
        self._file_extension = file_extension
        if self._compress:
            self._file_extension += ".gz"

    @property
    def file_extension(self) -> str:
        return self._file_extension

    def deserialize_value(self, value: bytes) -> Any:
        lines = value.decode("utf-8").splitlines()

        for line in lines:
            json_doc = self._loads(line)

            yield KafkaMessage(
                json_doc["key"],
                json.dumps(json_doc),
                None,
                timestamp=json_doc["timestamp"],
            )

    def serialize_batch_values(self, values: list[any]) -> bytes:
        with BytesIO() as f:
            with Writer(f, compact=True, dumps=self._dumps) as writer:
                for row in values:
                    obj = {
                        "timestamp": row.timestamp,
                        "key": bytes.decode(row.key),
                        "value": json.dumps(row.value),
                    }
                    writer.write(obj)
            value_bytes = f.getvalue()
            if self._compress:
                value_bytes = gzip_compress(value_bytes)
            return value_bytes
