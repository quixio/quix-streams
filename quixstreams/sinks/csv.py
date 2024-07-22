import csv
import json
import os

from .base import Sink, SinkBatch


class CSVSink(Sink):
    """
    A base CSV sink that appends data to a single file
    """

    def __init__(self, path: str, dialect: str = "excel", header: bool = True):
        super().__init__()
        self.path = path
        self.dialect = dialect
        self.header = header

    def write(self, batch: SinkBatch):
        is_new = not os.path.exists(self.path)
        fieldnames = (
            "__key",
            "__timestamp",
            "__value",
            "__topic",
            "__partition",
            "__offset",
        )
        with open(self.path, "a") as f:
            writer = csv.DictWriter(f, dialect=self.dialect, fieldnames=fieldnames)
            if is_new:
                writer.writeheader()

            for item in batch:
                writer.writerow(
                    {
                        "__key": json.dumps(item.key.decode()),
                        "__timestamp": json.dumps(item.timestamp),
                        "__value": json.dumps(item.value),
                        "__topic": batch.topic,
                        "__partition": batch.partition,
                        "__offset": item.offset,
                    }
                )
