import dataclasses
import time
import threading

from typing import Optional, Tuple, Union, List, Any

from confluent_kafka import OFFSET_INVALID

from quixstreams.sinks import BatchingSink
from quixstreams.sinks.base import SinkBatch
from quixstreams.sinks.base.item import SinkItem
from quixstreams.sources import Source

DEFAULT_TIMEOUT = 10.0


class Timeout:
    """
    Utility class to create time-limited `while` loops.

    It keeps track of the time passed since its creation, and checks if the timeout
    expired on each `bool(Timeout)` check.

    Use it while testing the `while` loops to make sure they exit at some point.
    """

    def __init__(self, seconds: float = DEFAULT_TIMEOUT):
        self._end = time.monotonic() + seconds

    def __bool__(self):
        expired = time.monotonic() >= self._end
        if expired:
            raise TimeoutError("Timeout expired")
        return True


@dataclasses.dataclass
class TopicPartitionStub:
    topic: str
    partition: int
    offset: int = OFFSET_INVALID


class ConfluentKafkaMessageStub:
    """
    A stub object to mock `confluent_kafka.Message`.

    Instances of `confluent_kafka.Message` cannot be directly created from Python,
    see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

    """

    def __init__(
        self,
        topic: str = "test",
        partition: int = 0,
        offset: int = 0,
        timestamp: Tuple[int, int] = (1, 123),
        key: bytes = None,
        value: bytes = None,
        headers: Optional[List[Tuple[str, bytes]]] = None,
        latency: float = None,
        leader_epoch: int = None,
    ):
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._timestamp = timestamp
        self._key = key
        self._value = value
        self._headers = headers
        self._latency = latency
        self._leader_epoch = leader_epoch

    def headers(self, *args, **kwargs) -> Optional[List[Tuple[str, bytes]]]:
        return self._headers

    def key(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        return self._key

    def offset(self, *args, **kwargs) -> int:
        return self._offset

    def partition(self, *args, **kwargs) -> int:
        return self._partition

    def timestamp(self, *args, **kwargs) -> (int, int):
        return self._timestamp

    def topic(self, *args, **kwargs) -> str:
        return self._topic

    def value(self, *args, **kwargs) -> Optional[Union[str, bytes]]:
        return self._value

    def latency(self, *args, **kwargs) -> Optional[float]:
        return self._latency

    def leader_epoch(self, *args, **kwargs) -> Optional[int]:
        return self._leader_epoch

    def __len__(self) -> int:
        return len(self._value)


class DummySink(BatchingSink):
    def __init__(self):
        super().__init__()
        self._results = []

    def write(self, batch: SinkBatch):
        for item in batch:
            self._results.append(item)

    @property
    def results(self) -> List[SinkItem]:
        return self._results

    @property
    def total_batched(self) -> int:
        return sum(batch.size for batch in self._batches.values())


class DummySource(Source):
    def __init__(
        self,
        name="dummy",
        values: Optional[List[Any]] = None,
        finished: threading.Event = None,
        error_in: Union[str, List[str]] = None,
        pickeable_error: bool = True,
    ) -> None:
        super().__init__(name, 1)

        self.key = "dummy"
        self.values = values or []
        self.finished = finished
        self.error_in = error_in or []
        self.pickeable_error = pickeable_error

    def run(self):
        for value in self.values:
            msg = self.serialize(key=self.key, value=value)
            self.produce(value=msg.value, key=msg.key)

        if "run" in self.error_in:
            self.error("test run error")

        if self.finished:
            self.finished.set()

        while self.running:
            time.sleep(0.1)

    def cleanup(self, failed):
        if "cleanup" in self.error_in:
            self.error("test cleanup error")
        super().cleanup(failed)

    def stop(self):
        if "stop" in self.error_in:
            self.error("test stop error")
        super().stop()

    def error(self, msg):
        if self.pickeable_error:
            raise ValueError(msg)
        else:
            raise UnpickleableError(msg)


class UnpickleableError(Exception):
    def __init__(self, *args: object) -> None:
        # threading.Lock can't be pickled
        self._ = threading.Lock()
