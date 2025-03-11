import abc
import functools
import logging
from abc import abstractmethod
from collections import deque
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Deque,
    Iterable,
    Optional,
    Protocol,
    TypedDict,
    cast,
)

from typing_extensions import TypeAlias

from quixstreams.context import message_context
from quixstreams.core.stream import TransformExpandedCallback
from quixstreams.processing import ProcessingContext
from quixstreams.state import WindowedPartitionTransaction

from .aggregations import Aggregator, Collector

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

logger = logging.getLogger(__name__)


class WindowResult(TypedDict):
    start: int
    end: int
    value: Any


WindowKeyResult: TypeAlias = tuple[Any, WindowResult]
Message: TypeAlias = tuple[WindowResult, Any, int, Any]

WindowAggregateFunc = Callable[[Any, Any], Any]

TransformRecordCallbackExpandedWindowed = Callable[
    [Any, Any, int, Any, WindowedPartitionTransaction],
    Iterable[Message],
]


class Window(abc.ABC):
    def __init__(
        self,
        name: str,
        dataframe: "StreamingDataFrame",
        aggregators: dict[str, Aggregator],
        collectors: dict[str, Collector],
    ) -> None:
        if not name:
            raise ValueError("Window name must not be empty")

        self._name = name
        self._dataframe = dataframe

        self._aggregators = aggregators
        self._aggregate = len(aggregators) > 0

        self._collectors = collectors
        self._collect = len(collectors) > 0

        if not self._collect and not self._aggregate:
            raise ValueError("At least one aggregation or collector must be defined")
        elif len(collectors) + len(aggregators) > 1:
            raise ValueError("Only one aggregation or collector can be defined")

    @property
    def name(self) -> str:
        return self._name

    @abstractmethod
    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        transaction: WindowedPartitionTransaction,
    ) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]:
        pass

    def register_store(self) -> None:
        self._dataframe.processing_context.state_manager.register_windowed_store(
            topic_name=self._dataframe.topic.name, store_name=self._name
        )

    def _apply_window(
        self,
        func: TransformRecordCallbackExpandedWindowed,
        name: str,
    ) -> "StreamingDataFrame":
        self.register_store()

        windowed_func = _as_windowed(
            func=func,
            processing_context=self._dataframe.processing_context,
            store_name=name,
        )
        # Manually modify the Stream and clone the source StreamingDataFrame
        # to avoid adding "transform" API to it.
        # Transform callbacks can modify record key and timestamp,
        # and it's prone to misuse.
        stream = self._dataframe.stream.add_transform(func=windowed_func, expand=True)
        return self._dataframe.__dataframe_clone__(stream=stream)

    def final(self) -> "StreamingDataFrame":
        """
        Apply the window aggregation and return results only when the windows are
        closed.

        The format of returned windows:
        ```python
        {
            "start": <window start time in milliseconds>,
            "end": <window end time in milliseconds>,
            "value: <aggregated window value>,
        }
        ```

        The individual window is closed when the event time
        (the maximum observed timestamp across the partition) passes
        its end timestamp + grace period.
        The closed windows cannot receive updates anymore and are considered final.

        >***NOTE:*** Windows can be closed only within the same message key.
        If some message keys appear irregularly in the stream, the latest windows
        can remain unprocessed until the message the same key is received.
        """

        def window_callback(
            value: Any,
            key: Any,
            timestamp_ms: int,
            _headers: Any,
            transaction: WindowedPartitionTransaction,
        ) -> Iterable[Message]:
            _, expired_windows = self.process_window(
                value=value,
                key=key,
                timestamp_ms=timestamp_ms,
                transaction=transaction,
            )
            # Use window start timestamp as a new record timestamp
            for key, window in expired_windows:
                yield (window, key, window["start"], None)

        return self._apply_window(
            func=window_callback,
            name=self._name,
        )

    def current(self) -> "StreamingDataFrame":
        """
        Apply the window transformation to the StreamingDataFrame to return results
        for each updated window.

        The format of returned windows:
        ```python
        {
            "start": <window start time in milliseconds>,
            "end": <window end time in milliseconds>,
            "value: <aggregated window value>,
        }
        ```

        This method processes streaming data and returns results as they come,
        regardless of whether the window is closed or not.
        """

        def window_callback(
            value: Any,
            key: Any,
            timestamp_ms: int,
            _headers: Any,
            transaction: WindowedPartitionTransaction,
        ) -> Iterable[Message]:
            updated_windows, expired_windows = self.process_window(
                value=value, key=key, timestamp_ms=timestamp_ms, transaction=transaction
            )

            # loop over the expired_windows generator to ensure the windows
            # are expired
            for key, window in expired_windows:
                pass

            # Use window start timestamp as a new record timestamp
            for key, window in updated_windows:
                yield (window, key, window["start"], None)

        return self._apply_window(func=window_callback, name=self._name)


def _noop() -> Any:
    """
    No-operation function for skipping messages due to None keys.

    Messages with None keys are ignored because keys are essential for performing
    accurate and meaningful windowed aggregation.
    """
    return []


def _as_windowed(
    func: TransformRecordCallbackExpandedWindowed,
    processing_context: ProcessingContext,
    store_name: str,
) -> TransformExpandedCallback:
    @functools.wraps(func)
    def wrapper(
        value: Any, key: Any, timestamp: int, headers: Any
    ) -> Iterable[Message]:
        ctx = message_context()
        transaction = cast(
            WindowedPartitionTransaction,
            processing_context.checkpoint.get_store_transaction(
                topic=ctx.topic, partition=ctx.partition, store_name=store_name
            ),
        )
        if key is None:
            logger.warning(
                f"Skipping window processing for a message because the key is None, "
                f"partition='{ctx.topic}[{ctx.partition}]' offset='{ctx.offset}'."
            )
            return _noop()
        return func(value, key, timestamp, headers, transaction)

    return wrapper


class WindowOnLateCallback(Protocol):
    def __call__(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        late_by_ms: int,
        start: int,
        end: int,
        store_name: str,
        topic: str,
        partition: int,
        offset: int,
    ) -> bool: ...


def get_window_ranges(
    timestamp_ms: int, duration_ms: int, step_ms: Optional[int] = None
) -> Deque[tuple[int, int]]:
    """
    Get a list of window ranges for the given timestamp.
    :param timestamp_ms: timestamp in milliseconds
    :param duration_ms: window duration in milliseconds
    :param step_ms: window step in milliseconds for hopping windows, optional.
    :return: a list of (<start>, <end>) tuples
    """
    if not step_ms:
        step_ms = duration_ms

    window_ranges: Deque[tuple[int, int]] = deque()
    current_window_start = timestamp_ms - (timestamp_ms % step_ms)

    while (
        current_window_start > timestamp_ms - duration_ms and current_window_start >= 0
    ):
        window_end = current_window_start + duration_ms
        window_ranges.appendleft((current_window_start, window_end))
        current_window_start -= step_ms

    return window_ranges
