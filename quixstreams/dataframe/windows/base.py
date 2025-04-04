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
    cast,
)

from typing_extensions import TypeAlias

from quixstreams.context import message_context
from quixstreams.core.stream import TransformExpandedCallback
from quixstreams.dataframe.exceptions import InvalidOperation
from quixstreams.models.topics.manager import TopicManager
from quixstreams.state import WindowedPartitionTransaction

from .aggregations import BaseAggregator, BaseCollector

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame
    from quixstreams.processing import ProcessingContext

logger = logging.getLogger(__name__)


WindowResult: TypeAlias = dict[str, Any]
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
    ) -> None:
        if not name:
            raise ValueError("Window name must not be empty")

        self._name = name
        self._dataframe = dataframe

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
        self._dataframe.ensure_topics_copartitioned()
        # Create a config for the changelog topic based on the underlying SDF topics
        changelog_config = TopicManager.derive_topic_config(self._dataframe.topics)
        self._dataframe.processing_context.state_manager.register_windowed_store(
            stream_id=self._dataframe.stream_id,
            store_name=self._name,
            changelog_config=changelog_config,
        )

    def _apply_window(
        self,
        func: TransformRecordCallbackExpandedWindowed,
        name: str,
    ) -> "StreamingDataFrame":
        self.register_store()

        windowed_func = _as_windowed(
            func=func,
            stream_id=self._dataframe.stream_id,
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

        if self.collect:
            raise InvalidOperation(
                "BaseCollectors are not supported by `current` windows"
            )

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

    # Implemented by SingleAggregationWindowMixin and MultiAggregationWindowMixin
    # Single aggregation and multi aggregation windows store aggregations and collections
    # values in a different format.
    @property
    @abstractmethod
    def collect(self) -> bool: ...

    @property
    @abstractmethod
    def aggregate(self) -> bool: ...

    @abstractmethod
    def _initialize_value(self) -> Any: ...

    @abstractmethod
    def _aggregate_value(self, state_value: Any, value: Any, timestamp) -> Any: ...

    @abstractmethod
    def _collect_value(self, value: Any): ...

    @abstractmethod
    def _results(
        self,
        aggregated: Any,
        collected: list[Any],
        start: int,
        end: int,
    ) -> WindowResult: ...


class SingleAggregationWindowMixin:
    """
    DEPRECATED: Use MultiAggregationWindowMixin instead.

    Single aggregation window mixin for windows with a single aggregation or collection.
    Store aggregated value directly in the window value.
    """

    def __init__(
        self,
        *,
        aggregators: dict[str, BaseAggregator],
        collectors: dict[str, BaseCollector],
        **kwargs,
    ) -> None:
        if (len(collectors) + len(aggregators)) > 1:
            raise ValueError("Only one aggregator or collector can be defined")

        if len(aggregators) > 0:
            self._aggregator: Optional[BaseAggregator] = aggregators["value"]
            self._collector: Optional[BaseCollector] = None
        elif len(collectors) > 0:
            self._collector = collectors["value"]
            self._aggregator = None
        else:
            raise ValueError("At least one aggregator or collector must be defined")

        super().__init__(**kwargs)

    @property
    def aggregate(self) -> bool:
        return self._aggregator is not None

    @property
    def collect(self) -> bool:
        return self._collector is not None

    def _initialize_value(self) -> Any:
        if self._aggregator:
            return self._aggregator.initialize()
        return None

    def _aggregate_value(self, state_value: Any, value: Any, timestamp: int) -> Any:
        if self._aggregator:
            return self._aggregator.agg(state_value, value, timestamp)
        return None

    def _collect_value(self, value: Any):
        # Single aggregation collect() always stores the full message
        return value

    def _results(
        self,
        aggregated: Any,
        collected: list[Any],
        start: int,
        end: int,
    ) -> WindowResult:
        result = {"start": start, "end": end}
        if self._aggregator:
            result["value"] = self._aggregator.result(aggregated)
        elif self._collector:
            result["value"] = self._collector.result(collected)

        return result


class MultiAggregationWindowMixin:
    def __init__(
        self,
        *,
        aggregators: dict[str, BaseAggregator],
        collectors: dict[str, BaseCollector],
        **kwargs,
    ) -> None:
        if not collectors and not aggregators:
            raise ValueError("At least one aggregator or collector must be defined")

        self._aggregators: dict[str, tuple[str, BaseAggregator]] = {
            f"{result_column}/{agg.state_suffix}": (result_column, agg)
            for result_column, agg in aggregators.items()
        }

        self._collect_all = False
        self._collect_columns: set[str] = set()
        self._collectors: dict[str, tuple[Optional[str], BaseCollector]] = {}
        for result_column, col in collectors.items():
            input_column = col.column
            if input_column is None:
                self._collect_all = True
            else:
                self._collect_columns.add(input_column)

            self._collectors[result_column] = (input_column, col)

        super().__init__(**kwargs)

    @property
    def aggregate(self) -> bool:
        return bool(self._aggregators)

    @property
    def collect(self) -> bool:
        return bool(self._collectors)

    def _initialize_value(self) -> dict[str, Any]:
        return {k: agg.initialize() for k, (_, agg) in self._aggregators.items()}

    def _aggregate_value(
        self, state_values: dict[str, Any], value: Any, timestamp: int
    ) -> dict[str, Any]:
        return {
            k: agg.agg(state_values[k], value, timestamp)
            if k in state_values
            else agg.agg(agg.initialize(), value, timestamp)
            for k, (_, agg) in self._aggregators.items()
        }

    def _collect_value(self, value) -> dict[str, Any]:
        if self._collect_all:
            return value
        return {col: value[col] for col in self._collect_columns}

    def _results(
        self,
        aggregated: dict[str, Any],
        collected: list[dict[str, Any]],
        start: int,
        end: int,
    ) -> WindowResult:
        result = {k: v for k, v in self._build_results(aggregated, collected)}
        result["start"] = start
        result["end"] = end
        return result

    def _build_results(
        self,
        aggregated: dict[str, Any],
        collected: list[dict[str, Any]],
    ) -> Iterable[tuple[str, Any]]:
        for key, (result_col, agg) in self._aggregators.items():
            if key in aggregated:
                yield result_col, agg.result(aggregated[key])
            else:
                yield result_col, agg.result(agg.initialize())

        collected_columns = self._collected_by_columns(collected)
        for result_col, (input_col, col) in self._collectors.items():
            if input_col is None:
                yield result_col, col.result(collected)
            else:
                yield result_col, col.result(collected_columns[input_col])

    def _collected_by_columns(
        self, collected: list[dict[str, Any]]
    ) -> dict[str, list[Any]]:
        if not self._collect_columns:
            return {}

        colums: dict[str, list[Any]] = {col: [] for col in self._collect_columns}
        for c in collected:
            for col in colums:
                colums[col].append(c[col])
        return colums


def _noop() -> Any:
    """
    No-operation function for skipping messages due to None keys.

    Messages with None keys are ignored because keys are essential for performing
    accurate and meaningful windowed aggregation.
    """
    return []


def _as_windowed(
    func: TransformRecordCallbackExpandedWindowed,
    processing_context: "ProcessingContext",
    store_name: str,
    stream_id: str,
) -> TransformExpandedCallback:
    @functools.wraps(func)
    def wrapper(
        value: Any, key: Any, timestamp: int, headers: Any
    ) -> Iterable[Message]:
        ctx = message_context()
        transaction = cast(
            WindowedPartitionTransaction,
            processing_context.checkpoint.get_store_transaction(
                stream_id=stream_id, partition=ctx.partition, store_name=store_name
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
