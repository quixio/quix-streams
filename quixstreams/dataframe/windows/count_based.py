import logging
from typing import TYPE_CHECKING, Any, Iterable, Optional, TypedDict, Union, cast

from quixstreams.core.stream import InvalidOperation
from quixstreams.state import WindowedPartitionTransaction

from .base import (
    Message,
    MultiAggregationWindowMixin,
    SingleAggregationWindowMixin,
    Window,
    WindowKeyResult,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


logger = logging.getLogger(__name__)

_MISSING = object()


class CountWindowData(TypedDict):
    count: int
    start: int
    end: int

    # Can be None for single aggregation windows not migrated
    aggregations: Union[Any, dict[str, Any]]
    collection_start_id: int

    # value: Any deprecated. Only used in single aggregation windows for both collection id tracking and aggregation


class CountWindowsData(TypedDict):
    windows: list[CountWindowData]


class CountWindow(Window):
    STATE_KEY = "metadata"

    def __init__(
        self,
        count: int,
        name: str,
        dataframe: "StreamingDataFrame",
        step: Optional[int] = None,
    ):
        super().__init__(
            name=name,
            dataframe=dataframe,
        )

        self._max_count = count
        self._step = step

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
                yield window, key, window["start"], None

        return self._apply_window(
            on_update=window_callback,
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
                value=value,
                key=key,
                timestamp_ms=timestamp_ms,
                transaction=transaction,
            )

            # loop over the expired_windows generator to ensure the windows
            # are expired
            for key, window in expired_windows:
                pass

            # Use window start timestamp as a new record timestamp
            for key, window in updated_windows:
                yield window, key, window["start"], None

        return self._apply_window(on_update=window_callback, name=self._name)

    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        transaction: WindowedPartitionTransaction[str, CountWindowsData],
    ) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]:
        """
        Count based windows are different from time based windows as we don't
        have a clear indicator on when a window starts, it depends on the
        previous window. On the other hand there is a clear marker on when it
        must end, we can close the window as soon as the window is full.

        As count windows can't rely on ordered timestamps, collection need
        another way to generate ordered ids. We can use a per-key counter for
        that, each incoming message will increment the key counter. Collection
        window then only need to know at what id they start, how many messages
        to get on completion and how many messages can be safely deleted.

        We can further optimise this by removing the global counter and relying
        on the previous window state to compute a message id. For example, if
        an active window starts at msg id 32 and has a count of 3 it means the
        next free msg id is 35 (32 + 3).

        For tumbling windows there is no window overlap so we can't rely on that
        optimisation. Instead, the msg id reset to 0 on every new window.
        """
        state = transaction.as_state(prefix=key)
        data = state.get(key=self.STATE_KEY, default=CountWindowsData(windows=[]))
        collect = self.collect
        aggregate = self.aggregate

        # Start at -1 to indicate that we don't have a collection id yet. If we go from a no-collection window
        # to collection window we add the count to the previous window collection id to get the new collection id.
        # The count is always bigger or equal to 1 so we can safely use -1 as a marker.
        collection_start_id = -1
        if len(data["windows"]) == 0:
            collection_start_id = 0
            data["windows"].append(
                CountWindowData(
                    count=0,
                    start=timestamp_ms,
                    end=timestamp_ms,
                    aggregations=self._initialize_value(),
                    collection_start_id=collection_start_id,
                )
            )
        elif self._step is not None and data["windows"][0]["count"] % self._step == 0:
            if collect:
                collection_start_id = (
                    self._get_collection_start_id(data["windows"][0])
                    + data["windows"][0]["count"]
                )

            data["windows"].append(
                CountWindowData(
                    count=0,
                    start=timestamp_ms,
                    end=timestamp_ms,
                    aggregations=self._initialize_value(),
                    collection_start_id=collection_start_id,
                )
            )

        if collect:
            if collection_start_id == -1:
                collection_start_id = (
                    self._get_collection_start_id(data["windows"][0])
                    + data["windows"][0]["count"]
                )

            state.add_to_collection(
                id=collection_start_id, value=self._collect_value(value)
            )

        updated_windows, expired_windows, to_remove = [], [], []
        for index, window in enumerate(data["windows"]):
            window["count"] += 1
            if timestamp_ms < window["start"]:
                window["start"] = timestamp_ms
            elif timestamp_ms > window["end"]:
                window["end"] = timestamp_ms

            if aggregate:
                window["aggregations"] = self._aggregate_value(
                    self._get_aggregations(window), value, timestamp_ms
                )
                updated_windows.append(
                    (
                        key,
                        self._results(
                            window["aggregations"], [], window["start"], window["end"]
                        ),
                    )
                )

            if window["count"] >= self._max_count:
                to_remove.append(index)

                if collect:
                    collection_start_id = self._get_collection_start_id(window)

                    collected = state.get_from_collection(
                        start=collection_start_id,
                        end=collection_start_id + self._max_count,
                    )
                    # for tumbling window we need to force deletion from 0
                    delete_start = 0 if self._step is None else None

                    # for hopping windows we can only delete the value in the first step, the rest is
                    # needed by follow up hopping windows
                    step = self._max_count if self._step is None else self._step
                    delete_end = collection_start_id + step

                    state.delete_from_collection(end=delete_end, start=delete_start)
                else:
                    collected = []

                expired_windows.append(
                    (
                        key,
                        self._results(
                            window["aggregations"],
                            collected,
                            window["start"],
                            window["end"],
                        ),
                    )
                )

        for i in to_remove:
            del data["windows"][i]

        state.set(key=self.STATE_KEY, value=data)
        return updated_windows, expired_windows

    def _get_collection_start_id(self, window: CountWindowData) -> int:
        start_id = window.get("collection_start_id", _MISSING)
        if start_id is _MISSING:
            start_id = cast(int, window["value"])  # type: ignore[typeddict-item]
            window["collection_start_id"] = start_id
        return start_id  # type: ignore[return-value]

    def _get_aggregations(self, window: CountWindowData) -> Union[Any, dict[str, Any]]:
        aggregations = window.get("aggregations", _MISSING)
        if aggregations is _MISSING:
            return window["value"]  # type: ignore[typeddict-item]
        return aggregations


class CountWindowSingleAggregation(SingleAggregationWindowMixin, CountWindow):
    pass


class CountWindowMultiAggregation(MultiAggregationWindowMixin, CountWindow):
    pass
