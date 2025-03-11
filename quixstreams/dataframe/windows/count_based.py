import logging
from typing import TYPE_CHECKING, Any, Iterable, Optional, TypedDict

from quixstreams.state import WindowedPartitionTransaction

from .aggregations import Aggregator, Collector
from .base import (
    Window,
    WindowKeyResult,
    WindowResult,
)

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


logger = logging.getLogger(__name__)


class CountWindowData(TypedDict):
    count: int
    start: int
    end: int
    value: Any


class CountWindowsData(TypedDict):
    windows: list[CountWindowData]


class CountWindow(Window):
    STATE_KEY = "metadata"

    def __init__(
        self,
        count: int,
        name: str,
        dataframe: "StreamingDataFrame",
        aggregators: dict[str, Aggregator],
        collectors: dict[str, Collector],
        step: Optional[int] = None,
    ):
        super().__init__(
            name=name,
            dataframe=dataframe,
            aggregators=aggregators,
            collectors=collectors,
        )

        self._max_count = count
        self._step = step

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
        optimisation. Instead the msg id reset to 0 on every new window.
        """
        state = transaction.as_state(prefix=key)
        data = state.get(key=self.STATE_KEY, default=CountWindowsData(windows=[]))

        msg_id = None
        if len(data["windows"]) == 0:
            # for new tumbling window, reset the collection id to 0
            if self._collect:
                window_value = msg_id = 0
            else:
                window_value = self._aggregators["value"].initialize()

            data["windows"].append(
                CountWindowData(
                    count=0,
                    start=timestamp_ms,
                    end=timestamp_ms,
                    value=window_value,
                )
            )
        elif self._step is not None and data["windows"][0]["count"] % self._step == 0:
            if self._collect:
                window_value = msg_id = (
                    data["windows"][0]["value"] + data["windows"][0]["count"]
                )
            else:
                window_value = self._aggregators["value"].initialize()

            data["windows"].append(
                CountWindowData(
                    count=0,
                    start=timestamp_ms,
                    end=timestamp_ms,
                    value=window_value,
                )
            )

        if self._collect:
            if msg_id is None:
                msg_id = data["windows"][0]["value"] + data["windows"][0]["count"]

            state.add_to_collection(id=msg_id, value=value)

        updated_windows, expired_windows, to_remove = [], [], []
        for index, window in enumerate(data["windows"]):
            window["count"] += 1
            if timestamp_ms < window["start"]:
                window["start"] = timestamp_ms
            elif timestamp_ms > window["end"]:
                window["end"] = timestamp_ms

            if self._collect:
                # window must close
                if window["count"] >= self._max_count:
                    values = state.get_from_collection(
                        start=window["value"],
                        end=window["value"] + self._max_count,
                    )

                    expired_windows.append(
                        (
                            key,
                            WindowResult(
                                start=window["start"],
                                end=window["end"],
                                value=self._collectors["value"].result(values),
                            ),
                        )
                    )
                    to_remove.append(index)

                    # for tumbling window we need to force deletion from 0
                    delete_start = 0 if self._step is None else None

                    # for hopping windows we can only delete the value in the first step, the rest is
                    # needed by follow up hopping windows
                    step = self._max_count if self._step is None else self._step
                    delete_end = window["value"] + step

                    state.delete_from_collection(end=delete_end, start=delete_start)
            else:
                window["value"] = self._aggregators["value"].agg(window["value"], value)

                result = (
                    key,
                    WindowResult(
                        start=window["start"],
                        end=window["end"],
                        value=self._aggregators["value"].result(window["value"]),
                    ),
                )

                updated_windows.append(result)

                if window["count"] >= self._max_count:
                    expired_windows.append(result)
                    to_remove.append(index)

        for i in to_remove:
            del data["windows"][i]

        state.set(key=self.STATE_KEY, value=data)
        return updated_windows, expired_windows
