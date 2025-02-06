import logging
from typing import TYPE_CHECKING, Any, Iterable, Optional, TypedDict

from quixstreams.state import WindowedState

from .base import (
    Window,
    WindowAggregateFunc,
    WindowMergeFunc,
    WindowResult,
    default_merge_func,
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
        name: str,
        count: int,
        aggregate_func: WindowAggregateFunc,
        aggregate_default: Any,
        aggregate_collection: bool,
        dataframe: "StreamingDataFrame",
        merge_func: Optional[WindowMergeFunc] = None,
        step: Optional[int] = None,
    ):
        super().__init__(name, dataframe)

        self._max_count = count
        self._aggregate_func = aggregate_func
        self._aggregate_default = aggregate_default
        self._aggregate_collection = aggregate_collection
        self._merge_func = merge_func or default_merge_func
        self._step = step

    def process_window(
        self,
        value: Any,
        key: Any,
        timestamp_ms: int,
        state: WindowedState,
    ) -> tuple[Iterable[WindowResult], Iterable[WindowResult]]:
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
        data = state.get(key=self.STATE_KEY)
        if data is None:
            data = CountWindowsData(windows=[])

        msg_id = None
        if len(data["windows"]) == 0:
            # for new tumbling window, reset the collection id to 0
            msg_id = 0

            data["windows"].append(
                CountWindowData(
                    count=0,
                    start=timestamp_ms,
                    end=timestamp_ms,
                    value=msg_id
                    if self._aggregate_collection
                    else self._aggregate_default,
                )
            )
        elif self._step is not None and data["windows"][0]["count"] % self._step == 0:
            if self._aggregate_collection:
                msg_id = data["windows"][0]["value"] + data["windows"][0]["count"]

            data["windows"].append(
                CountWindowData(
                    count=0,
                    start=timestamp_ms,
                    end=timestamp_ms,
                    value=msg_id
                    if self._aggregate_collection
                    else self._aggregate_default,
                )
            )

        if self._aggregate_collection:
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

            if self._aggregate_collection:
                # window must close
                if window["count"] >= self._max_count:
                    values = state.get_from_collection(
                        start=window["value"],
                        end=window["value"] + self._max_count,
                    )

                    expired_windows.append(
                        WindowResult(
                            start=window["start"],
                            end=window["end"],
                            value=self._merge_func(values),
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
                window["value"] = self._aggregate_func(window["value"], value)

                result = WindowResult(
                    start=window["start"],
                    end=window["end"],
                    value=self._merge_func(window["value"]),
                )
                updated_windows.append(result)

                if window["count"] >= self._max_count:
                    expired_windows.append(result)
                    to_remove.append(index)

        for i in to_remove:
            del data["windows"][i]

        state.set(key=self.STATE_KEY, value=data)
        return updated_windows, expired_windows
