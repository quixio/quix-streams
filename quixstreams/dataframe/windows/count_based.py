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
        data = state.get(key=self.STATE_KEY)
        if data is None:
            data = CountWindowsData(windows=[])

        msg_id = None
        if len(data["windows"]) == 0:
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
                msg_id = starting_value = data["windows"][-1]["value"] + (
                    self._step or self._max_count
                )
            else:
                starting_value = self._aggregate_default

            data["windows"].append(
                CountWindowData(
                    count=0,
                    start=timestamp_ms,
                    end=timestamp_ms,
                    value=starting_value,
                )
            )
        elif self._aggregate_collection:
            msg_id = data["windows"][0]["value"] + data["windows"][0]["count"]

        if self._aggregate_collection:
            state.add_to_collection(id=msg_id, value=value)

        updated_windows, expired_windows, to_remove = [], [], []
        for index, window in enumerate(data["windows"]):
            window["count"] += 1
            if timestamp_ms < window["start"]:
                window["start"] = timestamp_ms
            elif timestamp_ms > window["end"]:
                window["end"] = timestamp_ms

            if self._aggregate_collection:
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
                    state.delete_from_collection(
                        end=window["value"] + self._step
                        if self._step is not None
                        else self._max_count,
                        start=0 if self._step is None else None,
                    )
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
