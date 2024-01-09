from typing import Optional

from quixstreams.windows.base import (
    FixedWindowDefinition,
    WindowAggregateFunc,
    FixedWindow,
    StreamingDataFrame,
)


class TumblingWindowDefinition(FixedWindowDefinition):
    def __init__(
        self,
        duration: float,
        grace: float,
        dataframe: StreamingDataFrame,
        name: Optional[str] = None,
    ):
        super().__init__(duration=duration, grace=grace, dataframe=dataframe, name=name)

    def _get_name(self, func_name: str) -> str:
        return self._name or f"tumbling_window_{self._duration}_{func_name}"

    def _create_window(
        self, func_name: str, func: WindowAggregateFunc
    ) -> "TumblingWindow":
        return TumblingWindow(
            duration=self._duration,
            grace=self._grace,
            name=self._get_name(func_name=func_name),
            func=func,
            dataframe=self._dataframe,
        )


class TumblingWindow(FixedWindow):
    def __init__(
        self,
        duration: float,
        grace: float,
        name: str,
        func: WindowAggregateFunc,
        dataframe: StreamingDataFrame,
    ):
        super().__init__(
            duration=duration, grace=grace, name=name, func=func, dataframe=dataframe
        )
