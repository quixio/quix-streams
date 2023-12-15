from typing import Optional

from quixstreams.windows.base import (
    FixedWindowDefinition,
    WindowAggregateFunc,
    FixedWindow,
    StreamingDataFrame,
)


class HoppingWindowDefinition(FixedWindowDefinition):
    def __init__(
        self,
        duration: float,
        grace: float,
        step: float,
        dataframe: StreamingDataFrame,
        name: Optional[str] = None,
    ):
        super().__init__(
            duration=duration, grace=grace, dataframe=dataframe, name=name, step=step
        )

    def _get_name(self, func_name: str) -> str:
        return self._name or f"hopping_window_{self._duration}_{self._step}_{func_name}"

    def _create_window(
        self, func_name: str, func: WindowAggregateFunc
    ) -> "HoppingWindow":
        return HoppingWindow(
            duration=self._duration,
            grace=self._grace,
            step=self._step,
            name=self._get_name(func_name=func_name),
            func=func,
            dataframe=self._dataframe,
        )


class HoppingWindow(FixedWindow):
    def __init__(
        self,
        duration: float,
        grace: float,
        step: float,
        name: str,
        func: WindowAggregateFunc,
        dataframe: StreamingDataFrame,
    ):
        super().__init__(
            duration=duration,
            grace=grace,
            name=name,
            func=func,
            dataframe=dataframe,
            step=step,
        )
