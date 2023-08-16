from quixstreams.dataframes.dataframe.pipeline import Pipeline
from quixstreams.dataframes.models.rows import Row
from quixstreams.dataframes.dataframe.column import Column, OpValue
from copy import deepcopy
from typing import Self, Optional, Callable, TypeAlias, Union
import uuid
from functools import partial

RowApplier: TypeAlias = Callable[[Row], Optional[Union[Row, list[Row]]]]


class StreamingDataFrame:
    def __init__(self, name: str = None, _pipeline: Pipeline = None):
        self.name = name or str(uuid.uuid4())
        self._pipeline = _pipeline or Pipeline(name=self.name)
        self._producer: None

    @property
    def producer(self):
        return None

    def get_topics(self):
        ...

    def set_producer(self):
        ...

    def to_topic(self):
        ...

    def _produce(self):
        ...

    @staticmethod
    def _subset(keys: list[str], row: Row) -> Row:
        new_row = deepcopy(row)
        new_row.value = new_row[keys]
        return new_row

    @staticmethod
    def _setitem(k: str, v: Union[Column, OpValue], row: Row) -> Row:
        row = deepcopy(row)
        row[k] = v.eval(row) if isinstance(v, Column) else v
        return row

    @staticmethod
    # TODO: decide if `result` is not a dict or row that it should break here rather than later
    def _convert_result_to_row(row: Row, result: Union[Row, dict]) -> Row:
        if isinstance(result, dict):
            new_row = deepcopy(row)
            new_row.value = result
            return new_row
        return result

    @staticmethod
    def _column_filter(column: Column, row: Row) -> Optional[Row]:
        return row if column.eval(row) else None

    def __setitem__(self, key: str, value: Union[Column, OpValue, str]):
        self._apply(partial(self._setitem, key, value))

    def __getitem__(self, item: Union[str, list[str], Column, Self]) -> Union[Column, Self]:
        if isinstance(item, (Column, StreamingDataFrame)):
            return self._apply(self._filter(item))
        elif isinstance(item, list):
            return self._apply(partial(self._subset, item))
        else:
            return Column(col_name=item)

    def _clone(self):
        return StreamingDataFrame(_pipeline=self._pipeline._clone())

    def _row_apply(self, func: RowApplier, row: Row) -> Union[Row, list[Row]]:
        new_row = deepcopy(row)
        result = func(new_row)
        if isinstance(result, list):
            result = [self._convert_result_to_row(new_row, r) for r in result]
        else:
            result = self._convert_result_to_row(new_row, result)
        return result

    def _apply(self, func: RowApplier, func_name=None):
        self._pipeline.apply(func, func_name=func_name)
        return self

    def apply(self, func: Callable[[Row], Optional[Union[dict, Row]]]) -> Self:
        return self._clone()._apply(func=partial(self._row_apply, func), func_name=f'apply:{func.__name__}')

    def _dataframe_filter(self, other_dataframe: Self, row: Row) -> Optional[Row]:
        other_dataframe._pipeline._remove_redundant_functions(self._pipeline)
        return row if other_dataframe.process(row) else None

    def _filter(self, item: Union[Column, Self]) -> RowApplier:
        if isinstance(item, Column):
            return partial(self._column_filter, item)
        else:
            return partial(self._dataframe_filter, item)

    def process(self, row: Row) -> Optional[Union[Row, list[Row]]]:
        return self._pipeline.process(row)