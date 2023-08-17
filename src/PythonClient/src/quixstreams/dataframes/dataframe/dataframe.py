import uuid
from copy import deepcopy
from functools import partial
from typing import Self, Optional, Callable, TypeAlias, Union

from .column import Column, OpValue
from .pipeline import Pipeline, get_func_name
from ..models.rows import Row

RowApplier: TypeAlias = Callable[[Row], Optional[Union[Row, list[Row]]]]


__all__ = (
    "StreamingDataFrame"
)


def subset(keys: list[str], row: Row) -> Row:
    new_row = deepcopy(row)
    new_row.value = new_row[keys]
    return new_row


def setitem(k: str, v: Union[Column, OpValue], row: Row) -> Row:
    row = deepcopy(row)
    row[k] = v.eval(row) if isinstance(v, Column) else v
    return row


def column_filter(column: Column, row: Row) -> Optional[Row]:
    return row if column.eval(row) else None


def row_apply(func: RowApplier, row: Row) -> Union[Row, list[Row], None]:
    return func(deepcopy(row))


# TODO: make a pipeline merge function to avoid accessing private methods
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

    def __setitem__(self, key: str, value: Union[Column, OpValue, str]):
        self._apply(partial(setitem, key, value))

    def __getitem__(
            self, item: Union[str, list[str], Column, Self]
    ) -> Union[Column, Self]:
        if isinstance(item, (Column, StreamingDataFrame)):
            return self._apply(self._filter(item))
        elif isinstance(item, list):
            return self._apply(partial(subset, item))
        else:
            return Column(col_name=item)

    def _clone(self):
        return self.__class__(_pipeline=self._pipeline.clone())

    def _apply(self, func: RowApplier, func_name=None):
        self._pipeline.apply(func, func_name=func_name)
        return self

    def apply(
            self, func: Callable[[Row], Optional[Union[Row, list[Row], None]]]
    ) -> Self:
        """
        Add a function to the StreamingDataframe execution list.
        The provided function should accept a Quixstreams Row as its input.
        The provided function should operate on and return the same input Row, or None
        if its intended to be a "filtering" function.

        :param func: callable that accepts and (usually) returns a QuixStreams Row
        :return: self (StreamingDataFrame)
        """
        return self._clone()._apply(
            func=partial(row_apply, func),
            func_name=f'apply:{get_func_name(func)}'
        )

    def _dataframe_filter(self, other_dataframe: Self, row: Row) -> Optional[Row]:
        other_dataframe._pipeline.remove_redundant_functions(self._pipeline)
        return row if other_dataframe.process(row) else None

    def _filter(self, item: Union[Column, Self]) -> RowApplier:
        if isinstance(item, Column):
            return partial(column_filter, item)
        else:
            return partial(self._dataframe_filter, item)

    def process(self, row: Row) -> Optional[Union[Row, list[Row]]]:
        """
        Execute the previously defined StreamingDataframe operations on a provided Row.
        :param row: a QuixStreams Row object
        :return: Row, list of Rows, or None (if filtered)
        """
        return self._pipeline.process(row)