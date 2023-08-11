import uuid
from typing import Self, Optional, Any, Callable, TypeAlias, List
import operator
from copy import deepcopy
from quixstreams.dataframes.models import Row

UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}

OpValue: TypeAlias = int | float | bool
ColumnValue: TypeAlias = int | float | bool | list | dict
ColumnApplier: TypeAlias = Callable[[dict], OpValue]


class InvalidFilter(Exception):
    pass


class Column:
    def __init__(self, col_name: Optional[str] = None, _eval_func: Optional[ColumnApplier] = None):
        self.col_name = col_name
        self._eval_func = _eval_func if _eval_func else lambda row: row[self.col_name]

    @staticmethod
    def _as_column(value: Any) -> 'Column':
        if not isinstance(value, Column):
            return Column(_eval_func=lambda row: value)
        return value

    def _operation(self, other: Any, op: Callable[[OpValue, OpValue], OpValue]) -> Self:
        return Column(_eval_func=lambda row: op(self.eval(row), self._as_column(other).eval(row)))

    def eval(self, row: Row) -> ColumnValue:
        return self._eval_func(row)

    def apply(self, func: ColumnApplier) -> Self:
        return Column(_eval_func=lambda row: func(self.eval(row)))

    def __mod__(self, other):
        return self._operation(other, operator.mod)

    def __and__(self, other):
        return self._operation(other, operator.and_)

    def __or__(self, other):
        return self._operation(other, operator.or_)

    def __add__(self, other):
        return self._operation(other, operator.add)

    def __sub__(self, other):
        return self._operation(other, operator.sub)

    def __mul__(self, other):
        return self._operation(other, operator.mul)

    def __truediv__(self, other):
        return self._operation(other, operator.truediv)

    def __eq__(self, other):
        return self._operation(other, operator.eq)

    def __ne__(self, other):
        return self._operation(other, operator.ne)

    def __lt__(self, other):
        return self._operation(other, operator.lt)

    def __le__(self, other):
        return self._operation(other, operator.le)

    def __gt__(self, other):
        return self._operation(other, operator.gt)

    def __ge__(self, other):
        return self._operation(other, operator.ge)


RowApplier: TypeAlias = Callable[[Row], Optional[Row]]


class PipelineFunction:
    def __init__(self, func: RowApplier):
        self._id = str(uuid.uuid4())
        self._func = func

    def __call__(self, row: Row):
        return self._func(row)

    @property
    def id(self) -> str:
        return self._id


class Pipeline:
    def __init__(self, _functions: list[PipelineFunction] = None, _graph: dict = None, _parent: str = None):
        self._functions = _functions or []
        self._id = str(uuid.uuid4())
        self._graph = _graph or {}
        self._parent = _parent
        self._add_graph_node()

    def _clone(self):
        return Pipeline(_functions=self._functions[:], _graph=deepcopy(self._graph), _parent=self._id)

    def _add_graph_node(self):
        if self._parent:
            self._graph[self._parent] = self._id
        else:
            self._graph[self._id] = None

    def _check_child_contains_parent(self, parent: Self):
        for key in parent._graph:
            if parent_child := parent._graph[key]:
                try:
                    assert self._graph[key] == parent_child
                except AssertionError:
                    raise InvalidFilter

    def _remove_redundant_functions(self, parent: Self):
        self._check_child_contains_parent(parent)
        self._functions = self._functions[-(len(self._functions) - len(parent._functions)):]
        return self

    def _filter(self, item: Column | Self) -> RowApplier:
        if isinstance(item, Column):
            return lambda row: row if item.eval(row) else None
        return lambda row: row if item.process(row) else None

    @staticmethod
    def _subset(keys: list) -> RowApplier:
        return lambda row: row._clone(value={k: row.value[k] for k in keys})

    @staticmethod
    def _set_item(k: str, v: OpValue, row: Row) -> Row:
        new_row_value = deepcopy(row.value)
        new_row_value[k] = v
        return row._clone(value=new_row_value)

    @property
    def functions(self) -> list[PipelineFunction]:
        return self._functions

    def _apply(self, func: RowApplier):
        self._functions.append(PipelineFunction(func=func))
        return self

    def apply(self, func: Callable[[Row], Optional[dict | Row]]):
        return self._clone()._apply(func=lambda row: row._clone(value=func(row)))

    def __getitem__(self, item: str | list | Column | Self) -> Column | Self:
        if isinstance(item, Column):
            self._apply(self._filter(item))
            return self
        elif isinstance(item, list):
            self._apply(self._subset(item))
            return self
        elif isinstance(item, Pipeline):
            self._apply(self._filter(item._remove_redundant_functions(self)))
            return self
        else:
            return Column(col_name=item)

    def __setitem__(self, key: str, value: Column | OpValue):
        if isinstance(value, Column):
            self._apply(lambda row: self._set_item(key, value.eval(row), row))
        else:
            self._apply(lambda row: self._set_item(key, value, row))

    @staticmethod
    def _process_function(func: PipelineFunction, data: Row | List[Row]):
        if isinstance(data, Row):
            return func(data)
        else:
            data = [func(d) for d in data]
            return data if any(data) else None

    def process(self, event: Row | List[Row]) -> Optional[Row | List[Row]]:
        result = event
        for func in self._functions:
            print(f'processing func {func._func.__name__}')
            result = self._process_function(func, result)
            print(result)
            if not result:
                print('result was filtered')
                break
        return result
