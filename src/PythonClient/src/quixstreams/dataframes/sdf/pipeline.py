import uuid
from typing import Self, Optional, Any, Callable, TypeAlias, List
import operator
from datetime import timedelta

UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}

OpValue: TypeAlias = int | float | bool
ColumnValue: TypeAlias = int | float | bool | list | dict
ColumnApplier: TypeAlias = Callable[[dict], OpValue]


class Row:
    def __init__(self, data: dict, timestamp: str = None, _key: str = None, _db: dict = None):
        self._data = data
        self._timestamp = timestamp
        self._key = _key
        self._partition = ''
        self._db = db

    @property
    def data(self):
        return self._data

    @property
    def timestamp(self):
        return self._timestamp

    def __setitem__(self, key, value):
        self._data[key] = value

    def __getitem__(self, item):
        return self._data[item]

    def __bool__(self):
        return bool(self._data)

    def _set_data(self, data: dict) -> Self:
        self._data = data
        return self


class StatefulOperation:
    _method_name = None

    def __init__(self, segment: str, _origin):
        self._segment_size = segment
        self._origin = _origin

    @staticmethod
    def _convert_to_seconds(shorthand: str):
        value = int(shorthand[:-1])
        td = timedelta(**{UNITS[shorthand[-1]]: value})
        return td.seconds + 60 * 60 * 24 * td.days

    def _key_prefix(self, row: Row):
        return f'{self._method_name}-{self._segment_size}__{row._key}'

    def _query_for_records(self, row: Row):
        # would be replaced with real query
        min_timestamp = str(int(row.timestamp)-self._convert_to_seconds(self._segment_size))
        return [self._convert_query_to_row((k, v)) for k, v in row._db.items() if k.startswith(self._key_prefix(row)) and k.split('__')[-1] > min_timestamp]

    @staticmethod
    def _convert_query_to_row(query_row: tuple):
        # post-processing on any raw data retrieved from DB to convert them into Row objects
        return Row(timestamp=query_row[0], data=query_row[1])

    @staticmethod
    def _join_all_data(queried_rows: List[Row], row: Row):
        return queried_rows + [row]

    def _write_event_to_db(self, row: Row):
        row._db[f'{self._key_prefix(row)}__{row.timestamp}'] = row.data
        return row

    def _prep_data(self, row: Row):
        data_out = self._join_all_data(self._query_for_records(row), row)
        self._write_event_to_db(row)
        return data_out


class ShiftedWindow(StatefulOperation):
    _method_name = 'shifted'


class RollingWindow(StatefulOperation):
    _method_name = 'rolling'

    @staticmethod
    def _col_mean(col: str, rows: List[Row]):
        return sum([row[col] for row in rows])/len(rows)

    def _mean(self, rows: list[Row]):
        cols = list(rows[0].data.keys())
        return Row(data={col: self._col_mean(col, rows) for col in cols}, timestamp=rows[-1].timestamp)

    def mean(self):
        return self._origin._apply_rolling(lambda row, timestamp: self._mean(self._prep_data(row)))


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

    def rolling(self, segment: str):
        return RollingWindow(segment=segment, _origin=self)

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
    def __init__(self, _functions: list[PipelineFunction] = None):
        self._functions = _functions or []
        self._id = str(uuid.uuid4())

    @staticmethod
    def _filter(pf: Column) -> RowApplier:
        return lambda row: row if pf.eval(row) else None

    @staticmethod
    def _subset(keys: list) -> RowApplier:
        return lambda row: row._set_data({k: row[k] for k in keys})

    @staticmethod
    def _set_item(k: str, v: OpValue, row: Row) -> Row:
        row[k] = v
        return row

    @property
    def functions(self) -> list[PipelineFunction]:
        return self._functions

    def apply(self, func: RowApplier):
        self._functions.append(PipelineFunction(func=func))
        return self

    def __getitem__(self, item: str | list | Column) -> Column | Self:
        if isinstance(item, Column):
            self.apply(self._filter(item))
            return self
        elif isinstance(item, list):
            self.apply(self._subset(item))
            return self
        return Column(col_name=item)

    def __setitem__(self, key: str, value: Column | OpValue):
        if isinstance(value, Column):
            self.apply(lambda row: self._set_item(key, value.eval(row), row))
        else:
            self.apply(lambda row: self._set_item(key, value, row))

    def rolling(self, segment: str):
        return RollingWindow(segment=segment, _origin=self)

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
            if not result:
                print('result was filtered')
                break
        return result
