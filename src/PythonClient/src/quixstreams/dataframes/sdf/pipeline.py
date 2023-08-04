import uuid
from typing import Self, Optional, Any, Callable, TypeAlias, List
import operator

OpValue: TypeAlias = int | float | bool
ColumnValue: TypeAlias = int | float | bool | list | dict
RowApplier: TypeAlias = Callable[[dict], Optional[dict]]
ColumnApplier: TypeAlias = Callable[[dict], OpValue]


class Row:
    def __init__(self, data: dict, timestamp: str = None):
        self.data = data
        self.timestamp = timestamp


class Column:
    def __init__(self, name: Optional[str] = None, _eval_func: Optional[ColumnApplier] = None):
        self.name = name
        self._eval_func = _eval_func if _eval_func else lambda row: row[self.name]

    @staticmethod
    def _as_pipeline_field(value):
        if not isinstance(value, Column):
            return Column(_eval_func=lambda row: value)
        return value

    def _operation(self, other: Any, op: Callable[[OpValue, OpValue], OpValue]) -> Self:
        return Column(
            _eval_func=lambda row: op(self.eval(row), self._as_pipeline_field(other).eval(row)))

    def eval(self, row: dict) -> ColumnValue:
        return self._eval_func(row)

    def apply(self, f: ColumnApplier) -> Self:
        return Column(_eval_func=lambda row: f(self.eval(row)))

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


class PipelineFunction:
    def __init__(self, func: RowApplier):
        self._id = str(uuid.uuid4())
        self._func = func

    def __call__(self, row: dict):
        return self._func(row)

    @property
    def id(self) -> str:
        return self._id


class Pipeline:
    def __init__(self, functions: list[PipelineFunction] = None):
        self._functions = functions or []
        self._id = str(uuid.uuid4())

    @staticmethod
    def _filter(pf: Column) -> RowApplier:
        return lambda row: row if pf.eval(row) else None

    @staticmethod
    def _subset(keys: list) -> RowApplier:
        return lambda row: {k: row[k] for k in keys}

    @staticmethod
    def _set_item(k: str, v: OpValue, row: dict) -> dict:
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
        return Column(name=item)

    def __setitem__(self, key: str, value: Column | OpValue):
        if isinstance(value, Column):
            self.apply(lambda row: self._set_item(key, value.eval(row), row))
        else:
            self.apply(lambda row: self._set_item(key, value, row))

    @staticmethod
    def _process_function(func: PipelineFunction, data: list | dict):
        print(f'processing func {func._func.__name__}')
        if isinstance(data, dict):
            return func(data)
        else:
            print('processing list')
            data = [func(d) for d in data]
            print(data)
            return data if any(data) else None

    def process(self, event: dict | List[dict]) -> Optional[dict | List[dict]]:
        result = event
        for func in self._functions:
            print(f'processing func {func._func.__name__}')
            result = self._process_function(func, result)
            if not result:
                print('result was filtered')
                break
        return result


if __name__ == "__main__":
    def more_rows(data):
        data_out = []
        for row in data['numbers']:
            data_out.append({**data, 'numbers': row})
        return data_out
    p = Pipeline()
    p = p.apply(more_rows)
    p = p[['numbers']]
    result = p.process(event={'numbers': [1, 2, 3], 'letters': 'woo'})
    print(result)

    p = Pipeline()
    p["z"] = p["x"].apply(lambda _: _+500)
    p["a"] = p["x"] + p["y"]
    p["b"] = 12
    p = p[(p['x'] >= 1) & (p['y'] < 10)]
    result = p.process(event={"x": 1, "y": 2, "q": 3})

