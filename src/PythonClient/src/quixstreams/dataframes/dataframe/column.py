from typing import Self, Optional, Any, Callable, TypeAlias, Union
import operator
from quixstreams.dataframes.models.rows import Row

UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days", "w": "weeks"}

OpValue: TypeAlias = Union[int, float, bool]
ColumnValue: TypeAlias = Union[int, float, bool, list, dict]
ColumnApplier: TypeAlias = Callable[[dict], OpValue]


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

    @staticmethod
    def _and(a, b):
        return a and b

    @staticmethod
    def _or(a, b):
        return a or b

    def __and__(self, other):
        return self._operation(other, self._and)

    def __or__(self, other):
        return self._operation(other, self._or)

    def __mod__(self, other):
        return self._operation(other, operator.mod)

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
