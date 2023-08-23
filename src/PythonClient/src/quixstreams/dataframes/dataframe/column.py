import operator
from typing import Self, Optional, Any, Callable, TypeAlias, Union

from ..models import Row

OpValue: TypeAlias = Union[int, float, bool]
ColumnValue: TypeAlias = Union[int, float, bool, list, dict]
ColumnApplier: TypeAlias = Callable[[ColumnValue], OpValue]

__all__ = ("Column", "OpValue", "ColumnValue", "ColumnApplier")


class Column:
    def __init__(
        self,
        col_name: Optional[str] = None,
        _eval_func: Optional[ColumnApplier] = None,
    ):
        self.col_name = col_name
        self._eval_func = _eval_func if _eval_func else lambda row: row[self.col_name]

    def _operation(self, other: Any, op: Callable[[OpValue, OpValue], OpValue]) -> Self:
        return Column(
            _eval_func=lambda x: op(
                self.eval(x), other.eval(x) if isinstance(other, Column) else other
            ),
        )

    def eval(self, row: Row) -> ColumnValue:
        """
        Execute all the functions accumulated on this Column.

        :param row: A Quixstreams Row
        :return: A primitive type
        """
        return self._eval_func(row)

    def apply(self, func: ColumnApplier) -> Self:
        """
        Add a callable to the execution list for this column.

        The provided callable should accept a single argument, which will be its input.
        The provided callable should similarly return one output, or None

        :param func: a callable with one argument and one output
        :return: a new Column with the new callable added
        """
        return Column(_eval_func=lambda x: func(self.eval(x)))

    def __and__(self, other):
        return self._operation(other, operator.and_)

    def __or__(self, other):
        return self._operation(other, operator.or_)

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
