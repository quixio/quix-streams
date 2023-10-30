import operator
from typing import Optional, Any, Callable, Container

from typing_extensions import Self, TypeAlias, Union

from ..models import Row

ColumnApplier: TypeAlias = Callable[[Any], Any]

__all__ = ("Column", "ColumnApplier")


def invert(value):
    if isinstance(value, bool):
        return operator.not_(value)
    else:
        return operator.invert(value)


class Column:
    def __init__(
        self,
        col_name: Optional[str] = None,
        _eval_func: Optional[ColumnApplier] = None,
    ):
        self.col_name = col_name
        self._eval_func = _eval_func if _eval_func else lambda row: row[self.col_name]

    def __getitem__(self, item: Union[str, int]) -> Self:
        return self.__class__(_eval_func=lambda x: self.eval(x)[item])

    def _operation(self, other: Any, op: Callable[[Any, Any], Any]) -> Self:
        return self.__class__(
            _eval_func=lambda x: op(
                self.eval(x), other.eval(x) if isinstance(other, Column) else other
            ),
        )

    def eval(self, row: Row) -> Any:
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

    def isin(self, other: Container) -> Self:
        return self._operation(other, lambda a, b: operator.contains(b, a))

    def contains(self, other: Any) -> Self:
        return self._operation(other, operator.contains)

    def is_(self, other: Any) -> Self:
        """
        Check if column value refers to the same object as `other`
        :param other: object to check for "is"
        :return:
        """
        return self._operation(other, operator.is_)

    def isnot(self, other: Any) -> Self:
        """
        Check if column value refers to the same object as `other`
        :param other: object to check for "is"
        :return:
        """
        return self._operation(other, operator.is_not)

    def isnull(self) -> Self:
        """
        Check if column value is None
        """
        return self._operation(None, operator.is_)

    def notnull(self) -> Self:
        """
        Check if column value is not None
        """
        return self._operation(None, operator.is_not)

    def __and__(self, other: Any) -> Self:
        return self._operation(other, operator.and_)

    def __or__(self, other: Any) -> Self:
        return self._operation(other, operator.or_)

    def __mod__(self, other: Any) -> Self:
        return self._operation(other, operator.mod)

    def __add__(self, other: Any) -> Self:
        return self._operation(other, operator.add)

    def __sub__(self, other: Any) -> Self:
        return self._operation(other, operator.sub)

    def __mul__(self, other: Any) -> Self:
        return self._operation(other, operator.mul)

    def __truediv__(self, other: Any) -> Self:
        return self._operation(other, operator.truediv)

    def __eq__(self, other: Any) -> Self:
        return self._operation(other, operator.eq)

    def __ne__(self, other: Any) -> Self:
        return self._operation(other, operator.ne)

    def __lt__(self, other: Any) -> Self:
        return self._operation(other, operator.lt)

    def __le__(self, other: Any) -> Self:
        return self._operation(other, operator.le)

    def __gt__(self, other: Any) -> Self:
        return self._operation(other, operator.gt)

    def __ge__(self, other: Any) -> Self:
        return self._operation(other, operator.ge)

    def __invert__(self) -> Self:
        return self.__class__(_eval_func=lambda x: invert(self.eval(x)))
