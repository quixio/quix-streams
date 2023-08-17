import logging
import operator
import uuid
from functools import partial
from typing import Self, Optional, Any, Callable, TypeAlias, Union

from ..models import Row

logger = logging.getLogger(__name__)

OpValue: TypeAlias = Union[int, float, bool]
ColumnValue: TypeAlias = Union[int, float, bool, list, dict]
ColumnApplier: TypeAlias = Callable[[dict], OpValue]

__all__ = (
    'Column',
    'OpValue',
    'ColumnValue',
    'ColumnApplier'
)


def primitive_to_column(value: Any) -> 'Column':
    if not isinstance(value, Column):
        return Column(_eval_func=lambda row: value, _ops='to_column')
    return value


# TODO: Docstrings
class Column:
    def __init__(
        self, col_name: Optional[str] = None,
        _eval_func: Optional[ColumnApplier] = None, _ops: str = None
    ):
        self.col_name = col_name
        self._id = str(uuid.uuid4())
        self._ops = _ops
        self._eval_func = _eval_func if _eval_func else lambda row: row[self.col_name]
        logger.debug(f'Created column {self._id}, ops={self.name}')

    @property
    def name(self) -> str:
        return self.col_name or self._ops

    def _do_op(self, other, op, row):
        return op(self.eval(row), primitive_to_column(other).eval(row))

    def _operation(self, other: Any, op: Callable[[OpValue, OpValue], OpValue]) -> Self:
        other_name = other.name if isinstance(other, Column) else "to_column({other})"
        return Column(
            _eval_func=partial(self._do_op, other, op),
            _ops=f'{op.__name__}({self.name},{other_name})'
        )

    def eval(self, row: Row) -> ColumnValue:
        result = self._eval_func(row)
        logger.debug(f'{self._id} eval: {row}, column {self.name}; result: {result}')
        return result

    def apply(self, func: ColumnApplier) -> Self:
        return Column(
            _eval_func=lambda row: func(self.eval(row)),
            _ops=f'apply:{func.__name__}({self.name})'
        )

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
