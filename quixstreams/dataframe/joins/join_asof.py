from datetime import timedelta
from typing import TYPE_CHECKING, Any, Callable, Literal, Optional, Union, get_args

from .base import Join, OnOverlap

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = ("AsOfJoin",)

AsOfJoinHow = Literal["inner", "left"]

DISCARDED = object()
block_all = lambda value: False
block_discarded = lambda value: value is not DISCARDED


class AsOfJoin(Join):
    def __init__(
        self,
        how: AsOfJoinHow,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
    ) -> None:
        if how not in get_args(AsOfJoinHow):
            raise ValueError(f"Join type not supported: {how}")
        super().__init__(how, on_merge, grace_ms, store_name)

    def _prepare_join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame":
        self._register_store(right, keep_duplicates=False)

        tx = self._get_transaction
        is_inner_join = self._how == "inner"
        merger = self._merger

        def left_func(value, key, timestamp, headers):
            if right_value := tx(right).get_latest(timestamp=timestamp, prefix=key):
                return merger(value, right_value)
            return DISCARDED if is_inner_join else merger(value, None)

        def right_func(value, key, timestamp, headers):
            tx(right).set_for_timestamp(timestamp=timestamp, value=value, prefix=key)

        right = right.update(right_func, metadata=True).filter(block_all)
        left = left.apply(left_func, metadata=True).filter(block_discarded)
        return left.concat(right)
