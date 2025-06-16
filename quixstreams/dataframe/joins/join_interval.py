import typing
from datetime import timedelta
from typing import Any, Callable, Optional, Union

from quixstreams.dataframe.utils import ensure_milliseconds

from .base import Join, JoinHow, OnOverlap

if typing.TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = ("IntervalJoin",)


class IntervalJoin(Join):
    """A join that matches records based on time intervals.

    This join type allows matching records from two topics that fall within specified
    time intervals of each other. For each record, it looks for matches within a
    backward and forward time window.
    """

    def __init__(
        self,
        how: JoinHow,
        on_merge: Union[OnOverlap, Callable[[Any, Any], Any]],
        grace_ms: Union[int, timedelta],
        store_name: Optional[str] = None,
        backward_ms: Union[int, timedelta] = 0,
        forward_ms: Union[int, timedelta] = 0,
    ):
        super().__init__(how, on_merge, grace_ms, store_name)
        self._backward_ms = ensure_milliseconds(backward_ms)
        self._forward_ms = ensure_milliseconds(forward_ms)

        if self._backward_ms > self._grace_ms:
            raise ValueError(
                "The backward_ms must not be greater than the grace_ms "
                "to avoid losing data."
            )

    def _prepare_join(
        self,
        left: "StreamingDataFrame",
        right: "StreamingDataFrame",
    ) -> "StreamingDataFrame":
        self._register_store(left, keep_duplicates=True)
        self._register_store(right, keep_duplicates=True)

        tx = self._get_transaction
        is_inner_join = self._how == "inner"
        merger = self._merger
        backward_ms = self._backward_ms
        forward_ms = self._forward_ms

        def left_func(value, key, timestamp, headers):
            tx(left).set_for_timestamp(timestamp=timestamp, value=value, prefix=key)

            if right_values := tx(right).get_interval(
                start=timestamp - backward_ms,
                end=timestamp + 1,  # +1 because end is exclusive
                prefix=key,
            ):
                return [merger(value, right_value) for right_value in right_values]
            return [] if is_inner_join else [merger(value, None)]

        def right_func(value, key, timestamp, headers):
            tx(right).set_for_timestamp(timestamp=timestamp, value=value, prefix=key)

            left_values = tx(left).get_interval(
                start=timestamp - forward_ms,
                end=timestamp + 1,  # +1 because end is exclusive
                prefix=key,
            )
            return [merger(left_value, value) for left_value in left_values]

        right = right.apply(right_func, expand=True, metadata=True)
        left = left.apply(left_func, expand=True, metadata=True)
        return left.concat(right)
