import typing

from .base import Join

if typing.TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame


__all__ = ("AsOfJoin",)

DISCARDED = object()


class AsOfJoin(Join):
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
            right_value = tx(right).get_latest(timestamp=timestamp, prefix=key)
            if is_inner_join and not right_value:
                return DISCARDED
            return merger(value, right_value)

        def right_func(value, key, timestamp, headers):
            tx(right).set_for_timestamp(timestamp=timestamp, value=value, prefix=key)

        right = right.update(right_func, metadata=True).filter(lambda value: False)
        left = left.apply(left_func, metadata=True).filter(
            lambda value: value is not DISCARDED
        )
        return left.concat(right)
