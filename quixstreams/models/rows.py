from copy import deepcopy
from typing import Optional, Union, List, Any, KeysView, ValuesView, ItemsView, Mapping

from typing_extensions import Self

from .messages import MessageHeadersTuples
from .timestamps import MessageTimestamp
from .messagecontext import MessageContext


class Row:
    """
    Row is a dict-like interface on top of the message data + some Kafka props
    """

    __slots__ = (
        "value",
        "context",
    )

    def __init__(
        self,
        value: Optional[dict],
        context: MessageContext,
    ):
        self.value = value
        self.context = context

    @property
    def topic(self) -> str:
        return self.context.topic

    @property
    def partition(self) -> int:
        return self.context.partition

    @property
    def offset(self) -> int:
        return self.context.offset

    @property
    def size(self) -> int:
        return self.context.size

    @property
    def timestamp(self) -> MessageTimestamp:
        return self.context.timestamp

    @property
    def key(self) -> Optional[Any]:
        return self.context.key

    @property
    def headers(self) -> Optional[Union[Mapping, MessageHeadersTuples]]:
        return self.context.headers

    @property
    def latency(self) -> Optional[float]:
        return self.context.latency

    @property
    def leader_epoch(self) -> Optional[int]:
        return self.context.leader_epoch

    def __getitem__(self, item: Union[str, List[str]]) -> Any:
        if isinstance(item, list):
            return {k: self.value[k] for k in item}
        return self.value[item]

    def __setitem__(self, key: str, value: any):
        self.value[key] = value

    def keys(self) -> KeysView:
        """
        Also allows unpacking row.value via **row
        """
        return self.value.keys()

    def values(self) -> ValuesView:
        return self.value.values()

    def items(self) -> ItemsView:
        return self.value.items()

    def clone(self, value: dict) -> Self:
        """
        Manually clone the Row; doing it this way is much faster than doing a deepcopy
        on the entire Row object.
        """
        return self.__class__(value=deepcopy(value), context=self.context)
