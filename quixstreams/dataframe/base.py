import abc
from typing import Any, Optional

from quixstreams.core.stream import Stream, VoidExecutor
from quixstreams.models.messagecontext import MessageContext


class BaseStreaming:
    @property
    @abc.abstractmethod
    def stream(self) -> Stream: ...

    @abc.abstractmethod
    def compose(self, *args, **kwargs) -> dict[str, VoidExecutor]: ...

    @abc.abstractmethod
    def test(
        self,
        value: Any,
        key: Any,
        timestamp: int,
        headers: Optional[Any] = None,
        ctx: Optional[MessageContext] = None,
    ) -> Any: ...
