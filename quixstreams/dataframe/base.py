import abc
from typing import Optional, Any

from quixstreams.core.stream import Stream, StreamCallable
from quixstreams.models.messagecontext import MessageContext


class BaseStreaming:
    @property
    @abc.abstractmethod
    def stream(self) -> Stream:
        ...

    @abc.abstractmethod
    def compose(self, *args, **kwargs) -> StreamCallable:
        ...

    @abc.abstractmethod
    def test(self, value: Any, ctx: Optional[MessageContext] = None) -> Any:
        ...
