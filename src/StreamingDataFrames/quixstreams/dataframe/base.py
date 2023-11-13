import abc
from typing import Optional, Any

from quixstreams.models.messagecontext import MessageContext
from quixstreams.core.stream import Stream, StreamCallable


class BaseStreaming:
    @property
    @abc.abstractmethod
    def stream(self) -> Stream:
        ...

    @abc.abstractmethod
    def compile(self, *args, **kwargs) -> StreamCallable:
        ...

    @abc.abstractmethod
    def test(self, value: Any, ctx: Optional[MessageContext] = None) -> Any:
        ...
