from typing import List

from .base import Sink
from .exceptions import SinkAlreadyRegisteredError


class SinkManager:
    def __init__(self):
        self._sinks = {}

    def register(self, sink: Sink):
        if id(sink) in self._sinks:
            raise SinkAlreadyRegisteredError(f"Sink {sink} is already registered")
        self._sinks[id(sink)] = sink

    @property
    def sinks(self) -> List[Sink]:
        return list(self._sinks.values())
