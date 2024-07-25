from typing import List

from .base import Sink


class SinkManager:
    def __init__(self):
        self._sinks = {}

    def register(self, sink: Sink):
        sink_id = id(sink)
        if sink_id not in self._sinks:
            self._sinks[id(sink)] = sink

    @property
    def sinks(self) -> List[Sink]:
        return list(self._sinks.values())
