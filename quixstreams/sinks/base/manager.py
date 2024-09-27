from typing import List

from .sink import BaseSink

__all__ = ("SinkManager",)


class SinkManager:
    def __init__(self):
        self._sinks = {}

    def register(self, sink: BaseSink):
        sink_id = id(sink)
        if sink_id not in self._sinks:
            self._sinks[id(sink)] = sink

    @property
    def sinks(self) -> List[BaseSink]:
        return list(self._sinks.values())
