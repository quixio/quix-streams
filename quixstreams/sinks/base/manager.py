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

    def start_sinks(self):
        for sink in self.sinks:
            sink.start()

    @property
    def sinks(self) -> List[BaseSink]:
        return list(self._sinks.values())
