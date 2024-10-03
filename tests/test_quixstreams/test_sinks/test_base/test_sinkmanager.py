from quixstreams.sinks import SinkManager
from tests.utils import DummySink


class TestSinkManager:
    def test_sink_manager_register(self):
        sink_manager = SinkManager()
        sink = DummySink()

        sink_manager.register(sink)
        # The same sink can be registered twice
        sink_manager.register(sink)
        assert sink_manager.sinks == [sink]
