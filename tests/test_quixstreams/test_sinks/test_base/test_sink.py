from tests.utils import DummySink


class TestBatchingSink:
    def test_add_and_flush(self):
        sink = DummySink()
        topic, partition = "topic", 0
        key, value = "key", "value"

        sink.add(
            value=value,
            key=key,
            topic=topic,
            partition=partition,
            offset=0,
            timestamp=0,
            headers=[],
        )
        # Flush the sink twice to ensure that records are flushed once
        sink.flush()
        sink.flush()
        assert len(sink.results) == 1
        result = sink.results[0]
        assert result.value == value
        assert result.key == key
        assert result.offset == 0
        assert result.timestamp == 0
        assert result.headers == []

    def test_flush_empty(self):
        sink = DummySink()
        sink.flush()
        assert sink.results == []

    def test_on_paused(self):
        sink = DummySink()
        topic, partition = "topic", 0
        key, value = "key", "value"

        sink.add(
            value=value,
            key=key,
            topic=topic,
            partition=partition,
            offset=0,
            timestamp=0,
            headers=[],
        )
        sink.on_paused()
        sink.flush()
        assert sink.results == []

    def test_on_paused_no_batch(self):
        sink = DummySink()
        sink.on_paused()
        sink.flush()
        assert sink.results == []
