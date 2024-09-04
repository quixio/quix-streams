import pytest

from unittest.mock import create_autospec

from quixstreams.rowproducer import RowProducer
from quixstreams.models.topics import Topic
from quixstreams.sources.iterable import IterableSource


class TestIterableSource:
    @pytest.fixture
    def producer(self):
        producer = create_autospec(RowProducer)
        producer.flush.return_value = 0
        return producer

    def test_iterable_source_fixed_key(self, producer):
        messages = [0, 1, 2]

        source = IterableSource(
            name="test-source", key="test", callable=lambda: messages
        )
        source.configure(
            producer=producer, topic=Topic("test-topic", None, value_serializer="json")
        )
        source.run()

        producer.produce.assert_called()
        producer.produce.call_count == 3

        calls = producer.produce.call_args_list
        assert calls[0].kwargs["key"] == "test"
        assert calls[0].kwargs["value"] == b"0"
        assert calls[2].kwargs["key"] == "test"
        assert calls[2].kwargs["value"] == b"2"

    def test_iterable_source(self, producer):
        messages = {
            "key0": 0,
            "key1": 1,
            "key2": 2,
        }

        source = IterableSource(name="test-source", callable=messages.items)
        source.configure(
            producer=producer, topic=Topic("test-topic", None, value_serializer="json")
        )
        source.run()

        producer.produce.assert_called()
        producer.produce.call_count == 3

        calls = producer.produce.call_args_list
        assert calls[0].kwargs["key"] == "key0"
        assert calls[0].kwargs["value"] == b"0"
        assert calls[2].kwargs["key"] == "key2"
        assert calls[2].kwargs["value"] == b"2"

    def test_iterable_source_with_error(self, producer):
        def values():
            yield "test", 0
            yield "test", 1
            yield "test", 2
            raise RuntimeError("test error")

        source = IterableSource(name="test-source", callable=values)
        source.configure(
            producer=producer, topic=Topic("test-topic", None, value_serializer="json")
        )

        with pytest.raises(RuntimeError) as exc_info:
            source.run()

        assert str(exc_info.value) == "test error"

        producer.produce.assert_called()
        producer.produce.call_count == 3

        calls = producer.produce.call_args_list
        assert calls[0].kwargs["key"] == "test"
        assert calls[0].kwargs["value"] == b"0"
        assert calls[2].kwargs["key"] == "test"
        assert calls[2].kwargs["value"] == b"2"
