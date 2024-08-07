import pytest

from quixstreams.models.topics import Topic
from quixstreams.models.messages import KafkaMessage
from quixstreams.sources.iterable import ValueIterableSource, KeyValueIterableSource


class TestIterableSource:
    def test_valueiterable_source(self):
        source = ValueIterableSource(
            name="test-source", key="test", values=iter(range(10))
        )
        source.configure(Topic("test-topic", None, value_serializer="json"), None)

        result = []
        while len(result) < 10:
            result.append(source.poll())

        assert len(result) == 10
        assert isinstance(result[0], KafkaMessage)
        assert result[0].key == "test"
        assert result[0].value == b"0"
        assert result[9].key == "test"
        assert result[9].value == b"9"

    def test_keyvalueiterable_source(self):
        source = KeyValueIterableSource(
            name="test-source", iterable=zip(range(10), range(10))
        )
        source.configure(Topic("test-topic", None, value_serializer="json"), None)

        result = []
        while len(result) < 10:
            result.append(source.poll())

        assert len(result) == 10
        assert isinstance(result[0], KafkaMessage)
        assert result[0].key == 0
        assert result[0].value == b"0"
        assert result[9].key == 9
        assert result[9].value == b"9"

    def test_valueiterable_source_with_error(self):
        def values():
            yield 0
            yield 1
            yield 2
            raise RuntimeError("test error")

        source = ValueIterableSource(name="test-source", key="test", values=values())
        source.configure(Topic("test-topic", None, value_serializer="json"), None)

        result = []
        with pytest.raises(RuntimeError):
            while len(result) < 4:
                result.append(source.poll())

        assert len(result) == 3
        assert isinstance(result[0], KafkaMessage)
        assert result[0].key == "test"
        assert result[0].value == b"0"
