import pytest

from quixstreams.models.topics import Topic
from quixstreams.sources import ValueIterableSource
from quixstreams.sources.manager import SourceManager


class TestSourceManager:
    def test_source_manager_register(self):
        manager = SourceManager()

        source1 = ValueIterableSource(name="foo", values=iter(range(10)))
        source2 = ValueIterableSource(name="bar", values=iter(range(10)))

        topic1 = Topic("foo", None)
        topic2 = Topic("bar", None)

        manager.register(source1, None, topic1)

        # registering the same source twice fails
        with pytest.raises(ValueError):
            manager.register(source1, None, topic2)

        # registering a source with the same topic fails
        with pytest.raises(ValueError):
            manager.register(source2, None, topic1)

        manager.register(source2, None, topic2)
