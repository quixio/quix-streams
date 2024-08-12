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

        with pytest.raises(ValueError):
            manager.register(source1)

        source1.configure(topic1, None)
        manager.register(source1)

        # registering the same source twice fails
        with pytest.raises(ValueError):
            manager.register(source1)

        source2.configure(topic1, None)
        # registering a source with the same topic fails
        with pytest.raises(ValueError):
            manager.register(source2)

        source2.configure(topic2, None)
        manager.register(source2)
