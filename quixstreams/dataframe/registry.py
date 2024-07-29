from typing import List, Dict, Optional, Callable, Any, TYPE_CHECKING

from .exceptions import DuplicateStreamingDataFrame, GroupByLimitExceeded
from quixstreams.core.stream import Stream, VoidExecutor

from quixstreams.models import Topic

if TYPE_CHECKING:
    from .dataframe import StreamingDataFrame


class DataframeRegistry:
    def __init__(self):
        self._registry: Dict[str, Stream] = {}
        self._topics: List[Topic] = []
        self._branches: List[str] = []

    @property
    def registry(self):
        return self._registry

    def register(
        self,
        new_sdf: "StreamingDataFrame",
        original_sdf: Optional["StreamingDataFrame"] = None,
    ):
        if (topic := new_sdf.topic).name in self._registry:
            raise DuplicateStreamingDataFrame(
                f"There is already a StreamingDataFrame using topic {topic.name}"
            )
        if original_sdf is not None and original_sdf.topic.name in self._branches:
            raise GroupByLimitExceeded(
                "Only one GroupBy operation is allowed per StreamingDataFrame"
            )
        self._topics.append(topic)
        self._registry[topic.name] = new_sdf.stream

    def compose_all(
        self, sink: Optional[Callable[[Any, Any, int, Any], None]] = None
    ) -> Dict[str, VoidExecutor]:
        return {
            topic: stream.compose(sink=sink) for topic, stream in self._registry.items()
        }

    def consumer_topics(self) -> List[Topic]:
        return self._topics
