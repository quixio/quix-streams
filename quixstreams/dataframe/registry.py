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
    def registry(self) -> Dict[str, Stream]:
        return self._registry

    def register(
        self,
        new_sdf: "StreamingDataFrame",
        branched_sdf: Optional["StreamingDataFrame"] = None,
    ):
        if (topic := new_sdf.topic).name in self._registry:
            raise DuplicateStreamingDataFrame(
                f"There is already a StreamingDataFrame using topic {topic.name}"
            )
        if branched_sdf is not None:
            if branched_sdf.topic.name not in self._branches:
                self._branches.append(topic.name)
            else:
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
