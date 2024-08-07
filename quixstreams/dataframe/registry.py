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
        self._branch_origins: Dict[str, str] = {}

    @property
    def registry(self) -> Dict[str, Stream]:
        return self._registry

    def register_root(
        self,
        new_sdf: "StreamingDataFrame",
    ):
        if (topic := new_sdf.topic).name in self._registry:
            raise DuplicateStreamingDataFrame(
                f"There is already a StreamingDataFrame using topic {topic.name}"
            )
        self._topics.append(topic)
        self._registry[topic.name] = new_sdf.stream

    def _check_branch_count(self, current_branch: str):
        branch_count = 0
        while origin := self._branch_origins.get(current_branch):
            branch_count += 1
            current_branch = origin
        if branch_count >= 1:
            raise GroupByLimitExceeded(
                "Only one GroupBy operation is allowed per StreamingDataFrame"
            )

    def register_branch(
        self, branched_sdf: "StreamingDataFrame", new_branch: "StreamingDataFrame"
    ):
        branched_topic = branched_sdf.topic.name
        self._check_branch_count(branched_topic)
        self.register_root(new_branch)
        self._branch_origins[new_branch.topic.name] = branched_topic

    def compose_all(
        self, sink: Optional[Callable[[Any, Any, int, Any], None]] = None
    ) -> Dict[str, VoidExecutor]:
        return {
            topic: stream.compose(sink=sink) for topic, stream in self._registry.items()
        }

    def consumer_topics(self) -> List[Topic]:
        return self._topics
