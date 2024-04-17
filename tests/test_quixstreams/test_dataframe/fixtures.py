from typing import Optional
import pytest

from quixstreams.dataframe.dataframe import StreamingDataFrame
from quixstreams.models.topics import Topic


@pytest.fixture()
def dataframe_factory(topic_manager_topic_factory, app_factory):
    def factory(
        topic: Optional[Topic] = None,
        state_manager: bool = False,
    ) -> StreamingDataFrame:
        sdf = StreamingDataFrame(
            topic=topic or topic_manager_topic_factory("test"),
            application=app_factory(),
        )
        if state_manager:
            sdf._application._state_manager = state_manager
        return sdf

    return factory
