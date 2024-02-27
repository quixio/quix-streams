from typing import Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.dataframe.dataframe import StreamingDataFrame
from quixstreams.models.topics import Topic
from quixstreams.state import StateStoreManager


@pytest.fixture()
def dataframe_factory(topic_manager_topic_factory):
    def factory(
        topic: Optional[Topic] = None,
        state_manager: Optional[StateStoreManager] = None,
    ) -> StreamingDataFrame:
        return StreamingDataFrame(
            topic=topic or topic_manager_topic_factory("test"),
            state_manager=state_manager or MagicMock(spec=StateStoreManager),
        )

    return factory
