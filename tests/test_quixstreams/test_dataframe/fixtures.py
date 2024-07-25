from typing import Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.dataframe.dataframe import StreamingDataFrame
from quixstreams.models.topics import Topic, TopicManager
from quixstreams.processing import ProcessingContext, PausingManager
from quixstreams.rowconsumer import RowConsumer
from quixstreams.rowproducer import RowProducer
from quixstreams.state import StateStoreManager


@pytest.fixture()
def dataframe_factory(topic_manager_topic_factory, topic_manager_factory):
    def factory(
        topic: Optional[Topic] = None,
        topic_manager: Optional[TopicManager] = None,
        state_manager: Optional[StateStoreManager] = None,
        producer: Optional[RowProducer] = None,
    ) -> StreamingDataFrame:
        producer = producer if producer is not None else MagicMock(spec_set=RowProducer)
        topic_manager = topic_manager or MagicMock(spec=TopicManager)
        state_manager = state_manager or MagicMock(spec=StateStoreManager)
        topic = topic or topic_manager_topic_factory("test")

        processing_ctx = ProcessingContext(
            producer=producer,
            consumer=MagicMock(spec_set=RowConsumer),
            commit_interval=0,
            state_manager=state_manager,
        )
        processing_ctx.init_checkpoint()

        return StreamingDataFrame(
            topic=topic, topic_manager=topic_manager, processing_context=processing_ctx
        )

    return factory
