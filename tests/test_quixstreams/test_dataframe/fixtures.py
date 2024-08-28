from typing import Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.dataframe.dataframe import StreamingDataFrame
from quixstreams.dataframe.registry import DataframeRegistry
from quixstreams.models.topics import Topic, TopicManager
from quixstreams.processing import ProcessingContext, PausingManager
from quixstreams.rowconsumer import RowConsumer
from quixstreams.rowproducer import RowProducer
from quixstreams.sinks import SinkManager
from quixstreams.state import StateStoreManager


@pytest.fixture()
def dataframe_factory(topic_manager_topic_factory, topic_manager_factory):
    def factory(
        topic: Optional[Topic] = None,
        topic_manager: Optional[TopicManager] = None,
        state_manager: Optional[StateStoreManager] = None,
        producer: Optional[RowProducer] = None,
        registry: Optional[DataframeRegistry] = None,
    ) -> StreamingDataFrame:
        producer = producer if producer is not None else MagicMock(spec_set=RowProducer)
        topic_manager = topic_manager or MagicMock(spec=TopicManager)
        state_manager = state_manager or MagicMock(spec=StateStoreManager)
        topic = topic or topic_manager_topic_factory("test")
        consumer = MagicMock(spec_set=RowConsumer)
        pausing_manager = PausingManager(consumer=consumer)
        sink_manager = SinkManager()

        processing_ctx = ProcessingContext(
            producer=producer,
            consumer=consumer,
            commit_interval=0,
            state_manager=state_manager,
            pausing_manager=pausing_manager,
            sink_manager=sink_manager,
        )
        processing_ctx.init_checkpoint()

        if not registry:
            registry = DataframeRegistry()

        sdf = StreamingDataFrame(
            topic=topic,
            topic_manager=topic_manager,
            registry=registry,
            processing_context=processing_ctx,
        )
        registry.register_root(sdf)
        return sdf

    return factory
