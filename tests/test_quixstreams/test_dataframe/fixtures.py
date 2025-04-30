from typing import Optional
from unittest.mock import MagicMock

import pytest

from quixstreams.dataframe.dataframe import StreamingDataFrame
from quixstreams.dataframe.registry import DataFrameRegistry
from quixstreams.internal_consumer import InternalConsumer
from quixstreams.models.topics import Topic, TopicManager
from quixstreams.processing import ProcessingContext
from quixstreams.rowproducer import RowProducer
from quixstreams.sinks import SinkManager
from quixstreams.state import StateStoreManager


@pytest.fixture()
def dataframe_factory(topic_manager_topic_factory, topic_manager_factory):
    # Init default registry here to share between SDFs in the same test
    default_registry = DataFrameRegistry()

    def factory(
        topic: Optional[Topic] = None,
        topic_manager: Optional[TopicManager] = None,
        state_manager: Optional[StateStoreManager] = None,
        producer: Optional[RowProducer] = None,
        registry: Optional[DataFrameRegistry] = None,
    ) -> StreamingDataFrame:
        producer = producer if producer is not None else MagicMock(spec_set=RowProducer)
        topic_manager = topic_manager or topic_manager_factory()
        state_manager = state_manager or MagicMock(spec=StateStoreManager)
        topic = topic or topic_manager_topic_factory(
            "test", topic_manager=topic_manager
        )
        consumer = MagicMock(spec_set=InternalConsumer)
        sink_manager = SinkManager()
        registry = registry or default_registry

        processing_ctx = ProcessingContext(
            producer=producer,
            consumer=consumer,
            commit_interval=0,
            state_manager=state_manager,
            sink_manager=sink_manager,
            dataframe_registry=registry,
        )
        processing_ctx.init_checkpoint()

        sdf = StreamingDataFrame(
            topic,
            topic_manager=topic_manager,
            registry=registry,
            processing_context=processing_ctx,
        )
        registry.register_root(sdf)
        return sdf

    return factory
