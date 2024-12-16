from typing import Optional

import pytest

from quixstreams.checkpointing import Checkpoint
from quixstreams.kafka import Consumer
from quixstreams.processing import PausingManager
from quixstreams.rowproducer import RowProducer
from quixstreams.sinks import SinkManager
from quixstreams.state import StateStoreManager


@pytest.fixture()
def checkpoint_factory(state_manager, consumer, row_producer_factory):
    def factory(
        commit_interval: float = 1,
        commit_every: int = 0,
        consumer_: Optional[Consumer] = None,
        producer_: Optional[RowProducer] = None,
        state_manager_: Optional[StateStoreManager] = None,
        sink_manager_: Optional[SinkManager] = None,
        pausing_manager_: Optional[PausingManager] = None,
        exactly_once: bool = False,
    ):
        consumer_ = consumer_ or consumer
        sink_manager_ = sink_manager_ or SinkManager()
        pausing_manager_ = pausing_manager_ or PausingManager(consumer=consumer)
        producer_ = producer_ or row_producer_factory(transactional=exactly_once)
        state_manager_ = state_manager_ or state_manager
        return Checkpoint(
            commit_interval=commit_interval,
            commit_every=commit_every,
            producer=producer_,
            consumer=consumer_,
            state_manager=state_manager_,
            sink_manager=sink_manager_,
            pausing_manager=pausing_manager_,
            exactly_once=exactly_once,
        )

    return factory
