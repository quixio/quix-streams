from typing import Optional
from unittest.mock import PropertyMock, patch

import pytest

from quixstreams.app import Application, MessageProcessedCallback, ProcessingGuarantee
from quixstreams.error_callbacks import (
    ConsumerErrorCallback,
    ProcessingErrorCallback,
    ProducerErrorCallback,
)
from quixstreams.kafka import AutoOffsetReset
from quixstreams.models.topics import TopicManager


@pytest.fixture()
def app_factory(kafka_container, random_consumer_group, tmp_path, store_type):
    def factory(
        consumer_group: Optional[str] = None,
        auto_offset_reset: AutoOffsetReset = "latest",
        commit_interval: float = 5.0,
        commit_every: int = 0,
        consumer_extra_config: Optional[dict] = None,
        producer_extra_config: Optional[dict] = None,
        on_consumer_error: Optional[ConsumerErrorCallback] = None,
        on_producer_error: Optional[ProducerErrorCallback] = None,
        on_processing_error: Optional[ProcessingErrorCallback] = None,
        on_message_processed: Optional[MessageProcessedCallback] = None,
        state_dir: Optional[str] = None,
        auto_create_topics: bool = True,
        use_changelog_topics: bool = True,
        topic_manager: Optional[TopicManager] = None,
        processing_guarantee: ProcessingGuarantee = "at-least-once",
        request_timeout: float = 30,
    ) -> Application:
        state_dir = state_dir or (tmp_path / "state").absolute()
        return Application(
            broker_address=kafka_container.broker_address,
            consumer_group=consumer_group or random_consumer_group,
            auto_offset_reset=auto_offset_reset,
            commit_interval=commit_interval,
            commit_every=commit_every,
            consumer_extra_config=consumer_extra_config,
            producer_extra_config=producer_extra_config,
            on_consumer_error=on_consumer_error,
            on_producer_error=on_producer_error,
            on_processing_error=on_processing_error,
            on_message_processed=on_message_processed,
            state_dir=state_dir,
            auto_create_topics=auto_create_topics,
            use_changelog_topics=use_changelog_topics,
            topic_manager=topic_manager,
            processing_guarantee=processing_guarantee,
            request_timeout=request_timeout,
        )

    with patch(
        "quixstreams.state.manager.StateStoreManager.default_store_type",
        new_callable=PropertyMock,
    ) as m:
        m.return_value = store_type
        yield factory
