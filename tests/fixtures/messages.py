import pytest

from quixstreams.models import MessageContext
from quixstreams.models.rows import Row


@pytest.fixture()
def message_context_factory():
    def factory(
        topic: str = "test",
    ) -> MessageContext:
        return MessageContext(
            topic=topic,
            partition=0,
            offset=0,
            size=0,
        )

    return factory


@pytest.fixture()
def row_factory():
    """
    This factory includes only the fields typically handed to a producer when
    producing a message; more generally, the fields you would likely
    need to validate upon producing/consuming.
    """

    def factory(
        value,
        topic="input-topic",
        key=b"key",
        timestamp: int = 0,
        headers=None,
        partition: int = 0,
        offset: int = 0,
    ) -> Row:
        context = MessageContext(
            topic=topic,
            partition=partition,
            offset=offset,
            size=0,
        )
        return Row(
            value=value, key=key, timestamp=timestamp, context=context, headers=headers
        )

    return factory
