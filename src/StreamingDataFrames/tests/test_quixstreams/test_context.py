import contextvars

import pytest

from quixstreams.models import MessageTimestamp, MessageContext
from quixstreams.context import (
    get_current_context,
    set_current_context,
    get_current_key,
    MessageContextNotSetError,
)


@pytest.fixture()
def message_context_factory():
    def factory(key: object = "test") -> MessageContext:
        return MessageContext(
            key=key,
            topic="test",
            partition=0,
            offset=0,
            size=0,
            timestamp=MessageTimestamp.create(0, 0),
        )

    return factory


class TestContext:
    def test_get_current_context_not_set_fails(self):
        ctx = contextvars.copy_context()
        with pytest.raises(MessageContextNotSetError):
            ctx.run(get_current_context)

    def test_set_current_context_and_run(self, message_context_factory):
        ctx = contextvars.copy_context()
        message_ctx1 = message_context_factory(key="test")
        message_ctx2 = message_context_factory(key="test2")
        for message_ctx in [message_ctx1, message_ctx2]:
            ctx.run(set_current_context, message_ctx)
            assert ctx.run(lambda: get_current_context()) == message_ctx

    def test_get_current_key_success(self, message_context_factory):
        ctx = contextvars.copy_context()
        message_ctx = message_context_factory(key="test")
        ctx.run(set_current_context, message_ctx)
        assert ctx.run(get_current_key) == message_ctx.key

    def test_get_current_key_not_set_fails(self):
        ctx = contextvars.copy_context()
        with pytest.raises(MessageContextNotSetError):
            ctx.run(get_current_key)
