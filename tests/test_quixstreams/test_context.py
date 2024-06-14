import contextvars

import pytest

from quixstreams.context import (
    message_context,
    set_message_context,
    MessageContextNotSetError,
)


class TestContext:
    def test_get_current_context_not_set_fails(self):
        ctx = contextvars.copy_context()
        with pytest.raises(MessageContextNotSetError):
            ctx.run(message_context)

    def test_set_current_context_and_run(self, message_context_factory):
        ctx = contextvars.copy_context()
        message_ctx1 = message_context_factory(topic="test")
        message_ctx2 = message_context_factory(topic="test2")
        for message_ctx in [message_ctx1, message_ctx2]:
            ctx.run(set_message_context, message_ctx)
            assert ctx.run(lambda: message_context()) == message_ctx
