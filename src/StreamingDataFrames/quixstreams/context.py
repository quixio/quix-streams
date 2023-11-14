from contextvars import ContextVar, copy_context
from typing import Optional

from quixstreams.exceptions import QuixException
from quixstreams.models.messagecontext import MessageContext

__all__ = (
    "MessageContextNotSetError",
    "set_message_context",
    "message_key",
    "message_context",
    "copy_context",
)

_current_message_context = ContextVar("current_message_context")


class MessageContextNotSetError(QuixException):
    ...


def set_message_context(context: Optional[MessageContext]):
    """
    Set a MessageContext for the current message in the given `contextvars.Context`

    :param context: instance of `MessageContext`
    """
    _current_message_context.set(context)


def message_context() -> MessageContext:
    """
    Get a MessageContext for the current message
    :return: instance of `MessageContext`
    """
    try:
        return _current_message_context.get()

    except LookupError:
        raise MessageContextNotSetError("Message context is not set")


def message_key() -> object:
    """
    Get current a message key.

    :return: a deserialized message key
    """
    return message_context().key
