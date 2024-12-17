from contextvars import ContextVar, copy_context
from typing import Optional

from quixstreams.exceptions import QuixException
from quixstreams.models.messagecontext import MessageContext

__all__ = (
    "MessageContextNotSetError",
    "set_message_context",
    "message_context",
    "copy_context",
)

_current_message_context: ContextVar[Optional[MessageContext]] = ContextVar(
    "current_message_context"
)


class MessageContextNotSetError(QuixException): ...


def set_message_context(context: Optional[MessageContext]):
    """
    Set a MessageContext for the current message in the given `contextvars.Context`

    >***NOTE:*** This is for advanced usage only. If you need to change the message key,
    `StreamingDataFrame.to_topic()` has an argument for it.


    Example Snippet:

    ```python
    from quixstreams import Application, set_message_context, message_context

    # Changes the current sdf value based on what the message partition is.
    def alter_context(value):
        context = message_context()
        if value > 1:
            context.headers = context.headers + (b"cool_new_header", value.encode())
            set_message_context(context)

    app = Application()
    sdf = app.dataframe()
    sdf = sdf.update(lambda value: alter_context(value))
    ```


    :param context: instance of `MessageContext`
    """
    _current_message_context.set(context)


def message_context() -> MessageContext:
    """
    Get a MessageContext for the current message, which houses most of the message
    metadata, like:
        - key
        - timestamp
        - partition
        - offset


    Example Snippet:

    ```python
    from quixstreams import Application, message_context

    # Changes the current sdf value based on what the message partition is.

    app = Application()
    sdf = app.dataframe()
    sdf = sdf.apply(lambda value: 1 if message_context().partition == 2 else 0)
    ```

    :return: instance of `MessageContext`
    """
    try:
        ctx = _current_message_context.get()
    except LookupError:
        raise MessageContextNotSetError("Message context is not set")

    if ctx is None:
        raise MessageContextNotSetError("Message context is not set")

    return ctx
