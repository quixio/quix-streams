from contextvars import ContextVar, copy_context
from typing import Optional

from quixstreams.exceptions import QuixException
from quixstreams.models.messagecontext import MessageContext

__all__ = (
    "MessageContextNotSetError",
    "set_message_context",
    "message_context",
    "copy_context",
    "set_fence_watermark",
    "get_fence_watermark",
    "_FENCE_NO_LIMIT",
)

_current_message_context: ContextVar[Optional[MessageContext]] = ContextVar(
    "current_message_context"
)

# Sentinel: no fence active — expiry threshold is not capped.  Using a very large
# value means min(timestamp_ms, fence_wm) is always a no-op when no fence is set.
_FENCE_NO_LIMIT: int = 1 << 62

# Fence watermark: per-TP min watermark injected by Application._process_message
# before calling the dataframe executor.  TimeWindow.process_window caps the expiry
# threshold to this value so that:
#   - a fast replica cannot advance expiry past what the slowest replica has confirmed
#     (global fence = pre-receive global_watermark); and
#   - a TP whose own watermark has never been published (-1) causes no expiry at all
#     (per-TP fence = -1 → min(ts, -1) = -1 → threshold = -1 - grace = very negative).
# _FENCE_NO_LIMIT means "no fence" (sentinel); any real fence value ≤ is a cap.
_current_fence_watermark: ContextVar[int] = ContextVar(
    "current_fence_watermark", default=_FENCE_NO_LIMIT
)


def set_fence_watermark(wm: int) -> None:
    """Set the fence watermark for the current message context."""
    _current_fence_watermark.set(wm)


def get_fence_watermark() -> int:
    """
    Get the fence watermark for the current message context.
    Returns _FENCE_NO_LIMIT if no fence is set (pass-through, no capping).
    """
    return _current_fence_watermark.get()


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
