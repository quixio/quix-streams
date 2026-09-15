import logging
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Literal,
    Mapping,
    Union,
    get_args,
)

from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.models.topics.manager import TopicManager

from .base import BaseField, BaseLookup
from .buffer_operator import BufferOperator, LookupBufferOverflowError
from .quix_configuration_service.models import RAISE_ON_MISSING

if TYPE_CHECKING:
    from quixstreams.dataframe.dataframe import StreamingDataFrame

__all__ = ("LookupBuffer", "LookupBufferOverflowError", "OnOverflow", "OnTimeout")

logger = logging.getLogger(__name__)

OnTimeout = Literal["emit", "drop"]
OnOverflow = Literal["drop-newest", "raise"]

SUB_SECOND_GRACE_WARN_MS = 1000

_NO_DEFAULT = object()


class LookupBuffer:
    def __init__(
        self,
        grace_ms: Union[int, timedelta],
        is_resolved: Callable[[dict[str, Any]], bool],
        on_timeout: OnTimeout = "emit",
        max_buffered_per_key: int = 10_000,
        on_overflow: OnOverflow = "drop-newest",
        store_name: str = "lookup-buffer",
    ) -> None:
        self._grace_ms = ensure_milliseconds(grace_ms)
        if self._grace_ms <= 0:
            raise ValueError(
                "`grace_ms` must be > 0: a buffer with no window can never "
                "release anything. Use `join_lookup(..., buffer=None)` for the "
                "unbuffered behaviour."
            )

        if self._grace_ms < SUB_SECOND_GRACE_WARN_MS:
            logger.warning(
                "LookupBuffer grace_ms=%sms is shorter than the default "
                "Application(consumer_poll_timeout=1.0). With no traffic on a "
                "partition, a record's deadline is observed at most once per "
                "poll timeout, so its actual resolution latency can be up to "
                "grace_ms + consumer_poll_timeout. Lower consumer_poll_timeout "
                "if sub-second accuracy matters.",
                self._grace_ms,
            )

        if not callable(is_resolved):
            raise ValueError("`is_resolved` must be a callable")

        if on_timeout not in get_args(OnTimeout):
            raise ValueError(
                f'Invalid "on_timeout" value: {on_timeout}. '
                f"Provide one of {', '.join(get_args(OnTimeout))}."
            )

        if on_overflow not in get_args(OnOverflow):
            raise ValueError(
                f'Invalid "on_overflow" value: {on_overflow}. '
                f"Provide one of {', '.join(get_args(OnOverflow))}."
            )

        if max_buffered_per_key < 1:
            raise ValueError("`max_buffered_per_key` must be >= 1")

        self._is_resolved = is_resolved
        self._on_timeout: OnTimeout = on_timeout
        self._max_buffered_per_key = max_buffered_per_key
        self._on_overflow: OnOverflow = on_overflow
        self._store_name = store_name

    @property
    def grace_ms(self) -> int:
        return self._grace_ms

    @property
    def store_name(self) -> str:
        return self._store_name

    def validate_fields(self, fields: Mapping[str, BaseField]) -> None:
        for name, field in fields.items():
            if getattr(field, "default", _NO_DEFAULT) is RAISE_ON_MISSING:
                field_type = getattr(field, "type", None)
                raise ValueError(
                    f"Field {name!r} (type={field_type!r}) has no `default=`. "
                    f"With `join_lookup(..., buffer=...)`, a record whose "
                    f"configuration never arrives raises KeyError instead of "
                    f"being buffered, and an emitted timeout has no value to "
                    f"emit. Set `default=None` for nulls, or a concrete value, "
                    f"on every field used with a LookupBuffer."
                )

    def register_store(self, dataframe: "StreamingDataFrame") -> None:
        state_manager = dataframe.processing_context.state_manager
        if not state_manager.using_changelogs:
            logger.warning(
                "The lookup buffer store %r is being registered without changelog "
                "topics (`Application(use_changelog_topics=False)`). Records held "
                "by `join_lookup(..., buffer=...)` will be lost, with their "
                "offsets already committed, if this partition is reassigned or "
                "the state directory is lost. Enable changelog topics to make the "
                "buffer durable.",
                self._store_name,
            )
        state_manager.register_timestamped_store(
            stream_id=dataframe.stream_id,
            store_name=self._store_name,
            grace_ms=self._grace_ms,
            keep_duplicates=True,
            changelog_config=TopicManager.derive_topic_config(dataframe.topics),
        )

    def callback(
        self,
        dataframe: "StreamingDataFrame",
        lookup: BaseLookup,
        fields: Mapping[str, BaseField],
        on: Callable[[dict[str, Any], Any], str],
    ) -> BufferOperator:
        return BufferOperator(
            dataframe=dataframe,
            lookup=lookup,
            fields=fields,
            on=on,
            store_name=self._store_name,
            grace_ms=self._grace_ms,
            is_resolved=self._is_resolved,
            on_timeout=self._on_timeout,
            max_buffered_per_key=self._max_buffered_per_key,
            on_overflow=self._on_overflow,
        )
