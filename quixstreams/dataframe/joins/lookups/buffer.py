"""
The public configuration surface of the lookup buffer. The record path lives in
`buffer_operator.py`, the clock-driven deadline pass in `buffer_tick.py`, the
stored form of a withheld record in `buffer_envelope.py` and the index of which
keys hold one in `buffer_state.py`.
"""

import logging
from datetime import timedelta
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Literal,
    Mapping,
    Union,
    get_args,
)

from quixstreams.dataframe.utils import ensure_milliseconds
from quixstreams.models.serializers import (
    DoubleDeserializer,
    IntegerDeserializer,
    JSONDeserializer,
)
from quixstreams.models.topics import Topic
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

# Stands in for a field type carrying no `default` attribute at all, which is
# not the same as one whose default was left unset.
_NO_DEFAULT = object()

# Key deserializers producing something `prefix_for_key()` cannot store.
_UNBUFFERABLE_KEY_DESERIALIZERS = (
    DoubleDeserializer,
    IntegerDeserializer,
    JSONDeserializer,
)


class LookupBuffer:
    """
    Hold records whose lookup does not resolve yet, instead of enriching them
    with field defaults or dropping them. Pass an instance to
    `StreamingDataFrame.join_lookup(..., buffer=...)`.

    A held record is released - enriched, in arrival order, with its own
    timestamp, key and headers - by the next record under the same **message
    key** whose lookup resolves, or by the Application's periodic tick once it
    reaches `grace_ms`, whichever comes first. The tick means a partition with no
    traffic at all still drains. A record whose configuration is already there
    can overtake an older one under the same message key that is still waiting
    for a different one.

    Held records live in a changelog-backed state store, so a buffered
    `join_lookup` makes the service stateful: it needs a state directory and, on
    Quix Cloud, `state: {enabled: true}`. A value the store's serializer refuses
    (orjson by default) is settled by `on_timeout` at once rather than held.
    """

    def __init__(
        self,
        grace_ms: Union[int, timedelta],
        is_resolved: Callable[[dict[str, Any]], bool],
        on_timeout: OnTimeout = "emit",
        max_buffered_per_key: int = 10_000,
        on_overflow: OnOverflow = "drop-newest",
        store_name: str = "lookup-buffer",
    ) -> None:
        """
        :param grace_ms: How long a record may wait for its configuration,
            measured in wall-clock real time from the moment it enters the
            operator, not in event time. An `int` is milliseconds.
        :param is_resolved: Predicate called with the record value after
            `lookup.join()`, deciding whether the lookup succeeded. With
            `QuixConfigurationService`, set its `unresolved_types_field` and pass
            `lambda value: not value["__unresolved__"]`.
        :param on_timeout: What a record that reaches `grace_ms` unresolved gets:
            `"emit"` sends it downstream carrying each field's declared
            `default=`, `"drop"` discards it. Neither re-runs the lookup.
        :param max_buffered_per_key: Cap on records held per **message** key,
            including the null key an unkeyed topic shares per partition. Also a
            latency knob: one release re-joins a key's whole surviving buffer.
        :param on_overflow: What a record arriving at a full key gets:
            `"drop-newest"` discards it and logs, `"raise"` raises
            `LookupBufferOverflowError`. Such a record never entered the buffer,
            so `on_timeout` does not apply to it.
        :param store_name: Name of the state store holding the buffer. Change it
            if two buffered `join_lookup` calls share one stream.
        :raises ValueError: on a non-positive `grace_ms`, a non-callable
            `is_resolved`, an unknown `on_timeout` or `on_overflow`, or a
            `max_buffered_per_key` below 1.
        """
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
        """
        Reject at build time any field that would raise instead of buffering.

        :raises ValueError: if a field has no `default=`.
        """
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

    def validate_key_deserializers(self, topics: Iterable[Topic]) -> None:
        """
        Reject at build time a topic whose message keys cannot be stored.

        `prefix_for_key()` raises on any key that is not `bytes`, `str` or
        `None`, before the offset is stored: an uncommitted crash loop.

        :raises ValueError: if a topic's key deserializer is unbufferable.
        """
        for topic in topics:
            deserializer = topic._key_deserializer  # noqa: SLF001
            if isinstance(deserializer, _UNBUFFERABLE_KEY_DESERIALIZERS):
                raise ValueError(
                    f"Topic {topic.name!r} deserializes message keys with "
                    f"{type(deserializer).__name__}. `join_lookup(..., buffer=...)` "
                    f"stores withheld records under the message key, which must be "
                    f"`bytes`, `str` or `None`. Declare the topic with "
                    f'`key_deserializer="bytes"` or `key_deserializer="str"`.'
                )

    def register_store(self, dataframe: "StreamingDataFrame") -> None:
        """Register the timestamped store the buffer holds its records in."""
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
        # `keep_duplicates` keeps records that arrive in the same millisecond
        # from overwriting each other. The store's `grace_ms` is the buffer's, so
        # `TimestampedPartitionTransaction._expire()` deletes at the same cutoff
        # the operator settles at.
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
        """
        :return: The operator `join_lookup()` installs on the record path. Its
            `tick` is registered as a periodic task by the caller.
        """
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
