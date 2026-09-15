"""
The public configuration surface of the non-blocking lookup buffer.

`LookupBuffer` holds the knobs and the build-time checks; the record path lives
in `buffer_operator.py`, the stored form of a withheld record in
`buffer_envelope.py` and the index of which keys are holding one in
`buffer_state.py`.
"""

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

# Sentinel for "this field type has no `default` attribute at all", which is how
# a non-quix-configuration field is told apart from one whose default is unset.
_NO_DEFAULT = object()


class LookupBuffer:
    """
    Hold records whose lookup key cannot be resolved yet, instead of enriching
    them with defaults or dropping them immediately.

    A held record is released - enriched, in arrival order, with its own
    timestamp, key and headers - by the next record for the same key, provided
    its configuration arrived within `grace_ms`. A record that reaches
    `grace_ms` with no configuration meets the fate chosen by `on_timeout`, and
    is never enriched afterwards.

    Nothing ever sleeps: an unresolvable record is written to a durable,
    changelog-backed state store and the processing thread returns immediately,
    so other keys and other partitions are unaffected.

    Pass an instance to `StreamingDataFrame.join_lookup(..., buffer=...)`.
    Leaving `buffer` unset keeps today's behaviour exactly, with no store and no
    cost.

    Example:

    ```python
    from quixstreams import Application
    from quixstreams.dataframe.joins.lookups import (
        LookupBuffer,
        QuixConfigurationService,
    )

    app = Application()
    sdf = app.dataframe(app.topic("sensor-data"))

    lookup = QuixConfigurationService(
        topic=app.topic("device-configurations"),
        app_config=app.config,
        unresolved_types_field="__unresolved__",
    )
    fields = {
        "threshold": lookup.json_field("$.threshold", type="device", default=None),
        "region": lookup.json_field("$.region", type="device", default="unknown"),
    }

    sdf = sdf.join_lookup(
        lookup,
        fields,
        buffer=LookupBuffer(
            grace_ms=30_000,
            is_resolved=lambda value: not value["__unresolved__"],
        ),
    )
    ```

    Enabling a buffer turns a stateless service into a stateful one: it needs a
    state directory and, on Quix Cloud, `state: {enabled: true}`.
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
            measured in **wall-clock real time** from the moment the record
            enters the operator. An `int` is milliseconds; a `timedelta` is
            converted. This is real time and not event time on purpose: what the
            record is waiting for is a configuration message landing on another
            topic, which has no event-time analogue, and an event-time deadline
            would expire a backlog replay's records instantly.

            It is passive retention, exactly like the `grace_ms` of `join_asof`
            and `join_interval`: it never blocks a thread. Larger values cost
            disk, changelog volume and emission latency.

        :param is_resolved: Predicate deciding whether a record's lookup
            succeeded, called with the record value after `lookup.join()`. With
            `QuixConfigurationService`, set its `unresolved_types_field` and pass
            `lambda value: not value["__unresolved__"]`.

        :param on_timeout: What happens to a record that reaches `grace_ms` with
            no configuration.

            - `"emit"` (default): it goes downstream carrying each field's
              declared `default=`, which is exactly what an unbuffered
              `join_lookup` would have produced for it immediately. Spell nulls
              as `default=None` on the field.
            - `"drop"`: it is discarded permanently, and a rate-limited warning
              names the key.

            Neither mode re-runs the lookup, so a configuration arriving after a
            record's deadline never enriches it.

        :param max_buffered_per_key: Safety valve against a pathological rate of
            unresolvable records for one key. The primary bound is `grace_ms`:
            steady-state size is roughly `rate x grace_ms` records per key. This
            is also a latency knob, because a release deserializes a key's whole
            surviving buffer inside one callback.

        :param on_overflow: What happens to a record that arrives when a key is
            already holding `max_buffered_per_key` records. `"drop-newest"`
            (default) discards it and logs; `"raise"` fails the application. An
            overflowed record never entered the buffer, so it has no deadline and
            `on_timeout` does not apply to it.

        :param store_name: Name of the state store holding the buffer. Change it
            if two `join_lookup` buffers share one stream.
        """
        self._grace_ms = ensure_milliseconds(grace_ms)
        if self._grace_ms <= 0:
            raise ValueError(
                "`grace_ms` must be > 0: a buffer with no window can never "
                "release anything. Use `join_lookup(..., buffer=None)` for the "
                "unbuffered behaviour."
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
        """The grace window in milliseconds."""
        return self._grace_ms

    @property
    def store_name(self) -> str:
        """The name of the state store holding the buffer."""
        return self._store_name

    def validate_fields(self, fields: Mapping[str, BaseField]) -> None:
        """
        Reject fields that cannot survive buffering, before the application runs.

        A field with no `default=` raises `KeyError` from `missing()`, and
        `missing()` runs on **every** unresolvable record, before anything is
        stored - so such a field does not merely break `on_timeout="emit"`, it
        breaks buffering entirely. Turning that into a `ValueError` at build time
        replaces a crash on the first unconfigured record in production with a
        failure on the developer's machine.

        Fields that have no `default` attribute belong to some other lookup
        implementation and are skipped: only the Quix Configuration Service
        fields carry `default` / `missing()` semantics.

        :param fields: The field mapping passed to `join_lookup`.
        :raises ValueError: If any field has no `default=`.
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

    def register_store(self, dataframe: "StreamingDataFrame") -> None:
        """
        Register the timestamped store that holds the withheld records.

        The buffer's durability rests on that store being changelog-backed: a
        record's offset is committed because it was *consumed*, not because it
        was emitted, and there is no API to withhold an offset for a record still
        in flight. A held record that is not in a changelog is therefore gone for
        good the moment its partition moves to another consumer.

        Whether a changelog exists is not this buffer's decision to make -
        `StateStoreManager` produces one only when the application was built with
        a recovery manager, which is what `Application(use_changelog_topics=True)`
        (the default) does, and that setting governs every store in the
        application. So this is a warning and not an error: turning it into one
        would fail an application whose other stores are perfectly happy without
        changelogs. It is logged once per registration, at `WARNING`, naming the
        exact consequence.

        :param dataframe: The dataframe the operator is being added to.
        """
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
            # Withheld records that arrive in the same millisecond must all
            # survive, and the duplicate counter is what makes their order
            # within a key the arrival order.
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
        Build the expanded-transform callback for one `join_lookup` call.

        :param dataframe: The dataframe the operator is being added to.
        :param lookup: The lookup strategy.
        :param fields: The field mapping passed to `join_lookup`.
        :param on: The resolved lookup-key accessor.
        :return: A callable taking `(value, key, timestamp, headers)` and
            returning the records to emit.
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
