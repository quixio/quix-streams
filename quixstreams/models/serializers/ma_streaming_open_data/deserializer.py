"""``MAStreamingDeserializer`` — built-in MA Streaming Open Data v1 decoder.

Subclasses :class:`quixstreams.models.serializers.protobuf.ProtobufDeserializer`
to do **both stages** of decoding in one call:

1. Stage 1 — parse the outer ``ma.streaming.open_data.v1.Packet`` envelope
   (inherited from :class:`ProtobufDeserializer`).
2. Stage 2 — dispatch on ``Packet.type``, parse the matching inner-payload
   protobuf message, and replace the ``content`` field of the returned
   dict with the decoded sub-dict.

The class is additive: it does not modify the behaviour of the generic
``ProtobufDeserializer``. See ``docs/advanced/ma-streaming-open-data.md``
for the output shapes, the ``build_table`` projection and the ``filter``
parameter.
"""

from __future__ import annotations

import base64
import binascii
import logging
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Union, cast

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError, Message

from ..base import SerializationContext
from ..protobuf import ProtobufDeserializer
from ..schema_registry import (
    SchemaRegistryClientConfig,
    SchemaRegistrySerializationConfig,
)
from . import open_data_pb2 as pb
from .content_types import CONTENT_TYPES
from .filter import SAMPLE_BEARING_TYPES, ResolvedFilter, resolve_filter
from .sample_iter import (
    iter_periodic_proto,
    iter_row_proto,
    iter_synchro_proto,
)

logger = logging.getLogger(__name__)

# Module-level so multiple deserializer instances share a single
# "warn once per unknown type" set (mirrors the prior decoder behaviour).
_warned_unknown: set[str] = set()

# Sentinel for the ``build_table`` parameter so callers can distinguish
# "use the class default" from "no projection". Pass ``build_table=None``
# (or an empty mapping/sequence) to opt out and get raw packet dicts.
_USE_DEFAULT_BUILD_TABLE: Any = object()


def _to_tagged_union(packet_dict: dict) -> dict:
    """Wrap a packet dict into a single-root-key tagged-union record.

    The packet's ``type`` field becomes the sole top-level key. The
    inner value is the decoded payload only — ``packet_dict["content"]``
    after Stage 2. Envelope metadata (``session_key``, ``is_essential``,
    ``id``) is dropped because consumers can recover session_key from
    upstream Kafka keys / topic naming if needed, and the type-specific
    content is the only payload most sinks care about.

    Falls back to ``{"": <inner>}`` if ``type`` is missing or non-string;
    if ``content`` is missing or undecoded the full packet_dict is used
    as the inner value (fail-soft, matches the rest of the deserializer).
    """
    packet_type = packet_dict.get("type")
    inner = packet_dict.get("content")
    if not isinstance(inner, dict):
        # decode_content=False or missing content — return the envelope
        # unchanged (minus ``type`` so the root key isn't duplicated).
        envelope = dict(packet_dict)
        envelope.pop("type", None)
        inner = envelope
    if not isinstance(packet_type, str) or not packet_type:
        return {"": inner}
    return {packet_type: inner}


def _coerce_to_bytes(value: Any) -> Optional[bytes]:
    """Return ``value`` as ``bytes`` if possible, else ``None``.

    ``MessageToDict`` (which the parent ``ProtobufDeserializer`` uses)
    renders ``bytes`` fields as base64-encoded ``str``. We accept either
    form so the decoder is robust to both code paths.
    """
    if value is None:
        return None
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        if value == "":
            return b""
        try:
            return base64.b64decode(value, validate=True)
        except (binascii.Error, ValueError):
            return None
    return None


class MAStreamingDeserializer(ProtobufDeserializer):
    """Deserializer for **MA Streaming Open Data v1** wire packets.

    The class hard-codes ``msg_type=Packet`` and
    ``preserving_proto_field_name=True`` (the only sensible defaults for
    this protocol). It also runs the inner-payload decode, so user code
    does not need a per-app ``TYPE_MAP`` or a ``decode_content`` callback.

    Example:

    .. code-block:: python

        from quixstreams import Application
        from quixstreams.models.serializers import MAStreamingDeserializer

        app = Application(...)
        topic = app.topic(
            name="telemetry-input",
            value_deserializer=MAStreamingDeserializer(
                filter={
                    "types": ["PeriodicData", "RowData", "NewSession"],
                    "signals": ["vCar", "nEngine"],
                },
            ),
            key_deserializer="str",
        )

    With the filter above the deserializer emits a **list** per Kafka
    message. The record shape is asymmetric on purpose. Sample-bearing
    packets (``PeriodicData``, ``RowData``, ``SynchroData``) fan out into
    one **flat row dict** per sample, carrying the columns named by
    ``build_table`` (:attr:`DEFAULT_BUILD_TABLE` when not overridden) and
    keeping only rows whose ``name`` is in the signal allowlist. Every
    other surviving packet type is emitted as a single **tagged-union
    record** — one root key (the ``Packet.type`` string) whose value is
    the decoded payload. Packets outside the type allowlist are dropped
    (``[]``). Downstream code routes on the presence of the
    ``packet_type`` column:

    .. code-block:: python

        # one PeriodicData packet -> N flat rows
        [
            {"packet_type": "PeriodicData", "timestamp": 1715000000,
             "name": "vCar", "value": 312.4, "validity": "DATA_STATUS_VALID"},
            {"packet_type": "PeriodicData", "timestamp": 1715000010,
             "name": "vCar", "value": 313.1, "validity": "DATA_STATUS_VALID"},
        ]
        # one NewSession packet -> one tagged-union record
        [{"NewSession": {"session_key": "abc", "session_identifier": "..."}}]
    """

    #: Module-level protocol version constant; mirrors the
    #: ``ma.streaming.open_data.v1`` package name of the bundled schema.
    protocol_version: str = "v1"

    #: Columns supported by the ``build_table`` projection. Order does not
    #: matter — the user picks which columns to keep. Column names mirror
    #: fields in the canonical ``ma.streaming.open_data.v1`` schema:
    #: ``parameter_identifier`` comes from
    #: ``SampleDataFormat.parameter_identifiers.parameter_identifiers[i]``;
    #: ``timestamp``, ``value``, and ``status`` come from each sample inside
    #: the columns / rows.
    SUPPORTED_TABLE_COLUMNS: tuple[str, ...] = (
        "parameter_identifier",
        "timestamp",
        "value",
        "status",
        "session_key",
        "packet_type",
        "is_essential",
    )

    #: Built-in default projection used when ``build_table`` is omitted.
    #: Emits flat engineering rows with a ``packet_type`` discriminator
    #: column up front, so downstream lakehouse sinks can
    #: ``partitionBy("packet_type")`` and reason about source without
    #: branching. Non-engineering packets stay tagged-union under their
    #: type as the root key (asymmetric, intentional: engineering data
    #: is high-throughput time-series and benefits from a flat schema;
    #: non-engineering payloads vary too much to flatten uniformly).
    DEFAULT_BUILD_TABLE: Mapping[str, Sequence[str]] = {
        "packet_type": ("packet_type",),
        "timestamp": ("start_time", "timestamp"),
        "name": ("parameter_identifier",),
        "value": ("value",),
        "validity": ("status",),
    }

    def __init__(
        self,
        *,
        decode_content: bool = True,
        extra_content_types: Optional[Mapping[str, type[Message]]] = None,
        use_integers_for_enums: bool = False,
        build_table: Any = _USE_DEFAULT_BUILD_TABLE,
        filter: Optional[Mapping[str, Sequence[str]]] = None,
        schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
        schema_registry_serialization_config: Optional[
            SchemaRegistrySerializationConfig
        ] = None,
    ) -> None:
        """Construct a new MA Streaming Open Data v1 deserializer.

        :param decode_content: If True (default), perform Stage 2 — parse
            the inner payload bytes into a typed dict and replace
            ``content`` with that dict. If False, leave ``content`` as the
            Stage-1 base64 string. Useful for diagnostic tools that want
            envelope-only metadata.
        :param extra_content_types: Optional mapping from ``Packet.type``
            string to a protobuf ``Message`` class. Lets callers register
            private OEM extensions without forking the library. Built-in
            entries take precedence on key collision; subclass and rebind
            ``_content_types`` if you need the opposite.
        :param use_integers_for_enums: Forwarded to ``MessageToDict`` for
            both Stage 1 and Stage 2.
        :param build_table: Optional column projection. When set, the
            deserializer returns a **list of row dicts** (one row per
            sample) instead of the raw packet dict. For sample-bearing
            packet types (``PeriodicData``, ``RowData``, ``SynchroData``)
            each row carries the requested subset of
            :attr:`SUPPORTED_TABLE_COLUMNS`. Non-sample packets return an
            empty list unless ``filter`` is set (see below). Combine with
            ``sdf.apply(lambda x: x, expand=True)`` in your pipeline to
            fan out the rows.

            Accepts either form:

            * **list** of column names —
              ``["parameter_identifier", "timestamp", "value", "status"]``
            * **dict** mapping output column name to source-field list —
              ``{"parameter_identifier": ["parameter_identifier"],
              "timestamp": ["start_time", "timestamp"], ...}`` —
              keys are the output columns; each value lists candidate
              source fields and is resolved once at construction to the
              first entry that appears in
              :attr:`SUPPORTED_TABLE_COLUMNS` — that canonical field is
              what populates the column. A value whose entries are all
              unrecognized raises ``ValueError``. The lists themselves
              are never re-examined per message.
        :param filter: Optional mapping with two allowed keys:

            * ``"types"``: sequence of ``Packet.type`` strings to keep.
              Anything else is dropped (``[]``).
            * ``"signals"``: sequence of ``parameter_identifier`` strings
              to keep. Applies inside sample-bearing packets only; rows
              whose signal name is not in the allowlist are skipped.

            Both keys are optional; missing means "no constraint on
            this axis". ``filter={}`` is accepted as a no-op alias for
            ``filter=None``.

            **Shape contract.** When any axis of ``filter`` is set
            the deserializer **always returns a list**, and the record
            shape depends on the packet, not on the filter. With a
            ``build_table`` active and ``decode_content=True`` (both
            defaults) sample-bearing packets fan out into N **flat row
            dicts** (``[]`` when signals exclude every sample), while
            surviving non-sample-bearing packets emit one
            **tagged-union record**: a single root key — the
            ``Packet.type`` string — whose value is the
            ``MessageToDict`` packet dict minus its redundant
            top-level ``"type"`` field. With ``build_table=None`` or
            ``decode_content=False`` every surviving packet takes the
            slow path and emits one tagged-union record (signal
            filtering is rejected at construction in both cases).
            Packets outside the type allowlist are dropped (``[]``).
            Downstream code expands the list with
            ``sdf.apply(lambda x: x, expand=True)`` and routes on the
            presence of the ``packet_type`` column. When
            ``filter=None`` the fast path still emits flat rows for
            sample-bearing packets and ``[]`` for everything else, and
            the slow path returns the raw packet dict, not
            list-wrapped. See the "Output shape — asymmetric" section
            of ``docs/advanced/serialization.md`` for the full table.

            **Validation errors raised at construction:**

            * ``TypeError`` if ``filter`` is not a mapping.
            * ``ValueError`` for empty lists, unknown mapping keys
              (e.g. ``"signal"`` typo), unknown packet-type strings,
              ``signals`` set with ``build_table=None``, or ``signals``
              set with ``decode_content=False``.

            Example combining type + signal filters:

            .. code-block:: python

                MAStreamingDeserializer(
                    filter={
                        "types": ["PeriodicData", "RowData", "NewSession"],
                        "signals": ["vCar", "nEngine"],
                    },
                )
        :param schema_registry_client_config: Forwarded to the parent
            ``ProtobufDeserializer`` for the Stage-1 envelope only. Stage 2
            is raw inner bytes with no schema-registry framing.
        :param schema_registry_serialization_config: Likewise forwarded
            to the parent for Stage 1.
        """
        super().__init__(
            msg_type=pb.Packet,
            use_integers_for_enums=use_integers_for_enums,
            preserving_proto_field_name=True,
            to_dict=True,
            schema_registry_client_config=schema_registry_client_config,
            schema_registry_serialization_config=schema_registry_serialization_config,
        )
        self._decode_content = decode_content

        # Apply the built-in default projection when the caller didn't
        # specify ``build_table`` at all. Explicit ``None`` still means
        # "no projection — emit raw packet dicts".
        if build_table is _USE_DEFAULT_BUILD_TABLE:
            build_table = self.DEFAULT_BUILD_TABLE

        # Resolve build_table into an ordered mapping of
        #   output_column_name -> canonical_source_field
        # where canonical_source_field is one of SUPPORTED_TABLE_COLUMNS.
        column_mapping: dict[str, str] = {}
        if build_table is not None:
            if isinstance(build_table, Mapping):
                for output_name, sources in build_table.items():
                    if not sources:
                        raise ValueError(
                            f"build_table[{output_name!r}] must list at least one source field"
                        )
                    canonical = next(
                        (s for s in sources if s in self.SUPPORTED_TABLE_COLUMNS),
                        None,
                    )
                    if canonical is None:
                        raise ValueError(
                            f"build_table[{output_name!r}] sources {list(sources)} "
                            f"contain no recognized field. Supported: "
                            f"{list(self.SUPPORTED_TABLE_COLUMNS)}"
                        )
                    column_mapping[output_name] = canonical
            else:
                for c in build_table:
                    if c not in self.SUPPORTED_TABLE_COLUMNS:
                        raise ValueError(
                            f"build_table contains unsupported column: {c!r}. "
                            f"Supported columns: {list(self.SUPPORTED_TABLE_COLUMNS)}"
                        )
                    column_mapping[c] = c
        self._column_mapping: Optional[dict[str, str]] = (
            column_mapping if column_mapping else None
        )
        # Backwards-compat alias used by the projection code below.
        self._build_table: Optional[tuple[str, ...]] = (
            tuple(column_mapping.keys()) if column_mapping else None
        )

        # Merge built-in map with the optional extension map. Built-ins
        # win on collision (callers can subclass to invert this).
        merged: dict[str, type[Message]] = dict(CONTENT_TYPES)
        if extra_content_types:
            for key, cls in extra_content_types.items():
                merged.setdefault(key, cls)
        self._content_types: dict[str, type[Message]] = merged

        # Resolve the filter mapping into an immutable record. Done
        # after _content_types is built so we can validate type strings
        # against the merged registry. Done after _build_table is set
        # so we can cross-validate signal filtering needs row projection.
        self._filter: ResolvedFilter = resolve_filter(
            filter,
            known_packet_types=self._content_types.keys(),
            decode_content=self._decode_content,
            build_table_active=self._build_table is not None,
        )

    def __call__(
        self, value: bytes, ctx: SerializationContext
    ) -> Union[Iterable[Mapping], Mapping, Message]:
        # Fast path: when a build_table projection is configured we skip
        # both MessageToDict walks and access protobuf attributes
        # directly. This is the common case (per-sample table rows) and
        # is ~10–50× faster than the dict-based path because we avoid
        # the round-trip bytes → base64 str → bytes and the two slow
        # google.protobuf.json_format conversions.
        if self._decode_content and self._build_table is not None:
            return self._fast_path(value, ctx)

        # Slow path: parent does Stage 1 (envelope MessageToDict),
        # we do Stage 2 (inner MessageToDict). Preserves the raw-packet
        # dict shape that any non-projection consumer expects when
        # ``filter=None``; otherwise emits the
        # tagged-union shape (``[{<type>: inner_dict}]``).
        out = super().__call__(value, ctx)

        filter_active = self._filter.is_active
        filter_types = self._filter.types

        if not self._decode_content:
            if not isinstance(out, dict):
                return out
            if filter_types is not None and out.get("type") not in filter_types:
                return []
            if filter_active:
                return [_to_tagged_union(out)]
            return out

        if not isinstance(out, dict):
            return out

        if filter_types is not None and out.get("type") not in filter_types:
            return []

        decoded = self._decode_inner(out)
        if filter_active:
            return [_to_tagged_union(decoded)]
        return decoded

    def _fast_path(self, value: bytes, ctx: SerializationContext) -> List[dict]:
        """Parse Packet + inner payload + project to table rows in one walk.

        Avoids both stage-1 and stage-2 ``MessageToDict`` calls and the
        base64 round-trip for the ``content`` field. Filter-aware:
        drops type-filtered packets before any inner parse, and falls
        through to the slow path for non-sample-bearing types that
        survived the type filter (so we don't duplicate the
        dict-builder for the rare non-sample case).
        """
        packet = pb.Packet()
        try:
            packet.ParseFromString(value)
        except DecodeError:
            # Same fail-soft policy as the slow path: bad bytes don't
            # poison the topic.
            logger.warning("Failed to parse outer Packet (%d bytes)", len(value))
            return []

        filter_types = self._filter.types
        if filter_types is not None and packet.type not in filter_types:
            return []

        packet_type = packet.type
        filter_active = self._filter.is_active

        if packet_type not in SAMPLE_BEARING_TYPES:
            # Non-sample-bearing types only emit anything when the user
            # opted into filtering — without a filter the fast path
            # contract is "rows or nothing". Build the tagged-union
            # record from the canonical packet dict (parent's Stage 1 +
            # ``_decode_inner``) rather than duplicating the dict-builder
            # logic here.
            if not filter_active:
                return []
            return self._slow_path_single_packet(value, ctx)

        payload_cls = self._content_types.get(packet_type)
        if payload_cls is None:
            if packet_type not in _warned_unknown:
                _warned_unknown.add(packet_type)
                logger.warning(
                    "Unknown Packet.type=%r; cannot project to table "
                    "(this warning is logged once per type)",
                    packet_type,
                )
            return []

        # Annotated ``Any`` rather than ``Message``: the SAMPLE_BEARING_TYPES
        # gate above guarantees this is a PeriodicData / RowData / SynchroData
        # packet, all of which carry the ``data_format`` field read below, but
        # the registry's ``type[Message]`` annotation cannot express that.
        inner: Any = payload_cls()
        try:
            inner.ParseFromString(packet.content)
        except DecodeError as exc:
            logger.warning(
                "Failed to parse inner payload for type=%s (%d bytes): %s",
                packet_type,
                len(packet.content),
                exc,
            )
            return []

        signals = self._filter.signals
        if signals is not None:
            # Cheap whole-packet skip: if no column's parameter_identifier
            # is in the allowlist, return [] without iterating samples.
            pkt_signals = inner.data_format.parameter_identifiers.parameter_identifiers
            if not any(name in signals for name in pkt_signals):
                return []

        if packet_type == "PeriodicData":
            sample_iter = iter_periodic_proto(inner, signals=signals)
        elif packet_type == "SynchroData":
            sample_iter = iter_synchro_proto(inner, signals=signals)
        else:  # RowData
            sample_iter = iter_row_proto(inner, signals=signals)

        # Never None here: __call__ only routes to _fast_path when
        # _build_table is set, and __init__ populates _column_mapping and
        # _build_table from the same resolved projection.
        mapping = cast(dict[str, str], self._column_mapping)
        session_key = packet.session_key
        is_essential = packet.is_essential

        # Engineering rows are emitted flat (with ``packet_type`` as
        # one of the columns if the build_table maps it — the default
        # does). Non-engineering packets stay tagged-union (see
        # ``_slow_path_single_packet``). Asymmetric shape on purpose:
        # engineering = time-series, benefits from flat schema;
        # non-engineering = sparse metadata, kept under the type key.
        rows: List[dict] = []
        for parameter_identifier, timestamp, value_, status in sample_iter:
            row: dict[str, Any] = {}
            for output_name, source in mapping.items():
                if source == "parameter_identifier":
                    row[output_name] = parameter_identifier
                elif source == "timestamp":
                    row[output_name] = timestamp
                elif source == "value":
                    row[output_name] = value_
                elif source == "status":
                    row[output_name] = status
                elif source == "session_key":
                    row[output_name] = session_key
                elif source == "packet_type":
                    row[output_name] = packet_type
                elif source == "is_essential":
                    row[output_name] = is_essential
            rows.append(row)
        return rows

    def _slow_path_single_packet(
        self, value: bytes, ctx: SerializationContext
    ) -> List[dict]:
        """Build a single-element list with the tagged-union record.

        Reused by the fast path for non-sample-bearing types that
        survived the type filter — building the canonical packet dict
        is exactly what the parent + ``_decode_inner`` already do. The
        top-level ``"type"`` key is then popped (it is already the
        tagged-union root key) and the remaining dict becomes the
        inner value.
        """
        out = super().__call__(value, ctx)
        if not isinstance(out, dict):
            return []
        if self._decode_content:
            out = self._decode_inner(out)
        return [_to_tagged_union(out)]

    def _decode_inner(self, out: dict) -> dict:
        """Replace ``out['content']`` with the decoded inner-payload dict.

        Returns ``out`` unchanged when the inner payload can't be decoded
        (unknown type, missing content, or inner ``DecodeError``). Never
        raises — bad inner payloads must not poison the topic.
        """
        packet_type = out.get("type")
        if not isinstance(packet_type, str) or not packet_type:
            logger.warning(
                "Packet has missing or non-string 'type'; leaving content untouched",
            )
            return out

        payload_cls = self._content_types.get(packet_type)
        if payload_cls is None:
            if packet_type not in _warned_unknown:
                _warned_unknown.add(packet_type)
                logger.warning(
                    "Unknown Packet.type=%r; leaving content as base64 "
                    "(this warning is logged once per type)",
                    packet_type,
                )
            return out

        raw = _coerce_to_bytes(out.get("content"))
        if raw is None:
            # Empty/absent content: keep what we have, nothing to decode.
            return out

        msg = payload_cls()
        try:
            msg.ParseFromString(raw)
        except DecodeError as exc:
            logger.warning(
                "Failed to parse inner payload for type=%s (%d bytes): %s",
                packet_type,
                len(raw),
                exc,
            )
            return out

        out["content"] = MessageToDict(
            msg,
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
            use_integers_for_enums=self._use_integers_for_enums,
        )
        return out
