"""``FIAOpenDataDeserializer``, the built-in FIA Open Data decoder.

Parses the outer ``ma.streaming.open_data.v1.Packet`` envelope, decodes the
inner payload named by ``Packet.type``, and returns either flat sample rows or
one tagged-union record per packet. See
``docs/advanced/fia-open-data.md``.
"""

from __future__ import annotations

import base64
from typing import Any, Mapping, Optional, Sequence, Union, cast

from confluent_kafka import OFFSET_BEGINNING, TopicPartition
from google.protobuf.json_format import MessageToDict
from google.protobuf.message import DecodeError, Message

from ....kafka import Consumer, raise_for_msg_error
from ..base import SerializationContext
from ..exceptions import SerializationError
from ..protobuf import ProtobufDeserializer
from . import open_data_pb2 as pb
from .content_types import CONTENT_TYPES
from .filter import SAMPLE_BEARING_TYPES, ResolvedFilter, resolve_filter
from .sample_iter import (
    iter_periodic_proto,
    iter_row_proto,
    iter_synchro_proto,
)

# Distinguishes an omitted ``build_table`` from an explicit ``None``.
_USE_DEFAULT_BUILD_TABLE: Any = object()

_SEED_POLL_TIMEOUT = 5.0

_DATA_STATUS_NAMES: dict[int, str] = {
    v.number: v.name for v in pb.DataStatus.DESCRIPTOR.values
}


def _to_tagged_union(packet: dict) -> dict:
    """Return ``{packet["type"]: <packet without "type">}``."""
    return {packet.pop("type"): packet}


class FIAOpenDataDeserializer(ProtobufDeserializer):
    """Deserializer for **FIA Open Data** wire packets.

    The wire protocol is Motion Applied's MA Streaming Open Data v1.

    Parses the outer ``Packet`` envelope and the inner payload named by
    ``Packet.type`` in one call. Every call returns a list, and Quix Streams
    processes each item as its own row: one flat row per sample for
    ``PeriodicData``, ``SynchroData`` and ``RowData``, or one tagged-union
    record per packet (see ``filter`` and ``build_table``).

    Example:

    .. code-block:: python

        from quixstreams import Application
        from quixstreams.models.serializers.fia_open_data import (
            FIAOpenDataDeserializer,
        )

        app = Application(...)
        topic = app.topic(
            name="telemetry-input",
            value_deserializer=FIAOpenDataDeserializer(
                filter={"types": ["PeriodicData", "NewSession"]},
            ),
            key_deserializer="str",
        )

    .. code-block:: python

        # one PeriodicData packet -> one flat row per sample
        [{"packet_type": "PeriodicData", "timestamp": 1715000000,
          "name": "vCar", "value": 312.4, "validity": "DATA_STATUS_VALID"},
         {"packet_type": "PeriodicData", "timestamp": 1715000010,
          "name": "vCar", "value": 313.1, "validity": "DATA_STATUS_VALID"}]
        # one NewSession packet -> one tagged-union record
        [{"NewSession": {"session_key": "s1", "is_essential": True,
                         "content": {"data_source": "ecu1",
                                     "topic_partition_offsets": {}},
                         "id": "0"}}]

    Parameter names come from the packet's inline
    ``data_format.parameter_identifiers``, or, for a packet that carries only
    a ``data_format_identifier``, from ``DataFormatDefinition`` packets.
    Definitions are written once, to the essentials topic and to partition 0
    of the data topic, so identifier-form feeds need ``definitions_topic`` and
    ``consumer``: an unknown identifier then reads that topic from where the
    previous read stopped until the identifier appears or a poll times out.
    An identifier that is still unknown raises ``KeyError``.
    """

    #: Mirrors the ``v1`` of the bundled schema's ``ma.streaming.open_data.v1``.
    protocol_version: str = "v1"

    #: Columns supported by the ``build_table`` projection.
    #: ``parameter_identifier`` is the resolved parameter name; ``timestamp``,
    #: ``value`` and ``status`` come from each sample; ``session_key``,
    #: ``packet_type`` and ``is_essential`` come from the envelope.
    SUPPORTED_TABLE_COLUMNS: tuple[str, ...] = (
        "parameter_identifier",
        "timestamp",
        "value",
        "status",
        "session_key",
        "packet_type",
        "is_essential",
    )

    #: Projection used when ``build_table`` is omitted.
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
        definitions_topic: Optional[str] = None,
        consumer: Optional[Consumer] = None,
    ) -> None:
        """Construct a new FIA Open Data deserializer.

        :param decode_content: If True (default), parse the inner payload. If
            False, ``content`` stays the Stage-1 base64 string and every
            packet becomes a tagged-union record.
        :param extra_content_types: Optional mapping from ``Packet.type`` to
            a protobuf ``Message`` class, for private OEM extensions.
            Built-in entries take precedence on key collision; subclass and
            rebind ``_content_types`` for the opposite.
        :param use_integers_for_enums: Emit enums as integers instead of
            names, in tagged-union records and in the projected ``status``
            column.
        :param build_table: Column projection for sample packets; each sample
            becomes one row. Either a **list** of
            :attr:`SUPPORTED_TABLE_COLUMNS` names, or a **dict** mapping each
            output column to a list of candidate source fields, resolved at
            construction to the first entry found in
            :attr:`SUPPORTED_TABLE_COLUMNS`. ``build_table=None`` gives one
            tagged-union record per packet, sample packets included, with
            parameter names unresolved: ``content["data_format"]`` stays as
            on the wire.
        :param filter: Optional mapping with two optional keys: ``"types"``,
            the ``Packet.type`` strings to keep, and ``"signals"``, the
            parameter names to keep inside sample packets. ``filter={}`` is
            a no-op alias for ``filter=None``.

            **Output.** Every call returns a list:

            * With ``build_table`` set and ``decode_content=True`` (the
              defaults), a sample packet gives one flat row per sample,
              limited by ``signals``; any other packet gives ``[]`` when
              ``filter`` is unset, else one tagged-union record.
            * With ``build_table=None`` or ``decode_content=False``, every
              packet gives one tagged-union record.
            * A packet whose type is not in ``filter["types"]`` gives ``[]``.

            A tagged-union record is ``{<Packet.type>: <envelope>}``; the
            envelope holds ``session_key``, ``is_essential``, ``content``
            and ``id``.

            **Validation errors raised at construction:**

            * ``TypeError`` if ``filter`` is not a mapping.
            * ``ValueError`` for empty lists, unknown mapping keys
              (e.g. ``"signal"`` typo), unknown packet-type strings,
              ``signals`` set with ``build_table=None``, or ``signals``
              set with ``decode_content=False``, and for a ``build_table``
              with unsupported columns.
        :param definitions_topic: Full broker-side name of the data source's
            essentials topic, as named by the MA Stream API server
            (``<prefix>Essentials.<datasource>``). It is not passed through
            ``app.topic()``. Used together with ``consumer``.
        :param consumer: A ``quixstreams.kafka.Consumer`` created with
            ``app.get_consumer(auto_commit_enable=False)``. The deserializer
            only ``assign()``s and ``poll()``s it, and never commits or
            closes it.
        """
        super().__init__(
            msg_type=pb.Packet,
            use_integers_for_enums=use_integers_for_enums,
            preserving_proto_field_name=True,
            to_dict=True,
        )
        self._decode_content = decode_content
        self._definitions_topic = definitions_topic
        self._consumer = consumer
        self._formats: dict[int, list[str]] = {}
        self._seed_offsets: dict[int, int] = {}
        self._status_labels: Mapping[int, Union[str, int]] = _DATA_STATUS_NAMES
        if use_integers_for_enums:
            self._status_labels = {n: n for n in _DATA_STATUS_NAMES}

        if build_table is _USE_DEFAULT_BUILD_TABLE:
            build_table = self.DEFAULT_BUILD_TABLE

        # output column name -> the SUPPORTED_TABLE_COLUMNS entry that fills it
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

        merged: dict[str, type[Message]] = dict(CONTENT_TYPES)
        if extra_content_types:
            for key, cls in extra_content_types.items():
                merged.setdefault(key, cls)
        self._content_types: dict[str, type[Message]] = merged

        # Resolved after ``_content_types`` so ``types`` is checked against
        # the merged registry.
        self._filter: ResolvedFilter = resolve_filter(
            filter,
            known_packet_types=self._content_types.keys(),
            decode_content=self._decode_content,
            build_table_active=self._column_mapping is not None,
        )

    @property
    def split_values(self) -> bool:
        """Each item of the returned list is processed as its own row."""
        return True

    def __call__(self, value: bytes, ctx: SerializationContext) -> list[dict]:
        """Decode a serialized ``Packet`` into flat rows or tagged-union records.

        :raises SerializationError: if any protobuf parse fails.
        :raises KeyError: if a sample packet references a data format
            identifier that is not known, after seeding when configured.
        :raises KafkaConsumerException: if a seed poll returns an error event.
        """
        mapping = self._column_mapping
        if self._decode_content and mapping is not None:
            return self._fast_path(value, ctx, mapping)

        out = cast(dict, super().__call__(value, ctx))
        filter_types = self._filter.types
        if filter_types is not None and out["type"] not in filter_types:
            return []
        if self._decode_content:
            out = self._decode_inner(out)
        return [_to_tagged_union(out)]

    def _fast_path(
        self, value: bytes, ctx: SerializationContext, mapping: dict[str, str]
    ) -> list[dict]:
        """Project sample packets to rows by reading protobuf attributes."""
        packet = pb.Packet()
        try:
            packet.ParseFromString(value)
        except DecodeError as exc:
            raise SerializationError(str(exc)) from exc

        packet_type = packet.type
        if packet_type == "DataFormatDefinition":
            self._remember_format(packet.content)

        filter_types = self._filter.types
        if filter_types is not None and packet_type not in filter_types:
            return []

        if packet_type not in SAMPLE_BEARING_TYPES:
            if not self._filter.is_active:
                return []
            return self._slow_path_single_packet(value, ctx)

        inner = self._parse_inner(self._content_types[packet_type], packet.content)
        names = self._parameter_names(inner.data_format)
        signals = self._filter.signals
        if packet_type == "PeriodicData":
            sample_iter = iter_periodic_proto(inner, names, signals=signals)
        elif packet_type == "SynchroData":
            sample_iter = iter_synchro_proto(inner, names, signals=signals)
        else:  # RowData
            sample_iter = iter_row_proto(inner, names, signals=signals)

        session_key = packet.session_key
        is_essential = packet.is_essential
        status_labels = self._status_labels
        rows: list[dict] = []
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
                    row[output_name] = status_labels[status]
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
    ) -> list[dict]:
        """Return the packet as a one-item list holding its tagged-union record."""
        out = cast(dict, super().__call__(value, ctx))
        if self._decode_content:
            out = self._decode_inner(out)
        return [_to_tagged_union(out)]

    def _decode_inner(self, out: dict) -> dict:
        """Replace ``out['content']`` with the decoded payload dict; unknown
        types are returned unchanged. Raises ``SerializationError`` on
        undecodable content."""
        payload_cls = self._content_types.get(out["type"])
        if payload_cls is None:
            return out
        out["content"] = MessageToDict(
            self._parse_inner(payload_cls, base64.b64decode(out["content"])),
            preserving_proto_field_name=True,
            always_print_fields_with_no_presence=True,
            use_integers_for_enums=self._use_integers_for_enums,
        )
        return out

    def _parse_inner(self, message_cls: Any, raw: bytes) -> Any:
        """Parse ``raw`` as ``message_cls``; raises ``SerializationError``."""
        msg = message_cls()
        try:
            msg.ParseFromString(raw)
        except DecodeError as exc:
            raise SerializationError(str(exc)) from exc
        return msg

    def _remember_format(self, content: bytes) -> None:
        """Cache the parameter names of a ``DataFormatDefinitionPacket``."""
        definition = self._parse_inner(pb.DataFormatDefinitionPacket, content)
        if definition.type == pb.DataFormatType.DATA_FORMAT_TYPE_PARAMETER:
            self._formats[definition.identifier] = list(
                definition.parameter_identifiers.parameter_identifiers
            )

    def _parameter_names(self, data_format: Any) -> Sequence[str]:
        """Return the parameter names of a sample packet's ``data_format``."""
        if data_format.WhichOneof("format") == "parameter_identifiers":
            return data_format.parameter_identifiers.parameter_identifiers
        identifier = data_format.data_format_identifier
        consumer, topic = self._consumer, self._definitions_topic
        if (
            identifier not in self._formats
            and consumer is not None
            and topic is not None
        ):
            self._seed(consumer, topic, identifier)
        return self._formats[identifier]

    def _seed(self, consumer: Consumer, topic: str, identifier: int) -> None:
        """Read ``topic`` from where the previous seed stopped, caching every
        parameter definition, until ``identifier`` is cached or a poll times out.
        """
        partitions = consumer.list_topics(topic).topics[topic].partitions
        consumer.assign(
            [
                TopicPartition(topic, p, self._seed_offsets.get(p, OFFSET_BEGINNING))
                for p in partitions
            ]
        )
        while identifier not in self._formats:
            raw = consumer.poll(_SEED_POLL_TIMEOUT)
            if raw is None:
                return
            msg = raise_for_msg_error(raw)
            self._seed_offsets[msg.partition()] = msg.offset() + 1
            packet = self._parse_inner(pb.Packet, cast(bytes, msg.value()))
            if packet.type == "DataFormatDefinition":
                self._remember_format(packet.content)
