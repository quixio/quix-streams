"""Fast-path sample iterators for :class:`MAStreamingDeserializer`.

These helpers walk an already-parsed inner-payload protobuf message
(``PeriodicDataPacket`` / ``SynchroDataPacket`` / ``RowDataPacket``) and
yield ``(parameter_identifier, timestamp, value, status)`` tuples.

Each iterator accepts an optional ``signals: frozenset[str]`` allowlist.
``signals=None`` means "no constraint" — the iterator must avoid any
per-sample branching for the no-filter case (the fast-path benchmark
must not regress). When ``signals`` is set, whole columns (Periodic /
Synchro) or the non-surviving sample indices within each row (RowData)
are skipped before the inner sample loop runs.
"""

from __future__ import annotations

from typing import Optional

from . import open_data_pb2 as pb

# Per-enum integer→name map for DataStatus. Built once at import time
# so the fast path avoids a per-sample attribute lookup on the enum
# descriptor. Matches the names ``MessageToDict`` emits when
# ``use_integers_for_enums=False``.
_DATA_STATUS_NAMES = {v.number: v.name for v in pb.DataStatus.DESCRIPTOR.values}

_DEFAULT_STATUS = "DATA_STATUS_UNSPECIFIED"


def _proto_samples_iter(column):
    """Yield (value, status) tuples for whichever sample list is populated."""
    list_field = column.WhichOneof("list")
    if list_field is None:
        return
    block = getattr(column, list_field)
    for s in block.samples:
        yield s.value, _DATA_STATUS_NAMES.get(s.status, _DEFAULT_STATUS)


def iter_periodic_proto(packet, signals: Optional[frozenset[str]] = None):
    names = list(packet.data_format.parameter_identifiers.parameter_identifiers)
    start = packet.start_time
    interval = packet.interval
    if signals is None:
        for col_idx, column in enumerate(packet.columns):
            name = names[col_idx] if col_idx < len(names) else f"col{col_idx}"
            for i, (val, status) in enumerate(_proto_samples_iter(column)):
                yield name, start + i * interval, val, status
        return
    for col_idx, column in enumerate(packet.columns):
        name = names[col_idx] if col_idx < len(names) else f"col{col_idx}"
        if name not in signals:
            continue
        for i, (val, status) in enumerate(_proto_samples_iter(column)):
            yield name, start + i * interval, val, status


def iter_synchro_proto(packet, signals: Optional[frozenset[str]] = None):
    names = list(packet.data_format.parameter_identifiers.parameter_identifiers)
    intervals = list(packet.intervals)
    cum: list[int] = [packet.start_time]
    acc = packet.start_time
    for iv in intervals:
        acc += iv
        cum.append(acc)
    if signals is None:
        for col_idx, column in enumerate(packet.columns):
            name = names[col_idx] if col_idx < len(names) else f"col{col_idx}"
            for i, (val, status) in enumerate(_proto_samples_iter(column)):
                ts = cum[i] if i < len(cum) else (cum[-1] if cum else 0)
                yield name, ts, val, status
        return
    for col_idx, column in enumerate(packet.columns):
        name = names[col_idx] if col_idx < len(names) else f"col{col_idx}"
        if name not in signals:
            continue
        for i, (val, status) in enumerate(_proto_samples_iter(column)):
            ts = cum[i] if i < len(cum) else (cum[-1] if cum else 0)
            yield name, ts, val, status


def iter_row_proto(packet, signals: Optional[frozenset[str]] = None):
    names = list(packet.data_format.parameter_identifiers.parameter_identifiers)
    timestamps = list(packet.timestamps)
    if signals is None:
        for i, row in enumerate(packet.rows):
            ts = timestamps[i] if i < len(timestamps) else 0
            list_field = row.WhichOneof("list")
            if list_field is None:
                continue
            block = getattr(row, list_field)
            for col_idx, s in enumerate(block.samples):
                name = names[col_idx] if col_idx < len(names) else f"col{col_idx}"
                yield (
                    name,
                    ts,
                    s.value,
                    _DATA_STATUS_NAMES.get(s.status, _DEFAULT_STATUS),
                )
        return
    # RowData is row-oriented (one sample per signal per row); pre-compute
    # the column-keep mask once so per-row iteration only touches the
    # surviving indices instead of every sample.
    keep_indices = [i for i, n in enumerate(names) if n in signals]
    if not keep_indices:
        return
    for i, row in enumerate(packet.rows):
        ts = timestamps[i] if i < len(timestamps) else 0
        list_field = row.WhichOneof("list")
        if list_field is None:
            continue
        block = getattr(row, list_field)
        samples = block.samples
        n_samples = len(samples)
        for col_idx in keep_indices:
            if col_idx >= n_samples:
                continue
            s = samples[col_idx]
            yield (
                names[col_idx],
                ts,
                s.value,
                _DATA_STATUS_NAMES.get(s.status, _DEFAULT_STATUS),
            )
