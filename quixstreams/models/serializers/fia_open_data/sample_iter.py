"""Sample iterators for :class:`FIAOpenDataDeserializer` row projection.

Each iterator walks a parsed ``PeriodicDataPacket``, ``SynchroDataPacket`` or
``RowDataPacket`` and yields ``(parameter_name, timestamp, value, status)``
tuples, where ``status`` is the raw ``DataStatus`` integer.

``names`` holds the parameter name of each column (Periodic / Synchro) or of
each sample position within a row (RowData), in wire order. ``signals`` is an
optional allowlist of names; columns or sample positions whose name is not in
it are skipped.
"""

from __future__ import annotations

from typing import Any, Iterator, Optional, Sequence


def _proto_samples_iter(column: Any) -> Iterator[tuple[Any, int]]:
    """Yield ``(value, status)`` for whichever sample list is populated."""
    list_field = column.WhichOneof("list")
    if list_field is None:
        return
    for s in getattr(column, list_field).samples:
        yield s.value, s.status


def iter_periodic_proto(
    packet: Any, names: Sequence[str], signals: Optional[frozenset[str]] = None
) -> Iterator[tuple[str, int, Any, int]]:
    """Yield one tuple per sample; timestamps are ``start_time + i * interval``."""
    start = packet.start_time
    interval = packet.interval
    if signals is None:
        for name, column in zip(names, packet.columns):
            for i, (val, status) in enumerate(_proto_samples_iter(column)):
                yield name, start + i * interval, val, status
        return
    for name, column in zip(names, packet.columns):
        if name not in signals:
            continue
        for i, (val, status) in enumerate(_proto_samples_iter(column)):
            yield name, start + i * interval, val, status


def iter_synchro_proto(
    packet: Any, names: Sequence[str], signals: Optional[frozenset[str]] = None
) -> Iterator[tuple[str, int, Any, int]]:
    """Yield one tuple per sample; timestamps are ``start_time`` plus the
    cumulative ``intervals``."""
    cum: list[int] = [packet.start_time]
    acc = packet.start_time
    for iv in packet.intervals:
        acc += iv
        cum.append(acc)
    if signals is None:
        for name, column in zip(names, packet.columns):
            for ts, (val, status) in zip(cum, _proto_samples_iter(column)):
                yield name, ts, val, status
        return
    for name, column in zip(names, packet.columns):
        if name not in signals:
            continue
        for ts, (val, status) in zip(cum, _proto_samples_iter(column)):
            yield name, ts, val, status


def iter_row_proto(
    packet: Any, names: Sequence[str], signals: Optional[frozenset[str]] = None
) -> Iterator[tuple[str, int, Any, int]]:
    """Yield one tuple per sample; the samples of row ``i`` share
    ``timestamps[i]``."""
    if signals is None:
        for ts, row in zip(packet.timestamps, packet.rows):
            list_field = row.WhichOneof("list")
            if list_field is None:
                continue
            for name, s in zip(names, getattr(row, list_field).samples):
                yield name, ts, s.value, s.status
        return
    keep_indices = [i for i, n in enumerate(names) if n in signals]
    if not keep_indices:
        return
    for ts, row in zip(packet.timestamps, packet.rows):
        list_field = row.WhichOneof("list")
        if list_field is None:
            continue
        samples = getattr(row, list_field).samples
        for col_idx in keep_indices:
            s = samples[col_idx]
            yield names[col_idx], ts, s.value, s.status
