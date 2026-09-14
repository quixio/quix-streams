"""Filter resolution for :class:`MAStreamingDeserializer`.

The deserializer accepts a single ``filter`` mapping that can express
two orthogonal allowlists: packet ``types`` and signal names. This
module owns the validation logic and the immutable record the
deserializer consults at runtime, so ``deserializer.py`` can stay
focused on decode orchestration.

See the "Filtering" section of ``docs/advanced/ma-streaming-open-data.md``
for the user-facing semantics of both allowlists.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping, Optional

# Packet types that carry sample data (PeriodicData / SynchroData /
# RowData). Signal filtering only ever applies inside these; everything
# else is non-sample-bearing and passes through the type filter only.
SAMPLE_BEARING_TYPES: frozenset[str] = frozenset(
    {"PeriodicData", "RowData", "SynchroData"}
)

# Keys accepted inside the ``filter`` mapping. Anything else is a
# typo (e.g. ``"signal"`` for ``"signals"``) and we reject it at
# construction so the user sees the mistake immediately.
_ALLOWED_FILTER_KEYS: frozenset[str] = frozenset({"types", "signals"})


@dataclass(frozen=True, slots=True)
class ResolvedFilter:
    """Immutable resolved filter consulted by the deserializer.

    ``None`` on either attribute means "no constraint on this axis".
    When both are ``None`` the deserializer treats this as equivalent
    to ``filter=None`` (no filtering applied, today's shape contract
    preserved).
    """

    types: Optional[frozenset[str]]
    signals: Optional[frozenset[str]]

    @property
    def is_active(self) -> bool:
        """True when any filter axis is set (drives list-wrapping)."""
        return self.types is not None or self.signals is not None


def _validate_string_iterable(values: Iterable[str], field_name: str) -> frozenset[str]:
    """Coerce ``values`` into a non-empty frozenset of strings."""
    if isinstance(values, (str, bytes)):
        raise TypeError(
            f"filter[{field_name!r}] must be a sequence of strings, "
            f"got {type(values).__name__}"
        )
    resolved = frozenset(values)
    if not resolved:
        raise ValueError(
            f"filter[{field_name!r}] is empty; pass None or omit the key "
            f"to disable this axis"
        )
    for item in resolved:
        if not isinstance(item, str):
            raise TypeError(
                f"filter[{field_name!r}] entries must be strings, "
                f"got {type(item).__name__}"
            )
    return resolved


def resolve_filter(
    filter_arg: Optional[Mapping[str, Iterable[str]]],
    known_packet_types: Iterable[str],
    *,
    decode_content: bool,
    build_table_active: bool,
) -> ResolvedFilter:
    """Validate ``filter_arg`` and return a :class:`ResolvedFilter`.

    :param filter_arg: The constructor argument from the user.
    :param known_packet_types: All packet-type strings the deserializer
        can decode (built-ins merged with ``extra_content_types``). Used
        to reject typos in ``filter["types"]`` at construction.
    :param decode_content: ``False`` means signal filtering is
        impossible (inner payload is never parsed) and we must reject
        ``filter["signals"]``.
    :param build_table_active: ``True`` when the deserializer will emit
        projected rows (fast path). Signal filtering only makes sense
        when rows are being built, so we reject ``filter["signals"]``
        when this is ``False``.

    Raises ``TypeError`` for non-mapping input. Raises ``ValueError``
    for empty lists, unknown keys, unknown packet types, and the two
    cross-validation failures. Unknown signal names are accepted —
    signal names are runtime data.
    """
    if filter_arg is None:
        return ResolvedFilter(types=None, signals=None)

    if not isinstance(filter_arg, Mapping):
        raise TypeError(
            f"filter must be a Mapping (e.g. dict) or None, "
            f"got {type(filter_arg).__name__}"
        )

    unknown_keys = set(filter_arg.keys()) - _ALLOWED_FILTER_KEYS
    if unknown_keys:
        raise ValueError(
            f"filter has unknown keys: {sorted(unknown_keys)}. "
            f"Allowed keys: {sorted(_ALLOWED_FILTER_KEYS)}"
        )

    types: Optional[frozenset[str]] = None
    if "types" in filter_arg:
        types = _validate_string_iterable(filter_arg["types"], "types")
        known = frozenset(known_packet_types)
        unknown_types = types - known
        if unknown_types:
            raise ValueError(
                f"filter['types'] contains unknown packet types: "
                f"{sorted(unknown_types)}. Known types: {sorted(known)}"
            )

    signals: Optional[frozenset[str]] = None
    if "signals" in filter_arg:
        signals = _validate_string_iterable(filter_arg["signals"], "signals")
        # Cross-axis validation: signal filtering requires both the
        # inner payload to be decoded AND a row projection to be
        # active. Both errors are surfaced at construction so misuse
        # cannot reach runtime.
        if not decode_content:
            raise ValueError(
                "filter['signals'] is set but decode_content=False; "
                "signal filtering needs the inner payload to be parsed"
            )
        if not build_table_active:
            raise ValueError(
                "filter['signals'] is set but build_table is disabled; "
                "signal filtering only applies when row projection is active"
            )

    return ResolvedFilter(types=types, signals=signals)
