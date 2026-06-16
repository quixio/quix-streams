import enum

SEPARATOR = b"|"
SEPARATOR_LENGTH = len(SEPARATOR)

CHANGELOG_CF_MESSAGE_HEADER = "__column_family__"
CHANGELOG_PROCESSED_OFFSETS_MESSAGE_HEADER = "__processed_tp_offsets__"
# Per-record transport header (spec §8.7). Set on every ``default``-CF changelog
# record produced while the partition is in TTL mode (``uses_ttl_stamps`` True),
# i.e. the value carries the 8-byte expiry-stamp prefix. Recovery reads this bit
# to decide stamped-vs-legacy out-of-band, instead of sniffing value content.
# This is transport metadata only; the on-disk value layout (8B‖value) and the
# RocksDB metadata keys are unchanged.
CHANGELOG_TTL_STAMPED_HEADER = "__ttl_stamped__"
METADATA_CF_NAME = "__metadata__"
TTL_INDEX_CF_NAME = "__ttl_index__"
# Local-only census of leftover legacy keys seen during a cold-restore replay of
# a MIXED (incomplete-migration) changelog (spec §8.8 / spec-incomplete-migration-
# recovery.md §4.1). A header-absent default-CF replay PUTs its key here; a
# header-true replay of the same key DELETEs it (supersession). At end of
# recovery this CF holds exactly the leftover legacy keys, and the completion
# backfill iterates it (the per-key delete is the durable progress cursor). Never
# produced to the changelog.
TTL_BACKFILL_PENDING_CF_NAME = "__ttl_backfill_pending__"

# Column families whose writes must NOT be propagated to the changelog topic.
# Includes metadata (already excluded by virtue of not being touched from
# user-facing transactions), the TTL secondary expiry index, and the
# incomplete-migration pending-key census — all local to RocksDB only — see
# dev-planning/state-ttl/architecture.md.
LOCAL_ONLY_CFS = frozenset(
    {METADATA_CF_NAME, TTL_INDEX_CF_NAME, TTL_BACKFILL_PENDING_CF_NAME}
)

DEFAULT_PREFIX = b""


class Marker(enum.Enum):
    UNDEFINED = 1
    DELETED = 2
