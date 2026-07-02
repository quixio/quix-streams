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
# Local-only ledger of pre-existing legacy keys ALREADY STAMPED by the in-place
# *live* backfill (:meth:`RocksDBStorePartition.backfill_legacy_records`). It is
# the crash-safe resume cursor for that path (Bug 1 fix, see
# dev-planning/state-ttl-legacy-backfill/fix-cursor-and-pending-hygiene.md): each
# chunk PUTs the keys it stamped into this CF within the SAME WriteBatch as the
# stamped values, so the ledger and the data commit atomically. On resume the
# re-derived census excludes ledger members, which makes the backfill insensitive
# to interleaved legacy writes (a fresh key is simply a not-yet-stamped census
# key; an already-stamped key is a ledger member and is never re-read or
# re-wrapped) WITHOUT inspecting value content. Distinct from
# ``TTL_BACKFILL_PENDING_CF_NAME`` (recovery-completion "to-do" list, delete-on-
# commit); the two paths are mutually exclusive per partition lifecycle. Never
# produced to the changelog.
TTL_BACKFILL_STAMPED_CF_NAME = "__ttl_backfill_stamped__"

# Replicated system CF carrying the durable "migration done" marker (spec §13.1,
# spec-v3240-upgrade-and-recovery-clock.md). Deliberately NOT in
# ``LOCAL_ONLY_CFS`` so its single record IS produced to the changelog and
# therefore survives a cold rebuild onto a fresh volume — unlike the metadata /
# index / pending / stamped-ledger CFs, whose flip state is local-only and lost
# on a fresh volume. Produced flag-last when a legacy→TTL migration completes on
# ANY enable path (empty-store flip, live populated backfill, recovery
# completion). On recovery, seeing the marker latches
# ``_recovery_saw_migration_done`` so the store is flipped and the backfill is
# NEVER re-run (idempotent "never redo"); it is also the clean structural signal
# that closes the stock-v3.24.0 mis-classification class for every store written
# after the marker landed. It is a system CF: never surfaced as user data (user
# iteration only ever touches the ``default`` CF, exactly like ``__metadata__``).
TTL_SYSTEM_CF_NAME = "__ttl_system__"
# The single reserved key in ``TTL_SYSTEM_CF_NAME``. Its value is a small
# forward-compatible blob (currently ``int_to_bytes(STATE_FORMAT_VERSION)``, a
# schema seam for future fields). The unique reserved key means the marker
# survives log compaction and is never mistaken for a user record.
TTL_MIGRATION_DONE_KEY = b"__ttl_migration_done__"

# Column families whose writes must NOT be propagated to the changelog topic.
# Includes metadata (already excluded by virtue of not being touched from
# user-facing transactions), the TTL secondary expiry index, the incomplete-
# migration pending-key census, and the live-backfill stamped-key ledger — all
# local to RocksDB only — see dev-planning/state-ttl/architecture.md.
# NOTE: ``TTL_SYSTEM_CF_NAME`` is deliberately NOT here — its done-flag marker
# must ride the changelog to survive a cold rebuild (spec §13.1).
LOCAL_ONLY_CFS = frozenset(
    {
        METADATA_CF_NAME,
        TTL_INDEX_CF_NAME,
        TTL_BACKFILL_PENDING_CF_NAME,
        TTL_BACKFILL_STAMPED_CF_NAME,
    }
)

DEFAULT_PREFIX = b""


class Marker(enum.Enum):
    UNDEFINED = 1
    DELETED = 2
