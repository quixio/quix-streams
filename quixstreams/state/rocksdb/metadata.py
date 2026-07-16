PROCESSED_OFFSET_KEY = b"__topic_offset__"
CHANGELOG_OFFSET_KEY = b"__changelog_offset__"

GLOBAL_COUNTER_CF_NAME = "__global-counter__"
GLOBAL_COUNTER_KEY = b"__global_counter__"

# TTL feature constants.
# TTL_INDEX_CF_NAME / TTL_BACKFILL_PENDING_CF_NAME are shared with
# quixstreams.state.metadata so the base transaction can route writes for them
# locally, off the changelog.
from quixstreams.state.metadata import (  # noqa: E402, F401
    TTL_BACKFILL_PENDING_CF_NAME,
    TTL_BACKFILL_STAMPED_CF_NAME,
    TTL_INDEX_CF_NAME,
    TTL_MIGRATION_DONE_KEY,
    TTL_SYSTEM_CF_NAME,
)

# Highest record event-time observed by any transaction on this partition,
# persisted to the metadata CF on every flush so the sweep / read-time filter
# survive restarts.
TTL_HIGH_WATER_KEY = b"__ttl_high_water_ms__"

# On-disk format-version marker. Bumped whenever the value layout changes in
# an incompatible way. v3 of the TTL feature uses ``2``; the marker is written
# only when a partition flips into TTL mode (see ``TTL_ENABLED_KEY``). Stores
# that never see a ``state.set(..., ttl=...)`` write stay marker-free and are
# byte-identical to the v3.23.6 on-disk layout.
STATE_FORMAT_VERSION_KEY = b"__ttl_format_version__"
STATE_FORMAT_VERSION = 2

# Lowest on-disk format-version marker that a warm open is allowed to UPGRADE
# in place (see ``RocksDBStorePartition._enforce_format_version``). The v3.24.0
# preview persisted marker ``1`` (or none); those are forward-compatible with
# the current stamp codec and are rewritten to ``STATE_FORMAT_VERSION``. A
# marker below this floor (``0``, negative, or an undecodable value) is NOT a
# recognized preview shape — it keeps the forward-incompatibility guard and
# raises ``IncompatibleStateStoreError`` rather than being silently rewritten.
MIN_UPGRADEABLE_STATE_FORMAT_VERSION = 1

# Per-partition opt-in flag for the TTL machinery. Absent (or empty) means the
# partition is in legacy mode: writes are not stamped, ``__ttl_index__`` does
# not exist, the sweep is a no-op, and recovery replays values verbatim.
# Present-and-truthy means the partition has been flipped into TTL mode by the
# framework on the first ``state.set(..., ttl=...)`` write that landed on a
# fresh (empty) default CF; once flipped, it stays flipped.
TTL_ENABLED_KEY = b"__ttl_enabled__"

# Local-only marker recording that a COLD-heuristic v3.24.0-stamp adoption is
# currently PROVISIONAL: the pre-adoption originals are backed up to
# ``__ttl_adopt_backup__`` and the TTL sweep is suppressed until a live
# ``state.set(..., ttl=...)`` write corroborates the adoption (see
# :meth:`RocksDBStorePartition._adopt_v3240_stamps` /
# :meth:`RocksDBStorePartition.corroborate_adoption`). Its value is
# ``int_to_bytes(adoption_wallclock_ms)``. Present == "provisional: backup live,
# sweep suppressed". Cleared on corroboration or ``QUIXSTREAMS_STATE_TTL_ROLLBACK``
# rollback. Lives in the metadata CF (``LOCAL_ONLY_CFS``), never on the changelog;
# the sound warm-deterministic adopt path never sets it.
TTL_ADOPT_PENDING_KEY = b"__ttl_adopt_pending__"

# Operational rollback lever for the COLD-heuristic provisional adoption
# (spec §5.6), read via ``os.environ.get`` at partition open. Modelled on the
# ``QUIXSTREAMS_STATE_LOG_LEVEL`` env-var pattern: transient, Portal-settable, NOT
# a ``RocksDBOptions`` field. When set to ``"1"`` it restores a provisionally
# cold-adopted store to legacy byte-identical (warm restart), or suppresses the
# cold provisional adopt entirely (fresh volume). It never touches the sound
# warm-deterministic path (that has no backup / no pending marker) nor a
# corroborated store (done-marker present).
TTL_ROLLBACK_ENV_VAR = "QUIXSTREAMS_STATE_TTL_ROLLBACK"

# Persisted backfill cursor for the legacy-records backfill.
# Holds the integer count ``N`` of keys
# already stamped from the deterministically-sorted census key list. Advanced
# in the same ``WriteBatch`` as each chunk's puts so a crash mid-backfill
# resumes at exactly key index ``N`` (no byte-sniffing). Additive metadata key:
# legacy and already-flipped stores simply never have it (no format-version
# bump). Lives in the metadata CF, which is in ``LOCAL_ONLY_CFS`` and is never
# produced to the changelog.
TTL_BACKFILL_PROGRESS_KEY = b"__ttl_backfill_progress__"
