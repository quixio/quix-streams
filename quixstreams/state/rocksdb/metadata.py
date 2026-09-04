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

# Operational rollback lever for the COLD-heuristic provisional adoption,
# read via ``os.environ.get`` at partition open. Modelled on the
# ``QUIXSTREAMS_STATE_LOG_LEVEL`` env-var pattern: transient, Portal-settable, NOT
# a ``RocksDBOptions`` field. When set to ``"1"`` it restores a provisionally
# cold-adopted store to legacy byte-identical (warm restart), or suppresses the
# cold provisional adopt entirely (fresh volume). It never touches the sound
# warm-deterministic path (that has no backup / no pending marker) nor a
# corroborated store (done-marker present).
TTL_ROLLBACK_ENV_VAR = "QUIXSTREAMS_STATE_TTL_ROLLBACK"

# Operational REPAIR lever -- the sibling of ``TTL_ROLLBACK_ENV_VAR``, read the
# same way (``os.environ.get`` at partition open; transient, Portal-settable, NOT
# a ``RocksDBOptions`` field). When set to ``"1"`` it forces a store whose
# ``TTL_ENABLED_KEY`` is ABSENT into TTL mode and persists the flip, then lets the
# existing recovery-completion path finish any leftover migration.
#
# It exists because the automatic open-time repair
# (``RocksDBStorePartition._repair_unflagged_stamped_store``) can only fire on
# POSITIVE evidence: this-branch migration bookkeeping on disk plus at least one
# still-live stamp in a bounded sample of the default CF. A store whose local
# bookkeeping is gone -- a rebuilt state directory, or an earlier
# ``QUIXSTREAMS_STATE_TTL_ROLLBACK`` that deleted the flip flag and the format /
# high-water markers while leaving the untouched values stamped -- has no
# evidence left to identify, so it opens in legacy mode over stamped values and
# every read crashes in the value deserializer. This lever is the operator's
# override for exactly that state; the read-path guard's
# ``StateMigrationError`` names it.
#
# Mutually exclusive with ``TTL_ROLLBACK_ENV_VAR``: setting both raises at open
# (see ``RocksDBStorePartition.__init__``), since one reverts a store to legacy
# and the other forces it into TTL mode. Only presence of the exact value ``"1"``
# counts, matching the rollback lever. No-op on an already-flipped store
# (the persisted flag short-circuits the repair), so it is safe to leave set for
# one restart and then unset.
TTL_FORCE_FLIP_ENV_VAR = "QUIXSTREAMS_STATE_TTL_FORCE_FLIP"

# Persisted backfill cursor for the legacy-records backfill.
# Holds the integer count ``N`` of keys
# already stamped from the deterministically-sorted census key list. Advanced
# in the same ``WriteBatch`` as each chunk's puts so a crash mid-backfill
# resumes at exactly key index ``N`` (no byte-sniffing). Additive metadata key:
# legacy and already-flipped stores simply never have it (no format-version
# bump). Lives in the metadata CF, which is in ``LOCAL_ONLY_CFS`` and is never
# produced to the changelog.
TTL_BACKFILL_PROGRESS_KEY = b"__ttl_backfill_progress__"

# Durable "a live legacy backfill is in flight on this volume" marker, armed by
# ``RocksDBStorePartition.backfill_legacy_records`` in its OWN commit BEFORE the
# first chunk is produced to the changelog, and cleared once the last chunk has
# committed locally. It is the replay-independent half of the
# interrupted-live-backfill signature.
#
# Why the stamped ledger alone is not enough: ``__ttl_backfill_stamped__`` is
# written in the SAME WriteBatch as a chunk's stamped values, and the backfill is
# changelog-FIRST (produce + flush-confirm, then commit locally). A crash inside
# the FIRST chunk's produce->commit window therefore leaves that chunk durable on
# the changelog with an EMPTY ledger and an un-flipped store. On the next warm
# restart the replayed chunk flips the partition store-wide, while the leftovers
# -- never produced, so never replayed and never censused into
# ``__ttl_backfill_pending__`` -- stay raw legacy under a read path that now
# strips 8 bytes as an expiry. Both CF-based completion tracks are empty, so the
# migration looks finished and the done-marker would latch "done, never redo".
# This marker is the only on-disk fact that survives that window.
#
# Lives in the metadata CF (a ``LOCAL_ONLY_CFS`` member), so it never rides the
# changelog and a fresh-volume cold restore never sees it -- which is also why it
# is a safe discriminator: only this build's own backfill ever writes it, so a
# stock v3.24.0 store can never carry it and the v3.24.0 detection / adoption
# paths can never be reached through it. Additive: legacy and already-migrated
# stores simply never have it (no format-version bump). The value is a presence
# flag; only presence is ever read.
TTL_BACKFILL_IN_PROGRESS_KEY = b"__ttl_backfill_in_progress__"
