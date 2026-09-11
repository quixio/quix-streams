"""
Red-first coverage for the unclamped recovery-completion expiry on the memory
backend.

``complete_recovery`` finishes an interrupted legacy-TTL migration by stamping
the leftover legacy records with the later of ``wallclock-at-rebuild +
legacy_records_ttl`` and the surviving cohort's own expiry. The configured side
of that comparison is ADDITIVE, so an individually valid ``legacy_records_ttl``
can still land ``>= _MAX_PLAUSIBLE_STAMP_MS`` — beyond what the strict read
validator ``_safe_decode_stamp`` will accept.

RocksDB routes that sum through ``clamp_additive_expiry``, which turns an
over-range result into ``SENTINEL_NEVER`` (readable, never mass-deleted) and
warns. The memory backend computes it raw. The consequence is not cosmetic: a
record stamped over-range is refused on EVERY read with
``StateSerializationError`` and its index entry never sweeps, so the two
backends diverge permanently while replaying the same changelog.

``clamp_additive_expiry``'s own docstring states the intended contract: reject
at the per-write path, CLAMP on the backfill/completion paths, because rejecting
there "would strand the entire migration (and every legacy record with it)".
"""

from datetime import timedelta

from quixstreams.state.memory import MemoryStorePartition
from quixstreams.state.metadata import TTL_BACKFILL_PENDING_CF_NAME
from quixstreams.state.rocksdb.ttl_codec import (
    _MAX_PLAUSIBLE_STAMP_MS,
    SENTINEL_NEVER,
    decode_ttl_value,
    encode_ttl_value,
)

DAY_MS = 86_400_000
BASE_TS = 1_780_000_000_000


class _NoopProducer:
    """Accepts every produce and acks it immediately."""

    def produce(self, key, value, headers=None, migration=False, on_delivery=None):
        if on_delivery is not None:
            on_delivery(None)

    def flush(self, timeout=None, migration=False):
        return 0


def _build_interrupted_migration(legacy_records_ttl):
    """
    Replay a MIXED changelog -- one stamped survivor plus one header-absent
    legacy leftover -- so ``complete_recovery`` reaches the completion branch
    with a leftover to stamp.
    """
    partition = MemoryStorePartition(
        changelog_producer=_NoopProducer(),
        legacy_records_ttl=legacy_records_ttl,
    )
    partition._now_ms = lambda: BASE_TS  # noqa: E731

    partition.recover_from_changelog_message(
        key=b"pfx|survivor",
        value=encode_ttl_value(BASE_TS + 30 * DAY_MS, b"stamped"),
        cf_name="default",
        offset=0,
        ttl_stamped=True,
    )
    partition.recover_from_changelog_message(
        key=b"pfx|leftover",
        value=b"legacy-value",
        cf_name="default",
        offset=1,
        ttl_stamped=False,
    )
    assert b"pfx|leftover" in partition._state.get(TTL_BACKFILL_PENDING_CF_NAME, {})
    return partition


class TestRecoveryCompletionExpiryClamp:
    def test_over_range_completion_expiry_is_clamped_to_never_expire(self):
        """
        An additive sum past the readable bound must become ``SENTINEL_NEVER``,
        not an unreadable stamp.

        Chosen so ``BASE_TS + ttl`` exceeds ``_MAX_PLAUSIBLE_STAMP_MS`` while the
        ttl itself is an ordinary positive ``timedelta`` that no validation
        rejects.
        """
        over_range_ms = _MAX_PLAUSIBLE_STAMP_MS - BASE_TS + DAY_MS
        partition = _build_interrupted_migration(timedelta(milliseconds=over_range_ms))

        partition.complete_recovery()

        stored = partition._state["default"][b"pfx|leftover"]
        expires_at, payload = decode_ttl_value(stored)
        assert payload == b"legacy-value"
        assert expires_at == SENTINEL_NEVER, (
            f"expiry {expires_at} is >= the maximum readable stamp "
            f"({_MAX_PLAUSIBLE_STAMP_MS}), so every read of this record raises "
            f"StateSerializationError and its index entry never sweeps"
        )

    def test_in_range_completion_expiry_is_left_alone(self):
        """
        The clamp must not touch an ordinary expiry -- otherwise leftover legacy
        records would silently become permanent.

        The fixture's lone survivor is stamped at ``BASE_TS + 30d``, which beats
        the configured 7-day window, so the in-range expiry under test is the
        cohort expiry ``BASE_TS + 30d`` and the clamp must pass it through
        untouched.
        """
        partition = _build_interrupted_migration(timedelta(days=7))

        partition.complete_recovery()

        stored = partition._state["default"][b"pfx|leftover"]
        expires_at, payload = decode_ttl_value(stored)
        assert payload == b"legacy-value"
        assert expires_at == BASE_TS + 30 * DAY_MS
