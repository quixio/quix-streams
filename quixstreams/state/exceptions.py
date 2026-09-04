from quixstreams.exceptions import QuixException


class PartitionNotAssignedError(QuixException): ...


class PartitionStoreIsUsed(QuixException): ...


class StoreNotRegisteredError(QuixException): ...


class StoreAlreadyRegisteredError(QuixException): ...


class InvalidStoreTransactionStateError(QuixException): ...


class StoreTransactionFailed(QuixException): ...


class InvalidStoreChangelogOffset(QuixException): ...


class StateRecoveryOffsetOutOfRange(QuixException): ...


class StateError(QuixException): ...


class IncompatibleStateStoreError(StateError):
    """
    Raised when a TTL-aware state store cannot be opened or advanced safely.

    (The original "reject a populated legacy store on the first ``ttl=`` write"
    use was removed when it was replaced with an automatic legacy
    backfill; two distinct situations remain.)

    1. **Format-version incompatibility** (``_enforce_format_version``). A store
       already flipped into TTL mode must carry a format-version marker at least
       as new as the running version. If the marker is missing, or was written
       by an older on-disk layout, the store cannot be read safely. Operator
       action: stop the application, delete the affected state directory, and
       restart — recovery rebuilds the partition from the changelog topic.

    2. **Framework-invariant guard** (``_legacy_expiry_from_ttl_ms``). A
       should-be-unreachable condition at flip time — no event-time high-water,
       or no recorded ``ttl=`` duration for the triggering batch — that the
       ``state.set(..., ttl=...)`` validation should already have prevented. It
       signals a framework bug (not operator misconfiguration) and is raised
       rather than inventing a wall-clock expiry.
    """

    ...


class StateSerializationError(StateError): ...


class StateMigrationError(StateError):
    """
    Raised when a store's legacy-vs-TTL mode cannot be reconciled with what its
    ``default`` column family actually holds, or when the two TTL operational
    levers contradict each other. Both situations need an operator decision --
    the framework refuses to guess, because either guess can destroy data.

    1. **TTL-stamped values on a legacy-flagged store** (the read path,
       ``RocksDBPartitionTransaction._get_bytes``). The partition opened with TTL
       bookkeeping on disk but WITHOUT the ``__ttl_enabled__`` flag and nothing
       flipped it -- either the open-time repair
       (``RocksDBStorePartition._repair_unflagged_stamped_store``) declined on an
       interrupted legacy-TTL migration, or the rollback lever reverted a v3.24.0
       adoption (``RocksDBStorePartition._rollback_provisional_adopt``), which
       deliberately leaves the adopted originals byte-identical and therefore
       still stamped. A ``default``-CF read then returned a value whose 8-byte
       prefix decodes as a plausible, still-live expiry stamp. Returning it raw
       hands the stamp to the value deserializer -- the live crash loop this
       guard replaces, where every restart died with a
       ``StateSerializationError`` from ``orjson`` on ``8B||json``. Silently
       stripping eight bytes is worse: a genuine legacy value whose first eight
       bytes happen to decode would be corrupted with no way back. Operator
       action: set ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1`` and restart to flip
       the store into TTL mode (or, if ``QUIXSTREAMS_STATE_TTL_ROLLBACK=1`` is
       what suppressed the automatic repair, unset it), or rebuild the
       partition's state from its changelog.

    2. **Contradictory operational levers**
       (``RocksDBStorePartition.__init__``).
       ``QUIXSTREAMS_STATE_TTL_ROLLBACK=1`` ("revert this store to legacy") and
       ``QUIXSTREAMS_STATE_TTL_FORCE_FLIP=1`` ("force this store into TTL
       mode") are mutually exclusive; set together they would fight each other
       on every open, so the partition refuses to open at all.
    """

    ...


class StateTransactionError(StateError): ...


class ColumnFamilyHeaderMissing(StateError): ...


class ColumnFamilyDoesNotExist(StateError): ...


class InvalidChangelogOffset(StateError): ...


class ChangelogTopicPartitionNotAssigned(QuixException): ...


class ChangelogFlushError(StateError):
    """
    Raised by the legacy-TTL backfill / recovery-completion paths when a chunk's
    stamped changelog records could not be confirmed delivered within the bounded
    flush timeout.

    These paths MUST get each chunk durably onto the changelog before committing
    the matching stamps to the local store; proceeding with undelivered records
    would leave the local DB ahead of the changelog, so a peer rebuilding from the
    changelog would diverge. Failing loudly here (rather than writing the local
    batch) preserves that ordering invariant.
    """

    ...
