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
