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
    Raised when an existing populated state store cannot be transitioned into
    TTL mode (i.e. the user's pipeline started writing ``state.set(..., ttl=...)``
    on a partition that already contains un-stamped legacy entries).

    Operator action: stop the application, delete the affected state directory,
    restart — recovery will rebuild the partition from the changelog topic
    with TTL enabled from the first replayed record.
    """

    ...


class StateSerializationError(StateError): ...


class StateTransactionError(StateError): ...


class ColumnFamilyHeaderMissing(StateError): ...


class ColumnFamilyDoesNotExist(StateError): ...


class InvalidChangelogOffset(StateError): ...


class ChangelogTopicPartitionNotAssigned(QuixException): ...
