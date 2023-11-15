from quixstreams.exceptions import QuixException


class PartitionNotAssignedError(QuixException):
    ...


class PartitionStoreIsUsed(QuixException):
    ...


class StoreNotRegisteredError(QuixException):
    ...


class InvalidStoreTransactionStateError(QuixException):
    ...
