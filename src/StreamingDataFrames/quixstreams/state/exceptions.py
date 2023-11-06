from quixstreams.exceptions import QuixException


class PartitionNotAssignedError(QuixException):
    ...


class StoreNotRegisteredError(QuixException):
    ...


class InvalidStoreTransactionStateError(QuixException):
    ...
