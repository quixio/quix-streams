from quixstreams.state.exceptions import IncompatibleStateStoreError, StateError

__all__ = (
    "RocksDBCorruptedError",
    "RocksDBOpenAborted",
    "IncompatibleStateStoreError",
)


class RocksDBCorruptedError(StateError): ...


class RocksDBOpenAborted(StateError):
    """
    Raised when the open-retry loop is aborted because the application is
    stopping, instead of sleeping through the remaining retries.
    """
