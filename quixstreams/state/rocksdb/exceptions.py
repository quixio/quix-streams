from quixstreams.state.exceptions import IncompatibleStateStoreError, StateError

__all__ = ("RocksDBCorruptedError", "IncompatibleStateStoreError")


class RocksDBCorruptedError(StateError): ...
