from quixstreams.state.exceptions import StateError

__all__ = ("RocksDBCorruptedError", "IncompatibleStateStoreError")


class RocksDBCorruptedError(StateError): ...


class IncompatibleStateStoreError(StateError):
    """
    Raised when an existing state store directory uses an on-disk layout that
    is not forward-compatible with the running version of the library.

    The TTL feature (v2) prefixes every value in the main column family with
    an 8-byte expiry stamp; stores written before this layout was introduced
    cannot be decoded safely. Operators must delete the affected state
    directory and let recovery rebuild from the changelog topic.
    """

    ...
