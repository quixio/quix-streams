class SlateDBLockError(Exception):
    pass


class SlateDBCorruptedError(Exception):
    """Raised when a SlateDB database is detected as corrupted and cannot be opened."""

    pass
