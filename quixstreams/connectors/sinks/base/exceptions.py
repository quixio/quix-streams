class SinkException(Exception):
    """
    Base sink exception class
    """


class BatchError(SinkException):
    """
    Base batch error class
    """


class BatchFullError(BatchError):
    """
    Raised when the batch is full and must be flushed before adding new items
    """
