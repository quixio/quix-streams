from quixstreams.exceptions import QuixException

__all__ = ("SinkBackpressureError",)


class SinkBackpressureError(QuixException):
    """
    An exception to be raised by Sinks during flush() call
    to signal a backpressure event to the application.

    When raised, the app will drop the accumulated sink batches,
    pause all assigned topic partitions for
    a timeout specified in `retry_after`, and resume them when it's elapsed.

    :param retry_after: a timeout in seconds to pause for
    """

    def __init__(self, retry_after: float):
        self.retry_after = retry_after
