from quixstreams.exceptions import QuixException

__all__ = ("SinkBackpressureError",)


class SinkBackpressureError(QuixException):
    """
    An exception to be raised by Sinks during flush() call
    to signal a backpressure event to the application.

    When raised, the app will drop the accumulated sink batch,
    pause the corresponding topic partition for
    a timeout specified in `retry_after`, and resume it when it's elapsed.

    :param retry_after: a timeout in seconds to pause for
    :param topic: a topic name to pause
    :param partition: a partition number to pause
    """

    def __init__(self, retry_after: float, topic: str, partition: int):
        self.retry_after = retry_after
        self.topic = topic
        self.partition = partition
