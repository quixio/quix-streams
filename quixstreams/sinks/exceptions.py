from quixstreams.exceptions import QuixException


class SinkBackpressureError(QuixException):
    def __init__(self, retry_after: float, topic: str, partition: int):
        self.retry_after = retry_after
        self.topic = topic
        self.partition = partition
