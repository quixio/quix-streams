from quixstreams.connectors.sinks.base.exceptions import SinkException


class InvalidS3FormatterError(SinkException):
    """
    Raised when S3 formatter is specified incorrectly
    """


class S3SinkSerializationError(SinkException):
    """
    Raise when S3 Sink formatter fails to serialize items in the batch
    """
