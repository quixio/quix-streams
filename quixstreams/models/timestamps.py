import enum

__all__ = ("TimestampType", "MessageTimestamp")


class TimestampType(enum.IntEnum):
    TIMESTAMP_NOT_AVAILABLE = 0  # timestamps not supported by broker
    TIMESTAMP_CREATE_TIME = 1  # message creation time (or source / producer time)
    TIMESTAMP_LOG_APPEND_TIME = 2  # broker receive time


class MessageTimestamp:
    """
    Represents a timestamp of incoming Kafka message.

    It is made pseudo-immutable (i.e. public attributes don't have setters), and
    it should not be mutated during message processing.
    """

    __slots__ = ("_milliseconds", "_type")

    def __init__(
        self,
        milliseconds: int,
        type: TimestampType,
    ):
        self._milliseconds = milliseconds
        self._type = type

    @property
    def milliseconds(self) -> int:
        return self._milliseconds

    @property
    def type(self) -> TimestampType:
        return self._type

    @classmethod
    def create(cls, timestamp_type: int, milliseconds: int) -> "MessageTimestamp":
        """
        Create a Timestamp object based on data
        from `confluent_kafka.Message.timestamp()`.

        If timestamp type is "TIMESTAMP_NOT_AVAILABLE", the milliseconds are set to None

        :param timestamp_type: a timestamp type represented as a number
        Can be one of:
            - "0" - TIMESTAMP_NOT_AVAILABLE, timestamps not supported by broker.
            - "1" - TIMESTAMP_CREATE_TIME, message creation time (or source / producer time).
            - "2" - TIMESTAMP_LOG_APPEND_TIME, broker receive time.
        :param milliseconds: the number of milliseconds since the epoch (UTC).
        :return: Timestamp object
        """
        type_enum = TimestampType(timestamp_type)
        return cls(type=type_enum, milliseconds=milliseconds)
