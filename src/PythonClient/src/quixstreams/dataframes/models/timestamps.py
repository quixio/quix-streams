import dataclasses
import enum

__all__ = ("TimestampType", "MessageTimestamp")

from typing import Optional


class TimestampType(enum.IntEnum):
    TIMESTAMP_NOT_AVAILABLE = 0  # timestamps not supported by broker
    TIMESTAMP_CREATE_TIME = 1  # message creation time (or source / producer time)
    TIMESTAMP_LOG_APPEND_TIME = 2  # broker receive time


@dataclasses.dataclass(slots=True, init=True, frozen=True)
class MessageTimestamp:
    """
    Represents a timestamp of incoming Kafka message
    """

    milliseconds: Optional[int]
    type: TimestampType

    @classmethod
    def create(cls, timestamp_type: int, milliseconds: int):
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
        if type_enum == TimestampType.TIMESTAMP_NOT_AVAILABLE:
            milliseconds = None
        return cls(type=type_enum, milliseconds=milliseconds)
