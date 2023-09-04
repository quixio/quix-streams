import ctypes
import datetime as dt
from enum import Enum
from ..helpers.nativedecorator import nativedecorator
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils as iu
from ..native.Python.ConfluentKafka.Timestamp import Timestamp as tsi
from ..native.Python.ConfluentKafka.TimestampType import TimestampType as TimestampTypeInterop
from ..helpers.timeconverter import TimeConverter
from ..helpers.dotnet.datetimeconverter import DateTimeConverter
from ..helpers.enumconverter import EnumConverter as ec


class KafkaTimestampType(Enum):
    """
        Represents the type of timestamp

        Attributes:
            NotAvailable: Timestamp type is unknown.
            CreateTime: Timestamp relates to message creation time as set by a Kafka client.
            LogAppendTime: Timestamp relates to the time a message was appended to a Kafka log.
        """
    NotAvailable = 0
    CreateTime = 1
    LogAppendTime = 2


@nativedecorator
class KafkaTimestamp(object):
    """
    Encapsulates a Kafka timestamp and its type.
    """

    def __init__(self,
                 datetime: dt.datetime = None,
                 unix_timestamp_ms: int = None,
                 unix_timestamp_ns: int = None,
                 timestamp_type: KafkaTimestampType = KafkaTimestampType.CreateTime,
                 **kwargs):
        """
        Initialize a new instance of KafkaTimestamp.

        This constructor allows you to create a KafkaTimestamp instance using different timestamp representations.

        Args:
            datetime (datetime, optional): The timestamp of the message as a datetime object. Takes precedence over other options.
            unix_timestamp_ms (int, optional): The timestamp of the message in milliseconds since the epoch. Takes precedence over unix_timestamp_ns.
            unix_timestamp_ns (int, optional): The timestamp of the message in nanoseconds since the epoch. Precision loss may occur.
            timestamp_type (KafkaTimestampType, optional): The type of timestamp to associate with the message.

        Example:
            To create a KafkaTimestamp instance with the current time and default type:
            >>> kts = KafkaTimestamp()

            To create a KafkaTimestamp instance with a specified datetime:
            >>> kts = KafkaTimestamp(datetime=datetime(2000, 7, 8))

            To create a KafkaTimestamp instance with a nanosecond timestamp:
            >>> kts = KafkaTimestamp(unix_timestamp_ns=1692808881000000085)

            To create a KafkaTimestamp instance with a millisecond timestamp:
            >>> kts = KafkaTimestamp(unix_timestamp_ms=1692808881788)

            To create a KafkaTimestamp instance with log append time:
            >>> kts = KafkaTimestamp(unix_timestamp_ms=1692808881788, timestamp_type=KafkaTimestampType.LogAppendTime)
        """

        self._unix_timestamp_ns = None
        self._unix_timestamp_ms = None
        self._datetime = None
        self._timestamp_type = timestamp_type

        if kwargs is not None and "net_pointer" in kwargs:
            net_pointer = kwargs["net_pointer"]
            self._interop = tsi(net_pointer)
            self._unix_timestamp_ms = self._interop.get_UnixTimestampMs()
            self._unix_timestamp_ns = self._unix_timestamp_ms * 1000000
            self._datetime = TimeConverter.from_unix_nanoseconds(self._unix_timestamp_ns)
            net_type = self._interop.get_Type()
            python_kafka_timestamp_type = ec.enum_to_another(net_type, KafkaTimestampType)
            self._timestamp_type = python_kafka_timestamp_type
            return

        if datetime is not None:
            self._datetime = datetime.astimezone(dt.timezone.utc)
            self._unix_timestamp_ns = TimeConverter.to_unix_nanoseconds(self._datetime)
            self._unix_timestamp_ms = round(self._unix_timestamp_ns / 1000000)

        elif unix_timestamp_ms is not None:
            self._unix_timestamp_ms = unix_timestamp_ms
            self._unix_timestamp_ns = self._unix_timestamp_ms * 1000000
            self._datetime = TimeConverter.from_unix_nanoseconds(self._unix_timestamp_ns)

        elif unix_timestamp_ns is not None:
            self._unix_timestamp_ns = unix_timestamp_ns
            self._unix_timestamp_ms = round(self._unix_timestamp_ns / 1000000)
            self._datetime = TimeConverter.from_unix_nanoseconds(self._unix_timestamp_ns)

        else:
            self._datetime = dt.datetime.now(dt.timezone.utc)
            self._unix_timestamp_ns = TimeConverter.to_unix_nanoseconds(self._datetime)
            self._unix_timestamp_ms = round(self._unix_timestamp_ns / 1000000)

        try:
            dt_hptr = DateTimeConverter.datetime_to_dotnet(self._datetime)
            python_kafka_timestamp_type = ec.enum_to_another(timestamp_type, TimestampTypeInterop)
            net_pointer = tsi.Constructor2(dt_hptr, python_kafka_timestamp_type)
            self._interop = tsi(net_pointer)
        finally:
            iu.free_hptr(dt_hptr)

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the associated .net object pointer of the KafkaMessage instance.

        Returns:
            ctypes.c_void_p: The .net object pointer of the KafkaMessage instance.
        """

        return self._interop.get_interop_ptr__()

    @staticmethod
    def default():
        """
        Returns an unspecified timestamp.
        """
        return tsi(net_pointer=tsi.get_Default())

    @property
    def utc_datetime(self) -> dt:
        """
        Gets the time as utc date time

        Returns:
            datetime: The time component of the timestamp in UTC time zone
        """
        return self._datetime

    @property
    def unix_timestamp_ns(self) -> int:
        """
        Gets the time as unix timestamp in nanoseconds

        Returns:
            int: The time component of the timestamp in nanoseconds
        """
        return self._unix_timestamp_ns

    @property
    def unix_timestamp_ms(self) -> int:
        """
        Gets the time as unix timestamp in milliseconds

        Returns:
            int: The time component of the timestamp in milliseconds
        """
        return self._unix_timestamp_ms

    @property
    def type(self) -> KafkaTimestampType:
        """
        Gets the timestamp type

        Returns:
            KafkaTimestampType: The timestamp type
        """
        return self._timestamp_type
