import logging
import sys
from datetime import datetime, timedelta, timezone

from dateutil.parser import parse


class TimeConverter:
    """
    A utility class for converting between different time representations.
    """
    _epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    _epochNaive = datetime(1970, 1, 1)
    _epochTzInfo = _epoch.tzinfo
    _microsecond = timedelta(microseconds=1)

    offset_from_utc = 0
    """
    The local time ahead of utc by this amount of nanoseconds 
    """

    _localTZInfo = None
    try:
        _localTZInfo = datetime.now().astimezone().tzinfo
        offset_from_utc = (_epoch - datetime(1970, 1, 1, tzinfo=_localTZInfo)) / _microsecond * 1000
    except:
        logging.warning("Local timezone can't be determined, treating as UTC", exc_info=sys.exc_info())

    @staticmethod
    def to_unix_nanoseconds(value: datetime) -> int:
        """
        Converts a datetime object to UNIX timestamp in nanoseconds.

        Args:
            value: The datetime object to be converted.

        Returns:
            int: The UNIX timestamp in nanoseconds.
        """

        if value.tzinfo is None:
            if TimeConverter._localTZInfo is None:
                # the local time zone is something that is not supported, in this case there isn't much we can do
                # and treat is as naive datetime. Warning was logged by now
                return TimeConverter.to_nanoseconds(value - TimeConverter._epochNaive)

            value = value.replace(tzinfo=TimeConverter._localTZInfo)
        return TimeConverter.to_nanoseconds(value - TimeConverter._epoch)

    @staticmethod
    def to_nanoseconds(value: timedelta) -> int:
        """
        Converts a timedelta object to nanoseconds.

        Args:
            value: The timedelta object to be converted.

        Returns:
            int: The duration in nanoseconds.
        """

        time = value / TimeConverter._microsecond
        return int(time * 1000)

    @staticmethod
    def from_nanoseconds(value: int) -> timedelta:
        """
        Converts a duration in nanoseconds to a timedelta object.

        Args:
            value: The duration in nanoseconds.

        Returns:
            timedelta: The corresponding timedelta object.
        """

        time = value / 1000
        return timedelta(microseconds=time)

    @staticmethod
    def from_unix_nanoseconds(value: int) -> datetime:
        """
        Converts a UNIX timestamp in nanoseconds to a datetime object.

        Args:
            value: The UNIX timestamp in nanoseconds.

        Returns:
            datetime: The corresponding datetime object.
        """

        return TimeConverter._epoch + TimeConverter.from_nanoseconds(value)

    @staticmethod
    def from_string(value: str) -> int:
        """
        Converts a string representation of a timestamp to a UNIX timestamp in nanoseconds.

        Args:
            value: The string representation of a timestamp.

        Returns:
            int: The corresponding UNIX timestamp in nanoseconds.
        """
        if value is None:
            return 0
        if value.isnumeric():
            return int(value)

        dt = parse(value)
        return TimeConverter.to_unix_nanoseconds(dt)
