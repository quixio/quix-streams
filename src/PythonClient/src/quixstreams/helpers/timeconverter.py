import logging
import sys
from datetime import datetime, timedelta, timezone

from dateutil.parser import parse


class TimeConverter:
    _epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    _epochNaive = datetime(1970, 1, 1)
    _epochTzInfo = _epoch.tzinfo
    _microsecond = timedelta(microseconds=1)

    _localTZInfo = None
    try:
        _localTZInfo = datetime.now().astimezone().tzinfo
    except:
        logging.warning("Timezone can't be determined, treating as UTC", exc_info=sys.exc_info())

    @staticmethod
    def to_unix_nanoseconds(value: datetime) -> int:
        if value.tzinfo is None:
            if TimeConverter._localTZInfo is None:
                # the local time zone is something that is not supported, in this case there isn't much we can do
                # and treat is as naive datetime. Warning was logged by now
                return TimeConverter.to_nanoseconds(value - TimeConverter._epochNaive)

            value = value.replace(tzinfo=TimeConverter._localTZInfo)
        return TimeConverter.to_nanoseconds(value - TimeConverter._epoch)

    @staticmethod
    def to_nanoseconds(value: timedelta) -> int:
        time = value / TimeConverter._microsecond
        return int(time * 1000)

    @staticmethod
    def from_nanoseconds(value: int) -> timedelta:
        time = value / 1000
        return timedelta(microseconds=time)

    @staticmethod
    def from_unix_nanoseconds(value: int) -> datetime:
        return TimeConverter._epoch + TimeConverter.from_nanoseconds(value)

    @staticmethod
    def from_string(value: str) -> int:
        if value is None:
            return 0
        if value.isnumeric():
            return int(value)

        dt = parse(value)
        return TimeConverter.to_unix_nanoseconds(dt)
