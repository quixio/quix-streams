import math

import System
from datetime import datetime, timedelta, timezone
import pytz, tzlocal

class PythonToNetConverter(object):

    __utc_epoch = datetime(1970, 1, 1, tzinfo=pytz.utc)

    @staticmethod
    def convert_datetime(value: datetime) -> System.DateTime:
        if value is None:
            return None
        value_tzaware = value
        if value.tzinfo is None:  # Naive timezone (local timezone), add explicitly else fails later
            local_tz = tzlocal.get_localzone()
            # resulting value might be using different offset than what is active right now. It uses offset that was
            # for the given time. If behavior is not as expected, specify timezone when creating the date.
            # Note: Unless UTC is specified, similar behavior will occur
            value_tzaware = value.replace(tzinfo=local_tz)
        value_utc = value_tzaware.astimezone(pytz.utc)
        elapsed = math.floor((value_utc - PythonToNetConverter.__utc_epoch).total_seconds())
        netdatetime = System.DateTime(1970, 1, 1).AddSeconds(elapsed).AddTicks(value_utc.microsecond*10)
        return netdatetime

    @staticmethod
    def convert_timedelta(value: timedelta) -> System.TimeSpan:
        if value is None:
            return None
        elapsed = value.total_seconds()
        nettimespan = System.TimeSpan.FromSeconds(elapsed)
        return nettimespan

    @staticmethod
    def convert_type(item):
        if isinstance(item, datetime):
            return PythonToNetConverter.convert_datetime(item)
        if isinstance(item, timedelta):
            return PythonToNetConverter.convert_timedelta(item)
        return item
