"""Utils to get right Date parsing function."""
import datetime
import threading
from datetime import timezone as tz
from sys import version_info

from dateutil import parser

date_helper = None

lock_ = threading.Lock()


class DateHelper:
    """
    DateHelper to groups different implementations of date operations.

    If you would like to serialize the query results to custom timezone, you can use following code:

    .. code-block:: python

        from influxdb_client.client.util import date_utils
        from influxdb_client.client.util.date_utils import DateHelper
        import dateutil.parser
        from dateutil import tz

        def parse_date(date_string: str):
            return dateutil.parser.parse(date_string).astimezone(tz.gettz('ETC/GMT+2'))

        date_utils.date_helper = DateHelper()
        date_utils.date_helper.parse_date = parse_date
    """

    def __init__(self, timezone: datetime.tzinfo = tz.utc) -> None:
        """
        Initialize defaults.

        :param timezone: Default timezone used for serialization "datetime" without "tzinfo".
                         Default value is "UTC".
        """
        self.timezone = timezone

    def parse_date(self, date_string: str):
        """
        Parse string into Date or Timestamp.

        :return: Returns a :class:`datetime.datetime` object or compliant implementation
                 like :class:`class 'pandas._libs.tslibs.timestamps.Timestamp`
        """
        pass

    def to_nanoseconds(self, delta):
        """
        Get number of nanoseconds in timedelta.

        Solution comes from v1 client. Thx.
        https://github.com/influxdata/influxdb-python/pull/811
        """
        nanoseconds_in_days = delta.days * 86400 * 10 ** 9
        nanoseconds_in_seconds = delta.seconds * 10 ** 9
        nanoseconds_in_micros = delta.microseconds * 10 ** 3

        return nanoseconds_in_days + nanoseconds_in_seconds + nanoseconds_in_micros

    def to_utc(self, value: datetime):
        """
        Convert datetime to UTC timezone.

        :param value: datetime
        :return: datetime in UTC
        """
        if not value.tzinfo:
            return self.to_utc(value.replace(tzinfo=self.timezone))
        else:
            return value.astimezone(tz.utc)


def get_date_helper() -> DateHelper:
    """
    Return DateHelper with proper implementation.

    If there is a 'ciso8601' than use 'ciso8601.parse_datetime' else use 'dateutil.parse'.
    """
    global date_helper
    if date_helper is None:
        with lock_:
            # avoid duplicate initialization
            if date_helper is None:
                _date_helper = DateHelper()
                try:
                    import ciso8601
                    _date_helper.parse_date = ciso8601.parse_datetime
                except ModuleNotFoundError:
                    if (version_info.major, version_info.minor) >= (3, 11):
                        _date_helper.parse_date = datetime.datetime.fromisoformat
                    else:
                        _date_helper.parse_date = parser.parse
                date_helper = _date_helper

    return date_helper
