"""Utils to get right Date parsing function."""

import datetime
from datetime import timezone as tz
from sys import version_info

try:
    from dateutil import parser
except ImportError as exc:
    raise ImportError(
        'Package "dateutil" is missing: '
        "run pip install quixstreams[tdengine] to fix it"
    ) from exc


try:
    import ciso8601

    _parse_date = ciso8601.parse_datetime
except ModuleNotFoundError:
    if (version_info.major, version_info.minor) >= (3, 11):
        _parse_date = datetime.datetime.fromisoformat
    else:
        _parse_date = parser.parse


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
        return _parse_date(date_string)

    def to_nanoseconds(self, delta):
        """
        Get number of nanoseconds in timedelta.

        Solution comes from v1 client. Thx.
        https://github.com/influxdata/influxdb-python/pull/811
        """
        nanoseconds_in_days = delta.days * 86400 * 10**9
        nanoseconds_in_seconds = delta.seconds * 10**9
        nanoseconds_in_micros = delta.microseconds * 10**3

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
