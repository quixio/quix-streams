from datetime import datetime, timedelta
from dateutil.parser import parse

from .. import __importnet
import Quix.Sdk.Process
from quixstreaming.helpers import PythonToNetConverter, NetToPythonConverter


class TimeConverter:

    @staticmethod
    def to_unix_nanoseconds(value: datetime) -> int:
        netdatetime = PythonToNetConverter.convert_datetime(value)
        return Quix.Sdk.Process.Models.Utility.TimeExtensions.ToUnixNanoseconds(netdatetime)

    @staticmethod
    def to_nanoseconds(value: timedelta) -> int:
        nettimespan = PythonToNetConverter.convert_timedelta(value)
        return Quix.Sdk.Process.Models.Utility.TimeExtensions.ToNanoseconds(nettimespan)

    @staticmethod
    def from_nanoseconds(value: int) -> timedelta:
        nettimespan = Quix.Sdk.Process.Models.Utility.TimeExtensions.FromNanoseconds(value)
        td = NetToPythonConverter.convert_timespan(nettimespan)
        return td

    @staticmethod
    def from_unix_nanoseconds(value: int) -> datetime:
        netdatetime = Quix.Sdk.Process.Models.Utility.TimeExtensions.FromUnixNanoseconds(value)
        dt = NetToPythonConverter.convert_datetime(netdatetime)
        return dt

    @staticmethod
    def from_string(value: str) -> int:
        if value is None:
            return 0
        if value.isnumeric():
            return int(value)

        dt = parse(value)
        return TimeConverter.to_unix_nanoseconds(dt)
