import datetime
import math
import unittest
from datetime import timedelta

from src.quixstreams.native.Python.SystemPrivateCoreLib.System.DateTime import DateTime
from src.quixstreams.native.Python.SystemPrivateCoreLib.System.TimeSpan import TimeSpan

from src.quixstreams.helpers.dotnet.datetimeconverter import DateTimeConverter

class DateTimeConverterTests(unittest.TestCase):

    def test_timespan_to_python(self):
        # Arrange
        dotnet_ts_uptr = TimeSpan.Constructor(123456780)  # precision loss can be an issue, so going for accuracy by adding 0

        # Act
        pyts = DateTimeConverter.timespan_to_python(dotnet_ts_uptr)

        # Assert
        self.assertEqual(12345678, pyts.total_seconds()*1000000)


    def test_datetime_to_python(self):
        # Arrange
        dotnet_dt_hptr = DateTime.Constructor12(2023, 1, 20, 23, 17, 8, 1, 0)  # precision loss can be an issue, so going for accuracy by adding 0

        # Act
        pydt = DateTimeConverter.datetime_to_python(dotnet_dt_hptr)

        # Assert
        self.assertEqual(2023, pydt.year)
        self.assertEqual(1, pydt.month)
        self.assertEqual(20, pydt.day)
        self.assertEqual(23, pydt.hour)
        self.assertEqual(17, pydt.minute)
        self.assertEqual(8, pydt.second)
        self.assertEqual(1000, pydt.microsecond)


    def test_timespan_to_dotnet(self):
        # Arrange
        pyts = timedelta(hours=5, minutes=4, seconds=3, milliseconds=2, microseconds=1)

        # Act
        netts_hptr = DateTimeConverter.timedelta_to_dotnet(pyts)

        # Assert
        netts = TimeSpan(netts_hptr)
        expeced = round(pyts.total_seconds()*1000*1000*10)
        self.assertEqual(expeced, netts.get_Ticks())


    def test_datetime_to_dotnet(self):
        # Arrange
        pydt = datetime.datetime(2023, 1, 20, 23, 17, 8, 9873)  # precision loss can be an issue, so going for accuracy by adding 0

        # Act
        netdt_hptr = DateTimeConverter.datetime_to_dotnet(pydt)

        # Assert
        with (netdt := DateTime(netdt_hptr)):
            self.assertEqual(pydt.year, netdt.get_Year())
            self.assertEqual(pydt.month, netdt.get_Month())
            self.assertEqual(pydt.day, netdt.get_Day())
            self.assertEqual(pydt.hour, netdt.get_Hour())
            self.assertEqual(pydt.minute, netdt.get_Minute())
            self.assertEqual(pydt.second, netdt.get_Second())
            self.assertEqual(math.floor(pydt.microsecond/1000), netdt.get_Millisecond())
            self.assertEqual(pydt.microsecond % 1000 * 10, netdt.get_Ticks() % 10000) # for anything smaller than milliseconds we need the ticks
