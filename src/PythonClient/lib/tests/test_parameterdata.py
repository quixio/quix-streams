import unittest

import math
from datetime import datetime, timezone, timedelta
import tzlocal, pytz

import pandas
from quixstreaming.helpers.timeconverter import TimeConverter
from quixstreaming import ParameterData

from quixstreaming.models.parametervalue import ParameterValueType


class ParameterDataTests(unittest.TestCase):

    def test_convert_from_parameter_data_to_panda_data_frame(self):
        # Arrange
        parameter_data = ParameterData()
        parameter_data.add_timestamp_nanoseconds(100)\
                      .add_value("param1", 1) \
                      .add_value("param2", "two")\
                      .add_tag("tag1", "val1")
        parameter_data.add_timestamp_nanoseconds(100)\
                      .add_value("param1", 2) \
                      .add_value("param2", "two_b")\
                      .add_tag("tag1", "val2")
        parameter_data.add_timestamp_nanoseconds(200)\
                      .add_value("param1", 3)
        # Act
        pdf = parameter_data.to_panda_frame()
        # Assert
        self.assertEqual(100, pdf["time"][0])
        self.assertEqual(1, pdf["param1"][0])
        self.assertEqual("two", pdf["param2"][0])
        self.assertEqual("val1", pdf["TAG__tag1"][0])

        self.assertEqual(100, pdf["time"][1])
        self.assertEqual(2, pdf["param1"][1])
        self.assertEqual("two_b", pdf["param2"][1])
        self.assertEqual("val2", pdf["TAG__tag1"][1])

        self.assertEqual(200, pdf["time"][2])
        self.assertEqual(3, pdf["param1"][2])
        self.assertTrue(pdf["param2"][2] is None)
        self.assertTrue(pdf["TAG__tag1"][2] is None)

    def test_convert_from_panda_data_frame_with_numeric_header_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "time"] = 100
        pdf.at[0, 1] = 1

        pdf.at[1, "time"] = 100
        pdf.at[1, 1] = 2

        pdf.at[2, "time"] = 200
        pdf.at[2, 1] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100)\
                      .add_value("1", 1)
        expected.add_timestamp_nanoseconds(100)\
                      .add_value("1", 2)
        expected.add_timestamp_nanoseconds(200)\
                      .add_value("1", 3)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_time_as_string_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "time"] = str(datetime(1970, 1, 1, 0, 0, 1, 0, timezone.utc))
        pdf.at[0, "param1"] = 1

        pdf.at[1, "time"] = str(datetime(2025, 12, 30, 15, 16, 24, 123456, timezone.utc))
        pdf.at[1, "param1"] = 2

        tz = timezone(timedelta(seconds=3600))
        pdf.at[2, "time"] = str(datetime(2030, 12, 10, 14, 13, 13, 0, tz))
        pdf.at[2, "param1"] = 3

        pdf.at[3, "time"] = str(2023138793000000000)
        pdf.at[3, "param1"] = 4

        pdf.at[4, "time"] = str("2020-12-11 10:00")
        pdf.at[4, "param1"] = 5

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(1000000000) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(1767107784123456000) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(1923138793000000000) \
            .add_value("param1", 3)
        expected.add_timestamp_nanoseconds(2023138793000000000) \
            .add_value("param1", 4)
        expected.add_timestamp_nanoseconds(1607680800000000000) \
            .add_value("param1", 5)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_time_as_datetime_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "time"] = datetime(1970, 1, 1, 0, 0, 1, 0, timezone.utc)
        pdf.at[0, "param1"] = 1

        pdf.at[1, "time"] = datetime(2025, 12, 30, 15, 16, 24, 123456, timezone.utc)
        pdf.at[1, "param1"] = 2

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(1000000000) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(1767107784123456000) \
            .add_value("param1", 2)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_time_as_datetime_to_parameter_data_using_non_utc_timezone(self):
        # Arrange
        pdf = pandas.DataFrame()
        tz = timezone(timedelta(seconds=14400))
        pdf.at[0, "time"] = datetime(1970, 1, 1, 4, 0, 1, 0, tz)
        pdf.at[0, "param1"] = 1

        tz2 = timezone(timedelta(seconds=-14400))
        pdf.at[1, "time"] = datetime(2025, 12, 30, 11, 16, 24, 123456, tz2)
        pdf.at[1, "param1"] = 2

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(1000000000) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(1767107784123456000) \
            .add_value("param1", 2)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_time_as_datetime_to_parameter_data_using_none_timezone(self):
        # Arrange
        pdf = pandas.DataFrame()
        time1 = datetime(1970, 1, 1, 0, 0, 1, 0)
        pdf.at[0, "time"] = time1
        pdf.at[0, "param1"] = 1

        time2 = datetime(2025, 12, 30, 15, 16, 24, 123456)
        pdf.at[1, "time"] = time2
        pdf.at[1, "param1"] = 2

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        tz2 = tzlocal.get_localzone()
        local = time1.replace(tzinfo=tz2)
        expected1 = TimeConverter.to_unix_nanoseconds(time1.replace(tzinfo=tz2))
        expected2 = TimeConverter.to_unix_nanoseconds(time2.replace(tzinfo=tz2))
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(expected1) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(expected2) \
            .add_value("param1", 2)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_time_timecol_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "time"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "time"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "time"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_Time_timecol_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "Time"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "Time"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "Time"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_datetime_timecol_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "datetime"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "datetime"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "datetime"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_DateTime_timecol_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "DateTime"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "DateTime"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "DateTime"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_timestamp_timecol_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "timestamp"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "timestamp"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_with_Timestamp_timecol_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "Timestamp"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "Timestamp"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "Timestamp"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_to_parameter_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "time"] = 100
        pdf.at[0, "param1"] = 1
        pdf.at[0, "param2"] = "two"
        pdf.at[0, "TAG__tag1"] = "val1"

        pdf.at[1, "time"] = 100
        pdf.at[1, "param1"] = 2
        pdf.at[1, "param2"] = "two_b"
        pdf.at[1, "param3"] = bytes(bytearray("bytes", "UTF-8"))
        pdf.at[1, "TAG__tag1"] = "val2"

        pdf.at[2, "time"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        parameter_data = ParameterData.from_panda_frame(pdf)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(100)\
                      .add_value("param1", 1) \
                      .add_value("param2", "two")\
                      .add_tag("tag1", "val1")
        expected.add_timestamp_nanoseconds(100)\
                      .add_value("param1", 2) \
                      .add_value("param2", "two_b") \
                      .add_value("param3", bytes(bytearray("bytes", "UTF-8"))) \
                      .add_tag("tag1", "val2")
        expected.add_timestamp_nanoseconds(200)\
                      .add_value("param1", 3)


        print("-- Written --")
        print(parameter_data)
        print("-- Expected --")
        print(expected)

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_convert_from_panda_data_frame_to_parameter_data_with_epoch(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "time"] = 100
        pdf.at[0, "param1"] = 1
        pdf.at[0, "param2"] = "two"
        pdf.at[0, "TAG__tag1"] = "val1"
        # Act
        parameter_data = ParameterData.from_panda_frame(pdf, 2000)
        # Assert
        expected = ParameterData()
        expected.add_timestamp_nanoseconds(2100)\
                      .add_value("param1", 1) \
                      .add_value("param2", "two")\
                      .add_tag("tag1", "val1")

        self.assert_data_are_equal(expected, parameter_data)
        self.assert_data_are_equal(parameter_data, expected)

    def test_multiple_time_columns(self):
        def _assert_time(pdf, time):
            parameter_data_raw = ParameterData.from_panda_frame(pdf).to_panda_frame()
            parsed_time=parameter_data_raw.loc[0, 'time']
            self.assertEqual(time, parsed_time)

        _assert_time(pandas.DataFrame([{"value": 0.1,"time": 1000000}]), 1000000)
        _assert_time(pandas.DataFrame([{"TiMe": 2000000, "value": 0.1}]), 2000000)
        _assert_time(pandas.DataFrame([{"value": 0.1,"datetime": 3000000}]), 3000000)
        _assert_time(pandas.DataFrame([{"TiMeSTAMP": 5000000, "value": 0.1}]), 5000000)
        _assert_time(pandas.DataFrame([{"value": 0.1, "TiMeSTAMP": 5000000}]), 5000000)

    def assert_data_are_equal(self, data_a: ParameterData, data_b: ParameterData):
        self.assertEqual(len(data_a.timestamps), len(data_b.timestamps), "Timestamp count")
        for index_a, ts_a in enumerate(data_a.timestamps):
            ts_b = data_b.timestamps[index_a]
            self.assertEqual(ts_a.timestamp_nanoseconds, ts_b.timestamp_nanoseconds, "Timestamp")
            for param_id_a, parameter_value_a in ts_a.parameters.items():
                parameter_value_b = ts_b.parameters[param_id_a]
                self.assertEqual(parameter_value_a.type, parameter_value_b.type, "Value type")
                if parameter_value_a.type == ParameterValueType.String:
                    self.assertEqual(parameter_value_a.string_value, parameter_value_b.string_value,
                                     "Value (string)")
                if parameter_value_a.type == ParameterValueType.Numeric:
                    self.assertEqual(parameter_value_a.numeric_value, parameter_value_b.numeric_value,
                                     "Value (numeric)")
            for tag_id_a, tag_value_a in ts_a.tags.items():
                tag_value_b = ts_b.tags[tag_id_a]
                self.assertEqual(tag_value_a, tag_value_b, "tag")