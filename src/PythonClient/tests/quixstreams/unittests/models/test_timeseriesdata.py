import unittest

from datetime import datetime, timezone, timedelta

import pandas
import numpy as np
from src.quixstreams.helpers.timeconverter import TimeConverter
from src.quixstreams import TimeseriesData, TimeseriesDataTimestamp

from src.quixstreams.models.parametervalue import ParameterValueType


class TimeseriesDataTests(unittest.TestCase):

    def test_populate_timeseries_data_has_expected_length(self):
        # Arrange
        timeseries_data = TimeseriesData()
        timeseries_data.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        timeseries_data.add_timestamp_milliseconds(1) \
            .add_value("param1", 2)
        timeseries_data.add_timestamp(timedelta(milliseconds=2)) \
            .add_value("param1", 3)
        timeseries_data.add_timestamp(datetime.now()) \
            .add_value("param1", 3)

        print(timeseries_data)

        # Act
        self.assertEqual(4, len(timeseries_data.timestamps))

    def test_adding_null_value_is_silently_ignored(self):
        # Arrange
        timeseries_data = TimeseriesData()
        timeseries_data.add_timestamp_nanoseconds(1) \
            .add_value("param1", 1) \
            .add_value("param1", None)

        # Act
        self.assertEqual(1, timeseries_data.timestamps[0].parameters["param1"].value)

    def test_any_number_type_is_converted_to_float(self):
        # Arrange
        npy_float64 = np.float64(42.0)
        npy_int64 = np.int64(42)
        native_int = 42
        native_float = 42.0

        timeseries_data = TimeseriesData()
        timeseries_data.add_timestamp_nanoseconds(1) \
            .add_value("npy_float64", npy_float64) \
            .add_value("npy_int64", npy_int64) \
            .add_value("native_int", native_int) \
            .add_value("native_float", native_float)

        # Act
        self.assertEqual(float(42), timeseries_data.timestamps[0].parameters["npy_float64"].value)
        self.assertEqual(float(42), timeseries_data.timestamps[0].parameters["npy_int64"].value)
        self.assertEqual(float(42), timeseries_data.timestamps[0].parameters["native_int"].value)
        self.assertEqual(float(42), timeseries_data.timestamps[0].parameters["native_float"].value)

    def test_convert_from_timeseries_data_to_panda_data_frame(self):
        # Arrange
        timeseries_data = TimeseriesData()
        timeseries_data.add_timestamp_nanoseconds(100)\
                      .add_value("param1", 1) \
                      .add_value("param2", "string two")\
                      .add_tag("tag1", "tag val1")
        timeseries_data.add_timestamp_nanoseconds(100)\
                      .add_value("param1", 2) \
                      .add_value("param2", "string two_b")\
                      .add_tag("tag1", "tag val2")
        timeseries_data.add_timestamp_nanoseconds(200)\
                      .add_value("param1", 3)

        # Act
        pdf = timeseries_data.to_dataframe()
        # Assert
        self.assertEqual(100, pdf["timestamp"][0])
        self.assertEqual(1, pdf["param1"][0])
        self.assertEqual("string two", pdf["param2"][0])
        self.assertEqual("tag val1", pdf["TAG__tag1"][0])

        self.assertEqual(100, pdf["timestamp"][1])
        self.assertEqual(2, pdf["param1"][1])
        self.assertEqual("string two_b", pdf["param2"][1])
        self.assertEqual("tag val2", pdf["TAG__tag1"][1])

        self.assertEqual(200, pdf["timestamp"][2])
        self.assertEqual(3, pdf["param1"][2])
        self.assertTrue(pdf["param2"][2] is None)
        self.assertTrue(pdf["TAG__tag1"][2] is None)

    def test_convert_from_panda_data_frame_with_numeric_header_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = 100
        pdf.at[0, 1] = 1

        pdf.at[1, "timestamp"] = 100
        pdf.at[1, 1] = 2

        pdf.at[2, "timestamp"] = 200
        pdf.at[2, 1] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        timeseries_data_ptr = timeseries_data.get_net_pointer()
        reloaded_from_csharp = TimeseriesData(timeseries_data_ptr)  # this will force us to load data back from the assigned values in c#


        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100)\
                      .add_value("1", 1)
        expected.add_timestamp_nanoseconds(100)\
                      .add_value("1", 2)
        expected.add_timestamp_nanoseconds(200)\
                      .add_value("1", 3)

        self.assert_data_are_equal(expected, reloaded_from_csharp)
        self.assert_data_are_equal(reloaded_from_csharp, expected)

    def test_convert_from_panda_data_frame_with_time_as_string_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = str(datetime(1970, 1, 1, 0, 0, 1, 0, timezone.utc))
        pdf.at[0, "param1"] = 1

        pdf.at[1, "timestamp"] = str(datetime(2025, 12, 30, 15, 16, 24, 123456, timezone.utc))
        pdf.at[1, "param1"] = 2

        tz = timezone(timedelta(seconds=3600))
        pdf.at[2, "timestamp"] = str(datetime(2030, 12, 10, 14, 13, 13, 0, tz))
        pdf.at[2, "param1"] = 3

        pdf.at[3, "timestamp"] = str(2023138793000000000)
        pdf.at[3, "param1"] = 4

        pdf.at[4, "timestamp"] = str("2020-12-11 10:00")
        pdf.at[4, "param1"] = 5

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(1000000000) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(1767107784123456000) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(1923138793000000000) \
            .add_value("param1", 3)
        expected.add_timestamp_nanoseconds(2023138793000000000) \
            .add_value("param1", 4)
        expected_fromstr = TimeConverter.to_unix_nanoseconds(datetime(year=2020, month=12, day=11, hour=10))
        expected.add_timestamp_nanoseconds(expected_fromstr) \
            .add_value("param1", 5)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_time_as_datetime_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = datetime(1970, 1, 1, 0, 0, 1, 0, timezone.utc)
        pdf.at[0, "param1"] = 1

        pdf.at[1, "timestamp"] = datetime(2025, 12, 30, 15, 16, 24, 123456, timezone.utc)
        pdf.at[1, "param1"] = 2

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(1000000000) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(1767107784123456000) \
            .add_value("param1", 2)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_time_as_datetime_to_timeseries_data_using_non_utc_timezone(self):
        # Arrange
        pdf = pandas.DataFrame()
        tz = timezone(timedelta(seconds=14400))
        pdf.at[0, "timestamp"] = datetime(1970, 1, 1, 4, 0, 1, 0, tz)
        pdf.at[0, "param1"] = 1

        tz2 = timezone(timedelta(seconds=-14400))
        pdf.at[1, "timestamp"] = datetime(2025, 12, 30, 11, 16, 24, 123456, tz2)
        pdf.at[1, "param1"] = 2

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(1000000000) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(1767107784123456000) \
            .add_value("param1", 2)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_time_as_datetime_to_timeseries_data_using_none_timezone(self):
        # Arrange
        pdf = pandas.DataFrame()
        time1 = datetime(1970, 1, 1, 0, 0, 1, 0)
        pdf.at[0, "timestamp"] = time1
        pdf.at[0, "param1"] = 1

        time2 = datetime(2025, 12, 30, 15, 16, 24, 123456)
        pdf.at[1, "timestamp"] = time2
        pdf.at[1, "param1"] = 2

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        tz2 = datetime.now().astimezone().tzinfo
        expected1 = TimeConverter.to_unix_nanoseconds(time1.replace(tzinfo=tz2))
        expected2 = TimeConverter.to_unix_nanoseconds(time2.replace(tzinfo=tz2))
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(expected1) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(expected2) \
            .add_value("param1", 2)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_time_timecol_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "timestamp"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "timestamp"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_Time_timecol_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "Time"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "Time"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "Time"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_datetime_timecol_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "datetime"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "datetime"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "datetime"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_DateTime_timecol_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "DateTime"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "DateTime"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "DateTime"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_timestamp_timecol_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "timestamp"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "timestamp"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_with_Timestamp_timecol_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "Timestamp"] = 100
        pdf.at[0, "param1"] = 1

        pdf.at[1, "Timestamp"] = 100
        pdf.at[1, "param1"] = 2

        pdf.at[2, "Timestamp"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 1)
        expected.add_timestamp_nanoseconds(100) \
            .add_value("param1", 2)
        expected.add_timestamp_nanoseconds(200) \
            .add_value("param1", 3)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_to_timeseries_data(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = 100
        pdf.at[0, "param1"] = 1.32
        pdf.at[0, "param2"] = "two"
        pdf.at[0, "TAG__tag1"] = "val1"

        pdf.at[1, "timestamp"] = 100
        pdf.at[1, "param1"] = 2
        pdf.at[1, "param2"] = "two_b"
        pdf.at[1, "param3"] = bytes(bytearray("bytes", "UTF-8"))
        pdf.at[1, "TAG__tag1"] = "val2"

        pdf.at[2, "timestamp"] = 200
        pdf.at[2, "param1"] = 3

        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(100)\
                      .add_value("param1", 1.32) \
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
        print(timeseries_data)
        print("-- Expected --")
        print(expected)

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_convert_from_panda_data_frame_to_timeseries_data_with_epoch(self):
        # Arrange
        pdf = pandas.DataFrame()
        pdf.at[0, "timestamp"] = 100
        pdf.at[0, "param1"] = 1
        pdf.at[0, "param2"] = "two"
        pdf.at[0, "TAG__tag1"] = "val1"
        # Act
        timeseries_data = TimeseriesData.from_panda_dataframe(pdf, 2000)
        # Assert
        expected = TimeseriesData()
        expected.add_timestamp_nanoseconds(2100)\
                      .add_value("param1", 1) \
                      .add_value("param2", "two")\
                      .add_tag("tag1", "val1")

        self.assert_data_are_equal(expected, timeseries_data)
        self.assert_data_are_equal(timeseries_data, expected)

    def test_multiple_time_columns(self):
        def _assert_time(pdf, time):
            timeseries_data_raw = TimeseriesData.from_panda_dataframe(pdf).to_dataframe()
            parsed_time=timeseries_data_raw.loc[0, 'timestamp']
            self.assertEqual(time, parsed_time)

        _assert_time(pandas.DataFrame([{"value": 0.1,"timestamp": 1000000}]), 1000000)
        _assert_time(pandas.DataFrame([{"TiMe": 2000000, "value": 0.1}]), 2000000)
        _assert_time(pandas.DataFrame([{"value": 0.1,"datetime": 3000000}]), 3000000)
        _assert_time(pandas.DataFrame([{"TiMeSTAMP": 5000000, "value": 0.1}]), 5000000)
        _assert_time(pandas.DataFrame([{"value": 0.1, "TiMeSTAMP": 5000000}]), 5000000)

    def assert_data_are_equal(self, data_a: TimeseriesData, data_b: TimeseriesData) -> None:
        self.assertEqual(len(data_a.timestamps), len(data_b.timestamps), "Timestamp count")
        for index_a, ts_a in enumerate(data_a.timestamps):
            ts_b = data_b.timestamps[index_a]
            TimeseriesDataTests.assert_timestamps_are_equal(self, ts_a, ts_b)

    def assert_timestamps_are_equal(self, ts_a: TimeseriesDataTimestamp, ts_b: TimeseriesDataTimestamp) -> None:
        self.assertEqual(ts_a.timestamp_nanoseconds, ts_b.timestamp_nanoseconds, "Timestamp")
        for param_id_a, parameter_value_a in ts_a.parameters.items():
            parameter_value_b = ts_b.parameters.get(param_id_a)
            if parameter_value_b is None and parameter_value_a.value is None:
                # The value was removed from the sent timeseries data at some point, and for performance reasons is just nulled out rather than cleaned up
                continue
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