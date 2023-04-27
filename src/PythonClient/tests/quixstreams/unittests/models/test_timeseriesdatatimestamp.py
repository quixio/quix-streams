import time
import unittest

from src.quixstreams import TimeseriesData


class TimeseriesDataTimestampTests(unittest.TestCase):

    def test_add_double(self):
        # Arrange
        td = TimeseriesData()

        # Act
        td.add_timestamp_nanoseconds(100) \
            .add_value("double", 1.232)

        # Assert
        self.assertEqual(1.232, td.timestamps[0].parameters["double"].numeric_value)

    def test_add_string(self):
        # Arrange
        td = TimeseriesData()

        # Act
        td.add_timestamp_nanoseconds(100) \
            .add_value("str", "value")

        # Assert
        asdf = td.timestamps[0]
        param = asdf.parameters
        self.assertEqual(td.timestamps[0].parameters["str"].string_value, "value")

    def test_add_bytes(self):
        # Arrange
        td = TimeseriesData()
        expected = bytes("some bytes", "utf-8")

        # Act
        td.add_timestamp_nanoseconds(100) \
            .add_value("bytes", expected)

        # Assert
        self.assertEqual(expected, td.timestamps[0].parameters["bytes"].binary_value)

    def test_add_bytearray(self):
        # Arrange
        td = TimeseriesData()
        expected = bytearray("some bytes", "utf-8")

        # Act
        td.add_timestamp_nanoseconds(100) \
            .add_value("bytearray", expected)

        # Assert
        self.assertEqual(expected, td.timestamps[0].parameters["bytearray"].binary_value)

    def test_add_tag(self):
        # Arrange
        td = TimeseriesData()
        td.add_timestamp_nanoseconds(100)
        tdts = td.timestamps[0]
        tdts.add_tag("a", "b")

        # Act
        td.add_timestamp_nanoseconds(200)
        tdts2 = td.timestamps[1]
        tdts.add_tag("b", "c")
        tdts2.add_tags(tdts.tags)

        # Assert
        self.assertEqual(tdts2.tags["a"], "b")
        self.assertEqual(tdts2.tags["b"], "c")

    def test_add_tags(self):
        # Act
        timeseries_data = TimeseriesData()
        ts = timeseries_data.add_timestamp_nanoseconds(100)
        ts.add_value("param1", 1) \
          .add_tags({"tag1": "val1", "tag2": "val2"})
        # Assert
        self.assertEqual(len(ts.tags), 2)
        self.assertEqual(ts.tags["tag1"], "val1")
        self.assertEqual(ts.tags["tag2"], "val2")
