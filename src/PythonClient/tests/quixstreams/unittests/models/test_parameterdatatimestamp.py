import time
import unittest

from src.quixstreams import ParameterData


class ParameterDataTimestampTests(unittest.TestCase):

    def test_add_double(self):
        # Arrange
        pd = ParameterData()

        # Act
        pd.add_timestamp_nanoseconds(100) \
            .add_value("double", 1.232)

        # Assert
        self.assertEqual(1.232, pd.timestamps[0].parameters["double"].numeric_value)

    def test_add_string(self):
        # Arrange
        pd = ParameterData()

        # Act
        pd.add_timestamp_nanoseconds(100) \
            .add_value("str", "value")

        # Assert
        asdf = pd.timestamps[0]
        param = asdf.parameters
        self.assertEqual(pd.timestamps[0].parameters["str"].string_value, "value")

    def test_add_bytes(self):
        # Arrange
        pd = ParameterData()
        expected = bytes("some bytes", "utf-8")

        # Act
        pd.add_timestamp_nanoseconds(100) \
            .add_value("bytes", expected)

        # Assert
        self.assertEqual(expected, pd.timestamps[0].parameters["bytes"].binary_value)

    def test_add_tag(self):
        # Arrange
        pd = ParameterData()
        pd.add_timestamp_nanoseconds(100)
        pdts = pd.timestamps[0]
        pdts.add_tag("a", "b")

        # Act
        pd.add_timestamp_nanoseconds(200)
        pdts2 = pd.timestamps[1]
        pdts.add_tag("b", "c")
        pdts2.add_tags(pdts.tags)

        # Assert
        self.assertEqual(pdts2.tags["a"], "b")
        self.assertEqual(pdts2.tags["b"], "c")

    def test_add_tags(self):
        # Act
        parameter_data = ParameterData()
        ts = parameter_data.add_timestamp_nanoseconds(100)
        ts.add_value("param1", 1) \
          .add_tags({"tag1": "val1", "tag2": "val2"})
        # Assert
        self.assertEqual(len(ts.tags), 2)
        self.assertEqual(ts.tags["tag1"], "val1")
        self.assertEqual(ts.tags["tag2"], "val2")
