import unittest

from quixstreaming import ParameterData
from quixstreaming.models.parametervalue import ParameterValueType, ParameterValue


class ParameterDataTimestampTests(unittest.TestCase):

    def test_add_tags(self):
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
