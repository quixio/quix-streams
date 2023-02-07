import unittest
from datetime import datetime

from src.quixstreams import EventData

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
#InteropUtils.enable_debug()


class EventDataTests(unittest.TestCase):

    # TODO test with null lists

    def test_add_tags(self):
        # Act
        event_data = EventData("abcde", 123) \
          .add_tags({"tag1": "val1", "tag2": "val2"})
        # Assert
        self.assertEqual(len(event_data.tags), 2)
        self.assertEqual(event_data.tags["tag1"], "val1")
        self.assertEqual(event_data.tags["tag2"], "val2")

    def test_constructor_with_time_as_nanoseconds_int(self):
        # Act
        event_data = EventData("abcde", 123)
        # Assert
        self.assertEqual(event_data.timestamp_nanoseconds, 123)

    def test_constructor_with_time_as_nanoseconds_string(self):
        # Act
        event_data = EventData("abcde", "123")
        # Assert
        self.assertEqual(event_data.timestamp_nanoseconds, 123)

    def test_constructor_with_time_as_datetime_datetime(self):
        # Act
        event_data = EventData("abcde", datetime(2010, 1, 1))
        # Assert
        self.assertEqual(event_data.timestamp_nanoseconds, 1262304000000000000)

    def test_constructor_with_time_as_datetime_string(self):
        # Act
        event_data = EventData("abcde", str(datetime(2010, 1, 1)))
        # Assert
        self.assertEqual(event_data.timestamp_nanoseconds, 1262304000000000000)