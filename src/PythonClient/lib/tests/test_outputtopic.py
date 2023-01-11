import unittest

from quixstreaming import StreamWriter
from tests.testbase import TestBase
from quixstreaming.models.parametervalue import ParameterValueType, ParameterValue


class OutputTopicTests(unittest.TestCase):

    def test_created_stream_can_be_retrieved(self):
        # Arrange & Act
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        sw = output_topic.create_stream()

        # Act
        retrieved = output_topic.get_stream(sw.stream_id)

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertEqual(sw.stream_id, retrieved.stream_id)

    def test_disposed_topic_invokes_on_disposed(self):
        # Arrange & Act
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        callback_invoked = False
        def callback(topic):
            nonlocal callback_invoked
            callback_invoked = True
        output_topic.on_disposed += callback

        # Act
        output_topic.dispose()

        # Assert
        self.assertEqual(callback_invoked, True)

    def test_closed_stream_can_not_be_retrieved(self):
        # Arrange & Act
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        sw = output_topic.create_stream()
        sw.close()

        # Act
        retrieved = output_topic.get_stream(sw.stream_id)

        # Assert
        self.assertIsNone(retrieved)


    def test_get_or_create_stream_no_prev_stream_with_callback(self):
        # Arrange & Act
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)

        # Act
        callbacked : StreamWriter = None
        def on_create_callback(sw):
            nonlocal callbacked
            callbacked = sw

        retrieved = output_topic.get_or_create_stream("test_stream_id", on_create_callback)

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertIsNotNone(callbacked)
        self.assertEqual(callbacked.stream_id, retrieved.stream_id)
        self.assertEqual(retrieved.stream_id, "test_stream_id")

    def test_get_or_create_stream_no_prev_stream_without_callback(self):
        # Arrange & Act
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)

        # Act
        retrieved = output_topic.get_or_create_stream("test_stream_id")

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.stream_id, "test_stream_id")

    def test_get_or_create_stream_with_prev_stream_with_callback(self):
        # Arrange & Act
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        output_topic.create_stream("test_stream_id")

        # Act
        callbacked : StreamWriter = None
        def on_create_callback(sw):
            nonlocal callbacked
            callbacked = sw

        retrieved = output_topic.get_or_create_stream("test_stream_id", on_create_callback)

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertIsNone(callbacked)
        self.assertEqual(retrieved.stream_id, "test_stream_id")

    def test_get_or_create_stream_with_prev_stream_without_callback(self):
        # Arrange & Act
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        output_topic.create_stream("test_stream_id")

        # Act
        retrieved = output_topic.get_or_create_stream("test_stream_id")

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.stream_id, "test_stream_id")