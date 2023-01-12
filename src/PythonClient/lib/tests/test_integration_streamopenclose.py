import unittest
import threading

from tests.testbase import TestBase
from quixstreaming.streamreader import StreamReader, StreamEndType, AutoOffsetReset, CommitMode


class StreamOpenCloseTests(unittest.TestCase):

    def test_stream_open(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None # The outgoing stream
        stream_read = False
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                nonlocal stream_read
                stream_read = True
                event.set()

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.close()

        # Assert
        event.wait(timeout=10)
        input_topic.dispose() # cleanup
        self.assertTrue(stream_read, "Stream was expected to be read")

    def test_stream_open_with_offset(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group(), auto_offset_reset=AutoOffsetReset.Latest)

        stream = None # The outgoing stream
        stream_read = False
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                nonlocal stream_read
                stream_read = True
                event.set()

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.close()

        # Assert
        event.wait(timeout=10)
        input_topic.dispose() # cleanup
        self.assertTrue(stream_read, "Stream was expected to be read")

    def test_stream_open_with_offset_manualCommit(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group(), commit_settings=CommitMode.Manual, auto_offset_reset=AutoOffsetReset.Latest)

        stream = None # The outgoing stream
        stream_read = False
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                nonlocal stream_read
                stream_read = True
                event.set()

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.close()

        # Assert
        event.wait(timeout=10)
        input_topic.dispose() # cleanup
        self.assertTrue(stream_read, "Stream was expected to be read")

    def test_stream_close(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None # The outgoing stream
        stream_end_type = None
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_stream_close(close_type):
                    nonlocal stream_end_type
                    stream_end_type = close_type
                    event.set()
                reader.on_stream_closed += on_stream_close

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.close(StreamEndType.Aborted)

        # Assert
        event.wait(timeout=10)
        input_topic.dispose()  # cleanup
        self.assertEqual(StreamEndType.Aborted, stream_end_type, "Stream was expected to be closed with aborted")


if __name__ == '__main__':
    unittest.main()