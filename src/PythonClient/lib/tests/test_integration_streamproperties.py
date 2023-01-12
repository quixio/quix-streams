import unittest
import threading
from datetime import datetime, timezone

from tests.testbase import TestBase
from quixstreaming.streamreader import StreamReader


class StreamPropertiesTests(unittest.TestCase):

    def test_writer_configuration(self):
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)

        # Act
        stream = output_topic.create_stream()

        #change the variable and read it back
        stream.properties.flush_interval = 7000
        self.assertEqual(stream.properties.flush_interval, 7000)


    def test_properties_write_and_read(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_stream: StreamReader = None  # the incoming stream

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                nonlocal read_stream
                read_stream = reader

                def on_properties_changed():
                    event.set()

                reader.properties.on_changed += on_properties_changed

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.properties.name = "TestName"
        stream.properties.location = "/test/location/here"
        stream.properties.parents.append("IDDQD")
        stream.properties.parents.append("IDKFA")
        stream.properties.metadata["meta1"] = "value1"
        stream.properties.metadata["meta2"] = "value2"
        sent_time = datetime(2018, 1, 1, tzinfo=timezone.utc)
        stream.properties.time_of_recording = sent_time
        stream.properties.flush()

        # Assert
        event.wait(timeout=10)
        input_topic.dispose()  # cleanup
        self.assertEqual("TestName", read_stream.properties.name)
        self.assertEqual("/test/location/here", read_stream.properties.location)
        self.assertEqual("IDDQD", read_stream.properties.parents[0])
        self.assertEqual("value1", read_stream.properties.metadata["meta1"])
        self.assertEqual("value2", read_stream.properties.metadata["meta2"])
        self.assertEqual(sent_time, read_stream.properties.time_of_recording)
        output_topic.dispose()


if __name__ == '__main__':
    unittest.main()
