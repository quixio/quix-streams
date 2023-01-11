import unittest
import threading
import pandas as pd

from quixstreaming.app import App
from tests.cancellationtokensource import CancellationTokenSource
from tests.testbase import TestBase
from quixstreaming.streamreader import StreamReader, StreamEndType, EventData


class StreamEventDataTests(unittest.TestCase):

    def test_events_write_via_builder_and_read(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        read_data: EventData = None
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_event_data_handler(data: EventData):
                    nonlocal read_data
                    read_data = data
                    event.set()
                reader.events.on_read += on_event_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.events.add_timestamp_nanoseconds(100)\
            .add_value("event1", "value1")\
            .add_tag("tag1", "tag1val")\
            .write()

        expected = EventData("event1", 100, "value1").add_tag("tag1", "tag1val")

        # Assert
        event.wait(timeout=30)
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        input_topic.dispose()  # cleanup
        self.assert_data_are_equal(expected, read_data)
        self.assert_data_are_equal(read_data, expected)

    def test_events_write_using_app_run(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        read_data: EventData = None
        event = threading.Event()  # used for assertion
        cts = CancellationTokenSource()

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_event_data_handler(data: EventData):
                    nonlocal read_data
                    read_data = data
                    event.set()
                reader.events.on_read += on_event_data_handler

        input_topic.on_stream_received += on_new_stream

        # Act
        stream = output_topic.create_stream()
        stream.events.add_timestamp_nanoseconds(100)\
            .add_value("event1", "value1")\
            .add_tag("tag1", "tag1val")\
            .write()

        expected = EventData("event1", 100, "value1").add_tag("tag1", "tag1val")
        def eventCallback():
            print("A")
            event.wait(timeout=100)
            print("B")
            cts.cancel()
            print("C")

        event_thread = threading.Thread(target=eventCallback)
        event_thread.start()

        # Assert
        App.run(cts.token)
        event_thread.join()
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        input_topic.dispose()  # cleanup
        self.assert_data_are_equal(expected, read_data)
        self.assert_data_are_equal(read_data, expected)

    def test_events_write_direct_and_read(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        read_data: EventData = None
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_event_data_handler(data: EventData):
                    nonlocal read_data
                    read_data = data
                    event.set()
                reader.events.on_read += on_event_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        expected = EventData("event1", 100, "value1").add_tag("tag1", "tag1val")
        stream.events.write(expected)

        # Assert
        event.wait(timeout=10)
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        input_topic.dispose()  # cleanup
        self.assert_data_are_equal(expected, read_data)
        self.assert_data_are_equal(read_data, expected)

    def test_event_data_frame_write_direct_and_read(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())
        data = [("event1", 100, "value1")]
        df = pd.DataFrame(data, columns=['id', 'time', 'val'])
        expected = EventData("event1", 100, "value1")

        stream = None  # The outgoing stream
        read_data: EventData = None
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_event_data_handler(data: EventData):
                    nonlocal read_data
                    read_data = data
                    event.set()
                reader.events.on_read += on_event_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.events.write(data=df, timestamp='time', value='val')

        # Assert
        event.wait(timeout=10)
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        input_topic.dispose()  # cleanup
        self.assert_data_are_equal(expected, read_data)
        self.assert_data_are_equal(read_data, expected)

    def assert_data_are_equal(self, data_a: EventData, data_b: EventData):
        self.assertEqual(data_a.id, data_b.id, "Id")
        self.assertEqual(data_a.timestamp_nanoseconds, data_b.timestamp_nanoseconds, "Nanoseconds")
        self.assertEqual(data_a.value, data_b.value, "value")
        for tag_id_a, tag_value_a in data_a.tags.items():
            tag_value_b = data_b.tags[tag_id_a]
            self.assertEqual(tag_value_a, tag_value_b, "tag")

if __name__ == '__main__':
    unittest.main()