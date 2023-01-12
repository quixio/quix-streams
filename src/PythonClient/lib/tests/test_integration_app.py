import unittest
import threading
import pandas as pd

from quixstreaming.app import App
from tests.cancellationtokensource import CancellationTokenSource
from tests.testbase import TestBase
from quixstreaming.streamreader import StreamReader, EventData


class StreamEventDataTests(unittest.TestCase):

    def test_run_via_app(self):
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

        def eventCallback():
            event.wait(timeout=100)
            cts.cancel()

        event_thread = threading.Thread(target=eventCallback)
        event_thread.start()

        shutdown_callback_value = False
        def before_shutdown():
            nonlocal shutdown_callback_value
            shutdown_callback_value = True

        # Assert
        App.run(cts.token, before_shutdown=before_shutdown)
        event_thread.join()
        input_topic.dispose()  # cleanup
        self.assertIsNotNone(read_data)
        self.assertEqual(shutdown_callback_value, True)

if __name__ == '__main__':
    unittest.main()