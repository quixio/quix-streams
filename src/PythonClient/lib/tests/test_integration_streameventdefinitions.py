import unittest
import threading

from quixstreaming import EventLevel

from tests.testbase import TestBase
from quixstreaming.streamreader import StreamReader, StreamEndType, EventData


class StreamEventDefinitionsTests(unittest.TestCase):

    def test_event_definitions_write_and_read(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        read_stream: StreamReader = None  # the incoming stream
        event = threading.Event()  # used for assertion

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                nonlocal read_stream
                read_stream = reader

                def on_definitions_changed_handler():
                    event.set()
                read_stream.events.on_definitions_changed += on_definitions_changed_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.events\
            .add_definition("event1", "event one", "event one description")\
            .set_level(EventLevel.Critical)\
            .set_custom_properties("custom property")\
            .add_definition("event2") \
            .set_level(EventLevel.Debug)
        stream.events\
            .add_location("this/is/not/the/root/")\
            .add_definition("event3")\
            .add_definition("event4")

        stream.events.flush()  # just so we don't need to rely on waiting

        # Assert
        event.wait(timeout=10)
        definitions = read_stream.events.definitions
        print("------ READ ------")
        for definition in definitions:
            print(definition)
        input_topic.dispose()  # cleanup
        self.assertEqual("event1", definitions[0].id)
        self.assertEqual("", definitions[0].location)
        self.assertEqual("event one", definitions[0].name)
        self.assertEqual("event one description", definitions[0].description)
        self.assertEqual(EventLevel.Critical, definitions[0].level)
        self.assertEqual("custom property", definitions[0].custom_properties)
        self.assertEqual("event2", definitions[1].id)
        self.assertEqual("", definitions[1].location)
        self.assertEqual(None, definitions[1].name)
        self.assertEqual(None, definitions[1].description)
        self.assertEqual(EventLevel.Debug, definitions[1].level)
        self.assertEqual(None, definitions[1].custom_properties)
        self.assertEqual("event3", definitions[2].id)
        self.assertEqual("/this/is/not/the/root", definitions[2].location)
        self.assertEqual(None, definitions[2].name)
        self.assertEqual(None, definitions[2].description)
        self.assertEqual(EventLevel.Information, definitions[2].level)
        self.assertEqual(None, definitions[2].custom_properties)
        self.assertEqual("event4", definitions[3].id)
        self.assertEqual("/this/is/not/the/root", definitions[3].location)
        self.assertEqual(None, definitions[3].name)
        self.assertEqual(None, definitions[3].description)
        self.assertEqual(EventLevel.Information, definitions[3].level)
        self.assertEqual(None, definitions[3].custom_properties)


if __name__ == '__main__':
    unittest.main()