import unittest
import threading

from tests.testbase import TestBase
from quixstreaming.streamreader import StreamReader, StreamEndType, ParameterData


class StreamParameterDefinitionsTests(unittest.TestCase):

    def test_parameter_definitions_write_and_read(self):
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
                read_stream.parameters.on_definitions_changed += on_definitions_changed_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.parameters\
            .add_definition("parameter1", "parameter one", "parameter one description")\
            .set_range(0, 10)\
            .set_format("{0:0.0}")\
            .set_unit("kph")\
            .set_custom_properties("custom property")\
            .add_definition("parameter2")
        stream.parameters\
            .add_location("this/is/not/the/root/")\
            .add_definition("parameter3")\
            .add_definition("parameter4")

        stream.parameters.flush()  # just so we don't need to rely on waiting

        # Assert
        event.wait(timeout=10)
        definitions = read_stream.parameters.definitions
        print("------ READ ------")
        for definition in definitions:
            print(definition)
        input_topic.dispose()  # cleanup
        self.assertEqual("parameter1", definitions[0].id)
        self.assertEqual("", definitions[0].location)
        self.assertEqual("parameter one", definitions[0].name)
        self.assertEqual("parameter one description", definitions[0].description)
        self.assertEqual(0, definitions[0].minimum_value)
        self.assertEqual(10, definitions[0].maximum_value)
        self.assertEqual("kph", definitions[0].unit)
        self.assertEqual("{0:0.0}", definitions[0].format)
        self.assertEqual("custom property", definitions[0].custom_properties)
        self.assertEqual("parameter2", definitions[1].id)
        self.assertEqual("", definitions[1].location)
        self.assertEqual(None, definitions[1].name)
        self.assertEqual(None, definitions[1].description)
        self.assertEqual(None, definitions[1].minimum_value)
        self.assertEqual(None, definitions[1].maximum_value)
        self.assertEqual(None, definitions[1].unit)
        self.assertEqual(None, definitions[1].format)
        self.assertEqual(None, definitions[1].custom_properties)
        self.assertEqual("parameter3", definitions[2].id)
        self.assertEqual("/this/is/not/the/root", definitions[2].location)
        self.assertEqual(None, definitions[2].name)
        self.assertEqual(None, definitions[2].description)
        self.assertEqual(None, definitions[2].minimum_value)
        self.assertEqual(None, definitions[2].maximum_value)
        self.assertEqual(None, definitions[2].unit)
        self.assertEqual(None, definitions[2].format)
        self.assertEqual(None, definitions[2].custom_properties)
        self.assertEqual("parameter4", definitions[3].id)
        self.assertEqual("/this/is/not/the/root", definitions[3].location)
        self.assertEqual(None, definitions[3].name)
        self.assertEqual(None, definitions[3].description)
        self.assertEqual(None, definitions[3].minimum_value)
        self.assertEqual(None, definitions[3].maximum_value)
        self.assertEqual(None, definitions[3].unit)
        self.assertEqual(None, definitions[3].format)
        self.assertEqual(None, definitions[3].custom_properties)


if __name__ == '__main__':
    unittest.main()