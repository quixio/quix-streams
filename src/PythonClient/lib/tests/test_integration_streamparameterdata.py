import unittest
import threading
from datetime import datetime, timedelta

import pandas as pd

from quixstreaming.helpers import NetToPythonConverter
from quixstreaming.models.parameterdatatimestamp import ParameterDataTimestamp
from quixstreaming.models.parametersbufferconfiguration import ParametersBufferConfiguration
from quixstreaming.models.parametervalue import ParameterValueType
from tests.testbase import TestBase
from quixstreaming.streamreader import StreamReader, ParameterData, ParameterDataRaw


# noinspection DuplicatedCode
class StreamParametersTests(unittest.TestCase):

    def test_parameters_write_binary_read_binary_is_of_bytes(self):
        print("test_parameters_write_binary_read_binary_is_of_bytes")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterData = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data
                    event.set()

                param_buffer = reader.parameters.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_read += on_parameter_data_handler


        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.parameters.buffer.packet_size = 10  # this is to enforce buffering until we want
        # Send parameter Data for datetime
        utc_now = datetime.utcnow()  # for assertion purposes save it
        stream.parameters.buffer \
            .add_timestamp(utc_now) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .write()

        stream.parameters.buffer.flush()

        # Assert
        event.wait(timeout=10)
        print(read_data)
        input_topic.dispose()  # cleanup
        binary_value = read_data.timestamps[0].parameters["binary_param"].binary_value
        self.assertEqual(type(binary_value), bytes)
        self.assertEqual(binary_value, bytes(bytearray("binary_param", "UTF-8")))

    def test_parameters_write_via_buffer_and_read(self):
        print("test_parameters_write_via_buffer_and_read")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterData = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data
                    event.set()

                param_buffer = reader.parameters.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_read += on_parameter_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.parameters.buffer.packet_size = 10  # this is to enforce buffering until we want
        # Send parameter Data for datetime
        utc_now = datetime.utcnow()  # for assertion purposes save it
        stream.parameters.buffer \
            .add_timestamp(utc_now) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 123.43) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8"))) \
            .add_tag("Tag2", "tag two updated") \
            .add_tag("Tag3", "tag three") \
            .write()

        # Send parameter data in nanoseconds relative to epoch
        stream.parameters.buffer \
            .add_timestamp_nanoseconds(123456789) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param3", "UTF-8")) \
            .write()

        # Send parameter data in timedelta relative to a new epoch
        stream.parameters.buffer.epoch = datetime(2018, 1, 2)
        stream.parameters.buffer \
            .add_timestamp(timedelta(seconds=1, milliseconds=555)) \
            .add_value("num_param", 123.32) \
            .add_value("binary_param", bytearray("binary_param4", "UTF-8")) \
            .write()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(123456790) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param4", "UTF-8"))

        stream.parameters.buffer.write(written_data)

        stream.parameters.buffer.flush()

        # Assert
        event.wait(timeout=10)
        print(read_data)
        input_topic.dispose()  # cleanup
        self.assertEqual(4, len(read_data.timestamps))

    def test_parameters_write_direct_and_read_as_parameterdataraw(self):
        print("test_parameters_write_direct_and_read_as_parameterdataraw")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterDataRaw = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterDataRaw):
                    nonlocal read_data
                    read_data = data
                    event.set()

                reader.parameters.on_read_raw += on_parameter_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("bufferless_1", 1) \
            .add_value("bufferless_2", "test") \
            .add_value("bufferless_3", "test") \
            .add_value("bufferless_4", bytearray("bufferless_4", "UTF-8")) \
            .add_value("bufferless_5", bytes(bytearray("bufferless_5", "UTF-8"))) \
            .remove_value("bufferless_3") \
            .add_tag("tag1", "tag1val") \
            .add_tag("tag2", "tag2val") \
            .remove_tag("tag2")

        stream.parameters.write(written_data)

        # Assert
        event.wait(timeout=30)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        received = str(read_data)
        print(received)


        print("========================")
        input_topic.dispose()  # cleanup
        expected = "ParameterDataRaw:\r\n" + \
                   "  epoch: 0\r\n" + \
                   "            timestamps: [10]\r\n" + \
                   "        numeric_values: {'bufferless_1': [1.0]}\r\n" + \
                   "         string_values: {'bufferless_2': ['test'], 'bufferless_3': [None]}\r\n" + \
                   "         binary_values: {'bufferless_4': [b'bufferless_4'], 'bufferless_5': [b'bufferless_5']}\r\n" + \
                   "            tag_values: {'tag1': ['tag1val']}"
        self.assertEqual(expected, received)

        converted = read_data.convert_to_parameterdata()
        self.assert_data_are_equal(written_data, converted)  # evaluate neither contains more or less than should
        self.assert_data_are_equal(converted, written_data)  # and is done by checking both ways
        self.assertEqual(len(converted.timestamps), 1)
        self.assertEqual(len(converted.timestamps[0].parameters), 5, "Missing parameter")

    def test_parameters_write_direct_and_read(self):
        print("test_parameters_write_direct_and_read")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterData = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data
                    event.set()

                param_buffer = reader.parameters.create_buffer()
                param_buffer.on_read += on_parameter_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("bufferless_1", 1) \
            .add_value("bufferless_2", "test") \
            .add_value("bufferless_3", "test") \
            .add_value("bufferless_4", bytearray("bufferless_4", "UTF-8")) \
            .add_value("bufferless_5", bytes(bytearray("bufferless_5", "UTF-8"))) \
            .remove_value("bufferless_3") \
            .add_tag("tag1", "tag1val") \
            .add_tag("tag2", "tag2val") \
            .remove_tag("tag2")

        stream.parameters.write(written_data)

        # Assert
        event.wait(timeout=10)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        input_topic.dispose()  # cleanup
        self.assert_data_are_equal(written_data, read_data)  # evaluate neither contains more or less than should
        self.assert_data_are_equal(read_data, written_data)  # and is done by checking both ways
        self.assertEqual(len(read_data.timestamps), 1)
        self.assertEqual(len(read_data.timestamps[0].parameters), 4, "Missing parameter")


    def test_parameters_write_direct_and_read_all_options(self):
        print("test_parameters_write_direct_and_read_all_options")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        event_raw = threading.Event()  # used for assertion
        event_pandas = threading.Event()  # used for assertion
        read_data: pd.DataFrame = None
        read_data_raw: pd.DataFrame = None
        read_data_pandas: pd.DataFrame = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data.to_panda_frame()
                    event.set()
                def on_parameter_data_raw_handler(data: ParameterDataRaw):
                    nonlocal read_data_raw
                    read_data_raw = data.to_panda_frame()
                    event_raw.set()
                def on_parameter_data_pandas_handler(data: pd.DataFrame):
                    nonlocal read_data_pandas
                    read_data_pandas = data
                    event_pandas.set()


                param_buffer = reader.parameters.create_buffer()
                param_buffer.on_read += on_parameter_data_handler
                param_buffer.on_read_raw += on_parameter_data_raw_handler
                param_buffer.on_read_pandas += on_parameter_data_pandas_handler

        

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("bufferless_1", 1) \
            .add_value("bufferless_2", "test") \
            .add_value("bufferless_3", "test") \
            .add_value("bufferless_4", bytearray("bufferless_4", "UTF-8")) \
            .add_value("bufferless_5", bytes(bytearray("bufferless_5", "UTF-8"))) \
            .remove_value("bufferless_3") \
            .add_tag("tag1", "tag1val") \
            .add_tag("tag2", "tag2val") \
            .remove_tag("tag2")

        stream.parameters.write(written_data)

        # Assert
        event.wait(timeout=10)
        event_raw.wait(timeout=10)
        event_pandas.wait(timeout=10)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("------ READ RAW ------")
        print(read_data_raw)
        print("------ READ PANDAS ------")
        print(read_data_pandas)
        input_topic.dispose()  # cleanup
        def assertFrameEqual(df1, df2, **kwds ):
            """ Assert that two dataframes are equal, ignoring ordering of columns"""
            from pandas.util.testing import assert_frame_equal
            assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwds )

        assertFrameEqual(read_data, read_data_raw)
        assertFrameEqual(read_data_raw, read_data_pandas)


    def test_parameters_write_panda_via_buffer_and_read(self):
        print("test_parameters_write_panda_via_buffer_and_read")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterData = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data
                    event.set()

                param_buffer = reader.parameters.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_read += on_parameter_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(123456790) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8")))
        pf = written_data.to_panda_frame()

        stream.parameters.buffer.add_timestamp(datetime.utcnow()).add_value("a", "b").write()

        stream.parameters.buffer.flush()

        # Assert
        event.wait(timeout=10)
        print(read_data)
        input_topic.dispose()  # cleanup
        self.assertEqual(1, len(read_data.timestamps))

    def test_parameters_write_compare_pandas_different_exports(self):
        print("test_parameters_write_compare_pandas_different_exports")

        def assertFrameEqual(df1, df2, **kwds ):
            """ Assert that two dataframes are equal, ignoring ordering of columns"""
            from pandas.util.testing import assert_frame_equal
            assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwds )

        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        event2 = threading.Event()  # used for assertion
        event3 = threading.Event()  # used for assertion
        read_data: ParameterData = None
        read_data_raw: ParameterDataRaw = None
        read_data_pandas: pd.DataFrame = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data.to_panda_frame()
                    event.set()
                def on_parameter_raw_handler(data):
                    nonlocal read_data_raw
                    read_data_raw = data.to_panda_frame()
                    event2.set()
                def on_parameter_pandas_handler(data):
                    nonlocal read_data_pandas
                    read_data_pandas = data
                    event3.set()

                param_buffer = reader.parameters.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_read += on_parameter_data_handler

                params = reader.parameters
                params.on_read_raw += on_parameter_raw_handler
                params.on_read_pandas += on_parameter_pandas_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()
        stream.parameters.buffer.packet_size = 10  #to enforce disabling of output buffer
        written_data = ParameterData()

        written_data.add_timestamp_nanoseconds(10) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_tag("tag1", "tag2val") \
            .add_tag("tag2", "tagval") \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8")))

        written_data.add_timestamp_nanoseconds(20) \
            .add_value("string_param", "value2") \
            .add_value("num_param", 81.756123) \
            .add_tag("tag1", "tag1val_2") \
            .add_tag("tag2", "tagval_2") \
            .add_value("binary_param", bytearray("binary_paramer", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2_2", "UTF-8")))

        pf = written_data.to_panda_frame()

        stream.parameters.buffer.write(pf)

        stream.parameters.buffer.flush()

        # Assert
        event.wait(timeout=10)
        event2.wait(timeout=10)
        event3.wait(timeout=10)
        print("==== read_data ====")
        print(read_data)
        print("==== read_data raw ====")
        print(read_data_raw)
        print("==== read_data pandas ====")
        print(read_data_pandas)
        input_topic.dispose()  # cleanup

        assertFrameEqual(pf, read_data)
        assertFrameEqual(read_data, read_data_raw)
        assertFrameEqual(read_data_raw, read_data_pandas)


    def test_parameters_read_with_custom_trigger(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invokation_count = 0

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                param_buffer = reader.parameters.create_buffer()

                def custom_trigger_callback(parameter_data: ParameterData) -> bool:
                    nonlocal special_func_invokation_count
                    special_func_invokation_count += 1
                    print("==== Custom Trigger ====")
                    print(str(parameter_data))
                    if special_func_invokation_count == 3:
                        event.set()
                    return True

                param_buffer.custom_trigger = custom_trigger_callback

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.parameters.write(written_data)

        # Assert
        event.wait(timeout=10)
        input_topic.dispose()  # cleanup
        self.assertEqual(3, special_func_invokation_count)

    def test_parameters_read_with_custom_trigger_from_buffer_config(self):
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invokation_count = 0

        buffer_config = ParametersBufferConfiguration()

        def custom_trigger_callback(parameter_data: ParameterData) -> bool:
            nonlocal special_func_invokation_count
            special_func_invokation_count += 1
            print("==== Custom Trigger ====")
            print(str(parameter_data))
            if special_func_invokation_count == 3:
                event.set()
            return True

        buffer_config.custom_trigger = custom_trigger_callback

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                param_buffer = reader.parameters.create_buffer(buffer_config)

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.parameters.write(written_data)

        # Assert
        event.wait(timeout=10)
        input_topic.dispose()  # cleanup
        self.assertEqual(3, special_func_invokation_count)


    def test_parameters_write_panda_direct_and_read(self):
        print("test_parameters_write_panda_direct_and_read")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterData = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data
                    event.set()

                param_buffer = reader.parameters.create_buffer()
                param_buffer.on_read += on_parameter_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("bufferless_1", 1) \
            .add_value("bufferless_2", "test") \
            .add_value("bufferless_3", "test") \
            .add_value("bufferless_4", bytearray("bufferless_4", "UTF-8")) \
            .add_value("bufferless_5", bytes(bytearray("bufferless_5", "UTF-8"))) \
            .remove_value("bufferless_3") \
            .add_tag("tag1", "tag1val") \
            .add_tag("tag2", "tag2val") \
            .remove_tag("tag2")
        pf = written_data.to_panda_frame()

        stream.parameters.write(pf)

        # Assert
        event.wait(timeout=10)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        input_topic.dispose()  # cleanup
        self.assert_data_are_equal(written_data, read_data)  # evaluate neither contains more or less than should
        self.assert_data_are_equal(read_data, written_data)  # and is done by checking both ways

    def test_parameters_read_with_parameter_filter(self):
        print("test_parameters_read_with_parameter_filter")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterData = None

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    nonlocal read_data
                    read_data = data
                    event.set()

                param_buffer = reader.parameters.create_buffer("param1", "param3")
                param_buffer.buffer_timeout = 500  # to prevent raising each timestamp on its own
                param_buffer.on_read += on_parameter_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.parameters.write(written_data)

        # Assert
        expected_data = ParameterData()
        expected_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_tag("tag1", "tag1val")
        expected_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        event.wait(timeout=10)
        input_topic.dispose()  # cleanup
        event.wait(timeout=30)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("------ Expected ------")
        print(expected_data)
        self.assert_data_are_equal(expected_data, read_data)  # evaluate neither contains more or less than should
        self.assert_data_are_equal(read_data, expected_data)  # and is done by checking both ways

    def test_parameters_read_with_buffer_configuration(self):
        print("test_parameters_read_with_buffer_configuration");
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: ParameterData = None

        buffer_config = ParametersBufferConfiguration()
        buffer_config.packet_size = 2

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                def on_parameter_data_handler(data: ParameterData):
                    param_buffer.on_read -= on_parameter_data_handler
                    nonlocal read_data
                    read_data = data
                    event.set()

                param_buffer = reader.parameters.create_buffer(buffer_config)
                param_buffer.buffer_timeout = 1000  # to prevent raising each timestamp on its own
                param_buffer.on_read += on_parameter_data_handler

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.parameters.write(written_data)

        # Assert
        expected_data = ParameterData()
        expected_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")
        expected_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        event.wait(timeout=10)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("------ Expected ------")
        print(expected_data)
        input_topic.dispose()  # cleanup
        self.assert_data_are_equal(expected_data, read_data)  # evaluate neither contains more or less than should
        self.assert_data_are_equal(read_data, expected_data)  # and is done by checking both ways

    def test_parameters_read_with_filter(self):
        print("test_parameters_read_with_filter")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invokation_count = 0

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                param_buffer = reader.parameters.create_buffer()

                def filter(parameter_data_timestamp: ParameterDataTimestamp) -> bool:
                    nonlocal special_func_invokation_count
                    special_func_invokation_count += 1
                    print("==== Filter ====")
                    print(str(parameter_data_timestamp))
                    if special_func_invokation_count == 3:
                        event.set()
                    return True

                param_buffer.filter = filter

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.parameters.write(written_data)

        # Assert
        event.wait(timeout=10)
        input_topic.dispose()  # cleanup
        self.assertEqual(3, special_func_invokation_count)

    def test_parameters_read_with_filter_from_buffer_config(self):
        print("test_parameters_read_with_filter_from_buffer_config")
        # Arrange
        client = TestBase.create_streaming_client()
        test_topic = TestBase.get_test_topic()
        output_topic = client.open_output_topic(test_topic)
        input_topic = client.open_input_topic(test_topic, TestBase.get_consumer_group())

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invokation_count = 0

        buffer_config = ParametersBufferConfiguration()

        def filter_callback(parameter_data_timestamp: ParameterDataTimestamp) -> bool:
            nonlocal special_func_invokation_count
            special_func_invokation_count += 1
            print("==== Filter ====")
            print(str(parameter_data_timestamp))
            if special_func_invokation_count == 3:
                event.set()
            return True

        buffer_config.filter = filter_callback

        def on_new_stream(reader: StreamReader):
            if stream.stream_id == reader.stream_id:
                param_buffer = reader.parameters.create_buffer(buffer_config)

        input_topic.on_stream_received += on_new_stream
        input_topic.start_reading()

        # Act
        stream = output_topic.create_stream()

        written_data = ParameterData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.parameters.write(written_data)

        # Assert
        event.wait(timeout=10)
        input_topic.dispose()  # cleanup
        self.assertEqual(3, special_func_invokation_count)

    def assert_data_are_equal(self, data_a: ParameterData, data_b: ParameterData):
        self.assertEqual(len(data_a.timestamps), len(data_b.timestamps), "Timestamp count")
        for index_a, ts_a in enumerate(data_a.timestamps):
            ts_b = data_b.timestamps[index_a]
            self.assertEqual(ts_a.timestamp_nanoseconds, ts_b.timestamp_nanoseconds, "Timestamp")
            for param_id_a, parameter_value_a in ts_a.parameters.items():
                parameter_value_b = ts_b.parameters[param_id_a]
                if (parameter_value_a.type == ParameterValueType.Empty and parameter_value_b.type != ParameterValueType.Empty) or \
                    (parameter_value_b.type == ParameterValueType.Empty and parameter_value_a.type != ParameterValueType.Empty):
                    self.assertEqual(parameter_value_a.value, parameter_value_b.value, "Value")
                else:
                    self.assertEqual(parameter_value_a.type, parameter_value_b.type, "Value type")
                    if parameter_value_a.type == ParameterValueType.String:
                        self.assertEqual(parameter_value_a.string_value, parameter_value_b.string_value, "Value (string)")
                    if parameter_value_a.type == ParameterValueType.Numeric:
                        self.assertEqual(parameter_value_a.numeric_value, parameter_value_b.numeric_value, "Value (numeric)")
            for tag_id_a, tag_value_a in ts_a.tags.items():
                tag_value_b = ts_b.tags[tag_id_a]
                self.assertEqual(tag_value_a, tag_value_b, "tag")


if __name__ == '__main__':
    unittest.main()
