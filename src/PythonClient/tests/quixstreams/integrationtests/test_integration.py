import time
from typing import List
import unittest
import threading
import pandas as pd
import numpy as np
from src.quixstreams import Logging, LogLevel, AutoOffsetReset

from testcontainers.core.container import DockerContainer

from tests.quixstreams.unittests.models.test_timeseriesdata import TimeseriesDataTests
from src import quixstreams as qx
from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
InteropUtils.enable_debug()
Logging.update_factory(LogLevel.Debug)

from datetime import datetime, timedelta
import sys

from containerhelper import ContainerHelper


class TestIntegration(unittest.TestCase):
    """
        The integration tests class. Using a single class for all integration tests to avoid having to setup a
        docker for multiple classes. Ideas for better, easy to follow structure are welcome
    """

    default_max_test_time = 20  # seconds
    kafka_container: DockerContainer
    kafka_port: int
    broker_list: str
    zookeeper_port: int

    @classmethod
    def setUpClass(cls):

        (cls.kafka_container, cls.broker_list, cls.kafka_port, cls.zookeeper_port) = ContainerHelper.create_kafka_container()

        print("Starting Kafka container")
        ContainerHelper.start_kafka_container(cls.kafka_container)
        print("Started Kafka container")

    @classmethod
    def tearDownClass(cls):
        print("Stopping Kafka container")
        cls.kafka_container.stop()
        print("Stopped Kafka container")

    def waitforresult(self, event: threading.Event, max_test_time: int = None):
        if max_test_time is None:
            max_test_time = TestIntegration.default_max_test_time
        start = datetime.utcnow()
        success = event.wait(max_test_time)
        end = datetime.utcnow()
        print("Waited {} for result".format(end - start))
        self.assertIs(success, True)

# region stream properties

    def test_stream_properties_flush(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        print("---- Start publishing ----")
        with (topic_producer := client.get_topic_producer(topic_name)), (output_stream := topic_producer.create_stream()):
            print("---- Setting stream properties ----")
            output_stream.properties.flush_interval = 7000
            print("Closed")

            # Assert
            self.assertEqual(output_stream.properties.flush_interval, 7000)  # just property set test

    def test_stream_properties(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        incoming_stream: qx.StreamConsumer = None  # the object we will be testing here

        with (client := qx.KafkaStreamingClient(TestIntegration.broker_list, None)), (topic_consumer := client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=qx.AutoOffsetReset.Earliest)):
            output_stream = None  # output stream

            def on_stream_received(stream: qx.StreamConsumer):
                nonlocal incoming_stream
                if stream.stream_id == output_stream.stream_id:
                    print("---- Test stream read {} ----".format(stream.stream_id))
                    incoming_stream = stream
                    stream.properties.on_changed = on_properties_changed

            def on_properties_changed(stream: qx.StreamConsumer):
                event.set()

            topic_consumer.on_stream_received = on_stream_received

            # Act
            print("---- Subscribe & start consuming ----")
            topic_consumer.subscribe()

            print("---- Start publishing ----")
            with (topic_producer := client.get_topic_producer(topic_name)), (
            output_stream := topic_producer.create_stream()):

                print("---- Setting stream properties ----")
                output_stream.properties.name = "ABCDE"
                output_stream.properties.location = "/test/location"
                output_stream.properties.metadata["meta"] = "is"
                output_stream.properties.metadata["working"] = "well"
                output_stream.properties.parents.append("testParentId1")
                output_stream.properties.parents.append("testParentId2")
                output_stream.properties.time_of_recording = datetime.utcnow()
                output_stream.timeseries.buffer.add_timestamp(datetime.utcnow()).add_value("test", 1)
                output_stream.timeseries.flush()
                output_stream.properties.flush()
                output_time_of_recording = output_stream.properties.time_of_recording
                output_stream.close()
                print("Closed")

                # Assert
                self.waitforresult(event)

                self.assertIsNotNone(incoming_stream)
                self.assertEqual(incoming_stream.properties.name, "ABCDE")
                self.assertEqual(incoming_stream.properties.location, "/test/location")
                self.assertEqual(len(incoming_stream.properties.metadata), 2)
                self.assertEqual(incoming_stream.properties.metadata["meta"], "is")
                self.assertEqual(incoming_stream.properties.metadata["working"], "well")
                self.assertEqual(len(incoming_stream.properties.parents), 2)
                self.assertIn("testParentId1", incoming_stream.properties.parents)
                self.assertIn("testParentId2", incoming_stream.properties.parents)
                self.assertIsNotNone(incoming_stream.properties.time_of_recording)
                self.assertEqual(incoming_stream.properties.time_of_recording, output_time_of_recording)

# endregion

# region parameter definitions

    def test_parameter_definitions(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        incoming_stream: qx.StreamConsumer = None  # the object we will be testing here

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=qx.AutoOffsetReset.Earliest)
        output_stream = None  # output stream

        def on_stream_received(stream: qx.StreamConsumer):
            nonlocal incoming_stream
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                incoming_stream = stream
                stream.timeseries.on_definitions_changed = on_parameters_changed

        def on_parameters_changed(stream: qx.StreamConsumer):
            event.set()


        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()

        print("---- Setting stream parameter definitions ----")
        output_stream.timeseries.default_location = "/the/location"
        output_stream.timeseries.add_definition("ParameterA")
        output_stream.timeseries.add_location("/the/otherlocation") \
            .add_definition("ParameterB", "Parameter B", "Some description") \
            .set_range(1.23456, 7.890) \
            .set_unit("C") \
            .set_format("0.0000f") \
            .set_custom_properties("{""jsonprop"": true }") \

        output_stream.timeseries.flush()
        output_stream.close()
        print("Closed")
        topic_producer.dispose()

        # Assert
        self.waitforresult(event)

        self.assertIsNotNone(incoming_stream)

        pdefs = incoming_stream.timeseries.definitions

        self.assertEqual(len(pdefs), 2)

        pdef = pdefs[0]
        pdef2 = pdefs[1]
        pdefa = pdef if pdef.id == "ParameterA" else pdef2
        pdefb = pdef if pdef.id == "ParameterB" else pdef2

        self.assertIsNotNone(pdefa)
        self.assertEqual(pdefa.id, "ParameterA")
        self.assertEqual(pdefa.location, "/the/location")

        self.assertIsNotNone(pdefb)
        self.assertEqual(pdefb.id, "ParameterB")
        self.assertEqual(pdefb.name, "Parameter B")
        self.assertEqual(pdefb.description, "Some description")
        self.assertEqual(pdefb.location, "/the/otherlocation")
        self.assertEqual(pdefb.minimum_value, 1.23456)
        self.assertEqual(pdefb.maximum_value, 7.890)
        self.assertEqual(pdefb.unit, "C")
        self.assertEqual(pdefb.format, "0.0000f")
        self.assertEqual(pdefb.custom_properties, "{""jsonprop"": true }")

        # Cleanup
        topic_consumer.dispose()

# endregion

# region event definitions
    def test_event_definitions(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        incoming_stream: qx.StreamConsumer = None  # the object we will be testing here

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=qx.AutoOffsetReset.Earliest)
        output_stream = None  # output stream

        def on_stream_received(stream: qx.StreamConsumer):
            nonlocal incoming_stream
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                incoming_stream = stream
                stream.events.on_definitions_changed = on_events_changed

        def on_events_changed(stream: qx.StreamConsumer):
            event.set()


        topic_consumer.on_stream_received = on_stream_received


        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()


        print("---- Setting stream event definitions ----")
        output_stream.events.default_location = "/the/location"
        output_stream.events.add_definition("EventA")
        output_stream.events.add_location("/the/otherlocation") \
            .add_definition("EventB", "Event B", "Some description") \
            .set_level(qx.EventLevel.Critical) \
            .set_custom_properties("{""jsonprop"": true }")

        output_stream.events.flush()
        output_stream.close()
        print("Closed")
        topic_producer.dispose()

        # Assert
        self.waitforresult(event)

        self.assertIsNotNone(incoming_stream)

        edefs = incoming_stream.events.definitions

        self.assertEqual(len(edefs), 2)

        edef = edefs[0]
        edef2 = edefs[1]
        edefa = edef if edef.id == "EventA" else edef2
        edefb = edef if edef.id == "eventB" else edef2

        self.assertIsNotNone(edefa)
        self.assertEqual(edefa.id, "EventA")
        self.assertEqual(edefa.location, "/the/location")

        self.assertIsNotNone(edefb)
        self.assertEqual(edefb.id, "EventB")
        self.assertEqual(edefb.name, "Event B")
        self.assertEqual(edefb.description, "Some description")
        self.assertEqual(edefb.location, "/the/otherlocation")
        self.assertEqual(edefb.level, qx.EventLevel.Critical)
        self.assertEqual(edefb.custom_properties, "{""jsonprop"": true }")

        # Cleanup
        topic_consumer.dispose()

# endregion

    def test_run_via_app(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        output_stream = None  # The outgoing stream

        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        cts = qx.CancellationTokenSource()  # used for interrupting the App

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        output_stream.events.add_timestamp_nanoseconds(100)\
            .add_value("event1", "value1")\
            .add_tag("tag1", "tag1val")\
            .add_tags({"tag2": "tag2val", "tag3": "tag3val"})\
            .publish()

        def event_callback():
            try:
                self.waitforresult(event)
            finally:
                cts.cancel()

        # need to wait for result in another thread because main thread is taken by App.run
        event_thread = threading.Thread(target=event_callback)
        event_thread.start()

        shutdown_callback_value = False

        def before_shutdown():
            nonlocal shutdown_callback_value
            shutdown_callback_value = True

        # Assert
        qx.App.run(cts.token, before_shutdown=before_shutdown)
        event_thread.join()
        self.assertIsNotNone(read_data)
        self.assertEqual(shutdown_callback_value, True)

        # Cleanup
        topic_consumer.dispose()

# region eventdata interop tests
    def test_events_write_via_builder_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)
        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        output_stream.events.add_timestamp_nanoseconds(100)\
            .add_value("event1", "value1")\
            .add_tag("tag1", "tag1val")\
            .publish()

        # Assert
        self.waitforresult(event)

        expected = qx.EventData("event1", 100, "value1").add_tag("tag1", "tag1val")
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        self.assert_eventdata_are_equal(expected, read_data)
        self.assert_eventdata_are_equal(read_data, expected)

        # cleanup
        topic_consumer.dispose()

    def test_events_write_via_builder_and_read_using_timedelta(self):
        # Used to segfault. Because is using builder, difficult to test as only unittest
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)
        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        output_stream.events.add_timestamp(timedelta(seconds=1, milliseconds=555))\
            .add_value("event1", "value1")\
            .add_tag("tag1", "tag1val")\
            .publish()

        # Assert
        self.waitforresult(event)

        expected = qx.EventData("event1", 1555000000, "value1").add_tag("tag1", "tag1val")
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        self.assert_eventdata_are_equal(expected, read_data)
        self.assert_eventdata_are_equal(read_data, expected)

        # cleanup
        topic_consumer.dispose()

    def test_events_write_direct_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)
        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        expected = qx.EventData("event1", 100, "value1").add_tag("tag1", "tag1val")
        output_stream.events.publish(expected)

        # Assert
        self.waitforresult(event)

        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        self.assert_eventdata_are_equal(expected, read_data)
        self.assert_eventdata_are_equal(read_data, expected)

        # cleanup
        topic_consumer.dispose()

    def test_event_data_frame_write_direct_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        output_stream = None  # The outgoing stream

        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()


        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        data = [("event1", 100, "value1")]
        df = pd.DataFrame(data, columns=['id', 'time', 'val'])
        output_stream.events.publish(data=df, timestamp='time', value='val')

        # Assert
        self.waitforresult(event)

        expected = qx.EventData("event1", 100, "value1")
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        self.assert_eventdata_are_equal(expected, read_data)
        self.assert_eventdata_are_equal(read_data, expected)

        # cleanup
        topic_consumer.dispose()

    def assert_eventdata_are_equal(self, data_a: qx.EventData, data_b: qx.EventData):
        self.assertEqual(data_a.id, data_b.id, "Id")
        self.assertEqual(data_a.timestamp_nanoseconds, data_b.timestamp_nanoseconds, "Nanoseconds")
        self.assertEqual(data_a.value, data_b.value, "value")
        for tag_id_a, tag_value_a in data_a.tags.items():
            tag_value_b = data_b.tags[tag_id_a]
            self.assertEqual(tag_value_a, tag_value_b, "tag")
# endregion

# region TopicProducer tests
    def test_created_stream_can_be_retrieved(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)

        # Act
        stream = topic_producer.create_stream()
        retrieved = topic_producer.get_stream(stream.stream_id)

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertEqual(stream.stream_id, retrieved.stream_id)

    def test_closed_stream_can_not_be_retrieved(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        stream = topic_producer.create_stream()
        stream.close()

        # Act
        retrieved = topic_producer.get_stream(stream.stream_id)

        # Assert
        self.assertIsNone(retrieved)

    @unittest.skip("Pending work to make disposal function")
    def test_disposed_topic_invokes_on_disposed(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)

        callback_topic_disposed = None

        def callback(topic: qx.TopicProducer):
            nonlocal callback_topic_disposed
            callback_topic_disposed = topic

        topic_producer.on_disposed = callback

        # Act
        topic_producer.dispose()

        # Assert
        self.assertEqual(topic_producer, callback_topic_disposed)
# endregion

# region client.get_topic_consumer integration tests
    def test_stream_open(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        output_stream = None  # The outgoing stream

        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Start publishing ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()
        output_stream.close()

        # Assert
        self.waitforresult(event)  # enough assertion as if event times out, expected scenario did not happen

        # cleanup
        topic_producer.dispose()
        topic_consumer.dispose()

    def test_stream_open_with_latest_offset(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=qx.AutoOffsetReset.Latest)

        first_stream_read : qx.StreamConsumer = None

        def on_stream_received(stream: qx.StreamConsumer):
            print("---- Stream read {} ----".format(stream.stream_id))
            nonlocal first_stream_read
            first_stream_read = stream
            event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Write first stream ----")
        topic_producer = client.get_topic_producer(topic_name)
        first_stream = topic_producer.create_stream()
        first_stream.close()

        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()
        # as of now subscribe() returns as soon as connection open request passed to broker library
        # rather than when it is ready to serve messages from broker. In most cases this isn't necessarily an issue
        # because you wouldn't read from the topic you're publishing to in the same application,
        # especially so soon after read began with offset "Latest". "Earliest" would work just fine
        time.sleep(5)

        print("---- Write second stream ----")
        second_stream = topic_producer.create_stream()
        second_stream.close()

        # Assert
        self.waitforresult(event)

        self.assertEqual(first_stream_read.stream_id, second_stream.stream_id)

        # cleanup
        topic_producer.dispose()
        topic_consumer.dispose()

    def test_stream_open_with_manual_commit(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, commit_settings=qx.CommitMode.Manual, auto_offset_reset=qx.AutoOffsetReset.Earliest)

        last_stream_read: qx.StreamConsumer = None

        def on_stream_received(stream: qx.StreamConsumer):
            print("---- Stream read {} ----".format(stream.stream_id))
            nonlocal last_stream_read
            last_stream_read = stream
            stream.on_stream_closed = on_stream_closed

        def on_stream_closed(stream: qx.StreamConsumer, end_type: qx.StreamEndType):
            print("---- Committing ----".format(stream.stream_id))
            topic_consumer.commit()
            print("---- Committed ----".format(stream.stream_id))
            event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Write first stream ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()
        output_stream.close()
        print(f"---- Write first stream {output_stream.stream_id} ----")

        self.waitforresult(event)
        event.clear()
        topic_consumer.dispose()
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)  # should continue after first stream, as same consumer group
        topic_consumer.on_stream_received = on_stream_received
        topic_consumer.subscribe()

        print("---- Write second stream ----")
        output_stream = topic_producer.create_stream()  # output_stream points to second stream from now
        output_stream.close()
        print(f"---- Write second stream {output_stream.stream_id} ----")

        # Assert
        self.waitforresult(event, 30)

        self.assertEqual(last_stream_read.stream_id, output_stream.stream_id)

        # cleanup
        topic_producer.dispose()
        topic_consumer.dispose()
# endregion

# region stream close integration tests
    def test_stream_close_with_type(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        event = threading.Event()  # used to trigger evaluation

        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=qx.AutoOffsetReset.Earliest)

        output_stream = None  # The outgoing stream
        end_type_received = None

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id != output_stream.stream_id:
                return

            print("---- Stream read {} ----".format(stream.stream_id))
            stream.on_stream_closed = on_stream_closed

        def on_stream_closed(stream: qx.StreamConsumer, end_type: qx.StreamEndType):
            nonlocal end_type_received
            end_type_received = end_type
            event.set()

        topic_consumer.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer.subscribe()

        print("---- Write first stream ----")
        topic_producer = client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()
        output_stream.close(qx.StreamEndType.Aborted)

        # Assert
        self.waitforresult(event)

        self.assertEqual(end_type_received, qx.StreamEndType.Aborted)

        # cleanup
        topic_producer.dispose()
        topic_consumer.dispose()
# endregion

# region output topic integration tests
    def test_get_or_create_stream_no_prev_stream_with_callback(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        # Act
        topic_producer = client.get_topic_producer(topic_name)
        callback_invoked_streamproducer : qx.StreamProducer = None

        def on_create_callback(sw):
            nonlocal callback_invoked_streamproducer
            callback_invoked_streamproducer = sw

        retrieved = topic_producer.get_or_create_stream("test_stream_id", on_create_callback)

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertIsNotNone(callback_invoked_streamproducer)
        retrievedId = retrieved.stream_id
        self.assertEqual(callback_invoked_streamproducer.stream_id, retrieved.stream_id)
        self.assertEqual(retrieved.stream_id, "test_stream_id")

    def test_get_or_create_stream_no_prev_stream_without_callback(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        # Act
        topic_producer = client.get_topic_producer(topic_name)
        retrieved = topic_producer.get_or_create_stream("test_stream_id")

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.stream_id, "test_stream_id")

    def test_get_or_create_stream_with_prev_stream_with_callback(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        # Act
        topic_producer = client.get_topic_producer(topic_name)
        first_stream = topic_producer.create_stream("test_stream_id")  # will cause the stream to exist

        callback_invoked_streamproducer : qx.StreamProducer = None

        def on_create_callback(sw):
            nonlocal callback_invoked_streamproducer
            callback_invoked_streamproducer = sw

        retrieved = topic_producer.get_or_create_stream("test_stream_id", on_create_callback)

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertIsNone(callback_invoked_streamproducer)
        self.assertEqual(retrieved.stream_id, "test_stream_id")

    def test_get_or_create_stream_with_prev_stream_without_callback(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)

        # Act
        topic_producer = client.get_topic_producer(topic_name)
        first_stream = topic_producer.create_stream("test_stream_id")  # will cause the stream to exist

        retrieved = topic_producer.get_or_create_stream("test_stream_id")

        # Assert
        self.assertIsNotNone(retrieved)
        self.assertEqual(retrieved.stream_id, "test_stream_id")
# endregion

# region timeseries data integration tests
    def test_timeseries_builder_works_with_any_number_type_and_none(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        stream = qx.KafkaStreamingClient(TestIntegration.broker_list, None).get_topic_producer(topic_name).create_stream()

        # Act
        stream.timeseries.buffer \
            .add_timestamp_nanoseconds(1) \
            .add_value("npy_float64", np.float64(42.0)) \
            .add_value("npy_int64", np.int64(42)) \
            .add_value("native_int", int(42)) \
            .add_value("native_float", float(42)) \
            .add_value("none", None)

        # Assert that no exception got raised

    def test_parameters_write_binary_read_binary_is_of_bytes(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)
        with topic_producer, topic_producer:

            stream = None  # The outgoing stream
            event = threading.Event()  # used for assertion
            read_data: qx.TimeseriesData = None

            def on_stream_received_handler(stream_received: qx.StreamConsumer):
                if stream.stream_id == stream_received.stream_id:
                    param_buffer = stream_received.timeseries.create_buffer()
                    param_buffer.buffer_timeout = 100
                    param_buffer.on_data_released = on_parameter_data_handler

            def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
                nonlocal read_data
                read_data = data
                event.set()

            topic_consumer.on_stream_received = on_stream_received_handler
            topic_consumer.subscribe()

            # Act
            stream = topic_producer.create_stream()
            stream.timeseries.buffer.packet_size = 10  # this is to enforce buffering until we want
            # Send parameter Data for datetime
            utc_now = datetime.utcnow()  # for assertion purposes save it
            stream.timeseries.buffer \
                .add_timestamp(utc_now) \
                .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
                .publish()

            stream.timeseries.buffer.flush()

            # Assert
            self.waitforresult(event)
            print(read_data)
            binary_value = read_data.timestamps[0].parameters["binary_param"].binary_value
            self.assertEqual(type(binary_value), bytes)
            self.assertEqual(binary_value, bytes(bytearray("binary_param", "UTF-8")))

    def test_parameters_write_via_buffer_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()
        stream.timeseries.buffer.packet_size = 10  # this is to enforce buffering until we want
        # Send parameter Data for datetime
        utc_now = datetime.utcnow()  # for assertion purposes save it
        stream.timeseries.buffer \
            .add_timestamp(utc_now) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 123.43) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8"))) \
            .add_tag("Tag2", "tag two updated") \
            .add_tag("Tag3", "tag three") \
            .add_tags({"tag4": "tag4val", "tag5": "tag5val"}) \
            .publish()

        # Send timeseries data in nanoseconds relative to epoch
        stream.timeseries.buffer \
            .add_timestamp_nanoseconds(123456789) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param3", "UTF-8")) \
            .publish()

        # Send timeseries data in timedelta relative to a new epoch
        stream.timeseries.buffer.epoch = datetime(2018, 1, 2)
        stream.timeseries.buffer \
            .add_timestamp(timedelta(seconds=1, milliseconds=555)) \
            .add_value("num_param", 123.32) \
            .add_value("binary_param", bytearray("binary_param4", "UTF-8")) \
            .publish()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(123456790) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param4", "UTF-8"))

        stream.timeseries.buffer.publish(written_data)

        stream.timeseries.buffer.flush()

        # Assert
        self.waitforresult(event)
        print(read_data)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(4, len(read_data.timestamps))

    def test_parameters_write_direct_and_read_as_timeseriesDataraw(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesDataRaw = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                stream_received.timeseries.on_raw_received = on_raw_received_handler

        def on_raw_received_handler(stream: qx.StreamConsumer, data: qx.TimeseriesDataRaw):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
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

        stream.timeseries.publish(written_data)

        # Assert
        self.waitforresult(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)

        print("========================")
        topic_consumer.dispose()  # cleanup

        converted = read_data.convert_to_timeseriesdata()
        TimeseriesDataTests.assert_data_are_equal(self, written_data, converted)  # evaluate neither contains more or less than should
        TimeseriesDataTests.assert_data_are_equal(self, converted, written_data)  # and is done by checking both ways
        self.assertEqual(len(converted.timestamps), 1)
        self.assertEqual(len(converted.timestamps[0].parameters), 5, "Missing parameter")

    def test_parameters_write_direct_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
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

        stream.timeseries.publish(written_data)

        # Assert
        self.waitforresult(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(len(read_data.timestamps[0].parameters), 4, "Missing parameter")
        TimeseriesDataTests.assert_data_are_equal(self, written_data, read_data)  # evaluate neither contains more or less than should
        TimeseriesDataTests.assert_data_are_equal(self, read_data, written_data)  # and is done by checking both ways

    def test_timeseriesDataTimestamp_publish_direct_and_consume(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group,
                                                   auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        consumed_timeseries_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_timeseries_data_handler

        def on_timeseries_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal consumed_timeseries_data
            consumed_timeseries_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        published_timestamp = qx.TimeseriesData().add_timestamp_nanoseconds(10) \
            .add_value("bufferless_1", 1) \
            .add_value("bufferless_2", "test") \
            .add_value("bufferless_3", "test") \
            .add_value("bufferless_4", bytearray("bufferless_4", "UTF-8")) \
            .add_value("bufferless_5", bytes(bytearray("bufferless_5", "UTF-8"))) \
            .remove_value("bufferless_3") \
            .add_tag("tag1", "tag1val") \
            .add_tag("tag2", "tag2val") \
            .remove_tag("tag2")

        stream.timeseries.publish(published_timestamp)

        # Assert
        self.waitforresult(event)
        print("------ Written ------")
        print(published_timestamp)
        print("------ READ ------")
        print(consumed_timeseries_data)
        topic_consumer.dispose()  # cleanup

        self.assertEqual(len(consumed_timeseries_data.timestamps), 1, "Multiple timestamps received")
        self.assertEqual(len(consumed_timeseries_data.timestamps[0].parameters), 4, "Missing parameter")
        # evaluate neither contains more nor less than should, and is done by checking both ways
        TimeseriesDataTests.assert_timestamps_are_equal(self, published_timestamp, consumed_timeseries_data.timestamps[0])
        TimeseriesDataTests.assert_timestamps_are_equal(self, consumed_timeseries_data.timestamps[0], published_timestamp)

    def test_timeseriesDataTimestamp_publish_via_buffer_and_consume(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group,
                                                   auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        consumed_timeseries_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal consumed_timeseries_data
            consumed_timeseries_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()
        stream.timeseries.buffer.packet_size = 10  # this is to enforce buffering until we want
        # Send parameter Data for datetime
        utc_now = datetime.utcnow()  # for assertion purposes save it
        stream.timeseries.buffer \
            .add_timestamp(utc_now) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 123.43) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8"))) \
            .add_tag("Tag2", "tag two updated") \
            .add_tag("Tag3", "tag three") \
            .add_tags({"tag4": "tag4val", "tag5": "tag5val"}) \
            .publish()

        # Send timeseries data in nanoseconds relative to epoch
        stream.timeseries.buffer \
            .add_timestamp_nanoseconds(123456789) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param3", "UTF-8")) \
            .publish()

        # Send timeseries data in timedelta relative to a new epoch
        stream.timeseries.buffer.epoch = datetime(2018, 1, 2)
        stream.timeseries.buffer \
            .add_timestamp(timedelta(seconds=1, milliseconds=555)) \
            .add_value("num_param", 123.32) \
            .add_value("binary_param", bytearray("binary_param4", "UTF-8")) \
            .publish()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(123456790) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param4", "UTF-8"))

        stream.timeseries.buffer.publish(written_data)

        stream.timeseries.buffer.flush()

        # Assert
        self.waitforresult(event)
        print(consumed_timeseries_data)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(4, len(consumed_timeseries_data.timestamps))

    def test_parameters_write_direct_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
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

        stream.timeseries.publish(written_data)

        # Assert
        self.waitforresult(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(len(read_data.timestamps[0].parameters), 4, "Missing parameter")
        TimeseriesDataTests.assert_data_are_equal(self, written_data, read_data)  # evaluate neither contains more or less than should
        TimeseriesDataTests.assert_data_are_equal(self, read_data, written_data)  # and is done by checking both ways

    def test_parameters_write_direct_and_read_all_options(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        event_raw = threading.Event()  # used for assertion
        event_pandas_dataframe = threading.Event()  # used for assertion
        read_data: pd.DataFrame = None
        read_data_raw: pd.DataFrame = None
        read_pandas_dataframe: pd.DataFrame = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_parameter_data_handler
                param_buffer.on_raw_released = on_parameter_data_raw_handler
                param_buffer.on_dataframe_released = on_parameter_dataframe_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data.to_dataframe()
            event.set()

        def on_parameter_data_raw_handler(stream: qx.StreamConsumer, data: qx.TimeseriesDataRaw):
            nonlocal read_data_raw
            read_data_raw = data.to_dataframe()
            event_raw.set()

        def on_parameter_dataframe_handler(stream: qx.StreamConsumer, data: pd.DataFrame):
            nonlocal read_pandas_dataframe
            read_pandas_dataframe = data
            event_pandas_dataframe.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
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

        stream.timeseries.publish(written_data)

        # Assert
        print("------ Written ------")
        print(written_data)
        self.waitforresult(event)
        print("------ READ ------")
        print(read_data)
        self.waitforresult(event_raw)
        print("------ READ RAW ------")
        print(read_data_raw)
        self.waitforresult(event_pandas_dataframe)
        print("------ READ PANDAS ------")
        print(read_pandas_dataframe)
        topic_consumer.dispose()  # cleanup

        def assertFrameEqual(df1, df2, **kwds):
            """ Assert that two dataframes are equal, ignoring ordering of columns"""
            from pandas.util.testing import assert_frame_equal
            assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwds)

        assertFrameEqual(read_data, read_data_raw)
        assertFrameEqual(read_data_raw, read_pandas_dataframe)

    def test_parameters_write_panda_via_buffer_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()
        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(123456790) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8")))
        pf = written_data.to_dataframe()

        stream.timeseries.buffer.add_timestamp(datetime.utcnow()).add_value("a", "b").publish()

        stream.timeseries.buffer.flush()

        # Assert
        self.waitforresult(event)
        print(read_data)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(1, len(read_data.timestamps))

    def test_parameters_write_compare_panda_dataframe_different_exports(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        event2 = threading.Event()  # used for assertion
        event3 = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None
        read_data_raw: qx.TimeseriesDataRaw = None
        read_pandas_dataframe: pd.DataFrame = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_data_released_handler

                timeseries = stream_received.timeseries
                timeseries.on_raw_received = on_raw_received_handler
                timeseries.on_dataframe_received = on_dataframe_received_handler

        def on_data_released_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data.to_dataframe()
            event.set()

        def on_raw_received_handler(stream: qx.StreamConsumer, data):
            nonlocal read_data_raw
            read_data_raw = data.to_dataframe()
            event2.set()

        def on_dataframe_received_handler(stream: qx.StreamConsumer, data):
            nonlocal read_pandas_dataframe
            read_pandas_dataframe = data
            event3.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()
        stream.timeseries.buffer.packet_size = 10  # to enforce disabling of output buffer
        written_data = qx.TimeseriesData()

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

        pf = written_data.to_dataframe()

        stream.timeseries.buffer.publish(pf)

        stream.timeseries.buffer.flush()

        # Assert
        self.waitforresult(event)
        print("==== read_data ====")
        print(read_data)
        self.waitforresult(event2)
        print("==== read_data raw ====")
        print(read_data_raw)
        self.waitforresult(event3)
        print("==== read_data pandas ====")
        print(read_pandas_dataframe)
        topic_consumer.dispose()  # cleanup

        def assertFrameEqual(df1, df2, **kwds):
            """ Assert that two dataframes are equal, ignoring ordering of columns"""
            from pandas.util.testing import assert_frame_equal
            assert_frame_equal(df1.sort_index(axis=1), df2.sort_index(axis=1), check_names=True, **kwds)

        assertFrameEqual(pf, read_data)
        assertFrameEqual(read_data, read_data_raw)
        assertFrameEqual(read_data_raw, read_pandas_dataframe)

    def test_parameters_read_with_custom_trigger(self):
        return #TODO
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invocation_count = 0

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()

                def custom_trigger_callback(parameter_data: qx.TimeseriesData) -> bool:
                    nonlocal special_func_invocation_count
                    special_func_invocation_count += 1
                    print("==== Custom Trigger ====")
                    print(str(parameter_data))
                    if special_func_invocation_count == 3:
                        event.set()
                    return True

                param_buffer.custom_trigger = custom_trigger_callback

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.timeseries.publish(written_data)

        # Assert
        self.waitforresult(event)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(3, special_func_invocation_count)

    def test_parameters_read_with_custom_trigger_from_buffer_config(self):
        return # TODO
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invocation_count = 0

        buffer_config = qx.TimeseriesBufferConfiguration()

        def custom_trigger_callback(parameter_data: qx.TimeseriesData) -> bool:
            nonlocal special_func_invocation_count
            special_func_invocation_count += 1
            print("==== Custom Trigger ====")
            print(str(parameter_data))
            if special_func_invocation_count == 3:
                event.set()
            return True

        buffer_config.custom_trigger = custom_trigger_callback

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer(buffer_config)

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.timeseries.publish(written_data)

        # Assert
        self.waitforresult(event)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(3, special_func_invocation_count)

    def test_parameters_write_panda_direct_and_read(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
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
        pf = written_data.to_dataframe()

        stream.timeseries.publish(pf)

        # Assert
        print("------ Written ------")
        print(written_data)
        self.waitforresult(event)
        print("------ READ ------")
        print(read_data)
        topic_consumer.dispose()  # cleanup
        TimeseriesDataTests.assert_data_are_equal(self, written_data, read_data)  # evaluate neither contains more or less than should
        TimeseriesDataTests.assert_data_are_equal(self, read_data, written_data)  # and is done by checking both ways

    def test_parameters_read_with_parameter_filter(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer("param1", "param3")
                param_buffer.buffer_timeout = 500  # to prevent raising each timestamp on its own
                param_buffer.on_data_released = on_data_released_handler

        def on_data_released_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.timeseries.publish(written_data)

        # Assert
        expected_data = qx.TimeseriesData()
        expected_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_tag("tag1", "tag1val")
        expected_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        self.waitforresult(event)
        topic_consumer.dispose()  # cleanup
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("------ Expected ------")
        print(expected_data)
        TimeseriesDataTests.assert_data_are_equal(self, expected_data, read_data)  # evaluate neither contains more or less than should
        TimeseriesDataTests.assert_data_are_equal(self, read_data, expected_data)  # and is done by checking both ways

    def test_parameters_read_with_buffer_configuration(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        buffer_config = qx.TimeseriesBufferConfiguration()
        buffer_config.packet_size = 2

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer(buffer_config)
                param_buffer.buffer_timeout = 1000  # to prevent raising each timestamp on its own
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer, data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.timeseries.publish(written_data)

        # Assert
        expected_data = qx.TimeseriesData()
        expected_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")
        expected_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        self.waitforresult(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("------ Expected ------")
        print(expected_data)
        topic_consumer.dispose()  # cleanup
        TimeseriesDataTests.assert_data_are_equal(self, expected_data, read_data)  # evaluate neither contains more or less than should
        TimeseriesDataTests.assert_data_are_equal(self, read_data, expected_data)  # and is done by checking both ways

    def test_parameters_read_with_filter(self):
        return  # TODO high importance
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invocation_count = 0

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()

                def filter(parameter_data_timestamp: qx.TimeseriesDataTimestamp) -> bool:
                    nonlocal special_func_invocation_count
                    special_func_invocation_count += 1
                    print("==== Filter ====")
                    print(str(parameter_data_timestamp))
                    if special_func_invocation_count == 3:
                        event.set()
                    return True

                param_buffer.filter = filter

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.timeseries.publish(written_data)

        # Assert
        self.waitforresult(event)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(3, special_func_invocation_count)

    def test_parameters_read_with_filter_from_buffer_config(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_producer = client.get_topic_producer(topic_name)
        topic_consumer = client.get_topic_consumer(topic_name, consumer_group, auto_offset_reset=AutoOffsetReset.Earliest)

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invocation_count = 0

        buffer_config = qx.TimeseriesBufferConfiguration()

        def filter_callback(parameter_data_timestamp: qx.TimeseriesDataTimestamp) -> bool:
            nonlocal special_func_invocation_count
            special_func_invocation_count += 1
            print("==== Filter ====")
            print(str(parameter_data_timestamp))
            if special_func_invocation_count == 3:
                event.set()
            return True

        buffer_config.filter = filter_callback

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer(buffer_config)

        topic_consumer.on_stream_received = on_stream_received_handler
        topic_consumer.subscribe()

        # Act
        stream = topic_producer.create_stream()

        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(10) \
            .add_value("param1", 1) \
            .add_value("param2", "test") \
            .add_tag("tag1", "tag1val")

        written_data.add_timestamp_nanoseconds(12) \
            .add_value("param2", "test")

        written_data.add_timestamp_nanoseconds(13) \
            .add_value("param1", 2) \
            .add_value("param3", "test")

        stream.timeseries.publish(written_data)

        # Assert
        self.waitforresult(event)
        topic_consumer.dispose()  # cleanup
        self.assertEqual(3, special_func_invocation_count)
# endregion

# region raw
    def test_raw_read_write(self):
        # Arrange
        print("Starting Integration test {}".format(sys._getframe().f_code.co_name))
        topic_name = sys._getframe().f_code.co_name  # current method name
        consumer_group = "irrelevant"  # because the kafka we're testing against doesn't have topic initially, using consumer group and offset 'earliest' is the only stable way to read from it before beginning to write
        client = qx.KafkaStreamingClient(TestIntegration.broker_list, None)
        topic_consumer = client.get_raw_topic_consumer(topic_name, consumer_group=consumer_group)
        topic_producer = client.get_raw_topic_producer(topic_name)

        received_messages: List[qx.RawMessage] = []
        event = threading.Event()  # used for assertion
        counter = 0

        def on_message_received_handler(topic: qx.RawTopicConsumer, message: qx.RawMessage):
            nonlocal received_messages, counter
            received_messages.append(message)
            counter = counter + 1
            if counter == 3:
                event.set()

        topic_consumer.on_message_received = on_message_received_handler
        topic_consumer.subscribe()

        # Act
        message_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        topic_producer.publish(message_bytes)
        message_bytearray = bytearray("Test Quix Raw with bytearray", "utf-8")
        topic_producer.publish(message_bytearray)
        message_raw = qx.RawMessage(bytearray("Test Quix Raw message", "utf-8"))
        topic_producer.publish(message_raw)

        self.waitforresult(event)
        topic_consumer.dispose()  # cleanup

        # Assert
        self.assertEqual(len(received_messages), 3)
        self.assertEqual(received_messages[0].value, message_bytes)
        self.assertEqual(received_messages[1].value, message_bytearray)
        self.assertEqual(received_messages[2].value, message_raw.value)

        keys = received_messages[0].metadata.keys()
        self.assertIn('MessageGroupKey', keys)
        self.assertIn('KafkaTopic', keys)
        self.assertIn('KafkaKey', keys)
        self.assertIn('KafkaPartition', keys)
        self.assertIn('KafkaOffset', keys)
        self.assertIn('KafkaDateTime', keys)
        self.assertIn('KafkaMessageSize', keys)
# endregion
