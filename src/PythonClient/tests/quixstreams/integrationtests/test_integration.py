import threading
import time
from datetime import datetime, timedelta
from typing import List

import numpy as np
import pandas as pd
import pytest

from src import quixstreams as qx
from src.quixstreams import AutoOffsetReset
from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils, \
    InteropException
from tests.quixstreams.utils import assert_timeseries_data_equal, \
    assert_timestamps_equal, assert_dataframes_equal, assert_eventdata_are_equal

# Use In Memory storage instead of local storage to stop leaving leftover to clean up
state_inmem_storage = qx.InMemoryStorage()
qx.App.set_state_storage(state_inmem_storage)


class BaseIntegrationTest:
    def wait_for_result(self, event: threading.Event, max_test_time: int = 20):
        start = datetime.utcnow()
        success = event.wait(max_test_time)
        end = datetime.utcnow()
        print("Waited {} for result".format(end - start))
        assert success


class TestEventDataInterop(BaseIntegrationTest):
    def test_events_write_via_builder_and_read_using_timedelta(
            self,
            topic_consumer_earliest: qx.TopicConsumer,
            topic_producer: qx.TopicProducer
    ):
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here
        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        output_stream.events.add_timestamp(timedelta(seconds=1, milliseconds=555)) \
            .add_value("event1", "value1") \
            .add_tag("tag1", "tag1val") \
            .publish()

        # Assert
        self.wait_for_result(event)

        expected = qx.EventData("event1", 1555000000, "value1").add_tag("tag1",
                                                                        "tag1val")
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        assert_eventdata_are_equal(expected, read_data)
        assert_eventdata_are_equal(read_data, expected)

    def test_events_write_via_builder_and_read(self,
                                               test_name,
                                               topic_consumer_earliest: qx.TopicConsumer,
                                               topic_producer: qx.TopicProducer
                                               ):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        output_stream.events.add_timestamp_nanoseconds(100) \
            .add_value("event1", "value1") \
            .add_tag("tag1", "tag1val") \
            .publish()

        # Assert
        self.wait_for_result(event)

        expected = qx.EventData("event1", 100, "value1").add_tag("tag1", "tag1val")
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        assert_eventdata_are_equal(expected, read_data)
        assert_eventdata_are_equal(read_data, expected)

    def test_events_write_direct_and_read(
            self,
            test_name,
            topic_consumer_earliest: qx.TopicConsumer,
            topic_producer: qx.TopicProducer
    ):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        expected = qx.EventData("event1", 100, "value1").add_tag("tag1", "tag1val")
        output_stream.events.publish(expected)

        # Assert
        self.wait_for_result(event)

        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        assert_eventdata_are_equal(expected, read_data)
        assert_eventdata_are_equal(read_data, expected)

    def test_event_data_frame_write_direct_and_read(
            self,
            test_name,
            topic_consumer_earliest: qx.TopicConsumer,
            topic_producer: qx.TopicProducer
    ):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        data = [("event1", 100, "value1")]
        df = pd.DataFrame(data, columns=['id', 'time', 'val'])
        output_stream.events.publish(data=df, timestamp='time', value='val')

        # Assert
        self.wait_for_result(event)

        expected = qx.EventData("event1", 100, "value1")
        print("------ READ ------")
        print(read_data)
        print("---- EXPECTED ----")
        print(expected)
        assert_eventdata_are_equal(expected, read_data)
        assert_eventdata_are_equal(read_data, expected)


class TestStreamProperties(BaseIntegrationTest):
    def test_stream_properties_flush(self,
                                     test_name,
                                     topic_producer: qx.TopicProducer
                                     ):

        # Arrange
        print(f'Starting Integration test "{test_name}"')
        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()
        print("---- Setting stream properties ----")
        output_stream.properties.flush_interval = 7000
        print("Closed")
        assert output_stream.properties.flush_interval == 7000

    def test_stream_properties(
            self,
            test_name,
            topic_consumer_earliest: qx.TopicConsumer,
            topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        incoming_stream: qx.StreamConsumer = None  # the object we will be testing here
        output_stream = None  # output stream

        def on_stream_received(stream: qx.StreamConsumer):
            nonlocal incoming_stream
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                incoming_stream = stream
                stream.properties.on_changed = on_properties_changed

        def on_properties_changed(stream: qx.StreamConsumer):
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received
        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()

        print("---- Setting stream properties ----")
        output_stream.properties.name = "ABCDE"
        output_stream.properties.location = "/test/location"
        output_stream.properties.metadata["meta"] = "is"
        output_stream.properties.metadata["working"] = "well"
        output_stream.properties.parents.append("testParentId1")
        output_stream.properties.parents.append("testParentId2")
        output_stream.properties.time_of_recording = datetime.utcnow()
        output_stream.timeseries.buffer.add_timestamp(
            datetime.utcnow()).add_value("test", 1)
        output_stream.timeseries.flush()
        output_stream.properties.flush()
        output_time_of_recording = output_stream.properties.time_of_recording
        output_stream.close()
        print("Closed")

        # Assert
        self.wait_for_result(event)

        assert incoming_stream is not None
        assert incoming_stream.properties.name == "ABCDE"
        assert incoming_stream.properties.location == "/test/location"
        assert len(incoming_stream.properties.metadata) == 2
        assert incoming_stream.properties.metadata["meta"] == "is"
        assert incoming_stream.properties.metadata["working"] == "well"
        assert len(incoming_stream.properties.parents) == 2
        assert "testParentId1" in incoming_stream.properties.parents
        assert "testParentId2" in incoming_stream.properties.parents
        assert incoming_stream.properties.time_of_recording is not None
        assert incoming_stream.properties.time_of_recording == output_time_of_recording


class TestDefinitions(BaseIntegrationTest):
    def test_parameter_definitions(self,
                                   test_name,
                                   topic_consumer_earliest: qx.TopicConsumer,
                                   topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        incoming_stream: qx.StreamConsumer = None  # the object we will be testing here
        output_stream = None  # output stream

        def on_stream_received(stream: qx.StreamConsumer):
            nonlocal incoming_stream
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                incoming_stream = stream
                stream.timeseries.on_definitions_changed = on_parameters_changed

        def on_parameters_changed(stream: qx.StreamConsumer):
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()
        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()
        print("---- Setting stream parameter definitions ----")
        output_stream.timeseries.default_location = "/the/location"
        output_stream.timeseries.add_definition("ParameterA")
        output_stream.timeseries.add_location("/the/otherlocation") \
            .add_definition("ParameterB", "Parameter B", "Some description") \
            .set_range(1.23456, 7.890) \
            .set_unit("C") \
            .set_format("0.0000f") \
            .set_custom_properties("{""jsonprop"": true }")
        output_stream.timeseries.flush()
        output_stream.close()
        print("Closed")

        # Assert
        self.wait_for_result(event)

        assert incoming_stream is not None

        pdefs = incoming_stream.timeseries.definitions

        assert len(pdefs) == 2

        pdef = pdefs[0]
        pdef2 = pdefs[1]
        pdefa = pdef if pdef.id == "ParameterA" else pdef2
        pdefb = pdef if pdef.id == "ParameterB" else pdef2

        assert pdefa is not None
        assert pdefa.id == "ParameterA"
        assert pdefa.location == "/the/location"

        assert pdefb is not None
        assert pdefb.id == "ParameterB"
        assert pdefb.name == "Parameter B"
        assert pdefb.description == "Some description"
        assert pdefb.location == "/the/otherlocation"
        assert pdefb.minimum_value == 1.23456
        assert pdefb.maximum_value == 7.890
        assert pdefb.unit == "C"
        assert pdefb.format == "0.0000f"
        assert pdefb.custom_properties == "{""jsonprop"": true }"

    def test_event_definitions(self, test_name,
                               topic_consumer_earliest: qx.TopicConsumer,
                               topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        incoming_stream: qx.StreamConsumer = None  # the object we will be testing here

        output_stream = None  # output stream

        def on_stream_received(stream: qx.StreamConsumer):
            nonlocal incoming_stream
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                incoming_stream = stream
                stream.events.on_definitions_changed = on_events_changed

        def on_events_changed(stream: qx.StreamConsumer):
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Start publishing ----")
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

        # Assert
        self.wait_for_result(event)

        assert incoming_stream is not None

        edefs = incoming_stream.events.definitions

        assert len(edefs) == 2

        edef = edefs[0]
        edef2 = edefs[1]
        edefa = edef if edef.id == "EventA" else edef2
        edefb = edef if edef.id == "eventB" else edef2

        assert edefa is not None
        assert edefa.id == "EventA"
        assert edefa.location == "/the/location"

        assert edefb is not None
        assert edefb.id == "EventB"
        assert edefb.name == "Event B"
        assert edefb.description == "Some description"
        assert edefb.location == "/the/otherlocation"
        assert edefb.level == qx.EventLevel.Critical
        assert edefb.custom_properties == "{""jsonprop"": true }"


class TestTopicProducer(BaseIntegrationTest):
    def test_created_stream_can_be_retrieved(self,
                                             test_name,
                                             topic_consumer_earliest: qx.TopicConsumer,
                                             topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        # Act
        stream = topic_producer.create_stream()
        retrieved = topic_producer.get_stream(stream.stream_id)
        # Assert
        assert retrieved is not None
        assert stream.stream_id == retrieved.stream_id

    def test_closed_stream_can_not_be_retrieved(self,
                                                test_name,
                                                topic_consumer_earliest: qx.TopicConsumer,
                                                topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        stream = topic_producer.create_stream()
        stream.close()
        # Act
        retrieved = topic_producer.get_stream(stream.stream_id)
        # Assert
        assert retrieved is None

    @pytest.mark.skip("Pending work to make disposal function")
    def test_disposed_topic_invokes_on_disposed(self,
                                                test_name,
                                                topic_consumer_earliest: qx.TopicConsumer,
                                                topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        callback_topic_disposed = None

        def callback(topic: qx.TopicProducer):
            nonlocal callback_topic_disposed
            callback_topic_disposed = topic

        topic_producer.on_disposed = callback
        # Act
        topic_producer.dispose()
        # Assert
        assert topic_producer == callback_topic_disposed

    def test_flush_stream_data_is_flushed(self,
                                             test_name,
                                             topic_consumer_earliest: qx.TopicConsumer,
                                             topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        event = threading.Event()  # used to trigger evaluation

        # Act
        stream = topic_producer.create_stream()
        def received_stream_handler(received_stream: qx.StreamConsumer):
            if received_stream.stream_id != stream.stream_id:
                print("Received another stream")
                return

            print("Received correct stream")

            def raw_received_handler(stream, data):
                event.set()

            received_stream.timeseries.on_raw_received = raw_received_handler

        topic_consumer_earliest.on_stream_received = received_stream_handler
        topic_consumer_earliest.subscribe()

        stream.timeseries.buffer.packet_size = 10000  # to test if gets flushed
        stream.timeseries.buffer.add_timestamp_nanoseconds(100).add_value("some", "value").publish()

        topic_producer.flush()
        self.wait_for_result(event)


class TestTopicConsumer(BaseIntegrationTest):
    def test_stream_open(self,
                         test_name,
                         topic_consumer_earliest: qx.TopicConsumer,
                         topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        output_stream = None  # The outgoing stream

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream()
        output_stream.close()

        # Assert
        # enough assertion as if event times out, expected scenario did not happen
        self.wait_for_result(event)

    def test_stream_open_with_latest_offset(self,
                                            test_name,
                                            topic_name: str,
                                            kafka_streaming_client,
                                            topic_producer: qx.TopicProducer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation

        first_stream_read: qx.StreamConsumer = None

        def on_stream_received(stream: qx.StreamConsumer):
            print("---- Stream read {} ----".format(stream.stream_id))
            nonlocal first_stream_read
            first_stream_read = stream
            event.set()

        topic_consumer_latest = kafka_streaming_client.get_topic_consumer(
            topic=topic_name,
            consumer_group='irrelevant',
            auto_offset_reset=AutoOffsetReset.Latest
        )
        with topic_consumer_latest:
            topic_consumer_latest.on_stream_received = on_stream_received

            # Act
            print("---- Write first stream ----")
            first_stream = topic_producer.create_stream()
            first_stream.close()

            print("---- Subscribe & start consuming ----")
            topic_consumer_latest.subscribe()
            # as of now subscribe() returns as soon as connection open request passed to broker library
            # rather than when it is ready to serve messages from broker. In most cases this isn't necessarily an issue
            # because you wouldn't read from the topic you're publishing to in the same application,
            # especially so soon after read began with offset "Latest". "Earliest" would work just fine
            time.sleep(5)

            print("---- Write second stream ----")
            second_stream = topic_producer.create_stream()
            second_stream.close()

            # Assert
            self.wait_for_result(event)

            assert first_stream_read.stream_id == second_stream.stream_id

    def test_stream_open_with_manual_commit(self,
                                            test_name,
                                            topic_name,
                                            kafka_streaming_client,
                                            topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation

        topic_consumer = kafka_streaming_client.get_topic_consumer(
            topic=topic_name,
            consumer_group='irrelevant',
            commit_settings=qx.CommitMode.Manual,
            auto_offset_reset=AutoOffsetReset.Earliest
        )

        with topic_consumer:
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
            output_stream = topic_producer.create_stream()
            output_stream.close()
            print(f"---- Write first stream {output_stream.stream_id} ----")

            self.wait_for_result(event, 50)
            event.clear()

        topic_consumer = kafka_streaming_client.get_topic_consumer(
            topic=topic_name,
            consumer_group='irrelevant',
            auto_offset_reset=AutoOffsetReset.Earliest
        )
        # should continue after first stream, as same consumer group
        with topic_consumer:
            topic_consumer.on_stream_received = on_stream_received
            topic_consumer.subscribe()

            print("---- Write second stream ----")
            # output_stream points to second stream from now
            output_stream = topic_producer.create_stream()
            output_stream.close()
            print(f"---- Write second stream {output_stream.stream_id} ----")

            # Assert
            self.wait_for_result(event, 30)

            assert last_stream_read.stream_id == output_stream.stream_id


class TestCloseStream(BaseIntegrationTest):
    def test_stream_close_with_type(self,
                                    test_name,
                                    topic_consumer_earliest,
                                    topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation

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

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Subscribe & start consuming ----")
        topic_consumer_earliest.subscribe()

        print("---- Write first stream ----")
        output_stream = topic_producer.create_stream()
        output_stream.close(qx.StreamEndType.Aborted)

        # Assert
        self.wait_for_result(event)
        assert end_type_received == qx.StreamEndType.Aborted


class TestGetOrCreateStream(BaseIntegrationTest):
    def test_get_or_create_stream_no_prev_stream_with_callback(self,
                                                               test_name,
                                                               topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        # Act
        callback_invoked_streamproducer: qx.StreamProducer = None

        def on_create_callback(sw):
            nonlocal callback_invoked_streamproducer
            callback_invoked_streamproducer = sw

        retrieved = topic_producer.get_or_create_stream("test_stream_id",
                                                        on_create_callback)

        # Assert
        assert retrieved is not None
        assert callback_invoked_streamproducer is not None
        assert callback_invoked_streamproducer.stream_id == retrieved.stream_id

    def test_get_or_create_stream_no_prev_stream_without_callback(self,
                                                                  test_name,
                                                                  topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        retrieved = topic_producer.get_or_create_stream("test_stream_id")
        assert retrieved is not None
        assert retrieved.stream_id == "test_stream_id"

    def test_get_or_create_stream_with_prev_stream_with_callback(self,
                                                                 test_name,
                                                                 topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        # will cause the stream to exist
        topic_producer.create_stream("test_stream_id")

        callback_invoked_streamproducer: qx.StreamProducer = None

        def on_create_callback(sw):
            nonlocal callback_invoked_streamproducer
            callback_invoked_streamproducer = sw

        retrieved = topic_producer.get_or_create_stream("test_stream_id",
                                                        on_create_callback)

        # Assert
        assert retrieved is not None
        assert callback_invoked_streamproducer is None
        assert retrieved.stream_id == "test_stream_id"

    def test_get_or_create_stream_with_prev_stream_without_callback(self,
                                                                    test_name,
                                                                    topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        topic_producer.create_stream(
            "test_stream_id")  # will cause the stream to exist
        retrieved = topic_producer.get_or_create_stream("test_stream_id")

        assert retrieved is not None
        assert retrieved.stream_id == "test_stream_id"


class TestTimeseriesData(BaseIntegrationTest):

    def test_read_unavailable_parameter(self,
                                           test_name,
                                           topic_producer,
                                           topic_consumer_earliest,
                                           topic_name):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used for assertion


        print("---- Subscribe to streams ----")

        def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer,
                                          data: qx.TimeseriesData):
            for row in data.timestamps:
                some_integer = row.parameters["some_integer"].numeric_value
                event.set()

        def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
            print(f"Received stream {stream_consumer.stream_id}")
            stream_consumer.timeseries.on_data_received = on_dataframe_received_handler



        topic_consumer_earliest.on_stream_received = on_stream_received_handler

        event = threading.Event()  # used to block sending until the consumer actually subscribed
        print("---- Start publishing ----")

        output_stream = topic_producer.create_stream(f"test-stream")
        output_stream.timeseries.buffer \
            .add_timestamp_nanoseconds(1) \
            .add_value("some_string_param", "test") \
            .publish()
        output_stream.close()

        InteropUtils.log_debug("-------- FLUSHING PRODUCER ---------")
        topic_producer.flush()

        InteropUtils.log_debug("-------- SUBSCRIBING TO CONSUMER ---------")

        topic_consumer_earliest.subscribe()

        InteropUtils.log_debug("-------- WAITING FOR MSGS ---------")

        self.wait_for_result(event, 20)
        InteropUtils.log_debug("-------- TEST DONE ---------")

    def test_timeseries_builder_works_with_any_number_type_and_none(self,
                                                                    test_name,
                                                                    topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        stream = topic_producer.create_stream()

        # Act
        stream.timeseries.buffer \
            .add_timestamp_nanoseconds(1) \
            .add_value("npy_float64", np.float64(42.0)) \
            .add_value("npy_int64", np.int64(42)) \
            .add_value("native_int", int(42)) \
            .add_value("native_float", float(42)) \
            .add_value("none", None)

        # Assert that no exception got raised

    def test_parameters_write_binary_read_binary_is_of_bytes(self,
                                                             test_name,
                                                             topic_consumer_earliest,
                                                             topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        print(read_data)
        binary_value = read_data.timestamps[0].parameters[
            "binary_param"].binary_value
        assert isinstance(binary_value, bytes)
        assert binary_value == bytes(bytearray("binary_param", "UTF-8"))

    def test_parameters_write_via_buffer_and_read(self,
                                                  test_name,
                                                  topic_consumer_earliest,
                                                  topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        assert len(read_data.timestamps) == 4

    def test_parameters_write_direct_and_read_as_timeseries_data_raw(self,
                                                                     test_name,
                                                                     topic_consumer_earliest,
                                                                     topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesDataRaw = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                stream_received.timeseries.on_raw_received = on_raw_received_handler

        def on_raw_received_handler(stream: qx.StreamConsumer,
                                    data: qx.TimeseriesDataRaw):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("========================")

        converted = read_data.convert_to_timeseriesdata()
        # evaluate neither contains more or less than should
        assert_timeseries_data_equal(written_data, converted)
        # and is done by checking both ways
        assert_timeseries_data_equal(converted, written_data)
        assert len(converted.timestamps) == 1
        assert len(converted.timestamps[0].parameters) == 5, "Missing parameter"

    def test_parameters_write_direct_and_read(self,
                                              test_name,
                                              topic_producer,
                                              topic_consumer_earliest):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)

        assert len(read_data.timestamps[0].parameters) == 4, "Missing parameter"
        # evaluate neither contains more or less than should
        assert_timeseries_data_equal(written_data, read_data)
        # and is done by checking both ways
        assert_timeseries_data_equal(read_data, written_data)

    def test_timeseries_data_raw_publish_via_buffer_and_consume(self,
                                                                test_name,
                                                                topic_consumer_earliest,
                                                                topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        consumed_timeseries_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_timeseries_data_handler

        def on_timeseries_data_handler(stream: qx.StreamConsumer,
                                       data: qx.TimeseriesData):
            nonlocal consumed_timeseries_data
            consumed_timeseries_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

        # Act
        stream = topic_producer.create_stream()

        published_timeseries_data_raw = qx.TimeseriesDataRaw()
        published_timeseries_data_raw.set_values(
            epoch=0,
            timestamps=[1, 2, 3],
            numeric_values={"numeric": [3, 12.32, None]},
            string_values={"string": ["one", None, "three"]},
            binary_values={"binary": [bytes("byte1", "utf-8"), None, None]},
            tag_values={"tag1": ["t1", "t2", None]},
        )

        stream.timeseries.publish(published_timeseries_data_raw)

        # Assert
        self.wait_for_result(event)
        print("------ PUBLISHED ------")
        print(published_timeseries_data_raw)
        print("------ CONSUMED ------")
        print(consumed_timeseries_data)

        assert len(
            consumed_timeseries_data.timestamps) == 3, "Received wrong number of timestamps"
        published_timeseries_data = published_timeseries_data_raw.convert_to_timeseriesdata()
        # evaluate neither contains more nor less than should, and is done by checking both ways
        assert_timeseries_data_equal(published_timeseries_data,
                                     consumed_timeseries_data)
        assert_timeseries_data_equal(consumed_timeseries_data,
                                     published_timeseries_data)

    def test_timeseries_data_timestamp_publish_direct_and_consume(self,
                                                                  test_name,
                                                                  topic_producer,
                                                                  topic_consumer_earliest):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        consumed_timeseries_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_timeseries_data_handler

        def on_timeseries_data_handler(stream: qx.StreamConsumer,
                                       data: qx.TimeseriesData):
            nonlocal consumed_timeseries_data
            consumed_timeseries_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        print("------ PUBLISHED ------")
        print(published_timestamp)
        print("------ CONSUMED ------")
        print(consumed_timeseries_data)

        assert len(
            consumed_timeseries_data.timestamps) == 1, "Multiple timestamps received"
        assert len(
            consumed_timeseries_data.timestamps[0].parameters) == 4, "Missing parameter"
        # evaluate neither contains more nor less than should, and is done by checking both ways
        assert_timestamps_equal(published_timestamp,
                                consumed_timeseries_data.timestamps[0])
        assert_timestamps_equal(consumed_timeseries_data.timestamps[0],
                                published_timestamp)

    def test_timeseries_data_timestamp_publish_via_buffer_and_consume(self,
                                                                      test_name,
                                                                      topic_producer,
                                                                      topic_consumer_earliest):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        consumed_timeseries_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal consumed_timeseries_data
            consumed_timeseries_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

        # Act
        stream = topic_producer.create_stream()
        stream.timeseries.buffer.packet_size = 10  # this is to enforce buffering until we want

        utc_now = datetime.utcnow()  # for assertion purposes save it
        timestamp1 = qx.TimeseriesData().add_timestamp(utc_now) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 123.43) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8"))) \
            .add_tag("Tag2", "tag two updated") \
            .add_tag("Tag3", "tag three") \
            .add_tags({"tag4": "tag4val", "tag5": "tag5val"})

        timestamp2 = qx.TimeseriesData().add_timestamp_milliseconds(123456789) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param3", "UTF-8"))

        stream.timeseries.buffer.publish(timestamp1)
        stream.timeseries.buffer.publish(timestamp2)

        stream.timeseries.buffer.flush()

        # Assert
        self.wait_for_result(event)
        print("------ PUBLISHED ------")
        print(timestamp1)
        print(timestamp2)
        print("------ CONSUMED ------")
        print(consumed_timeseries_data)
        assert len(consumed_timeseries_data.timestamps) == 2

    def test_parameters_write_direct_and_read_all_options(self,
                                                          test_name,
                                                          topic_producer,
                                                          topic_consumer_earliest):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

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

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data.to_dataframe()
            event.set()

        def on_parameter_data_raw_handler(stream: qx.StreamConsumer,
                                          data: qx.TimeseriesDataRaw):
            nonlocal read_data_raw
            read_data_raw = data.to_dataframe()
            event_raw.set()

        def on_parameter_dataframe_handler(stream: qx.StreamConsumer,
                                           data: pd.DataFrame):
            nonlocal read_pandas_dataframe
            read_pandas_dataframe = data
            event_pandas_dataframe.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        print("------ READ ------")
        print(read_data)
        self.wait_for_result(event_raw)
        print("------ READ RAW ------")
        print(read_data_raw)
        self.wait_for_result(event_pandas_dataframe)
        print("------ READ PANDAS ------")
        print(read_pandas_dataframe)

        assert_dataframes_equal(read_data, read_data_raw)
        assert_dataframes_equal(read_data_raw, read_pandas_dataframe)

    def test_parameters_write_pandas_via_buffer_and_read(self,
                                                         test_name,
                                                         topic_consumer_earliest,
                                                         topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.buffer_timeout = 100
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

        # Act
        stream = topic_producer.create_stream()
        written_data = qx.TimeseriesData()
        written_data.add_timestamp_nanoseconds(123456790) \
            .add_value("string_param", "value1") \
            .add_value("num_param", 83.756) \
            .add_value("binary_param", bytearray("binary_param", "UTF-8")) \
            .add_value("binary_param2", bytes(bytearray("binary_param2", "UTF-8")))
        written_data.to_dataframe()

        stream.timeseries.buffer.add_timestamp(datetime.utcnow()).add_value("a",
                                                                            "b").publish()

        stream.timeseries.buffer.flush()

        # Assert
        self.wait_for_result(event)
        print(read_data)
        assert len(read_data.timestamps) == 1

    def test_parameters_write_compare_panda_dataframe_different_exports(self,
                                                                        test_name,
                                                                        topic_producer,
                                                                        topic_consumer_earliest):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

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

        def on_data_released_handler(stream: qx.StreamConsumer,
                                     data: qx.TimeseriesData):
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

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        print("==== read_data ====")
        print(read_data)
        self.wait_for_result(event2)
        print("==== read_data raw ====")
        print(read_data_raw)
        self.wait_for_result(event3)
        print("==== read_data pandas ====")
        print(read_pandas_dataframe)

        assert_dataframes_equal(pf, read_data)
        assert_dataframes_equal(read_data, read_data_raw)
        assert_dataframes_equal(read_data_raw, read_pandas_dataframe)

    @pytest.mark.skip('TODO')
    def test_parameters_read_with_custom_trigger(self):
        pass

    @pytest.mark.skip('TODO')
    def test_parameters_read_with_custom_trigger_from_buffer_config(self):
        # TODO
        pass

    def test_parameters_write_pandas_direct_and_read(self,
                                                     test_name,
                                                     topic_consumer_earliest,
                                                     topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer()
                param_buffer.on_data_released = on_parameter_data_handler

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        print("------ READ ------")
        print(read_data)
        assert_timeseries_data_equal(written_data, read_data)
        assert_timeseries_data_equal(read_data, written_data)

    def test_parameters_read_with_parameter_filter(self,
                                                   test_name,
                                                   topic_consumer_earliest,
                                                   topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        read_data: qx.TimeseriesData = None

        def on_stream_received_handler(stream_received: qx.StreamConsumer):
            if stream.stream_id == stream_received.stream_id:
                param_buffer = stream_received.timeseries.create_buffer("param1",
                                                                        "param3")
                param_buffer.buffer_timeout = 500  # to prevent raising each timestamp on its own
                param_buffer.on_data_released = on_data_released_handler

        def on_data_released_handler(stream: qx.StreamConsumer,
                                     data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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

        self.wait_for_result(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("------ Expected ------")
        print(expected_data)
        # evaluate neither contains more or less than should
        # and is done by checking both ways
        assert_timeseries_data_equal(expected_data, read_data)
        assert_timeseries_data_equal(read_data, expected_data)

    def test_parameters_read_with_buffer_configuration(self,
                                                       test_name,
                                                       topic_consumer_earliest,
                                                       topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

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

        def on_parameter_data_handler(stream: qx.StreamConsumer,
                                      data: qx.TimeseriesData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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

        self.wait_for_result(event)
        print("------ Written ------")
        print(written_data)
        print("------ READ ------")
        print(read_data)
        print("------ Expected ------")
        print(expected_data)
        # evaluate neither contains more or less than should
        # and is done by checking both ways
        assert_timeseries_data_equal(expected_data, read_data)
        assert_timeseries_data_equal(read_data, expected_data)

    @pytest.mark.skip('TODO high importance')
    def test_parameters_read_with_filter(self, topic_consumer_earliest, topic_producer):
        pass

    def test_parameters_read_with_filter_from_buffer_config(self,
                                                            test_name,
                                                            topic_consumer_earliest,
                                                            topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        stream = None  # The outgoing stream
        event = threading.Event()  # used for assertion
        special_func_invocation_count = 0

        buffer_config = qx.TimeseriesBufferConfiguration()

        def filter_callback(
                parameter_data_timestamp: qx.TimeseriesDataTimestamp) -> bool:
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
                param_buffer = stream_received.timeseries.create_buffer(
                    buffer_configuration=buffer_config)

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

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
        self.wait_for_result(event)
        assert special_func_invocation_count == 3


class TestRawData(BaseIntegrationTest):

    def test_raw_read_write(self, test_name, raw_topic_consumer, raw_topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        received_messages: List[qx.KafkaMessage] = []
        event = threading.Event()  # used for assertion
        counter = 0

        def on_message_received_handler(topic: qx.RawTopicConsumer,
                                        message: qx.KafkaMessage):
            nonlocal received_messages, counter
            received_messages.append(message)
            counter = counter + 1
            if counter == 5:
                event.set()

        raw_topic_consumer.on_message_received = on_message_received_handler
        raw_topic_consumer.subscribe()

        # Act
        value_bytes = bytes("value bytes", "utf-8")
        value_bytearray = bytearray("value bytearray", "utf-8")
        key_bytearray = bytearray("key bytearray", "utf-8")
        headers = []
        headers.append(qx.KafkaHeader("header_key", "header_value"))

        raw_topic_producer.publish(value_bytes)
        raw_topic_producer.publish(value_bytearray)
        message_val_only = qx.KafkaMessage(value=value_bytes)
        raw_topic_producer.publish(message_val_only)
        message_with_key = qx.KafkaMessage(key=key_bytearray, value=value_bytearray)
        raw_topic_producer.publish(message_with_key)
        message_with_header = qx.KafkaMessage(key=key_bytearray, value=value_bytearray, headers=headers)
        raw_topic_producer.publish(message_with_header)

        self.wait_for_result(event)

        # Assert
        assert len(received_messages) == 5
        assert received_messages[0].value == value_bytes
        assert received_messages[1].value == value_bytearray
        assert received_messages[2].value == value_bytes
        assert received_messages[3].key == key_bytearray
        assert received_messages[3].value == value_bytearray
        assert received_messages[4].key == key_bytearray
        assert received_messages[4].value == value_bytearray
        assert len(received_messages[4].headers) == len(headers) and len(headers) == 1
        assert received_messages[4].headers[0].key == headers[0].key
        assert received_messages[4].headers[0].value == headers[0].value
        assert received_messages[4].headers[0].get_value_as_str() == headers[0].get_value_as_str()

        tpo = received_messages[1].topic_partition_offset
        assert tpo is not None
        assert tpo.partition is not None
        assert tpo.topic is not None
        assert tpo.topic_partition is not None
        assert tpo.offset is not None

        assert tpo.topic_partition.partition == tpo.partition
        assert tpo.topic_partition.topic == tpo.topic
        assert tpo.offset.value == 1
        assert not tpo.offset.is_special
        assert not tpo.partition.is_special

        assert str(tpo) == "integrationtests.test_integration.TestRawData.test_raw_read_write [[0]] @1"
        assert str(tpo.topic_partition) == "integrationtests.test_integration.TestRawData.test_raw_read_write [[0]]"
        assert str(tpo.offset) == "1"
        assert str(tpo.partition) == "[0]"



    def test_dispose_read_write(self, test_name, raw_topic_consumer, raw_topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        received_messages: List[qx.KafkaMessage] = []
        event = threading.Event()  # used for assertion
        counter = 0

        def on_message_received_handler(topic: qx.RawTopicConsumer,
                                        message: qx.KafkaMessage):
            nonlocal received_messages, counter
            received_messages.append(message)
            counter = counter + 1
            if counter == 3:
                event.set()

        raw_topic_consumer.on_message_received = on_message_received_handler
        raw_topic_consumer.subscribe()

        # Act
        message_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        raw_topic_producer.publish(message_bytes)
        message_bytearray = bytearray("Test Quix Raw with bytearray", "utf-8")
        raw_topic_producer.publish(message_bytearray)
        message_raw = qx.KafkaMessage(value=bytearray("Test Quix Raw message", "utf-8"))
        raw_topic_producer.publish(message_raw)

        raw_topic_producer.dispose()

        self.wait_for_result(event)

        # Assert
        assert len(received_messages) == 3

    def test_dispose_read_write(self, test_name, raw_topic_consumer, raw_topic_producer):
        # Arrange
        print(f'Starting Integration test "{test_name}"')

        received_messages: List[qx.KafkaMessage] = []
        event = threading.Event()  # used for assertion
        counter = 0

        def on_message_received_handler(topic: qx.RawTopicConsumer,
                                        message: qx.KafkaMessage):
            nonlocal received_messages, counter
            received_messages.append(message)
            counter = counter + 1
            if counter == 3:
                event.set()

        raw_topic_consumer.on_message_received = on_message_received_handler
        raw_topic_consumer.subscribe()

        # Act
        message_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        raw_topic_producer.publish(message_bytes)
        message_bytearray = bytearray("Test Quix Raw with bytearray", "utf-8")
        raw_topic_producer.publish(message_bytearray)
        message_raw = qx.KafkaMessage(value=bytearray("Test Quix Raw message", "utf-8"))
        raw_topic_producer.publish(message_raw)

        raw_topic_producer.flush()

        self.wait_for_result(event)

        # Assert
        assert len(received_messages) == 3


class TestStreamState(BaseIntegrationTest):

    def test_in_dict_state(self):
        app_state_manager = qx.App.get_state_manager()
        topic_state_manager = app_state_manager.get_topic_state_manager("topic")
        stream_state_manager = topic_state_manager.get_stream_state_manager("test-stream")
        dict_state = stream_state_manager.get_dict_state("test")

        assert "a" not in dict_state

        dict_state["a"] = "b"

        assert "a" in dict_state

    def test_stream_state_manager(self,
                                  test_name,
                                  topic_consumer_earliest,
                                  topic_producer,
                                  topic_name):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used for assertion

        print("---- Start publishing ----")
        output_stream = topic_producer.create_stream("test-stream")
        print("---- Subscribe to streams ----")

        def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
            rolling_sum_state = stream_consumer.get_dict_state("rollingsum",
                                                               lambda key: float(0))
            rolling_sum_state['somevalue'] = 5
            object_state = stream_consumer.get_dict_state("objectstate",
                                                          lambda key: {})

            dict = {}
            dict['key'] = 'value'
            object_state['somevalue'] = dict
            object_state['somevalue']['someotherkey'] = 'thatshouldntsave'
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

        print("---- Send some data to have a stream ----")
        output_stream.timeseries.buffer \
            .add_timestamp_nanoseconds(200) \
            .add_value("numeric-param", 34) \
            .publish()

        output_stream.timeseries.flush()
        self.wait_for_result(event, 10)
        print("Committing")
        topic_consumer_earliest.commit()
        print("Closed")

        # Assert
        app_state_manager = qx.App.get_state_manager()
        topic_state_manager = app_state_manager.get_topic_state_manager(topic_name)
        stream_state_manager = topic_state_manager.get_stream_state_manager(
            "test-stream")

        rolling_sum_state_somevalue = stream_state_manager.get_dict_state('rollingsum')[
            'somevalue']
        assert rolling_sum_state_somevalue == 5
        object_state_somevalue = stream_state_manager.get_dict_state('objectstate')[
            'somevalue']
        assert {'key': 'value'} == object_state_somevalue

    def test_stream_state_used_from_data_handler(self,
                                                 test_name,
                                                 topic_producer,
                                                 topic_consumer_earliest, topic_name):
        # Arrange
        repeat_count = 50
        print(f'Starting Integration test "{test_name}"')

        event = threading.Event()  # used for assertion

        print("---- Start publishing ----")
        actual_values_sum = []

        output_stream = topic_producer.create_stream("test-stream")
        print("---- Subscribe to streams ----")
        row_count_received = 0

        def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer,
                                          data: qx.TimeseriesData):
            dict_stream_state = stream_consumer.get_dict_state("app_state",
                                                               lambda missing_key: 0)
            scalar_stream_state = stream_consumer.get_scalar_state("scalar_state",
                                                                   lambda: 0)
            for row in data.timestamps:
                some_integer = row.parameters["some_integer"].numeric_value
                dict_stream_state["some_integer_sum"] += some_integer

                scalar_stream_state.value += some_integer

                actual_values_sum.append(dict_stream_state["some_integer_sum"])

            nonlocal row_count_received
            row_count_received += 1

            print(row_count_received)
            if row_count_received == repeat_count:
                event.set()

        def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
            stream_consumer.timeseries.on_data_received = on_dataframe_received_handler

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

        print("---- Send some data to have a stream ----")
        for i in range(1, repeat_count + 1):
            output_stream.timeseries.buffer \
                .add_timestamp_nanoseconds(i) \
                .add_value("some_integer", i) \
                .publish()

        output_stream.flush()

        self.wait_for_result(event, 10)
        print("Committing")
        topic_consumer_earliest.commit()
        print("Closed")

        # Assert
        app_state_manager = qx.App.get_state_manager()
        topic_state_manager = app_state_manager.get_topic_state_manager(topic_name)
        stream_state_manager = topic_state_manager.get_stream_state_manager(
            "test-stream")

        # Assert that the dict state is correct
        some_integer_sum_from_dict_state = \
            stream_state_manager.get_dict_state('app_state', state_type=int)[
                'some_integer_sum']
        assert repeat_count * (repeat_count + 1) / 2 == some_integer_sum_from_dict_state

        # Assert that the scalar state is correct
        some_integer_sum_from_scalar_state = stream_state_manager.get_scalar_state(
            'scalar_state')
        assert repeat_count * (
                repeat_count + 1) / 2 == some_integer_sum_from_scalar_state.value

        # The following should not raise exception, meant to raise only warning
        some_integer_sum_from_dict_state = \
            stream_state_manager.get_dict_state('app_state', state_type=float)[
                'some_integer_sum']
        assert repeat_count * (repeat_count + 1) / 2 == some_integer_sum_from_dict_state

        rolling_sum = 0
        print(actual_values_sum)
        for i in range(0, repeat_count):
            rolling_sum += i + 1
            assert rolling_sum == actual_values_sum[i]

    @pytest.mark.skip('TODO: fix test hanging sometimes')
    def test_stream_state_committed_from_background_thread(self,
                                                           test_name,
                                                           topic_producer,
                                                           topic_consumer_earliest,
                                                           topic_name):
        # Arrange
        duration = timedelta(seconds=10)
        print(f'Starting Integration test "{test_name}"')

        event = threading.Event()  # used to block sending until the consumer actually subscribed
        error_occurred = 0
        row_count_received = 0
        print("---- Start publishing ----")

        output_stream = topic_producer.create_stream("test-stream")
        print("---- Subscribe to streams ----")

        def on_dataframe_received_handler(stream_consumer: qx.StreamConsumer,
                                          data: qx.TimeseriesData):

            try:
                stream_state = stream_consumer.get_dict_state("app_state",
                                                              lambda x: 0,
                                                              int)  # default value for state name.

                for row in data.timestamps:
                    some_integer = row.parameters["some_integer"].numeric_value

                    stream_state["some_integer_sum"] += some_integer

                    nonlocal row_count_received
                    row_count_received += 1

                # print(f"Read {row_count_received}")
            except InteropException as ex:
                InteropUtils.log_debug(
                    f'Exception in consumer thread: {ex.message}')
                print(f'Exception in consumer thread {ex.message}')
                nonlocal error_occurred
                error_occurred += 1

        def on_stream_received_handler(stream_consumer: qx.StreamConsumer):
            stream_consumer.timeseries.on_data_received = on_dataframe_received_handler
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received_handler
        topic_consumer_earliest.subscribe()

        output_stream.properties.name = "juststartmystream"
        output_stream.flush()
        self.wait_for_result(event,
                             10)  # wait for 10 seconds max to get the stream read via consumer

        print("---- Send some data to have a stream ----")
        end = datetime.utcnow() + duration
        print(f"Test will end at {end}")

        def background_committer():
            while datetime.utcnow() <= end:
                try:
                    topic_consumer_earliest.commit()
                    # print(f'Background thread committed')
                except InteropException as ex:
                    nonlocal error_occurred
                    error_occurred += 1
                    InteropUtils.log_debug(
                        f'Exception in background thread: {ex.message}')
                    print(f'Exception in background {error_occurred}')
                time.sleep(0.03)

        committer_thread = threading.Thread(target=background_committer)
        committer_thread.start()

        def background_sender():
            iteration = 0
            nonlocal error_occurred
            while datetime.utcnow() <= end and error_occurred == 0:
                iteration += 1

                output_stream.timeseries.buffer \
                    .add_timestamp_nanoseconds(iteration) \
                    .add_value("some_integer", iteration) \
                    .publish()

                if iteration % 5 == 0:
                    output_stream.timeseries.buffer.flush()
                    time.sleep(0.005)
            print(f"Sender finished. Error occurred: {error_occurred}")

        sender_thread = threading.Thread(target=background_sender)
        sender_thread.start()

        sender_thread.join()

        output_stream.flush()

        print("Committing")
        topic_consumer_earliest.commit()

        # Assert
        print("Closed")
        assert error_occurred == 0
        time.sleep(1)


class TestApp(BaseIntegrationTest):
    def test_run_via_app(self,
                         test_name,
                         topic_consumer_earliest,
                         topic_name,
                         kafka_streaming_client):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used to trigger evaluation
        read_data: qx.EventData = None  # the object we will be testing here

        output_stream = None  # The outgoing stream
        cts = qx.CancellationTokenSource()  # used for interrupting the App

        def on_stream_received(stream: qx.StreamConsumer):
            if stream.stream_id == output_stream.stream_id:
                print("---- Test stream read {} ----".format(stream.stream_id))
                stream.events.on_data_received = on_event_data_handler

        def on_event_data_handler(stream: qx.StreamConsumer, data: qx.EventData):
            nonlocal read_data
            read_data = data
            event.set()

        topic_consumer_earliest.on_stream_received = on_stream_received

        # Act
        print("---- Start publishing ----")
        topic_producer = kafka_streaming_client.get_topic_producer(topic_name)
        output_stream = topic_producer.create_stream()

        print("---- Writing event data ----")
        output_stream.events.add_timestamp_nanoseconds(100) \
            .add_value("event1", "value1") \
            .add_tag("tag1", "tag1val") \
            .add_tags({"tag2": "tag2val", "tag3": "tag3val"}) \
            .publish()

        def event_callback():
            try:
                self.wait_for_result(event)
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
        qx.App.run(cts.token, before_shutdown=before_shutdown, subscribe=True)
        event_thread.join()
        assert read_data is not None
        assert shutdown_callback_value


class TestDictionary(BaseIntegrationTest):

    def test_stream_state(self,
                          test_name,
                          topic_consumer_earliest):
        print(f'Starting Integration test "{test_name}"')

        mngr = topic_consumer_earliest.get_state_manager()
        stmngr = mngr.get_stream_state_manager("test")

        state = stmngr.get_dict_state("thestatename")
        state["statekey"] = "statevalue"

        assert state["statekey"] == "statevalue"

class TestUseCases(BaseIntegrationTest):

    def test_multiple_retrieval_of_same_stream(self,
                                               test_name,
                                               topic_producer):
        # Also tests if the dispose/finalize is done correctly

        print(f'Starting Integration test "{test_name}"')

        stream_created = 0
        def on_stream_create(stream: qx.StreamProducer):
            nonlocal stream_created
            stream_created = stream_created + 1

        print("---- Get stream for the first time ----")

        topic_producer.get_or_create_stream("test", on_stream_create).properties.location = "test"

        print("---- Get stream for the second time ----")

        topic_producer.get_or_create_stream("test", on_stream_create).properties.location = "test"

        print("---- Assert ----")

        assert stream_created == 1


    def test_multiple_streams_created_and_closed(self,
                                           test_name,
                                           topic_producer,
                                           topic_consumer_earliest,
                                           topic_name):
        # Arrange
        print(f'Starting Integration test "{test_name}"')
        event = threading.Event()  # used for assertion


        print("---- Subscribe to streams ----")

        total = 5
        counter = 0
        def received_stream_handler(received_stream: qx.StreamConsumer):
            print(f"Received stream {received_stream.stream_id}")

            InteropUtils.log_debug("Getting Dict State")
            dict_state = received_stream.get_dict_state("dict_stuff")
            dict_state.on_flushed = lambda : InteropUtils.log_debug(f"Flushed {received_stream.stream_id}")
            dict_state.on_flushing = lambda: InteropUtils.log_debug(f"Flushing {received_stream.stream_id}")
            InteropUtils.log_debug("Setting Dict State value")
            dict_state["someKey"] = "somevalue"
            InteropUtils.log_debug("Done Setting Dict State value")

            InteropUtils.log_debug("Getting Scalar State")
            scalar_state = received_stream.get_scalar_state("scalar_stuff")
            scalar_state.on_flushed = lambda: InteropUtils.log_debug(f"Flushed {received_stream.stream_id}")
            scalar_state.on_flushing = lambda: InteropUtils.log_debug(f"Flushing {received_stream.stream_id}")
            InteropUtils.log_debug("Setting Scalar State value")
            scalar_state.value = "some_scalar_value"
            InteropUtils.log_debug("Done Setting Scalar State value")


            def stream_closes_handler(stream, mode):
                print(f"Stream closes: {received_stream.stream_id}")
                nonlocal counter
                nonlocal total
                counter = counter + 1
                if counter == total:
                    event.set()

            received_stream.on_stream_closed = stream_closes_handler

        topic_consumer_earliest.on_stream_received = received_stream_handler

        event = threading.Event()  # used to block sending until the consumer actually subscribed
        print("---- Start publishing ----")

        for i in range(0, total):
            output_stream = topic_producer.create_stream(f"test-stream-{i}")
            print(output_stream.stream_id)
            output_stream.close()

        InteropUtils.log_debug("-------- FLUSHING PRODUCER ---------")
        topic_producer.flush()

        InteropUtils.log_debug("-------- SUBSCRIBING TO CONSUMER ---------")

        topic_consumer_earliest.subscribe()

        InteropUtils.log_debug("-------- WAITING FOR MSGS ---------")

        self.wait_for_result(event, 20)
        InteropUtils.log_debug("-------- COMMITTING ---------")
        topic_consumer_earliest.commit()
        InteropUtils.log_debug("-------- COMMIT DONE ---------")
        InteropUtils.log_debug("-------- TEST DONE ---------")

