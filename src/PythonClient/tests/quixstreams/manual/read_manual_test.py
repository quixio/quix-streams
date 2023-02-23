# This is the very first integration test created for the python wrapper.
# It tests most of the client and provides samples all around
# Best would be to split it up with a revised method of test-suite, but for now will leave it here
import datetime
import sys

import pandas as pd

from src.quixstreams.logging import Logging, LogLevel

from src import quixstreams as qx
from src.quixstreams.models.parametervalue import ParameterValueType
from src.quixstreams.app import App
from src.quixstreams.models.streampackage import StreamPackage

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
#InteropUtils.enable_debug()

Logging.update_factory(LogLevel.Debug)
client = qx.KafkaStreamingClient('127.0.0.1:9092', None)
commit_settings = qx.CommitOptions()
commit_settings.commit_every = 10000
commit_settings.commit_interval = None
commit_settings.auto_commit_enabled = False
topic_consumer = client.create_topic_consumer('generated-data', None, commit_settings=commit_settings, auto_offset_reset=qx.AutoOffsetReset.Earliest)


def on_streams_revoked_handler(topic: qx.TopicConsumer, readers: [qx.StreamConsumer]):
    try:
        for reader in readers:
            print("Stream " + reader.stream_id + " got revoked")
    except:
        print("Exception occurred in on_streams_revoked_handler: " + sys.exc_info()[1])
    print(readers)


def on_committed_handler(topic_consumer: qx.TopicConsumer):
    print("Committed!")


def on_committing_handler(topic_consumer: qx.TopicConsumer):
    print("Committing!")


def on_revoking_handler(topic_consumer: qx.topicconsumer):
    print("Revoking!")


topic_consumer.on_streams_revoked = on_streams_revoked_handler
topic_consumer.on_committed = on_committed_handler
topic_consumer.on_committing = on_committing_handler
topic_consumer.on_revoking = on_revoking_handler

#read streams
test_close_count = 0
test_properties_count = 0
test_event_data_count = 0
test_event_definition_count = 0
test_parameter_data_count = 0
test_parameter_data_filtered_count = 0
test_parameter_definition_count = 0

start = None


def read_stream(topic_consumer: qx.topicconsumer, new_stream: qx.StreamConsumer):
    import datetime
    global start
    if start is None:
        start = datetime.datetime.now()

    print("New Stream read!" + str(datetime.datetime.now()))
    new_stream.on_stream_closed = on_stream_closed_handler
    # new_stream.properties.on_changed = on_stream_properties_changed_handler
    # new_stream.parameters.on_read_dataframe = on_parameters_dataframe_handler
    # param_buffer = new_stream.parameters.create_buffer()
    # param_buffer.on_read = on_parameter_data_handler

    # param_buffer_filtered = new_stream.parameters.create_buffer("numeric param 1", "string param 2")
    # param_buffer_filtered.on_read = on_parameter_data_filtered_handler
    # new_stream.parameters.on_definitions_changed = on_parameter_definitions_changed_handler
    # new_stream.events.on_definitions_changed = on_event_definitions_changed_handler
    # new_stream.events.on_read = on_event_data_handler

    # TODO implementation missing
    # new_stream.on_package_received = on_package_received_handler


def on_stream_closed_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer, end_type: qx.StreamEndType):
    try:
        print("Stream", stream.stream_id, "closed with", end_type, " started at ", start, " finished at ", datetime.datetime.now())
        global test_close_count
        test_close_count = test_close_count + 1
    except:
        print("Exception occurred in on_stream_closed_handler: " + sys.exc_info()[1])


def on_stream_properties_changed_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer):
    try:
        print("Stream properties read for stream: " + stream.stream_id)
        print("Name", stream.properties.name, sep=": ")
        print("Location", stream.properties.location, sep=": ")
        print("Metadata", stream.properties.metadata, sep=": ")
        # print(properties.metadata["meta"]) # or by index
        print("Parents", stream.properties.parents, sep=": ")
        # print(properties.parents[0]) # or by index
        print("TimeOfRecording", stream.properties.time_of_recording, sep=": ")
        global test_properties_count
        test_properties_count = test_properties_count + 1
    except:
        print("Exception occurred in on_stream_properties_changed_handler: " + sys.exc_info()[1])


def on_parameters_dataframe_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer, data: pd.DataFrame):
    print("RECEIVED DATAFRAME")
    print(data)


def on_parameter_data_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer, data: qx.TimeseriesData):
    with data:
        try:
            print("Committing")
            # topic_consumer.commit()
            global test_parameter_data_count
            print("Timeseries data read for stream: " + stream.stream_id)
            # can convert to panda if wanted
            pf = data.to_panda_dataframe()
            # but for the following code, using original data, this is how you convert back:
            pfdata = qx.TimeseriesData.from_panda_dataframe(pf)
            with pfdata:
                print("  Length:", len(data.timestamps))
                for index, val in enumerate(data.timestamps):
                    print("    Time:", val.timestamp_nanoseconds)
                    print("      Tags: ", str(val.tags))
                    print("      Params:")
                    for param_id, param_val in val.parameters.items():
                        test_parameter_data_count = test_parameter_data_count + 1
                        if param_val.type == ParameterValueType.Numeric:
                            print("        " + str(param_id) + ": " + str(param_val.numeric_value))
                            continue
                        if param_val.type == ParameterValueType.String:
                            print("        " + str(param_id) + ": " + str(param_val.string_value))
                            continue
                        if param_val.type == ParameterValueType.Binary:
                            print("        " + str(param_id) + ": byte[" + str(len(param_val.binary_value)) + "]")
                            continue
        except:
            print("Exception occurred in on_parameter_data_handler: " + sys.exc_info()[1])


def on_parameter_data_filtered_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer, data: qx.TimeseriesData):
    with data:
        try:
            def print(*args, **kwargs):
                pass
            global test_parameter_data_filtered_count
            print("Timeseries data read for stream: " + stream.stream_id)
            # can convert to panda if wanted
            pf = data.to_panda_dataframe()
            # but for the following code, using original data, this is how you convert back:
            pfdata = qx.TimeseriesData.from_panda_dataframe(pf)
            with pfdata:
                print("  Length:", len(pfdata.timestamps))
                for index, val in enumerate(pfdata.timestamps):
                    print("    Time:", val.timestamp_nanoseconds)
                    print("      Tags: ", str(val.tags))
                    print("      Params:")
                    for param_id, param_val in val.parameters.items():
                        test_parameter_data_filtered_count = test_parameter_data_filtered_count + 1
                        if param_val.type == ParameterValueType.Numeric:
                            print("        " + str(param_id) + ": " + str(param_val.numeric_value))
                            continue
                        if param_val.type == ParameterValueType.String:
                            print("        " + str(param_id) + ": " + str(param_val.string_value))
                            continue
                        if param_val.type == ParameterValueType.Binary:
                            print("        " + str(param_id) + ": byte[" + str(len(param_val.binary_value)) + "]")
                            continue
        except:
            print("Exception occurred in on_parameter_data_handler: " + sys.exc_info()[1])


def on_parameter_definitions_changed_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer):
    try:
        print("Parameter definitions read for stream: " + stream.stream_id)

        definitions = stream.parameters.definitions

        print("==== Parameter Definitions ====")
        for definition in definitions:
            global test_parameter_definition_count
            test_parameter_definition_count = test_parameter_definition_count + 1
            print("Definition: " + str(definition.id))
            print(" |_Name: " + str(definition.name))
            print(" |_Location: " + str(definition.location))
            print(" |_Description: " + str(definition.description))
            print(" |_Format: " + str(definition.format))
            print(" |_Unit: " + str(definition.unit))
            print(" |_Maximum value: " + str(definition.maximum_value))
            print(" |_Minimum value: " + str(definition.minimum_value))
            print(" |_Custom_properties: " + str(definition.custom_properties))
    except:
        print("Exception occurred in on_parameter_definitions_changed_handler: " + sys.exc_info()[1])


def on_event_definitions_changed_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer):
    try:
        print("Event definitions read for stream: " + stream.stream_id)

        definitions = stream.events.definitions

        print("==== Event Definitions ====")
        for definition in definitions:
            global test_event_definition_count
            test_event_definition_count = test_event_definition_count + 1
            print("Definition: " + str(definition.id))
            print(" |_Name: " + str(definition.name))
            print(" |_Location: " + str(definition.location))
            print(" |_Description: " + str(definition.description))
            print(" |_Level: " + str(definition.level))
            print(" |_Custom_properties: " + str(definition.custom_properties))
    except:
        print("Exception occurred in on_event_definitions_changed_handler: " + sys.exc_info()[1])


def on_event_data_handler(topic_consumer: qx.topicconsumer, stream: qx.StreamConsumer, data: qx.EventData):
    with data:
        try:
            print("Event data read for stream: " + stream.stream_id)
            print("  Time: " + str(data.timestamp_nanoseconds) + " ns")
            print("  Time: " + str(data.timestamp_milliseconds) + " ms")
            print("  Time:", data.timestamp_as_time_span)
            print("  Time:", data.timestamp)
            print("  Id:", data.id)
            print("  Tags: " + str(data.tags))
            print("  Value: " + data.value)
            global test_event_data_count
            test_event_data_count = test_event_data_count + 1
        except:
            print("Exception occurred in on_event_data_handler: " + sys.exc_info()[1])


def on_package_received_handler(topic_consumer: qx.topicconsumer, sender: qx.StreamConsumer, package: StreamPackage):
    with package:
        try:
            print("Package: " + package.to_json())
        except:
            print("Exception occurred in on_package_received_handler: " + sys.exc_info()[1])


topic_consumer.on_stream_received = read_stream

App.run()