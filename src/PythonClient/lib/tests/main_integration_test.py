# This is the very first integration test created for the python wrapper.
# It tests most of the client and provides samples all around
# Best would be to split it up with a revised method of test-suite, but for now will leave it here

import datetime
import threading

import pandas as pd
import time

from quixstreaming import *
from quixstreaming.models.parametervalue import ParameterValueType
from quixstreaming.app import App
from quixstreaming.models.streampackage import StreamPackage
from tests.cancellationtokensource import CancellationTokenSource

Logging.update_factory(LogLevel.Debug)
client = StreamingClient('127.0.0.1:9092', None)
output_topic = client.open_output_topic('Telemetry2')
commit_settings = CommitOptions()
commit_settings.commit_every = 10000
commit_settings.commit_interval = None
commit_settings.auto_commit_enabled = False
input_topic = client.open_input_topic('Telemetry2', "my-test-consumer-group", commit_settings=commit_settings, auto_offset_reset=AutoOffsetReset.Earliest)
def on_streams_revoked_handler(readers: [StreamReader]):
    try:
        for reader in readers:
            print("Stream " + reader.stream_id + " got revoked")
    except:
        print("Exception occurred in on_streams_revoked_handler: " + sys.exc_info()[1])
    print(readers)

input_topic.on_streams_revoked += on_streams_revoked_handler

def on_committed_handler():
    print("Committed!")

input_topic.on_committed += on_committed_handler

def on_committing_handler():
    print("Committing!")

input_topic.on_committing += on_committing_handler

def on_revoking_handler():
    print("Revoking!")

input_topic.on_revoking += on_revoking_handler

import sys


#read streams
test_close_count = 0
test_properties_count = 0
test_event_data_count = 0
test_event_definition_count = 0
test_parameter_data_count = 0
test_parameter_data_filtered_count = 0
test_parameter_definition_count = 0

def read_stream(new_stream : StreamReader):
    print("New Stream read!")

    def on_stream_closed_handler(end_type: StreamEndType):
        try:
            print("Stream", new_stream.stream_id, "closed with", end_type)
            global test_close_count
            test_close_count = test_close_count + 1
        except:
            print("Exception occurred in on_stream_closed_handler: " + sys.exc_info()[1])
    new_stream.on_stream_closed += on_stream_closed_handler

    def on_stream_properties_changed_handler():
        try:
            print("Stream properties read for stream: " + new_stream.stream_id)
            print("Name", new_stream.properties.name, sep=": ")
            print("Location", new_stream.properties.location, sep=": ")
            print("Metadata", new_stream.properties.metadata, sep=": ")
            # print(properties.metadata["meta"]) # or by index
            print("Parents", new_stream.properties.parents, sep=": ")
            # print(properties.parents[0]) # or by index
            print("TimeOfRecording", new_stream.properties.time_of_recording, sep=": ")
            global test_properties_count
            test_properties_count = test_properties_count + 1
        except:
            print("Exception occurred in on_stream_properties_changed_handler: " + sys.exc_info()[1])

    new_stream.properties.on_changed += on_stream_properties_changed_handler

    def on_parameter_data_handler(data: ParameterData):
        try:
            print("Committing")
            # input_topic.commit()
            global test_parameter_data_count
            print("Parameter data read for stream: " + new_stream.stream_id)
            # can convert to panda if wanted
            pf = data.to_panda_frame()
            # but for the following code, using original data, this is how you convert back:
            data = ParameterData.from_panda_frame(pf)
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

    def on_parameters_pandas_dataframe_handler(data: pd.DataFrame):
        print("RECEIVED DATAFRAME")
        print(data)

    new_stream.parameters.on_read_pandas += on_parameters_pandas_dataframe_handler

    param_buffer = new_stream.parameters.create_buffer()
    param_buffer.on_read += on_parameter_data_handler

    def on_parameter_data_filtered_handler(data: ParameterData):
        try:
            global test_parameter_data_filtered_count
            print("Parameter data read for stream: " + new_stream.stream_id)
            # can convert to panda if wanted
            pf = data.to_panda_frame()
            # but for the following code, using original data, this is how you convert back:
            data = ParameterData.from_panda_frame(pf)
            print("  Length:", len(data.timestamps))
            for index, val in enumerate(data.timestamps):
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

    param_buffer_filtered = new_stream.parameters.create_buffer("panda_param", "bufferless_2")
    param_buffer_filtered.on_read += on_parameter_data_filtered_handler

    def on_parameter_definitions_changed_handler():
        try:
            print("Parameter definitions read for stream: " + new_stream.stream_id)

            definitions = new_stream.parameters.definitions

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

    new_stream.parameters.on_definitions_changed += on_parameter_definitions_changed_handler

    def on_event_definitions_changed_handler():
        try:
            print("Event definitions read for stream: " + new_stream.stream_id)

            definitions = new_stream.events.definitions

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

    new_stream.events.on_definitions_changed += on_event_definitions_changed_handler

    def on_event_data_handler(data: EventData):
        try:
            print("Event data read for stream: " + new_stream.stream_id)
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

    new_stream.events.on_read += on_event_data_handler

    def on_package_received_handler(sender: StreamReader, package: StreamPackage):
        try:
            print("Package: " + package.to_json())
        except:
            print("Exception occurred in on_package_received_handler: " + sys.exc_info()[1])

    new_stream.on_package_received += on_package_received_handler

input_topic.on_stream_received += read_stream

try:

    stream = output_topic.create_stream()
    def on_write_exception_handler(ex: BaseException):
        print(ex.args[0])
    stream.on_write_exception += on_write_exception_handler
    print("---- Setting stream properties ----")
    stream.properties.name = "ABCDE"
    stream.properties.location = "/test/location"
    stream.properties.metadata["meta"] = "is"
    stream.properties.metadata["working"] = "well"
    stream.properties.parents.append("testParentId1")
    stream.properties.parents.append("testParentId2")
    stream.properties.time_of_recording = datetime.datetime.utcnow()
    print("---- Sending parameter definitions ----")
    stream.parameters.add_definition("param_at_root", "Root Parameter", "This is a root parameter as there is no default location")
    stream.parameters.default_location = "/Base/"
    stream.parameters.add_definition("string_param", "String parameter", "This is a string parameter description")\
                     .add_definition("parameter2", description="This is parameter 2").set_unit("%").set_format("{0}%").set_range(0, 100)
    stream.parameters.add_location("/NotBase").add_definition("num_param").set_unit("kph").set_range(0, 300).set_custom_properties("{test}")
    print("---- Sending parameter data ----")
    stream.parameters.buffer.packet_size = 500
    stream.parameters.buffer.time_span_in_nanoseconds = 5e+9  # 5000 ms
    stream.parameters.buffer.buffer_timeout = 400
    stream.parameters.buffer.default_location = "/default"
    stream.parameters.buffer.default_tags["Tag1"] = "tag one"
    stream.parameters.buffer.default_tags["Tag2"] = "tag two"
    stream.parameters.buffer.epoch = datetime.datetime(2018, 1, 1)

    # Send parameter Data for datetime
    stream.parameters.buffer\
        .add_timestamp(datetime.datetime.utcnow())\
        .add_value("string_param", "value1")\
        .add_value("num_param", 123.43) \
        .add_value("binary_param", bytearray("binary_val1", "UTF-8")) \
        .add_tag("Tag2", "tag two updated")\
        .add_tag("Tag3", "tag three")\
        .write()

    # Send parameter data in nanoseconds relative to epoch
    stream.parameters.buffer\
        .add_timestamp_nanoseconds(123456789)\
        .add_value("string_param", "value1")\
        .add_value("num_param", 83.756) \
        .add_value("binary_param", bytearray("binary_val2", "UTF-8")) \
        .write()

    # Send parameter data in timedelta relative to a new epoch
    stream.parameters.buffer.epoch = datetime.datetime(2018, 1, 2)
    stream.parameters.buffer\
        .add_timestamp(datetime.timedelta(seconds=1, milliseconds=555))\
        .add_value("num_param", 123.32) \
        .add_value("binary_param", bytearray("binary_val3", "UTF-8")) \
        .write()

    # Send data as panda frame in nanoseconds,
    # relative to stream.parameter.epoch, which by default is unix epoch 01/01/1970
    df = pd.DataFrame({'time': [1, 5, 10],
                       'panda_param': [123.2, None, 12],
                       'panda_param2': ["val1", "val2", None],
                       'TAG__Tag1': ["v1", 2, "v3"],
                       'TAG__Tag2': [1, 2, 3]})
    stream.parameters.write(df)

    # send parameter data without buffering
    parameterData = ParameterData()
    parameterData.add_timestamp_nanoseconds(10)\
        .add_value("bufferless_1", 1)\
        .add_value("bufferless_2", "test") \
        .add_value("bufferless_3", "test") \
        .add_value("bufferless_4", bytearray("Bufferless_4", "UTF-8")) \
        .remove_value("bufferless_3")\
        .add_tag("tag1", "tag1val")\
        .add_tag("tag2", "tag2val")\
        .remove_tag("tag2")

    stream.parameters.write(parameterData)


    print("---- Sending event definitions ----")
    stream.events.add_definition("event_at_root", "Root Event", "This is a root event as there is no default location")
    stream.events.default_location = "/Base"
    stream.events.add_definition("event_1", "Event One", "This is test event number one")\
                 .add_definition("event_2", description="This is event 2").set_level(EventLevel.Debug).set_custom_properties("{test prop for event}")
    stream.events.add_location("/NotBase").add_definition("event_3").set_level(EventLevel.Critical)
    print("---- Sending event data ----")
    stream.events.default_location = "/default"
    stream.events.default_tags["Tag1"] = "tag one"
    stream.events.default_tags["Tag2"] = "tag two"
    stream.events.epoch = datetime.datetime(2018, 1, 1)
    stream.events\
        .add_timestamp(datetime.datetime.utcnow())\
        .add_value("event_1", "event value 1")\
        .add_tag("Tag2", "tag two updated")\
        .add_tag("Tag3", "tag three")\
        .write()
    stream.events\
        .add_timestamp_nanoseconds(123456789)\
        .add_value("event_1", "event value 1.1")\
        .add_value("event_2", "event value 2")\
        .write()
    stream.events.epoch = datetime.datetime(1970, 1, 1)
    stream.events\
        .add_timestamp(datetime.timedelta(seconds=1, milliseconds=555))\
        .add_value("event_3", "3232")\
        .write()

    ed = EventData("event4", 12345678, "direct event val")
    ed.add_tag("a", "b")\
      .add_tag("b", "c")\
      .add_tag("c", "d")\
      .remove_tag("c")
    stream.events.write(ed)

    print("Closing")
    stream.close(StreamEndType.Aborted)  # Comment this out when testing stream revocation & set time.sleep to higher
    print("Closed")
except:
    print("Exception occurred while sending stream: " + sys.exc_info()[1])


def setupShutdownAndStart():
    # Set up, so the application only runs for several seconds
    cts = CancellationTokenSource()

    def shutdownCallback():
        print("Sleeping for 5 seconds.")
        time.sleep(5)
        cts.cancel()

    event_thread = threading.Thread(target=shutdownCallback)
    event_thread.start()
    App.run(cts.token)


setupShutdownAndStart()

# do a few VERY SIMPLE validation
print("======== Test result ==========")
if test_close_count != 1:
    print("Test: Close failed with " + str(test_close_count) + " reads")
else:
    print("Test: Close succeeded with " + str(test_close_count) + " reads")

if test_properties_count != 1:
    print("Test: Properties failed with " + str(test_properties_count) + " reads (might fail due to re-broadcast logic)")  # see quix-1229
else:
    print("Test: Properties succeeded with " + str(test_properties_count) + " reads")

if test_event_data_count != 5:
    print("Test: Events data failed with " + str(test_event_data_count) + " reads")
else:
    print("Test: Events data succeeded with " + str(test_event_data_count) + " reads")

if test_event_definition_count != 4:
    print("Test: Event definitions failed with " + str(test_event_definition_count) + " reads")
else:
    print("Test: Event definitions succeeded with " + str(test_event_definition_count) + " reads")

if test_parameter_data_count != 15:
    print("Test: Parameter data failed with " + str(test_parameter_data_count) + " reads")
else:
    print("Test: Parameter data succeeded with " + str(test_parameter_data_count) + " reads")

if test_parameter_data_filtered_count != 3:
    print("Test: Parameter data filtered failed with " + str(test_parameter_data_filtered_count) + " reads")
else:
    print("Test: Parameter data filtered succeeded with " + str(test_parameter_data_filtered_count) + " reads")

if test_parameter_definition_count != 4:
    print("Test: Parameter definitions failed with " + str(test_parameter_definition_count) + " reads")
else:
    print("Test: Parameter definitions succeeded with " + str(test_parameter_definition_count) + " reads")
print("Done")
