# This is the very first integration test created for the python wrapper.
# It tests most of the client and provides samples all around
# Best would be to split it up with a revised method of test-suite, but for now will leave it here
import pandas
import datetime
import random
import threading

import pandas as pd
import time

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
#InteropUtils.enable_debug()

from src import quixstreams as qx
from src.quixstreams.models.parametervalue import ParameterValueType

client = qx.KafkaStreamingClient('127.0.0.1:9092', None)
commit_settings = qx.CommitOptions()
commit_settings.commit_every = 10000
commit_settings.commit_interval = None
commit_settings.auto_commit_enabled = False
output_topic = client.open_output_topic('generated-data')

number_of_stream = 10

number_of_parameters_numeric = 5
number_of_parameters_string = 5
number_of_parameters_binary = 0
number_of_parameters_tag = 2  # should be lower than number_of_parameters_default_tag
number_of_parameters_default_tag = 5

number_of_events = 5
number_of_events_tag = 2  # should be lower than number_of_events_default_tag
number_of_events_default_tag = 5

number_of_streamproperties_parents = 5
number_of_streamproperties_metadata = 5

number_of_data_loop = 100
number_of_data_points_per_loop = 10

def on_write_exception_handler(stream: qx.StreamWriter, ex: BaseException):
    print(ex.args[0])

for stream_number in range(number_of_stream):
    print(f"--- Sending Stream {stream_number} ---")
    stream = output_topic.create_stream()
    stream.on_write_exception = on_write_exception_handler

    print(f"--- Setting stream properties for stream {stream_number} ---")
    stream.properties.name = f"Stream {stream_number}"
    stream.properties.location = "/generated/python"
    stream.properties.time_of_recording = datetime.datetime.utcnow()
    for metadata_number in range(number_of_streamproperties_metadata):
        stream.properties.metadata[f"meta {metadata_number}"] = f"meta value {metadata_number}"

    for parent_number in range(number_of_streamproperties_parents):
        stream.properties.parents.append(f"stream parent id {parent_number}")

    parameter_names_numerics = [f"numeric param {number}" for number in range(number_of_parameters_numeric)]
    parameter_names_strings = [f"string param {number}" for number in range(number_of_parameters_string)]
    parameter_names_binary = [f"binary param {number}" for number in range(number_of_parameters_binary)]
    parameter_tags = [f"parameter tag {number}" for number in range(max(number_of_parameters_tag, number_of_parameters_tag))]
    event_names = [f"event {number}" for number in range(number_of_events)]
    event_tags = [f"event tag {number}" for number in range(max(number_of_parameters_tag, number_of_parameters_tag))]

    print(f"--- Sending parameter definitions for stream {stream_number} ---")
    stream.parameters.default_location = "/params/"
    for parameter in parameter_names_numerics:
        stream.parameters.add_location("/params/numeric") \
            .add_definition(parameter, parameter, f"This is {parameter}") \
            .set_unit("hz").set_format("{0} hz").set_range(-1000, 1000)

    for parameter in parameter_names_strings:
        stream.parameters.add_location("/params/string") \
            .add_definition(parameter, parameter, f"This is {parameter}") \
            .set_custom_properties("{\"generator\":\"python\"")

    for parameter in parameter_names_binary:
        stream.parameters.add_location("/params/binary") \
            .add_definition(parameter, parameter, f"This is {parameter}") \
            .set_custom_properties("{\"type\":\"base64\"")

    print(f"--- Sending event definitions for stream {stream_number} ---")
    stream.events.default_location = "/events/overridethis"
    for index, event in enumerate(event_names):
        level = qx.EventLevel(index % 5 + 1)
        stream.events.add_location("/events/") \
            .add_definition(event, event, f"This is {event}") \
            .set_level(level) \
            .set_custom_properties("{\"test\":\"value\"")

    print(f"--- Setting default tags for stream {stream_number} ---")
    for index, tag in enumerate(parameter_tags):
        if index == number_of_parameters_default_tag:
            break
        stream.parameters.buffer.default_tags[tag] = f"parameter tag value {index}"

    for index, tag in enumerate(event_tags):
        if index == number_of_events_default_tag:
            break
        stream.events.default_tags[tag] = f"event tag value {index}"

    print(f"--- Sending data for stream {stream_number} ---")
    for loop_index in range(number_of_data_loop):
        variant = loop_index % 3

        # use buffer
        if variant == 0:
            param_builder = stream.parameters.buffer
            event_builder = stream.events
            for points_index in range(number_of_data_points_per_loop):
                # send parameters
                param_tsbuilder = param_builder.add_timestamp(datetime.datetime.utcnow())

                for parameter in parameter_names_numerics:
                    param_tsbuilder = param_tsbuilder.add_value(parameter, random.uniform(-1000, 1000))

                for parameter in parameter_names_strings:
                    numericrandom = round(random.random()*20)
                    param_tsbuilder = param_tsbuilder.add_value(parameter, str(numericrandom))

                for parameter in parameter_names_binary:
                    numericrandom = round(random.random() * 20)
                    param_tsbuilder = param_tsbuilder.add_value(parameter, bytearray(str(numericrandom), "UTF-8"))

                for index, tag in enumerate(parameter_tags):
                    if index == number_of_parameters_tag:
                        break
                    param_tsbuilder.add_tag(tag, f"new parameter tag value {index}")

                param_tsbuilder.write()
                # send events
                event_tsbuilder = event_builder.add_timestamp(datetime.datetime.utcnow())

                for event in event_names:
                    event_tsbuilder = event_tsbuilder.add_value(event, f"{{\"value\": {random.uniform(-1000, 1000)}}}")

                for index, tag in enumerate(event_tags):
                    if index == number_of_events_tag:
                        break
                    event_tsbuilder.add_tag(tag, f"new event tag value {index}")

                event_tsbuilder.write()

            param_builder.flush()
            event_builder.flush()

        # use writer directly rather than via buffer
        if variant == 1:
            pdata = qx.ParameterData()
            with pdata:
                for points_index in range(number_of_data_points_per_loop):
                    # send parameters
                    pdts = pdata.add_timestamp(datetime.datetime.utcnow())

                    for parameter in parameter_names_numerics:
                        pdts = pdts.add_value(parameter, random.uniform(-1000, 1000))

                    for parameter in parameter_names_strings:
                        numericrandom = round(random.random()*20)
                        pdts = pdts.add_value(parameter, str(numericrandom))

                    for parameter in parameter_names_binary:
                        numericrandom = round(random.random() * 20)
                        pdts = pdts.add_value(parameter, bytearray(str(numericrandom), "UTF-8"))

                    for index, tag in enumerate(parameter_tags):
                        if index == number_of_parameters_tag:
                            break
                        pdts.add_tag(tag, f"new parameter tag value {index}")

                stream.parameters.write(pdata)

        if variant == 2:

            headers = [
                "time",
                *map(lambda x: x, parameter_names_numerics),
                *map(lambda x: x, parameter_names_strings),
                *map(lambda x: x, parameter_names_binary),
                *map(lambda x: f"TAG__{x}", parameter_tags[:number_of_parameters_tag])
            ]

            rows = [None] * number_of_data_points_per_loop
            item = datetime.datetime.utcnow()
            for rowindex in range(number_of_data_points_per_loop):
                row = [None] * len(headers)

                row[0] = item + datetime.timedelta(microseconds=rowindex)

                i = 1

                # Add numerics
                for param in parameter_names_numerics:
                    row[i] = random.uniform(-1000, 1000)
                    i = i + 1

                # Add strings
                for param in parameter_names_strings:
                    numericrandom = round(random.random() * 20)
                    row[i] = str(numericrandom)
                    i = i + 1

                # Add binaries
                for param in parameter_names_binary:
                    numericrandom = round(random.random() * 20)
                    row[i] = bytearray(str(numericrandom), "UTF-8")
                    i = i + 1

                # add tags
                for index in range(number_of_parameters_tag):
                    row[i] = f"new parameter tag value {index}"
                    i = i + 1

                rows[rowindex] = row

            pdf = pd.DataFrame(rows, columns=headers)
            stream.parameters.write(pdf)

    print(f"--- Closing stream {stream_number} ---")
    endtype = qx.StreamEndType(stream_number % 2 + 1)
    stream.close(qx.StreamEndType.Aborted)

output_topic.dispose()
exit(0)

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

# Send data as panda data frame in nanoseconds,
# relative to stream.parameter.epoch, which by default is unix epoch 01/01/1970
df = pd.DataFrame({'time': [1, 5, 10],
                   'panda_param': [123.2, None, 12],
                   'panda_param2': ["val1", "val2", None],
                   'TAG__Tag1': ["v1", 2, "v3"],
                   'TAG__Tag2': [1, 2, 3]})
#    stream.parameters.write(df)

# send parameter data without buffering
parameterData = qx.ParameterData()
parameterData.add_timestamp_nanoseconds(10)\
    .add_value("bufferless_1", 1)\
    .add_value("bufferless_2", "test") \
    .add_value("bufferless_3", "test") \
    .remove_value("bufferless_3")\
    .add_tag("tag1", "tag1val")\
    .add_tag("tag2", "tag2val")\
    .remove_tag("tag2")

stream.parameters.write(parameterData)


print("---- Sending event definitions ----")
stream.events.add_definition("event_at_root", "Root Event", "This is a root event as there is no default location")
stream.events.default_location = "/Base"
stream.events.add_definition("event_1", "Event One", "This is test event number one")\
             .add_definition("event_2", description="This is event 2").set_level(qx.EventLevel.Debug).set_custom_properties("{test prop for event}")
stream.events.add_location("/NotBase").add_definition("event_3").set_level(qx.EventLevel.Critical)
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

ed = qx.EventData("event4", 12345678, "direct event val")
ed.add_tag("a", "b")\
  .add_tag("b", "c")\
  .add_tag("c", "d")\
  .remove_tag("c")
stream.events.write(ed)

print("Closing")
stream.close(qx.StreamEndType.Aborted)  # Comment this out when testing stream revocation & set time.sleep to higher
print("Closed")