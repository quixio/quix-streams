import datetime
import random

import pandas as pd

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
#InteropUtils.enable_debug()

from src import quixstreams as qx
from src.quixstreams.models.parametervalue import ParameterValueType
qx.logging.Logging.update_factory(qx.logging.LogLevel.Debug)
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
            tsdata = qx.TimeseriesData()
            with tsdata:
                for points_index in range(number_of_data_points_per_loop):
                    # send parameters
                    pdts = tsdata.add_timestamp(datetime.datetime.utcnow())

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

                stream.parameters.write(tsdata)

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