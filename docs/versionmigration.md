# Migrating from previous versions

We aim to not do breaking changes unless necessary, and if we do, do them at the same time. However, when they're necessary we'll include migration steps from previous versions here. To avoid excessive verboseness, unless there is a language specific difference more than naming conventions (casing vs underscore), only one will be shown.

## 0.4.* -> 0.5.0

### The library is renamed

For python, the library is renamed to `quixstreams` from `quixstreaming`, while for C# the packages will be available under `Quix.Streams.*` rather than `Quix.Sdk.*`. The latter also resulted in namespace changes.

### Library availability

The library used to be closed source and distributed through our public feed.

For python it was done by using 
```
pip install quixstreaming --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
``` 
Now it should be installed from the official PyPi feed using
```
pip install quixstreams
```
We currently publish in-dev versions to test pypi, you can try these using
```
pip install quixstreams --extra-index-url https://test.pypi.org/simple/
```
Note: The original feed will be maintained for some time, but should be treated as deprecated.

For C#, we are still using our feed, but we're working on our public nuget packages which will be availabe very soon.
```
https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/nuget/v3/index.json
```

### StreamingClient renamed to KafkaStreamingClient

We renamed the StreamingClient to be more specific to the technology it works with.

### OutputTopic and InputTopic classes renamed to TopicProducer and TopicConsumer

This also brought several other changes to the code, see them below

=== Python before
``` python
output_topic = client.open_output_topic(TOPIC)
input_topic = client.open_input_topic(TOPIC)
# !!! Important change. input_topic used to default to a consumer group called 'Default' with Earliest offset.
```

=== Python after
``` python
topic_producer = client.get_topic_producer(TOPIC)
topic_consumer = client.get_topic_consumer(TOPIC)
# !!! Important change. topic_consumer no longer uses consumer group by default and defaults to Latest offset. (Latest to avoid always reprocessing entire topic)
```

### Readers and Writers renamed to Consumers and Producers

This will mainly affect code in python where typehint is provided for functions or in C# when event is subscribed with a method having specific signiture rather than a lambda. The changes are:

- `StreamReader|Writer` -> `StreamConsumer|Producer`
- `StreamPropertiesReader|Writer` -> `StreamPropertiesConsumer|Producer`
- `StreamParametersReader|writer` -> `StreamTimeseriesConsumer|Producer`  (see section below about `Parameters`->`TimeSeries` rename)
- `StreamEventsReader|Writer` -> `StreamEventsConsumer|Producer`
- `ParametersBufferReader|Writer` -> `TimeseriesConsumer|Producer` (see section below about `Parameters`->`TimeSeries` rename)


### ParameterData renamed to TimeseriesData

The class itself is renamed, see below for changes:

=== Python before
``` python
from quixstreams import ParameterData

data = ParameterData()

data.add_timestamp_nanoseconds(1) \
    .add_value("Speed", 120) \
    .add_value("Gear", 3)
```

=== Python after
``` python
from quixstreams import TimeseriesData

data = TimeseriesData()

data.add_timestamp_nanoseconds(1) \
    .add_value("Speed", 120) \
    .add_value("Gear", 3)
```

And the property on streams is also renamed:

=== Python before
``` python
stream.parameters.…
```

=== Python after
``` python
stream.timeseries.…
```

### .Write renamed to .Publish to be in sync with Producer

=== Python before
``` python
stream.parameters.write(…)

stream.parameters.buffer \
    .add_timestamp(datetime.utcnow()) \
    .add_value("parameter", 10) \
    .write()

stream.events.write(…)

raw_output_topic.write(…)
```

=== Python after
``` python
stream.timeseries.publish(…)

stream.timeseries.buffer \
    .add_timestamp(datetime.utcnow()) \
    .add_value("parameter", 10) \
    .publish()

stream.events.publish(…)

raw_topic_producer.publish(…)
```


### .StartReading renamed to .Subscribe to be in sync with Producer

=== Python before
``` python
input_topic.start_reading()
```

=== Python after
``` python
topic_consumer.subscribe()
```

### Event changes

Some of the callbacks have updated signature in the name or amount of arguments.

In addition, in python the event subscriptions ( +=, -= ) changed to callback assignments. 

=== Python before
``` python
… the rest of your code, such as client and input/output creation

def on_stream_received_handler(stream_read : StreamReader):

    buffer = stream_read.parameters.create_buffer() # or stream_read.parameters.buffer if don't want separate buffer with different filters and buffer condition

    # data event subscriptions
    def on_parameter_pandas_dataframe_handler(data: pandas.DataFrame):
        pass

    stream_read.parameters.on_read_pandas += on_parameters_pandas_dataframe_handler
    buffer.on_read_pandas += on_parameters_pandas_dataframe_handler


    def on_parameter_data_handler(data: ParameterData):
        pass

    stream_read.parameters.on_read += on_parameter_data_handler
    buffer.on_read += on_parameter_data_handler


    def on_parameter_raw_data_handler(data: ParameterDataRaw):
        pass

    stream_read.parameters.on_read_raw += on_parameter_raw_data_handler
    buffer.on_read_raw += on_parameter_raw_data_handler

    def on_event_data_handler(data: EventData):
        pass

    new_stream.events.on_read += on_event_data_handler


    # metadata event subscriptions
    def on_stream_closed_handler(end_type: StreamEndType):
        pass 

    stream_read.on_stream_closed += on_stream_closed_handler


    def on_stream_properties_changed_handler():
        pass
    
    stream_read.properties.on_changed += on_stream_properties_changed_handler
    

    def on_parameter_definitions_changed_handler():
        pass

    new_stream.parameters.on_definitions_changed += on_parameter_definitions_changed_handler


    def on_event_definitions_changed_handler():
        pass

    new_stream.events.on_definitions_changed += on_event_definitions_changed_handler

    def on_package_received_handler(stream: StreamReader, package: StreamPackage):
        pass

    new_stream.on_package_received += on_package_received_handler

input_topic.on_stream_received += on_stream_received_handler

… the rest of your code
```

=== Python after
``` python
… the rest of your code, such as client and consumer/producer creation

# Note, that in the new version you have access to all necessary scopes in the callback
# without having to rely on the scope of the on_stream_received_handler
# This allows you to have the callbacks defined elsewhere more easily
# Another example will be given in a different section, but here maintaining
# the previous structure to allow for easier understanding of the changes
def on_stream_received_handler(stream_received : StreamConsumer):

    buffer = stream_received.timeseries.create_buffer() # or stream_received.timeseries.buffer if don't want separate buffer with different filters and buffer condition

    # data callback assignments
    def on_dataframe_received_handler(stream: StreamConsumer, data: pandas.DataFrame):  # Note the stream being available
        pass

    stream_received.parameters.on_dataframe_received = on_parameters_pandas_dataframe_handler  # note the rename and it is no longer +=
    buffer.on_dataframe_released = on_parameters_pandas_dataframe_handler  # note the rename and it is no longer +=


    def on_data_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesData):  # Note the stream being available
        pass

    stream_received.parameters.on_data_received = on_data_releasedorreceived_handler  # note the rename and it is no longer +=
    buffer.on_data_released = on_data_releasedorreceived_handler  # note the rename and it is no longer +=


    def on_rawdata_releasedorreceived_handler(stream: StreamConsumer, data: ParameterDataRaw):  # Note the stream being available
        pass

    stream_received.parameters.on_raw_received = on_rawdata_releasedorreceived_handler  # note the rename and it is no longer +=
    buffer.on_raw_released = on_rawdata_releasedorreceived_handler  # note the rename and it is no longer +=

    def on_event_data_handler(stream: StreamConsumer, data: EventData):  # Note the stream being available
        pass

    new_stream.events.on_data_received = on_event_data_handler  # note the rename and it is no longer +=


    # metadata callback assignments
    def on_stream_closed_handler(stream: StreamConsumer, end_type: StreamEndType):  # Note the stream being available
        pass 

    stream_received.on_stream_closed = on_stream_closed_handler  # note it is no longer +=


    def on_stream_properties_changed_handler(stream: StreamConsumer):  # Note the stream being available
        pass
    
    stream_received.properties.on_changed = on_stream_properties_changed_handler  # note it is no longer +=
    

    def on_parameter_definitions_changed_handler(stream: StreamConsumer):  # Note the stream being available
        pass

    new_stream.parameters.on_definitions_changed = on_parameter_definitions_changed_handler  # note it is no longer +=


    def on_event_definitions_changed_handler(stream: StreamConsumer):  # Note the stream being available
        pass

    new_stream.events.on_definitions_changed = on_event_definitions_changed_handler  # note it is no longer +=


    def on_package_received_handler(stream: StreamConsumer, package: StreamPackage):
        pass

    new_stream.on_package_received = on_package_received_handler  # note it is no longer +=

input_topic.on_stream_received = on_stream_received_handler  # note it is no longer +=

… the rest of your code
```

### In python topic is now available for the stream

This paired with the event changes (read above), lets you drastically alter your callback setup. The code above can now be expressed us such:

``` python
… the rest of your code, such as client and consumer/producer creation

def on_stream_received_handler(stream_received : StreamConsumer):

    buffer = stream_received.timeseries.create_buffer() # or stream_received.timeseries.buffer if don't want separate buffer with different filters and buffer condition

    # data callback assignments.
    stream_received.parameters.on_dataframe_received = on_parameters_pandas_dataframe_handler
    buffer.on_dataframe_released = on_parameters_pandas_dataframe_handler
    stream_received.parameters.on_data_received = on_data_releasedorreceived_handler
    buffer.on_data_released = on_data_releasedorreceived_handler
    stream_received.parameters.on_raw_received = on_rawdata_releasedorreceived_handler
    buffer.on_raw_released = on_rawdata_releasedorreceived_handler
    new_stream.events.on_data_received = on_event_data_handler
    # metadata callback assignments
    stream_received.on_stream_closed = on_stream_closed_handler
    stream_received.properties.on_changed = on_stream_properties_changed_handler
    new_stream.parameters.on_definitions_changed = on_parameter_definitions_changed_handler
    new_stream.events.on_definitions_changed = on_event_definitions_changed_handler
    new_stream.on_package_received = on_package_received_handler

input_topic.on_stream_received = on_stream_received_handler

# Note that these could be in a different file completely, defined by other classes, having access to all context of the stream and topic it is for
def on_dataframe_received_handler(stream: StreamConsumer, data: pandas.DataFrame):
    # do great things
    stream.topic.commit() # or any other topic method/property

def on_data_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesData):
    pass
    
def on_rawdata_releasedorreceived_handler(stream: StreamConsumer, data: ParameterDataRaw):
    pass

def on_event_data_handler(stream: StreamConsumer, data: EventData):
    pass

def on_stream_closed_handler(stream: StreamConsumer, end_type: StreamEndType):
    pass 

def on_stream_properties_changed_handler(stream: StreamConsumer):
    pass

def on_parameter_definitions_changed_handler(stream: StreamConsumer):
    pass

def on_event_definitions_changed_handler(stream: StreamConsumer):
    pass

def on_package_received_handler(stream: StreamConsumer, package: StreamPackage):
    pass

… the rest of your code
```
