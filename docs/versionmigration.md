# Migrating from previous versions

Our goal is to minimize the occurrence of breaking changes, and if we do need to make them, we'll do so simultaneously. In cases where such changes are necessary, we'll provide migration steps from previous versions to assist in the transition. To prevent undue verbosity, we'll only show one difference unless it's language-specific, such as naming conventions (casing vs underscore).

## 0.4.* -> 0.5.0

### The library is renamed

For Python, the library is renamed to `quixstreams` from `quixstreaming`, while for C# the packages will be available under `QuixStreams.*` rather than `Quix.Sdk.*`. The latter also resulted in namespace changes.

### Library availability

Previously, the library was not open source and was distributed via our public feed.

For Python it was done by using:

```
pip install quixstreaming --extra-index-url https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/pypi/simple/
``` 

Now it should be installed from the official PyPi feed using:

```
pip install quixstreams
```

Quix currently publishes in-development versions to test PyPi, you can try these using:

```
pip install quixstreams --extra-index-url https://test.pypi.org/simple/
```

**Note:** The original feed will be maintained for some time, but should be treated as deprecated.

We are currently using our feed for C#, but we're in the process of developing our public NuGet packages, which will be made available shortly.

```
https://pkgs.dev.azure.com/quix-analytics/53f7fe95-59fe-4307-b479-2473b96de6d1/_packaging/public/nuget/v3/index.json
```

### StreamingClient renamed to KafkaStreamingClient

We renamed the `StreamingClient` to be more specific to the technology it works with.

### OutputTopic and InputTopic classes renamed to TopicProducer and TopicConsumer

This also brought several other changes to the code, see them below:

=== "Python before"

    ``` python
    output_topic = client.open_output_topic(TOPIC)
    input_topic = client.open_input_topic(TOPIC)
    # !!! There was a significant modification made to `open_input_topic`,
    # which changed its default behavior from using a consumer group named 'Default'
    # with the `Earliest` offset to no consumer group and `Latest` offset.
    ```

=== "Python after"

    ``` python
    topic_producer = client.get_topic_producer(TOPIC)
    topic_consumer = client.get_topic_consumer(TOPIC)
    # !!! There was a significant modification made to `get_topic_consumer`,
    # which changed its default behavior from using a consumer group named 'Default'
    # with the `Earliest` offset to no consumer group and `Latest` offset.
    ```

### Readers and Writers renamed to Consumers and Producers

The modifications will have the most significant impact on Python code that includes type hints in functions or C# code that subscribes events using a method with a particular signature rather than a lambda expression. The alterations are as follows:

- `StreamReader|Writer` -> `StreamConsumer|Producer`
- `StreamPropertiesReader|Writer` -> `StreamPropertiesConsumer|Producer`
- `StreamParametersReader|writer` -> `StreamTimeseriesConsumer|Producer`  (see section below about `Parameters`->`TimeSeries` rename)
- `StreamEventsReader|Writer` -> `StreamEventsConsumer|Producer`
- `ParametersBufferReader|Writer` -> `TimeseriesConsumer|Producer` (see section below about `Parameters`->`TimeSeries` rename)

### ParameterData renamed to TimeseriesData

The class itself is renamed, see below for changes:

=== "Python before"

    ``` python
    from quixstreams import ParameterData

    data = ParameterData()

    data.add_timestamp_nanoseconds(1) \
        .add_value("Speed", 120) \
        .add_value("Gear", 3)
    ```

=== "Python after"

    ``` python
    from quixstreams import TimeseriesData

    data = TimeseriesData()

    data.add_timestamp_nanoseconds(1) \
        .add_value("Speed", 120) \
        .add_value("Gear", 3)
    ```

And the property on streams is also renamed:

=== "Python before"

    ``` python
    stream.parameters.…
    ```

=== "Python after"

    ``` python
    stream.timeseries.…
    ```

### pandas DataFrame changes

All pandas DataFrames provided to you by callbacks or methods will expose the timestamp as 'timestamp' instead of 'time'.

In addition `from|to_panda_frame` has been renamed to `from|to_dataframe`:

=== "Python before"

    ``` python
    data = ParameterData()

    data.add_timestamp_nanoseconds(1) \
        .add_value("Speed", 120) \
        .add_value("Gear", 3)

    df = data.to_panda_frame()
    time_col = df['time']

    ```

=== "Python after"

    ``` python

    data = TimeseriesData()

    data.add_timestamp_nanoseconds(1) \
        .add_value("Speed", 120) \
        .add_value("Gear", 3)

    df = data.to_dataframe()
    timestamp_col = df['timestamp']
    ```

### .Write renamed to .Publish to be in sync with Producer

=== "Python before"

    ``` python
    stream.parameters.write(…)

    stream.parameters.buffer \
        .add_timestamp(datetime.utcnow()) \
        .add_value("parameter", 10) \
        .write()

    stream.events.write(…)

    raw_output_topic.write(…)
    ```

=== "Python after"

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

=== "Python before"

    ``` python
    input_topic.start_reading()
    ```

=== "Python after"

    ``` python
    topic_consumer.subscribe()
    ```

### Event changes

Certain callbacks have altered signatures, either in the name or number of arguments. In the case of C#, detecting these changes will be straightforward, and thus, the specifics will be omitted.

Furthermore, in Python, event subscriptions (+=, -=) have been replaced with callback assignments.

=== "Python before"

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

=== "Python after"

    ``` python
    … the rest of your code, such as client and consumer/producer creation

    # Please note that in the new version, you'll have access to all the required scopes within the callback,
    # eliminating the need to rely on the on_stream_received_handler's scope. This makes it simpler to define
    # callbacks in other locations. Another example will be provided in a separate section, but here we'll
    # maintain the previous structure for easier comprehension of the changes.
    def on_stream_received_handler(stream_received : StreamConsumer):

        buffer = stream_received.timeseries.create_buffer() # or stream_received.timeseries.buffer if don't want separate buffer with different filters and buffer condition

        # data callback assignments
        def on_dataframe_received_handler(stream: StreamConsumer, data: pandas.DataFrame):  # Note the stream being available
            pass

        stream_received.timeseries.on_dataframe_received = on_dataframe_received_handler  # note the rename and it is no longer +=
        buffer.on_dataframe_released = on_dataframe_received_handler  # note the rename and it is no longer +=


        def on_data_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesData):  # Note the stream being available
            pass

        stream_received.timeseries.on_data_received = on_data_releasedorreceived_handler  # note the rename and it is no longer +=
        buffer.on_data_released = on_data_releasedorreceived_handler  # note the rename and it is no longer +=


        def on_rawdata_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesDataRaw):  # Note the stream being available
            pass

        stream_received.timeseries.on_raw_received = on_rawdata_releasedorreceived_handler  # note the rename and it is no longer +=
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

        new_stream.timeseries.on_definitions_changed = on_parameter_definitions_changed_handler  # note it is no longer +=


        def on_event_definitions_changed_handler(stream: StreamConsumer):  # Note the stream being available
            pass

        new_stream.events.on_definitions_changed = on_event_definitions_changed_handler  # note it is no longer +=


        def on_package_received_handler(stream: StreamConsumer, package: StreamPackage):
            pass

        new_stream.on_package_received = on_package_received_handler  # note it is no longer +=

    input_topic.on_stream_received = on_stream_received_handler  # note it is no longer +=

    … the rest of your code
    ```

### In Python topic is now available for the stream

This, paired with the event changes (read above), enables you improve your callback setup. The code above can now be expressed as follows:

``` python
… the rest of your code, such as client and consumer/producer creation

def on_stream_received_handler(stream_received : StreamConsumer):

    buffer = stream_received.timeseries.create_buffer() # or stream_received.timeseries.buffer if don't want separate buffer with different filters and buffer condition

    # data callback assignments.
    stream_received.timeseries.on_dataframe_received = on_dataframe_received_handler
    buffer.on_dataframe_released = on_dataframe_received_handler
    stream_received.timeseries.on_data_received = on_data_releasedorreceived_handler
    buffer.on_data_released = on_data_releasedorreceived_handler
    stream_received.timeseries.on_raw_received = on_rawdata_releasedorreceived_handler
    buffer.on_raw_released = on_rawdata_releasedorreceived_handler
    new_stream.events.on_data_received = on_event_data_handler
    # metadata callback assignments
    stream_received.on_stream_closed = on_stream_closed_handler
    stream_received.properties.on_changed = on_stream_properties_changed_handler
    new_stream.timeseries.on_definitions_changed = on_parameter_definitions_changed_handler
    new_stream.events.on_definitions_changed = on_event_definitions_changed_handler
    new_stream.on_package_received = on_package_received_handler

input_topic.on_stream_received = on_stream_received_handler

# Note that these could be in a different file completely, defined by other classes, having access to all context of the stream and topic it is for
def on_dataframe_received_handler(stream: StreamConsumer, data: pandas.DataFrame):
    # do great things
    stream.topic.commit() # or any other topic method/property

def on_data_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesData):
    pass
    
def on_rawdata_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesDataRaw):
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

### 'with' statement should be used with some classes in python

Certain classes now use unmanaged resources, and to prevent memory leaks, we have incorporated the Python 'with' syntax for resource management.

These are:

- `EventData`: important to be disposed whenever manually created or received in callbacks.
- `TimeseriesData`: important to be disposed whenever manually created or received in callbacks.
- `TimeseriesDataRaw`: important to be disposed whenever manually created or received in callbacks.
- `StreamPackage`: important to be disposed whenever manually created or received in callbacks.
- `StreamConsumer`: also supports `dispose()` and automatically disposes when stream is closed.
- `StreamProducer`: also supports `dispose()` and automatically disposes when stream is closed.
- `TopicConsumer`: unless you're frequently subscribing to topics, this is not something you have to be too concerned about.
- `TopicProducer`: unless you're frequently subscribing to topics, this is not something you have to be too concerned about.

Example code:

``` python
… the rest of your code, such as client and consumer/producer creation

def on_stream_received_handler(stream_received : StreamConsumer):

    # data callback assignments.
    stream_received.timeseries.on_dataframe_received = on_dataframe_received_handler
    stream_received.timeseries.on_data_received = on_data_releasedorreceived_handler
    stream_received.timeseries.on_raw_received = on_rawdata_releasedorreceived_handler
    new_stream.events.on_data_received = on_event_data_handler
    # metadata callback assignments
    stream_received.on_stream_closed = on_stream_closed_handler
    stream_received.properties.on_changed = on_stream_properties_changed_handler
    new_stream.timeseries.on_definitions_changed = on_parameter_definitions_changed_handler
    new_stream.events.on_definitions_changed = on_event_definitions_changed_handler
    new_stream.on_package_received = on_package_received_handler

input_topic.on_stream_received = on_stream_received_handler

# Please note that these could be defined in a separate file by other classes with access to the context of the stream and the associated topic.
def on_dataframe_received_handler(stream: StreamConsumer, data: pandas.DataFrame):
    pfdata = TimeseriesData.from_panda_dataframe(data)
    with pfdata:  # should be used because TimeseriesData needs it
        pass

def on_data_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesData):
    with data:
        pass
    
def on_rawdata_releasedorreceived_handler(stream: StreamConsumer, data: TimeseriesDataRaw):
    with data:
        pass

def on_event_data_handler(stream: StreamConsumer, data: EventData):
    with data:
        pass

… the rest of your code
```