<a id="quixstreams"></a>

## quixstreams

<a id="quixstreams.logging"></a>

## quixstreams.logging

<a id="quixstreams.logging.configure_logging"></a>

#### configure\_logging

```python
def configure_logging(loglevel: Optional[LogLevel]) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/logging.py#L24)

Configure "quixstreams" logger.

>***NOTE:*** If "quixstreams" logger already has pre-defined handlers
(e.g. logging has already been configured via `logging`, or the function
is called twice), it will skip configuration and return `False`.

**Arguments**:

- `loglevel`: a valid log level as a string or None.
If None passed, this function is no-op and no logging will be configured.

**Returns**:

True if logging config has been updated, otherwise False.

<a id="quixstreams.error_callbacks"></a>

## quixstreams.error\_callbacks

<a id="quixstreams.platforms"></a>

## quixstreams.platforms

<a id="quixstreams.platforms.quix.config"></a>

## quixstreams.platforms.quix.config

<a id="quixstreams.platforms.quix.config.TopicCreationConfigs"></a>

### TopicCreationConfigs

```python
@dataclasses.dataclass
class TopicCreationConfigs()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L54)

<a id="quixstreams.platforms.quix.config.TopicCreationConfigs.name"></a>

#### name

Required when not created by a Quix App.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder"></a>

### QuixKafkaConfigsBuilder

```python
class QuixKafkaConfigsBuilder()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L63)

Retrieves all the necessary information from the Quix API and builds all the
objects required to connect a confluent-kafka client to the Quix Platform.

If not executed within the Quix platform directly, you must provide a Quix
"streaming" (aka "sdk") token, or Personal Access Token.

Ideally you also know your workspace name or id. If not, you can search for it
using a known topic name, but note the search space is limited to the access level
of your token.

It also currently handles the app_auto_create_topics setting for Application.Quix.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.__init__"></a>

#### QuixKafkaConfigsBuilder.\_\_init\_\_

```python
def __init__(quix_portal_api_service: Optional[QuixPortalApiService] = None,
             workspace_id: Optional[str] = None,
             workspace_cert_path: Optional[str] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L79)

**Arguments**:

- `quix_portal_api_service`: A QuixPortalApiService instance (else generated)
- `workspace_id`: A valid Quix Workspace ID (else searched for)
- `workspace_cert_path`: path to an existing workspace cert (else retrieved)

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.append_workspace_id"></a>

#### QuixKafkaConfigsBuilder.append\_workspace\_id

```python
def append_workspace_id(s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L169)

Add the workspace ID to a given string, typically a topic or consumer group id

**Arguments**:

- `s`: the string to append to

**Returns**:

the string with workspace_id appended

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_workspace"></a>

#### QuixKafkaConfigsBuilder.search\_for\_workspace

```python
def search_for_workspace(
        workspace_name_or_id: Optional[str] = None) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L178)

Search for a workspace given an expected workspace name or id.

**Arguments**:

- `workspace_name_or_id`: the expected name or id of a workspace

**Returns**:

the workspace data dict if search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_workspace_info"></a>

#### QuixKafkaConfigsBuilder.get\_workspace\_info

```python
def get_workspace_info(known_workspace_topic: Optional[str] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L201)

Queries for workspace data from the Quix API, regardless of instance cache,

and updates instance attributes from query result.

**Arguments**:

- `known_workspace_topic`: a topic you know to exist in some workspace

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_workspace_for_topic"></a>

#### QuixKafkaConfigsBuilder.search\_workspace\_for\_topic

```python
def search_workspace_for_topic(workspace_id: str, topic: str) -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L228)

Search through all the topics in the given workspace id to see if there is a

match with the provided topic.

**Arguments**:

- `workspace_id`: the workspace to search in
- `topic`: the topic to search for

**Returns**:

the workspace_id if success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_topic_workspace"></a>

#### QuixKafkaConfigsBuilder.search\_for\_topic\_workspace

```python
def search_for_topic_workspace(topic: str) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L244)

Find what workspace a topic belongs to.

If there is only one workspace altogether, it is assumed to be the workspace.
More than one means each workspace will be searched until the first hit.

**Arguments**:

- `topic`: the topic to search for

**Returns**:

workspace data dict if topic search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_workspace_ssl_cert"></a>

#### QuixKafkaConfigsBuilder.get\_workspace\_ssl\_cert

```python
def get_workspace_ssl_cert(extract_to_folder: Optional[Path] = None) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L265)

Gets and extracts zipped certificate from the API to provided folder.

If no path was provided, will dump to /tmp. Expects cert named 'ca.cert'.

**Arguments**:

- `extract_to_folder`: path to folder to dump zipped cert file to

**Returns**:

full cert filepath as string

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.create_topics"></a>

#### QuixKafkaConfigsBuilder.create\_topics

```python
def create_topics(topics: Iterable[TopicCreationConfigs],
                  finalize_timeout_seconds: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L328)

Create topics in a Quix cluster.

**Arguments**:

- `topics`: an iterable with TopicCreationConfigs instances
- `finalize_timeout_seconds`: How long to wait for the topics to be
marked as "Ready" (and thus ready to produce to/consume from).

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.confirm_topics_exist"></a>

#### QuixKafkaConfigsBuilder.confirm\_topics\_exist

```python
def confirm_topics_exist(topics: Iterable[Union[Topic, TopicCreationConfigs]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L374)

Confirm whether the desired set of topics exists in the Quix workspace.

**Arguments**:

- `topics`: an iterable with Either Topic or TopicCreationConfigs instances

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_confluent_broker_config"></a>

#### QuixKafkaConfigsBuilder.get\_confluent\_broker\_config

```python
def get_confluent_broker_config(known_topic: Optional[str] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L414)

Get the full client config dictionary required to authenticate a confluent-kafka

client to a Quix platform broker/workspace.

The returned config can be used directly by any confluent-kafka-python consumer/
producer (add your producer/consumer-specific configs afterward).

**Arguments**:

- `known_topic`: a topic known to exist in some workspace

**Returns**:

a dict of confluent-kafka-python client settings (see librdkafka
config for more details)

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_confluent_client_configs"></a>

#### QuixKafkaConfigsBuilder.get\_confluent\_client\_configs

```python
def get_confluent_client_configs(
    topics: list,
    consumer_group_id: Optional[str] = None
) -> Tuple[dict, List[str], Optional[str]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/config.py#L457)

Get all the values you need in order to use a confluent_kafka-based client

with a topic on a Quix platform broker/workspace.

The returned config can be used directly by any confluent-kafka-python consumer/
producer (add your producer/consumer-specific configs afterward).

The topics and consumer group are appended with any necessary values.

**Arguments**:

- `topics`: list of topics
- `consumer_group_id`: consumer group id, if needed

**Returns**:

a tuple with configs and altered versions of the topics
and consumer group name

<a id="quixstreams.platforms.quix.env"></a>

## quixstreams.platforms.quix.env

<a id="quixstreams.platforms.quix.env.QuixEnvironment"></a>

### QuixEnvironment

```python
class QuixEnvironment()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/env.py#L7)

Class to access various Quix platform environment settings

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_management_enabled"></a>

#### QuixEnvironment.state\_management\_enabled

```python
@property
def state_management_enabled() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/env.py#L19)

Check whether "State management" is enabled for the current deployment

**Returns**:

True if state management is enabled, otherwise False

<a id="quixstreams.platforms.quix.env.QuixEnvironment.deployment_id"></a>

#### QuixEnvironment.deployment\_id

```python
@property
def deployment_id() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/env.py#L27)

Return current Quix deployment id.

This variable is meant to be set only by Quix Platform and only
when the application is deployed.

**Returns**:

deployment id or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.workspace_id"></a>

#### QuixEnvironment.workspace\_id

```python
@property
def workspace_id() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/env.py#L39)

Return Quix workspace id if set

**Returns**:

workspace id or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.portal_api"></a>

#### QuixEnvironment.portal\_api

```python
@property
def portal_api() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/env.py#L47)

Return Quix Portal API url if set

**Returns**:

portal API URL or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.sdk_token"></a>

#### QuixEnvironment.sdk\_token

```python
@property
def sdk_token() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/env.py#L56)

Return Quix SDK token if set

**Returns**:

sdk token or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_dir"></a>

#### QuixEnvironment.state\_dir

```python
@property
def state_dir() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/env.py#L64)

Return application state directory on Quix.

**Returns**:

path to state dir

<a id="quixstreams.platforms.quix.checks"></a>

## quixstreams.platforms.quix.checks

<a id="quixstreams.platforms.quix.checks.check_state_management_enabled"></a>

#### check\_state\_management\_enabled

```python
def check_state_management_enabled()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/checks.py#L11)

Check if State Management feature is enabled for the current deployment on
Quix platform.
If it's disabled, the exception will be raised.

<a id="quixstreams.platforms.quix.checks.check_state_dir"></a>

#### check\_state\_dir

```python
def check_state_dir(state_dir: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/checks.py#L28)

Check if Application "state_dir" matches the state dir on Quix platform.

If it doesn't match, the warning will be logged.

**Arguments**:

- `state_dir`: application state_dir path

<a id="quixstreams.platforms.quix"></a>

## quixstreams.platforms.quix

<a id="quixstreams.platforms.quix.api"></a>

## quixstreams.platforms.quix.api

<a id="quixstreams.platforms.quix.api.QuixPortalApiService"></a>

### QuixPortalApiService

```python
class QuixPortalApiService()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/platforms/quix/api.py#L14)

A light wrapper around the Quix Portal Api. If used in the Quix Platform, it will
use that workspaces auth token and portal endpoint, else you must provide it.

Function names closely reflect the respective API endpoint,
each starting with the method [GET, POST, etc.] followed by the endpoint path.

Results will be returned in the form of request's Response.json(), unless something
else is required. Non-200's will raise exceptions.

See the swagger documentation for more info about the endpoints.

<a id="quixstreams.dataframe.dataframe"></a>

## quixstreams.dataframe.dataframe

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame"></a>

### StreamingDataFrame

```python
class StreamingDataFrame(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L29)

`StreamingDataFrame` is the main object you will use for ETL work.

Typically created with an `app = quixstreams.app.Application()` instance,
via `sdf = app.dataframe()`.


What it Does:

- Builds a data processing pipeline, declaratively (not executed immediately)
    - Executes this pipeline on inputs at runtime (Kafka message values)
- Provides functions/interface similar to Pandas Dataframes/Series
- Enables stateful processing (and manages everything related to it)


How to Use:

Define various operations while continuously reassigning to itself (or new fields).

These operations will generally transform your data, access/update state, or produce
to kafka topics.

We recommend your data structure to be "columnar" (aka a dict/JSON) in nature so
that it works with the entire interface, but simple types like `ints`, `str`, etc.
are also supported.

See the various methods and classes for more specifics, or for a deep dive into
usage, see `streamingdataframe.md` under the `docs/` folder.

>***NOTE:*** column referencing like `sdf["a_column"]` and various methods often
    create other object types (typically `quixstreams.dataframe.StreamingSeries`),
    which is expected; type hinting should alert you to any issues should you
    attempt invalid operations with said objects (however, we cannot infer whether
    an operation is valid with respect to your data!).


Example Snippet:

```python
sdf = StreamingDataframe()
sdf = sdf.apply(a_func)
sdf = sdf.filter(another_func)
sdf = sdf.to_topic(topic_obj)
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.apply"></a>

#### StreamingDataFrame.apply

```python
def apply(func: Union[DataFrameFunc, DataFrameStatefulFunc],
          stateful: bool = False,
          expand: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L99)

Apply a function to transform the value and return a new value.

The result will be passed downstream as an input value.


Example Snippet:

```python
# This stores a string in state and capitalizes every column with a string value.
# A second apply then keeps only the string value columns (shows non-stateful).
def func(d: dict, state: State):
    value = d["store_field"]
    if value != state.get("my_store_key"):
        state.set("my_store_key") = value
    return {k: v.upper() if isinstance(v, str) else v for k, v in d.items()}

sdf = StreamingDataframe()
sdf = sdf.apply(func, stateful=True)
sdf = sdf.apply(lambda d: {k: v for k,v in d.items() if isinstance(v, str)})

```

**Arguments**:

- `func`: a function to apply
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.
- `expand`: if True, expand the returned iterable into individual values
downstream. If returned value is not iterable, `TypeError` will be raised.
Default - `False`.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.update"></a>

#### StreamingDataFrame.update

```python
def update(func: Union[DataFrameFunc, DataFrameStatefulFunc],
           stateful: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L142)

Apply a function to mutate value in-place or to perform a side effect

that doesn't update the value (e.g. print a value to the console).

The result of the function will be ignored, and the original value will be
passed downstream.


Example Snippet:

```python
# Stores a value and mutates a list by appending a new item to it.
# Also prints to console.

def func(values: list, state: State):
    value = values[0]
    if value != state.get("my_store_key"):
        state.set("my_store_key") = value
    values.append("new_item")

sdf = StreamingDataframe()
sdf = sdf.update(func, stateful=True)
sdf = sdf.update(lambda value: print("Received value: ", value))
```

**Arguments**:

- `func`: function to update value
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.filter"></a>

#### StreamingDataFrame.filter

```python
def filter(func: Union[DataFrameFunc, DataFrameStatefulFunc],
           stateful: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L181)

Filter value using provided function.

If the function returns True-like value, the original value will be
passed downstream.
Otherwise, the `Filtered` exception will be raised (further processing for that
message will be skipped).


Example Snippet:

```python
# Stores a value and allows further processing only if the value is greater than
# what was previously stored.

def func(d: dict, state: State):
    value = d["my_value"]
    if value > state.get("my_store_key"):
        state.set("my_store_key") = value
        return True
    return False

sdf = StreamingDataframe()
sdf = sdf.filter(func, stateful=True)
```

**Arguments**:

- `func`: function to filter value
- `stateful`: if `True`, the function will be provided with second argument
of type `State` to perform stateful operations.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.contains"></a>

#### StreamingDataFrame.contains

```python
@staticmethod
def contains(key: str) -> StreamingSeries
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L234)

Check if the key is present in the Row value.

Example Snippet:

```python
# Add new column 'has_column' which contains a boolean indicating
# the presence of 'column_x'

sdf = StreamingDataframe()
sdf['has_column'] = sdf.contains('column_x')
```

**Arguments**:

- `key`: a column name to check.

**Returns**:

a Column object that evaluates to True if the key is present
or False otherwise.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.to_topic"></a>

#### StreamingDataFrame.to\_topic

```python
def to_topic(topic: Topic,
             key: Optional[Callable[[object], object]] = None) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L257)

Produce current value to a topic. You can optionally specify a new key.

>***NOTE:*** A `RowProducer` instance must be assigned to
`StreamingDataFrame.producer` if not using :class:`quixstreams.app.Application`
 to facilitate the execution of StreamingDataFrame.


Example Snippet:

```python
from quixstreams import Application

# Produce to two different topics, changing the key for one of them.

app = Application()
input_topic = app.topic("input_x")
output_topic_0 = app.topic("output_a")
output_topic_1 = app.topic("output_b")

sdf = app.dataframe(input_topic)
sdf = sdf.to_topic(output_topic_0)
sdf = sdf.to_topic(output_topic_1, key=lambda data: data["a_field"])
```

**Arguments**:

- `topic`: instance of `Topic`
- `key`: a callable to generate a new message key, optional.
If passed, the return type of this callable must be serializable
by `key_serializer` defined for this Topic object.
By default, the current message key will be used.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.compose"></a>

#### StreamingDataFrame.compose

```python
def compose() -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L296)

Compose all functions of this StreamingDataFrame into one big closure.

Closures are more performant than calling all the functions in the
`StreamingDataFrame` one-by-one.

Generally not required by users; the `quixstreams.app.Application` class will
do this automatically.


Example Snippet:

```python
from quixstreams import Application
sdf = app.dataframe()
sdf = sdf.apply(apply_func)
sdf = sdf.filter(filter_func)
sdf = sdf.compose()

result_0 = sdf({"my": "record"})
result_1 = sdf({"other": "record"})
```

**Returns**:

a function that accepts "value"
and returns a result of StreamingDataFrame

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.test"></a>

#### StreamingDataFrame.test

```python
def test(value: object, ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L326)

A shorthand to test `StreamingDataFrame` with provided value

and `MessageContext`.

**Arguments**:

- `value`: value to pass through `StreamingDataFrame`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingDataFrame instance calls `to_topic()`,
has stateful functions or functions calling `get_current_key()`.
Default - `None`.

**Returns**:

result of `StreamingDataFrame`

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.tumbling_window"></a>

#### StreamingDataFrame.tumbling\_window

```python
def tumbling_window(
        duration_ms: Union[int, timedelta],
        grace_ms: Optional[Union[int,
                                 timedelta]] = 0) -> TumblingWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L344)

Create a tumbling window transformation on this StreamingDataFrame.

Tumbling windows divide time into fixed-sized, non-overlapping windows.

They allow to perform stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

The time windows always use the current event time.

Example Snippet:

```python
app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 60s and grace period of 10s
    sdf.tumbling_window(
        duration_ms=timedelta(seconds=60), grace_ms=timedelta(seconds=10.0)
    )

    # Specify the aggregation function
    .sum()

    # Specify how the results should be emitted downstream.
    # "all()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .all()
)
```

**Arguments**:

- `duration_ms`: The length of each window.
Can be specified as either an `int` representing milliseconds or a
`timedelta` object.
>***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
value.
- `grace_ms`: The grace period for data arrival.
It allows late-arriving data (data arriving after the window
has theoretically closed) to be included in the window.
Can be specified as either an `int` representing milliseconds
or as a `timedelta` object.
>***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
value.

**Returns**:

`TumblingWindowDefinition` instance representing the tumbling window
configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.hopping_window"></a>

#### StreamingDataFrame.hopping\_window

```python
def hopping_window(
        duration_ms: Union[int, timedelta],
        step_ms: Union[int, timedelta],
        grace_ms: Optional[Union[int,
                                 timedelta]] = 0) -> HoppingWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/dataframe.py#L409)

Create a hopping window transformation on this StreamingDataFrame.

Hopping windows divide the data stream into overlapping windows based on time.
The overlap is controlled by the `step_ms` parameter.

They allow to perform stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

The time windows always use the current event time.

Example Snippet:

```python

app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a a hopping window of 60s with step 30s and grace period of 10s
    sdf.hopping_window(
        duration_ms=timedelta(seconds=60),
        step_ms=timedelta(seconds=30),
        grace_ms=timedelta(seconds=10)
    )

    # Specify the aggregation function
    .sum()

    # Specify how the results should be emitted downstream.
    # "all()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .all()
)
```

**Arguments**:

- `duration_ms`: The length of each window. It defines the time span for
which each window aggregates data.
Can be specified as either an `int` representing milliseconds
or a `timedelta` object.
>***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
value.
- `step_ms`: The step size for the window.
It determines how much each successive window moves forward in time.
Can be specified as either an `int` representing milliseconds
or a `timedelta` object.
>***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
value.
- `grace_ms`: The grace period for data arrival.
It allows late-arriving data to be included in the window,
even if it arrives after the window has theoretically moved forward.
Can be specified as either an `int` representing milliseconds
or a `timedelta` object.
>***NOTE:*** `timedelta` objects will be rounded to the closest millisecond
value.

**Returns**:

`HoppingWindowDefinition` instance representing the hopping
window configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. and applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.series"></a>

## quixstreams.dataframe.series

<a id="quixstreams.dataframe.series.StreamingSeries"></a>

### StreamingSeries

```python
class StreamingSeries(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L16)

`StreamingSeries` are typically generated by `StreamingDataframes` when getting
elements from, or performing certain operations on, a `StreamingDataframe`,
thus acting as a representation of "column" value.

They share some operations with the `StreamingDataframe`, but also provide some
additional functionality.

Most column value operations are handled by this class, and `StreamingSeries` can
generate other `StreamingSeries` as a result of said operations.


What it Does:

- Allows ways to do simple operations with dataframe "column"/dictionary values:
    - Basic ops like add, subtract, modulo, etc.
- Enables comparisons/inequalities:
    - Greater than, equals, etc.
    - and/or, is/not operations
- Can check for existence of columns in `StreamingDataFrames`
- Enables chaining of various operations together


How to Use:

For the most part, you may not even notice this class exists!
They will naturally be created as a result of typical `StreamingDataFrame` use.

Auto-complete should help you with valid methods and type-checking should alert
you to invalid operations between `StreamingSeries`.

In general, any typical Pands dataframe operation between columns should be valid
with `StreamingSeries`, and you shouldn't have to think about them explicitly.


Example Snippet:

```python
# Random methods for example purposes. More detailed explanations found under
# various methods or in the docs folder.

sdf = StreamingDataframe()
sdf = sdf["column_a"].apply(a_func).apply(diff_func, stateful=True)
sdf["my_new_bool_field"] = sdf["column_b"].contains("this_string")
sdf["new_sum_field"] = sdf["column_c"] + sdf["column_d"] + 2
sdf = sdf[["column_a"] & (sdf["new_sum_field"] >= 10)]
```

<a id="quixstreams.dataframe.series.StreamingSeries.from_func"></a>

#### StreamingSeries.from\_func

```python
@classmethod
def from_func(cls, func: StreamCallable) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L76)

Create a StreamingSeries from a function.

The provided function will be wrapped into `Apply`

**Arguments**:

- `func`: a function to apply

**Returns**:

instance of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.apply"></a>

#### StreamingSeries.apply

```python
def apply(func: StreamCallable) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L90)

Add a callable to the execution list for this series.

The provided callable should accept a single argument, which will be its input.
The provided callable should similarly return one output, or None

They can be chained together or included with other operations.


Example Snippet:

```python
# The `StreamingSeries` are generated when `sdf["COLUMN_NAME"]` is called.
# This stores a string in state and capitalizes the column value; the result is
# assigned to a new column.
#  Another apply converts a str column to an int, assigning it to a new column.

def func(value: str, state: State):
    if value != state.get("my_store_key"):
        state.set("my_store_key") = value
    return v.upper()

sdf = StreamingDataframe()
sdf["new_col"] = sdf["a_column"]["nested_dict_key"].apply(func, stateful=True)
sdf["new_col_2"] = sdf["str_col"].apply(lambda v: int(v)) + sdf["str_col2"] + 2
```

**Arguments**:

- `func`: a callable with one argument and one output

**Returns**:

a new `StreamingSeries` with the new callable added

<a id="quixstreams.dataframe.series.StreamingSeries.compose"></a>

#### StreamingSeries.compose

```python
def compose(allow_filters: bool = True,
            allow_updates: bool = True) -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L124)

Compose all functions of this StreamingSeries into one big closure.

Closures are more performant than calling all the functions in the
`StreamingDataFrame` one-by-one.

Generally not required by users; the `quixstreams.app.Application` class will
do this automatically.


Example Snippet:

```python
from quixstreams import Application

app = Application(...)

sdf = app.dataframe()
sdf = sdf["column_a"].apply(apply_func)
sdf = sdf["column_b"].contains(filter_func)
sdf = sdf.compose()

result_0 = sdf({"my": "record"})
result_1 = sdf({"other": "record"})
```

**Arguments**:

- `allow_filters`: If False, this function will fail with ValueError if
the stream has filter functions in the tree. Default - True.
- `allow_updates`: If False, this function will fail with ValueError if
the stream has update functions in the tree. Default - True.

**Raises**:

- `ValueError`: if disallowed functions are present in the tree of
underlying `Stream`.

**Returns**:

a function that accepts "value"
and returns a result of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.test"></a>

#### StreamingSeries.test

```python
def test(value: Any, ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L171)

A shorthand to test `StreamingSeries` with provided value

and `MessageContext`.

**Arguments**:

- `value`: value to pass through `StreamingSeries`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingSeries instance has
functions calling `get_current_key()`.
Default - `None`.

**Returns**:

result of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.isin"></a>

#### StreamingSeries.isin

```python
def isin(other: Container) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L202)

Check if series value is in "other".

Same as "StreamingSeries in other".

Runtime result will be a `bool`.


Example Snippet:

```python
from quixstreams import Application

# Check if "str_column" is contained in a column with a list of strings and
# assign the resulting `bool` to a new column: "has_my_str".

sdf = app.dataframe()
sdf["has_my_str"] = sdf["str_column"].isin(sdf["column_with_list_of_strs"])
```

**Arguments**:

- `other`: a container to check

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.contains"></a>

#### StreamingSeries.contains

```python
def contains(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L229)

Check if series value contains "other"

Same as "other in StreamingSeries".

Runtime result will be a `bool`.


Example Snippet:

```python
from quixstreams import Application

# Check if "column_a" contains "my_substring" and assign the resulting
# `bool` to a new column: "has_my_substr"

sdf = app.dataframe()
sdf["has_my_substr"] = sdf["column_a"].contains("my_substring")
```

**Arguments**:

- `other`: object to check

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.is_"></a>

#### StreamingSeries.is\_

```python
def is_(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L254)

Check if series value refers to the same object as `other`

Runtime result will be a `bool`.


Example Snippet:

```python
# Check if "column_a" is the same as "column_b" and assign the resulting `bool`
#  to a new column: "is_same"

from quixstreams import Application
sdf = app.dataframe()
sdf["is_same"] = sdf["column_a"].is_(sdf["column_b"])
```

**Arguments**:

- `other`: object to check for "is"

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.isnot"></a>

#### StreamingSeries.isnot

```python
def isnot(other: object) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L277)

Check if series value does not refer to the same object as `other`

Runtime result will be a `bool`.


Example Snippet:

```python
from quixstreams import Application

# Check if "column_a" is the same as "column_b" and assign the resulting `bool`
# to a new column: "is_not_same"

sdf = app.dataframe()
sdf["is_not_same"] = sdf["column_a"].isnot(sdf["column_b"])
```

**Arguments**:

- `other`: object to check for "is_not"

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.isnull"></a>

#### StreamingSeries.isnull

```python
def isnull() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L301)

Check if series value is None.

Runtime result will be a `bool`.


Example Snippet:

```python
from quixstreams import Application

# Check if "column_a" is null and assign the resulting `bool` to a new column:
# "is_null"

sdf = app.dataframe()
sdf["is_null"] = sdf["column_a"].isnull()
```

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.notnull"></a>

#### StreamingSeries.notnull

```python
def notnull() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L324)

Check if series value is not None.

Runtime result will be a `bool`.


Example Snippet:

```python
from quixstreams import Application

# Check if "column_a" is not null and assign the resulting `bool` to a new column:
# "is_not_null"

sdf = app.dataframe()
sdf["is_not_null"] = sdf["column_a"].notnull()
```

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe.series.StreamingSeries.abs"></a>

#### StreamingSeries.abs

```python
def abs() -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/series.py#L347)

Get absolute value of the series value.

Example Snippet:

```python
from quixstreams import Application

# Get absolute value of "int_col" and add it to "other_int_col".
# Finally, assign the result to a new column: "abs_col_sum".

sdf = app.dataframe()
sdf["abs_col_sum"] = sdf["int_col"].abs() + sdf["other_int_col"]
```

**Returns**:

new StreamingSeries

<a id="quixstreams.dataframe"></a>

## quixstreams.dataframe

<a id="quixstreams.dataframe.utils"></a>

## quixstreams.dataframe.utils

<a id="quixstreams.dataframe.utils.ensure_milliseconds"></a>

#### ensure\_milliseconds

```python
def ensure_milliseconds(delta: Union[int, timedelta]) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/utils.py#L5)

Convert timedelta to milliseconds.

If the `delta` is not
This function will also round the value to the closest milliseconds in case of
higher precision.

**Arguments**:

- `delta`: `timedelta` object

**Returns**:

timedelta value in milliseconds as `int`

<a id="quixstreams.dataframe.windows.definitions"></a>

## quixstreams.dataframe.windows.definitions

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition"></a>

### FixedTimeWindowDefinition

```python
class FixedTimeWindowDefinition(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/definitions.py#L20)

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.sum"></a>

#### FixedTimeWindowDefinition.sum

```python
def sum() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/definitions.py#L68)

Configure the window to aggregate data by summing up values within

each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to perform sum aggregation.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.count"></a>

#### FixedTimeWindowDefinition.count

```python
def count() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/definitions.py#L95)

Configure the window to aggregate data by counting the number of values

within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to perform record count.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.mean"></a>

#### FixedTimeWindowDefinition.mean

```python
def mean() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/definitions.py#L122)

Configure the window to aggregate data by calculating the mean of the values

within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to calculate the mean
of the values.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.reduce"></a>

#### FixedTimeWindowDefinition.reduce

```python
def reduce(reducer: Callable[[Any, Any], Any],
           initializer: Callable[[Any], Any]) -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/definitions.py#L153)

Configure the window to perform a custom aggregation using `reducer`

and `initializer` functions.

Example Snippet:
```python
sdf = StreamingDataFrame(...)

# Using "reduce()" to calculate multiple aggregates at once
def reducer(agg: dict, current: int):
    aggregated = {
        'min': min(agg['min'], current),
        'max': max(agg['max'], current)
        'count': agg['count'] + 1
    }
    return aggregated

def initializer(current) -> dict:
    return {'min': current, 'max': current, 'count': 1}

window = (
    sdf.tumbling_window(duration_ms=1000)
    .reduce(reducer=reducer, initializer=initializer)
    .final()
)
```

**Arguments**:

- `reducer`: A function that takes two arguments
(the accumulated value and a new value) and returns a single value.
The returned value will be saved to the state store and sent downstream.
- `initializer`: A function to call for every first element of the window.
This function is used to initialize the aggreation within a window.

**Returns**:

A window configured to perform custom reduce aggregation on the data.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.max"></a>

#### FixedTimeWindowDefinition.max

```python
def max() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/definitions.py#L213)

Configure a window to aggregate the maximum value within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to calculate the maximum
value within each window period.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.min"></a>

#### FixedTimeWindowDefinition.min

```python
def min() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/definitions.py#L242)

Configure a window to aggregate the minimum value within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to calculate the maximum
value within each window period.

<a id="quixstreams.dataframe.windows"></a>

## quixstreams.dataframe.windows

<a id="quixstreams.dataframe.windows.time_based"></a>

## quixstreams.dataframe.windows.time\_based

<a id="quixstreams.dataframe.windows.time_based.FixedTimeWindow"></a>

### FixedTimeWindow

```python
class FixedTimeWindow()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/time_based.py#L29)

<a id="quixstreams.dataframe.windows.time_based.FixedTimeWindow.final"></a>

#### FixedTimeWindow.final

```python
def final(expand: bool = True) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/time_based.py#L92)

Apply the window aggregation and return results only when the windows are

closed.

The format of returned windows:
```python
{
    "start": <window start time in milliseconds>,
    "end": <window end time in milliseconds>,
    "value: <aggregated window value>,
}
```

The individual window is closed when the event time
(the maximum observed timestamp across the partition) passes
its end timestamp + grace period.
The closed windows cannot receive updates anymore and are considered final.

>***NOTE:*** Windows can be closed only within the same message key.
If some message keys appear irregularly in the stream, the latest windows
can remain unprocessed until the message the same key is received.

**Arguments**:

- `expand`: if `True`, each window result will be sent downstream as
an individual item. Otherwise, the list of window results will be sent.
Default - `True`

<a id="quixstreams.dataframe.windows.time_based.FixedTimeWindow.current"></a>

#### FixedTimeWindow.current

```python
def current(expand: bool = True) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/time_based.py#L129)

Apply the window transformation to the StreamingDataFrame to return results

for each updated window.

The format of returned windows:
```python
{
    "start": <window start time in milliseconds>,
    "end": <window end time in milliseconds>,
    "value: <aggregated window value>,
}
```

This method processes streaming data and returns results as they come,
regardless of whether the window is closed or not.

**Arguments**:

- `expand`: if `True`, each window result will be sent downstream as
an individual item. Otherwise, the list of window results will be sent.
Default - `True`

<a id="quixstreams.dataframe.windows.base"></a>

## quixstreams.dataframe.windows.base

<a id="quixstreams.dataframe.windows.base.get_window_ranges"></a>

#### get\_window\_ranges

```python
def get_window_ranges(timestamp_ms: int,
                      duration_ms: int,
                      step_ms: Optional[int] = None) -> List[Tuple[int, int]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/dataframe/windows/base.py#L22)

Get a list of window ranges for the given timestamp.

**Arguments**:

- `timestamp_ms`: timestamp in milliseconds
- `duration_ms`: window duration in milliseconds
- `step_ms`: window step in milliseconds for hopping windows, optional.

**Returns**:

a list of (<start>, <end>) tuples

<a id="quixstreams.dataframe.base"></a>

## quixstreams.dataframe.base

<a id="quixstreams.rowproducer"></a>

## quixstreams.rowproducer

<a id="quixstreams.rowproducer.RowProducer"></a>

### RowProducer

```python
class RowProducer(Producer, RowProducerProto)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/rowproducer.py#L25)

A producer class that is capable of serializing Rows to bytes and send them to Kafka.

The serialization is performed according to the Topic serialization settings.

    It overrides `.subscribe()` method of Consumer class to accept `Topic`
    objects instead of strings.

    :param broker_address: Kafka broker host and port in format `<host>:<port>`.
        Passed as `bootstrap.servers` to `confluent_kafka.Producer`.
    :param partitioner: A function to be used to determine the outgoing message
        partition.
        Available values: "random", "consistent_random", "murmur2", "murmur2_random",
        "fnv1a", "fnv1a_random"
        Default - "murmur2".
    :param extra_config: A dictionary with additional options that
        will be passed to `confluent_kafka.Producer` as is.
        Note: values passed as arguments override values in `extra_config`.
    :param on_error: a callback triggered when `RowProducer.produce_row()`
        or `RowProducer.poll()` fail`.
        If producer fails and the callback returns `True`, the exception
        will be logged but not propagated.
        The default callback logs an exception and returns `False`.


<a id="quixstreams.rowproducer.RowProducer.produce_row"></a>

#### RowProducer.produce\_row

```python
def produce_row(row: Row,
                topic: Topic,
                key: Optional[Any] = None,
                partition: Optional[int] = None,
                timestamp: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/rowproducer.py#L66)

Serialize Row to bytes according to the Topic serialization settings

and produce it to Kafka

If this method fails, it will trigger the provided "on_error" callback.

**Arguments**:

- `row`: Row object
- `topic`: Topic object
- `key`: message key, optional
- `partition`: partition number, optional
- `timestamp`: timestamp in milliseconds, optional

<a id="quixstreams.rowproducer.RowProducer.poll"></a>

#### RowProducer.poll

```python
def poll(timeout: float = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/rowproducer.py#L103)

Polls the producer for events and calls `on_delivery` callbacks.

If poll fails, it will trigger the provided "on_error" callback

**Arguments**:

- `timeout`: timeout in seconds

<a id="quixstreams.core.stream.functions"></a>

## quixstreams.core.stream.functions

<a id="quixstreams.core.stream.functions.StreamFunction"></a>

### StreamFunction

```python
class StreamFunction(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L27)

A base class for all the streaming operations in Quix Streams.

It provides two methods that return closures to be called on the input values:
- `get_executor` - a wrapper to execute on a single value
- `get_executor_expanded` - a wrapper to execute on an expanded value.
    Expanded value is a list, where each item should be treated as a separate value.

<a id="quixstreams.core.stream.functions.StreamFunction.func"></a>

#### StreamFunction.func

```python
@property
def func() -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L44)

The original function

<a id="quixstreams.core.stream.functions.StreamFunction.get_executor"></a>

#### StreamFunction.get\_executor

```python
@abc.abstractmethod
def get_executor() -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L51)

Returns a wrapper to be called on a single value.

<a id="quixstreams.core.stream.functions.StreamFunction.get_executor_expanded"></a>

#### StreamFunction.get\_executor\_expanded

```python
@abc.abstractmethod
def get_executor_expanded() -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L57)

Returns a wrapper to be called on a list of expanded values.

<a id="quixstreams.core.stream.functions.ApplyFunction"></a>

### ApplyFunction

```python
class ApplyFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L63)

Wrap a function into "Apply" function.

The provided function is expected to return a new value based on input,
and its result will always be passed downstream.

<a id="quixstreams.core.stream.functions.ApplyExpandFunction"></a>

### ApplyExpandFunction

```python
class ApplyExpandFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L86)

Wrap a function into "Apply" function and expand the returned iterable
into separate values downstream.

The provided function is expected to return an `Iterable`.
If the returned value is not `Iterable`, `TypeError` will be raised.

<a id="quixstreams.core.stream.functions.FilterFunction"></a>

### FilterFunction

```python
class FilterFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L115)

Wraps a function into a "Filter" function.
The result of a Filter function is interpreted as boolean.
If it's `True`, the input will be return downstream.
If it's `False`, the `Filtered` exception will be raised to signal that the
value is filtered out.

<a id="quixstreams.core.stream.functions.UpdateFunction"></a>

### UpdateFunction

```python
class UpdateFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L147)

Wrap a function into an "Update" function.

The provided function is expected to mutate the value
or to perform some side effect.
Its result will always be ignored, and its input is passed
downstream.

<a id="quixstreams.core.stream.functions.compose"></a>

#### compose

```python
def compose(functions: List[StreamFunction],
            allow_filters: bool = True,
            allow_updates: bool = True,
            allow_expands: bool = True) -> StreamCallable
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L176)

Composes a list of functions and its parents into a single

big closure like this:
```
[func, func, func] -> func(func(func()))
```

Closures are more performant than calling all functions one by one in a loop.

**Arguments**:

- `functions`: list of `StreamFunction` objects to compose
- `allow_filters`: If False, will fail with `ValueError` if
the list has `FilterFunction`. Default - True.
- `allow_updates`: If False, will fail with `ValueError` if
the list has `UpdateFunction`. Default - True.
- `allow_expands`: If False, will fail with `ValueError` if
the list has `ApplyFunction` with "expand=True". Default - True.

**Raises**:

- `ValueError`: if disallowed functions are present in the list of functions.

<a id="quixstreams.core.stream.functions.composer"></a>

#### composer

```python
def composer(outer_func: StreamCallable,
             inner_func: StreamCallable) -> Callable[[T], R]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/functions.py#L226)

A function that wraps two other functions into a closure.

It passes the result of the inner function as an input to the outer function.

**Returns**:

a function with one argument (value)

<a id="quixstreams.core.stream"></a>

## quixstreams.core.stream

<a id="quixstreams.core.stream.stream"></a>

## quixstreams.core.stream.stream

<a id="quixstreams.core.stream.stream.Stream"></a>

### Stream

```python
class Stream()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L22)

<a id="quixstreams.core.stream.stream.Stream.__init__"></a>

#### Stream.\_\_init\_\_

```python
def __init__(func: Optional[StreamFunction] = None,
             parent: Optional[Self] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L23)

A base class for all streaming operations.

`Stream` is an abstraction of a function pipeline.
Each Stream has a function and a parent (None by default).
When adding new function to the stream, it creates a new `Stream` object and
sets "parent" to the previous `Stream` to maintain an order of execution.

Streams supports 3 types of functions:
- "Apply" - generate new values based on a previous one.
    The result of an Apply function is passed downstream to the next functions.
    If "expand=True" is passed and the function returns an `Iterable`,
    each item of it will be treated as a separate value downstream.
- "Update" - update values in-place.
    The result of an Update function is always ignored, and its input is passed
    downstream.
- "Filter" - to filter values from the Stream.
    The result of a Filter function is interpreted as boolean.
    If it's `True`, the input will be passed downstream.
    If it's `False`, the `Filtered` exception will be raised to signal that the
    value is filtered out.

To execute the functions on the `Stream`, call `.compose()` method, and
it will return a closure to execute all the functions accumulated in the Stream
and its parents.

**Arguments**:

- `func`: a function to be called on the stream.
It is expected to be wrapped into one of "Apply", "Filter" or "Update" from
`quixstreams.core.stream.functions` package.
Default - "Apply(lambda v: v)".
- `parent`: a parent `Stream`

<a id="quixstreams.core.stream.stream.Stream.add_filter"></a>

#### Stream.add\_filter

```python
def add_filter(func: Callable[[T], R]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L79)

Add a function to filter values from the Stream.

The return value of the function will be interpreted as `bool`.
If the function returns `False`-like result, the Stream will raise `Filtered`
exception during execution.

**Arguments**:

- `func`: a function to filter values from the stream

**Returns**:

a new `Stream` derived from the current one

<a id="quixstreams.core.stream.stream.Stream.add_apply"></a>

#### Stream.add\_apply

```python
def add_apply(func: Callable[[T], R], expand: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L92)

Add an "apply" function to the Stream.

The function is supposed to return a new value, which will be passed
further during execution.

**Arguments**:

- `func`: a function to generate a new value
- `expand`: if True, expand the returned iterable into individual values
downstream. If returned value is not iterable, `TypeError` will be raised.
Default - `False`.

**Returns**:

a new `Stream` derived from the current one

<a id="quixstreams.core.stream.stream.Stream.add_update"></a>

#### Stream.add\_update

```python
def add_update(func: Callable[[T], object]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L109)

Add an "update" function to the Stream, that will mutate the input value.

The return of this function will be ignored and its input
will be passed downstream.

**Arguments**:

- `func`: a function to mutate the value

**Returns**:

a new Stream derived from the current one

<a id="quixstreams.core.stream.stream.Stream.diff"></a>

#### Stream.diff

```python
def diff(other: "Stream") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L121)

Takes the difference between Streams `self` and `other` based on their last

common parent, and returns a new `Stream` that includes only this difference.

It's impossible to calculate a diff when:
 - Streams don't have a common parent.
 - When the `self` Stream already includes all the nodes from
    the `other` Stream, and the resulting diff is empty.

**Arguments**:

- `other`: a `Stream` to take a diff from.

**Raises**:

- `ValueError`: if Streams don't have a common parent
or if the diff is empty.

**Returns**:

new `Stream` instance including all the Streams from the diff

<a id="quixstreams.core.stream.stream.Stream.tree"></a>

#### Stream.tree

```python
def tree() -> List[Self]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L150)

Return a list of all parent Streams including the node itself.

The tree is ordered from child to parent (current node comes first).

**Returns**:

a list of `Stream` objects

<a id="quixstreams.core.stream.stream.Stream.compose"></a>

#### Stream.compose

```python
def compose(allow_filters: bool = True,
            allow_updates: bool = True,
            allow_expands: bool = True) -> Callable[[T], R]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/core/stream/stream.py#L164)

Compose a list of functions from this `Stream` and its parents into one

big closure using a "composer" function.

Closures are more performant than calling all the functions in the
`Stream.tree()` one-by-one.

**Arguments**:

- `allow_filters`: If False, this function will fail with `ValueError` if
the stream has filter functions in the tree. Default - True.
- `allow_updates`: If False, this function will fail with `ValueError` if
the stream has update functions in the tree. Default - True.
- `allow_expands`: If False, this function will fail with `ValueError` if
the stream has functions with "expand=True" in the tree. Default - True.

**Raises**:

- `ValueError`: if disallowed functions are present in the stream tree.

<a id="quixstreams.core"></a>

## quixstreams.core

<a id="quixstreams.utils"></a>

## quixstreams.utils

<a id="quixstreams.utils.json"></a>

## quixstreams.utils.json

<a id="quixstreams.utils.json.dumps"></a>

#### dumps

```python
def dumps(value: Any) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/utils/json.py#L8)

Serialize to JSON using `orjson` package.

**Arguments**:

- `value`: value to serialize to JSON

**Returns**:

bytes

<a id="quixstreams.utils.json.loads"></a>

#### loads

```python
def loads(value: bytes) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/utils/json.py#L18)

Deserialize from JSON using `orjson` package.

Main differences:
- It returns `bytes`
- It doesn't allow non-str keys in dictionaries

**Arguments**:

- `value`: value to deserialize from

**Returns**:

object

<a id="quixstreams.types"></a>

## quixstreams.types

<a id="quixstreams.models.timestamps"></a>

## quixstreams.models.timestamps

<a id="quixstreams.models.timestamps.TimestampType"></a>

### TimestampType

```python
class TimestampType(enum.IntEnum)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/timestamps.py#L9)

<a id="quixstreams.models.timestamps.TimestampType.TIMESTAMP_NOT_AVAILABLE"></a>

#### TIMESTAMP\_NOT\_AVAILABLE

timestamps not supported by broker

<a id="quixstreams.models.timestamps.TimestampType.TIMESTAMP_CREATE_TIME"></a>

#### TIMESTAMP\_CREATE\_TIME

message creation time (or source / producer time)

<a id="quixstreams.models.timestamps.TimestampType.TIMESTAMP_LOG_APPEND_TIME"></a>

#### TIMESTAMP\_LOG\_APPEND\_TIME

broker receive time

<a id="quixstreams.models.timestamps.MessageTimestamp"></a>

### MessageTimestamp

```python
class MessageTimestamp()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/timestamps.py#L15)

Represents a timestamp of incoming Kafka message.

It is made pseudo-immutable (i.e. public attributes don't have setters), and
it should not be mutated during message processing.

<a id="quixstreams.models.timestamps.MessageTimestamp.create"></a>

#### MessageTimestamp.create

```python
@classmethod
def create(cls, timestamp_type: int, milliseconds: int) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/timestamps.py#L42)

Create a Timestamp object based on data

from `confluent_kafka.Message.timestamp()`.

If timestamp type is "TIMESTAMP_NOT_AVAILABLE", the milliseconds are set to None

**Arguments**:

- `timestamp_type`: a timestamp type represented as a number
Can be one of:
- "0" - TIMESTAMP_NOT_AVAILABLE, timestamps not supported by broker.
- "1" - TIMESTAMP_CREATE_TIME, message creation time (or source / producer time).
- "2" - TIMESTAMP_LOG_APPEND_TIME, broker receive time.
- `milliseconds`: the number of milliseconds since the epoch (UTC).

**Returns**:

Timestamp object

<a id="quixstreams.models"></a>

## quixstreams.models

<a id="quixstreams.models.messagecontext"></a>

## quixstreams.models.messagecontext

<a id="quixstreams.models.messagecontext.MessageContext"></a>

### MessageContext

```python
class MessageContext()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/messagecontext.py#L7)

An object with Kafka message properties.

It is made pseudo-immutable (i.e. public attributes don't have setters), and
it should not be mutated during message processing.

<a id="quixstreams.models.types"></a>

## quixstreams.models.types

<a id="quixstreams.models.types.ConfluentKafkaMessageProto"></a>

### ConfluentKafkaMessageProto

```python
class ConfluentKafkaMessageProto(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/types.py#L10)

An interface of `confluent_kafka.Message`.

Use it to not depend on exact implementation and simplify testing.

Instances of `confluent_kafka.Message` cannot be directly created from Python,
see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

<a id="quixstreams.models.serializers"></a>

## quixstreams.models.serializers

<a id="quixstreams.models.serializers.exceptions"></a>

## quixstreams.models.serializers.exceptions

<a id="quixstreams.models.serializers.exceptions.IgnoreMessage"></a>

### IgnoreMessage

```python
class IgnoreMessage(exceptions.QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/exceptions.py#L51)

Raise this exception from Deserializer.__call__ in order to ignore the processing
of the particular message.

<a id="quixstreams.models.serializers.quix"></a>

## quixstreams.models.serializers.quix

<a id="quixstreams.models.serializers.quix.QuixDeserializer"></a>

### QuixDeserializer

```python
class QuixDeserializer(JSONDeserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L70)

Handles Deserialization for any Quix-formatted topic.

Parses JSON data from either `TimeseriesData` and `EventData` (ignores the rest).

<a id="quixstreams.models.serializers.quix.QuixDeserializer.__init__"></a>

#### QuixDeserializer.\_\_init\_\_

```python
def __init__(column_name: Optional[str] = None,
             loads: Callable[[Union[bytes, bytearray]], Any] = default_loads)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L77)

**Arguments**:

- `column_name`: if provided, the deserialized value will be wrapped into
dictionary with `column_name` as a key.
- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.split_values"></a>

#### QuixDeserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L97)

Each Quix message might contain data for multiple Rows.
This property informs the downstream processors about that, so they can
expect an Iterable instead of Mapping.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.deserialize"></a>

#### QuixDeserializer.deserialize

```python
def deserialize(model_key: str, value: Union[List[Mapping],
                                             Mapping]) -> Iterable[Mapping]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L150)

Deserialization function for particular data types (Timeseries or EventData).

**Arguments**:

- `model_key`: value of "__Q_ModelKey" message header
- `value`: deserialized JSON value of the message, list or dict

**Returns**:

Iterable of dicts

<a id="quixstreams.models.serializers.quix.QuixSerializer"></a>

### QuixSerializer

```python
class QuixSerializer(JSONSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L268)

<a id="quixstreams.models.serializers.quix.QuixSerializer.__init__"></a>

#### QuixSerializer.\_\_init\_\_

```python
def __init__(as_legacy: bool = True,
             dumps: Callable[[Any], Union[str, bytes]] = default_dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L272)

Serializer that returns data in json format.

**Arguments**:

- `as_legacy`: parse as the legacy format; Default = True
- `dumps`: a function to serialize objects to json.
Default - :py:func:`quixstreams.utils.json.dumps`

<a id="quixstreams.models.serializers.quix.QuixTimeseriesSerializer"></a>

### QuixTimeseriesSerializer

```python
class QuixTimeseriesSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L315)

Serialize data to JSON formatted according to Quix Timeseries format.

The serializable object must be dictionary, and each item must be of `str`, `int`,
`float`, `bytes` or `bytearray` type.
Otherwise, the `SerializationError` will be raised.

Input:
```python
{'a': 1, 'b': 1.1, 'c': "string", 'd': b'bytes', 'Tags': {'tag1': 'tag'}}
```

Output:
```json
{
    "Timestamps": [123123123],
    "NumericValues": {"a": [1], "b": [1.1]},
    "StringValues": {"c": ["string"]},
    "BinaryValues": {"d": ["Ynl0ZXM="]},
    "TagValues": {"tag1": ["tag"]}
}
```

<a id="quixstreams.models.serializers.quix.QuixEventsSerializer"></a>

### QuixEventsSerializer

```python
class QuixEventsSerializer(QuixSerializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/quix.py#L403)

Serialize data to JSON formatted according to Quix EventData format.
The input value is expected to be a dictionary with the following keys:
    - "Id" (type `str`, default - "")
    - "Value" (type `str`, default - ""),
    - "Tags" (type `dict`, default - {})

>***NOTE:*** All the other fields will be ignored.

Input:
```python
{
    "Id": "an_event",
    "Value": "any_string",
    "Tags": {"tag1": "tag"}}
}
```

Output:
```json
{
    "Id": "an_event",
    "Value": "any_string",
    "Tags": {"tag1": "tag"}},
    "Timestamp":1692703362840389000
}
```

<a id="quixstreams.models.serializers.simple_types"></a>

## quixstreams.models.serializers.simple\_types

<a id="quixstreams.models.serializers.simple_types.BytesDeserializer"></a>

### BytesDeserializer

```python
class BytesDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L44)

A deserializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.BytesSerializer"></a>

### BytesSerializer

```python
class BytesSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L55)

A serializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.StringDeserializer"></a>

### StringDeserializer

```python
class StringDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L64)

<a id="quixstreams.models.serializers.simple_types.StringDeserializer.__init__"></a>

#### StringDeserializer.\_\_init\_\_

```python
def __init__(column_name: Optional[str] = None, codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L65)

Deserializes bytes to strings using the specified encoding.

**Arguments**:

- `codec`: string encoding
A wrapper around `confluent_kafka.serialization.StringDeserializer`.

<a id="quixstreams.models.serializers.simple_types.IntegerDeserializer"></a>

### IntegerDeserializer

```python
class IntegerDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L84)

Deserializes bytes to integers.

A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.

<a id="quixstreams.models.serializers.simple_types.DoubleDeserializer"></a>

### DoubleDeserializer

```python
class DoubleDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L103)

Deserializes float to IEEE 764 binary64.

A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.

<a id="quixstreams.models.serializers.simple_types.StringSerializer"></a>

### StringSerializer

```python
class StringSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L122)

<a id="quixstreams.models.serializers.simple_types.StringSerializer.__init__"></a>

#### StringSerializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L123)

Serializes strings to bytes using the specified encoding.

**Arguments**:

- `codec`: string encoding

<a id="quixstreams.models.serializers.simple_types.IntegerSerializer"></a>

### IntegerSerializer

```python
class IntegerSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L135)

Serializes integers to bytes

<a id="quixstreams.models.serializers.simple_types.DoubleSerializer"></a>

### DoubleSerializer

```python
class DoubleSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/simple_types.py#L148)

Serializes floats to bytes

<a id="quixstreams.models.serializers.json"></a>

## quixstreams.models.serializers.json

<a id="quixstreams.models.serializers.json.JSONSerializer"></a>

### JSONSerializer

```python
class JSONSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/json.py#L13)

<a id="quixstreams.models.serializers.json.JSONSerializer.__init__"></a>

#### JSONSerializer.\_\_init\_\_

```python
def __init__(dumps: Callable[[Any], Union[str, bytes]] = default_dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/json.py#L14)

Serializer that returns data in json format.

**Arguments**:

- `dumps`: a function to serialize objects to json.
Default - :py:func:`quixstreams.utils.json.dumps`

<a id="quixstreams.models.serializers.json.JSONDeserializer"></a>

### JSONDeserializer

```python
class JSONDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/json.py#L35)

<a id="quixstreams.models.serializers.json.JSONDeserializer.__init__"></a>

#### JSONDeserializer.\_\_init\_\_

```python
def __init__(column_name: Optional[str] = None,
             loads: Callable[[Union[bytes, bytearray]], Any] = default_loads)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/json.py#L36)

Deserializer that parses data from JSON

**Arguments**:

- `column_name`: if provided, the deserialized value will be wrapped into
dictionary with `column_name` as a key.
- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.

<a id="quixstreams.models.serializers.base"></a>

## quixstreams.models.serializers.base

<a id="quixstreams.models.serializers.base.SerializationContext"></a>

### SerializationContext

```python
class SerializationContext()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/base.py#L22)

Provides additional context for message serialization/deserialization.

Every `Serializer` and `Deserializer` receives an instance of `SerializationContext`

<a id="quixstreams.models.serializers.base.SerializationContext.to_confluent_ctx"></a>

#### SerializationContext.to\_confluent\_ctx

```python
def to_confluent_ctx(field: MessageField) -> _SerializationContext
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/base.py#L35)

Convert `SerializationContext` to `confluent_kafka.SerializationContext`

in order to re-use serialization already provided by `confluent_kafka` library.

**Arguments**:

- `field`: instance of `confluent_kafka.serialization.MessageField`

**Returns**:

instance of `confluent_kafka.serialization.SerializationContext`

<a id="quixstreams.models.serializers.base.Deserializer"></a>

### Deserializer

```python
class Deserializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/base.py#L47)

<a id="quixstreams.models.serializers.base.Deserializer.__init__"></a>

#### Deserializer.\_\_init\_\_

```python
def __init__(column_name: Optional[str] = None, *args, **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/base.py#L48)

A base class for all Deserializers

**Arguments**:

- `column_name`: if provided, the deserialized value will be wrapped into
dictionary with `column_name` as a key.

<a id="quixstreams.models.serializers.base.Deserializer.split_values"></a>

#### Deserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/base.py#L58)

Return True if the deserialized message should be considered as Iterable
and each item in it should be processed as a separate message.

<a id="quixstreams.models.serializers.base.Serializer"></a>

### Serializer

```python
class Serializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/base.py#L75)

A base class for all Serializers

<a id="quixstreams.models.serializers.base.Serializer.extra_headers"></a>

#### Serializer.extra\_headers

```python
@property
def extra_headers() -> MessageHeadersMapping
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/serializers/base.py#L81)

Informs producer to set additional headers

for the message it will be serializing

Must return a dictionary with headers.
Keys must be strings, and values must be strings, bytes or None.

**Returns**:

dict with headers

<a id="quixstreams.models.topics"></a>

## quixstreams.models.topics

<a id="quixstreams.models.topics.Topic"></a>

### Topic

```python
class Topic()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/topics.py#L64)

A representation of a Kafka topic and its expected data format via
designated key and value serializers/deserializers.

Typically created with an `app = quixstreams.app.Application()` instance via
`app.topic()`, and used by `quixstreams.dataframe.StreamingDataFrame`
instance.

<a id="quixstreams.models.topics.Topic.__init__"></a>

#### Topic.\_\_init\_\_

```python
def __init__(
        name: str,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = BytesDeserializer(),
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = BytesSerializer(),
        timestamp_extractor: Optional[TimestampExtractor] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/topics.py#L74)

Can specify serialization that should be used when consuming/producing

to the topic in the form of a string name (i.e. "json" for JSON) or a
serialization class instance directly, like JSONSerializer().


Example Snippet:

```python
from quixstreams.dataframe import StreamingDataFrame
from quixstreams.models import Topic, JSONSerializer

# Specify an input and output topic for a `StreamingDataFrame` instance,
# where the output topic requires adjusting the key serializer.
input_topic = Topic("input-topic", value_deserializer="json")
output_topic = Topic(
    "output-topic", key_serializer="str", value_serializer=JSONSerializer()
)
sdf = StreamingDataFrame(input_topic)
sdf.to_topic(output_topic)
```

**Arguments**:

- `name`: topic name
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message.

Example Snippet:

```python
def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    return value["timestamp"]
topic = Topic("input-topic", timestamp_extractor=custom_ts_extractor)
```

<a id="quixstreams.models.topics.Topic.name"></a>

#### Topic.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/topics.py#L135)

Topic name

<a id="quixstreams.models.topics.Topic.row_serialize"></a>

#### Topic.row\_serialize

```python
def row_serialize(row: Row, key: Optional[Any] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/topics.py#L141)

Serialize Row to a Kafka message structure

**Arguments**:

- `row`: Row to serialize
- `key`: message key to serialize, optional. Default - current Row key.

**Returns**:

KafkaMessage object with serialized values

<a id="quixstreams.models.topics.Topic.row_deserialize"></a>

#### Topic.row\_deserialize

```python
def row_deserialize(
        message: ConfluentKafkaMessageProto) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/topics.py#L164)

Deserialize incoming Kafka message to a Row.

**Arguments**:

- `message`: an object with interface of `confluent_kafka.Message`

**Returns**:

Row, list of Rows or None if the message is ignored.

<a id="quixstreams.models.messages"></a>

## quixstreams.models.messages

<a id="quixstreams.models.rows"></a>

## quixstreams.models.rows

<a id="quixstreams.models.rows.Row"></a>

### Row

```python
class Row()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/rows.py#L11)

Row is a dict-like interface on top of the message data + some Kafka props

<a id="quixstreams.models.rows.Row.keys"></a>

#### Row.keys

```python
def keys() -> KeysView
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/rows.py#L73)

Also allows unpacking row.value via **row

<a id="quixstreams.models.rows.Row.clone"></a>

#### Row.clone

```python
def clone(value: dict) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/models/rows.py#L85)

Manually clone the Row; doing it this way is much faster than doing a deepcopy
on the entire Row object.

<a id="quixstreams.state.rocksdb.windowed.store"></a>

## quixstreams.state.rocksdb.windowed.store

<a id="quixstreams.state.rocksdb.windowed.partition"></a>

## quixstreams.state.rocksdb.windowed.partition

<a id="quixstreams.state.rocksdb.windowed.partition.WindowedRocksDBStorePartition"></a>

### WindowedRocksDBStorePartition

```python
class WindowedRocksDBStorePartition(RocksDBStorePartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/partition.py#L21)

A base class to access windowed state in RocksDB.

It represents a single RocksDB database.

Besides the data, it keeps track of the latest observed timestamp and
stores the expiration index to delete expired windows.

**Arguments**:

- `path`: an absolute path to the RocksDB folder
- `options`: RocksDB options. If `None`, the default options will be used.
- `open_max_retries`: number of times to retry opening the database
if it's locked by another process. To disable retrying, pass 0.
- `open_retry_backoff`: number of seconds to wait between each retry.

<a id="quixstreams.state.rocksdb.windowed.transaction"></a>

## quixstreams.state.rocksdb.windowed.transaction

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction"></a>

### WindowedRocksDBPartitionTransaction

```python
class WindowedRocksDBPartitionTransaction(RocksDBPartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/transaction.py#L16)

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction.expire_windows"></a>

#### WindowedRocksDBPartitionTransaction.expire\_windows

```python
def expire_windows(duration_ms: int,
                   grace_ms: int = 0) -> List[Tuple[Tuple[int, int], Any]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/transaction.py#L74)

Get a list of expired windows from RocksDB considering latest timestamp,

window size and grace period.
It marks the latest found window as expired in the expiration index, so
calling this method multiple times will yield different results for the same
"latest timestamp".

How it works:
- First, it looks for the start time of the last expired window for the current
  prefix using expiration cache. If it's found, it will be used to reduce
  the search space and to avoid returning already expired windows.
- Then it goes over window segments and fetches the windows
  that should be expired.
- At last, it updates the expiration cache with the start time of the latest
  found windows

**Returns**:

sorted list of tuples in format `((start, end), value)`

<a id="quixstreams.state.rocksdb.windowed"></a>

## quixstreams.state.rocksdb.windowed

<a id="quixstreams.state.rocksdb.windowed.serialization"></a>

## quixstreams.state.rocksdb.windowed.serialization

<a id="quixstreams.state.rocksdb.windowed.serialization.parse_window_key"></a>

#### parse\_window\_key

```python
def parse_window_key(key: bytes) -> Tuple[bytes, int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/serialization.py#L12)

Parse the window key from Rocksdb into (message_key, start, end) structure.

Expected window key format:
<message_key>|<start>|<end>

**Arguments**:

- `key`: a key from Rocksdb

**Returns**:

a tuple with message key, start timestamp, end timestamp

<a id="quixstreams.state.rocksdb.windowed.serialization.encode_window_key"></a>

#### encode\_window\_key

```python
def encode_window_key(start_ms: int, end_ms: int) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/serialization.py#L39)

Encode window start and end timestamps into bytes of the following format:

```<start>|<end>```

Encoding window keys this way make them sortable in RocksDB within the same prefix.

**Arguments**:

- `start_ms`: window start in milliseconds
- `end_ms`: window end in milliseconds

**Returns**:

window timestamps as bytes

<a id="quixstreams.state.rocksdb.windowed.serialization.encode_window_prefix"></a>

#### encode\_window\_prefix

```python
def encode_window_prefix(prefix: bytes, start_ms: int) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/serialization.py#L53)

Encode window prefix and start time to iterate over keys in RocksDB

Format:
```<prefix>|<start>```

**Arguments**:

- `prefix`: transaction prefix
- `start_ms`: window start time in milliseconds

**Returns**:

bytes

<a id="quixstreams.state.rocksdb.windowed.state"></a>

## quixstreams.state.rocksdb.windowed.state

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState"></a>

### WindowedTransactionState

```python
class WindowedTransactionState(WindowedState)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/state.py#L9)

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.__init__"></a>

#### WindowedTransactionState.\_\_init\_\_

```python
def __init__(transaction: "WindowedRocksDBPartitionTransaction")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/state.py#L12)

A windowed state to be provided into `StreamingDataFrame` window functions.

**Arguments**:

- `transaction`: instance of `WindowedRocksDBPartitionTransaction`

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.get_window"></a>

#### WindowedTransactionState.get\_window

```python
def get_window(start_ms: int,
               end_ms: int,
               default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/state.py#L20)

Get the value of the window defined by `start` and `end` timestamps

if the window is present in the state, else default

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.update_window"></a>

#### WindowedTransactionState.update\_window

```python
def update_window(start_ms: int, end_ms: int, value: Any, timestamp_ms: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/state.py#L36)

Set a value for the window.

This method will also update the latest observed timestamp in state partition
using the provided `timestamp`.

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `value`: value of the window
- `timestamp_ms`: current message timestamp in milliseconds

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.get_latest_timestamp"></a>

#### WindowedTransactionState.get\_latest\_timestamp

```python
def get_latest_timestamp() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/state.py#L53)

Get the latest observed timestamp for the current state partition.

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest observed event timestamp in milliseconds

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.expire_windows"></a>

#### WindowedTransactionState.expire\_windows

```python
def expire_windows(duration_ms: int,
                   grace_ms: int = 0) -> List[Tuple[Tuple[int, int], Any]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/windowed/state.py#L65)

Get a list of expired windows from RocksDB considering the current
latest timestamp, window duration and grace period.

It also marks the latest found window as expired in the expiration index, so
calling this method multiple times will yield different results for the same
"latest timestamp".

<a id="quixstreams.state.rocksdb.options"></a>

## quixstreams.state.rocksdb.options

<a id="quixstreams.state.rocksdb.options.RocksDBOptions"></a>

### RocksDBOptions

```python
@dataclasses.dataclass(frozen=True)
class RocksDBOptions(RocksDBOptionsType)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/options.py#L25)

Common RocksDB database options.

Please see `rocksdict.Options` for a complete description of each option.

To provide extra options that are not presented in this class, feel free
to override it and specify the additional values.

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.write_buffer_size"></a>

#### write\_buffer\_size

64MB

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.target_file_size_base"></a>

#### target\_file\_size\_base

64MB

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.block_cache_size"></a>

#### block\_cache\_size

128MB

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.max_total_wal_size"></a>

#### max\_total\_wal\_size

128MB

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.to_options"></a>

#### RocksDBOptions.to\_options

```python
def to_options() -> rocksdict.Options
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/options.py#L48)

Convert parameters to `rocksdict.Options`

**Returns**:

instance of `rocksdict.Options`

<a id="quixstreams.state.rocksdb.store"></a>

## quixstreams.state.rocksdb.store

<a id="quixstreams.state.rocksdb.store.RocksDBStore"></a>

### RocksDBStore

```python
class RocksDBStore(Store)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L18)

<a id="quixstreams.state.rocksdb.store.RocksDBStore.__init__"></a>

#### RocksDBStore.\_\_init\_\_

```python
def __init__(name: str,
             topic: str,
             base_dir: str,
             options: Optional[RocksDBOptionsType] = None,
             open_max_retries: int = 10,
             open_retry_backoff: float = 3.0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L19)

RocksDB-based state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

**Arguments**:

- `name`: a unique store name
- `topic`: a topic name for this store
- `base_dir`: path to a directory with the state
- `options`: RocksDB options. If `None`, the default options will be used.
- `open_max_retries`: number of times to retry opening the database
if it's locked by another process. To disable retrying, pass 0.
- `open_retry_backoff`: number of seconds to wait between each retry.

<a id="quixstreams.state.rocksdb.store.RocksDBStore.topic"></a>

#### RocksDBStore.topic

```python
@property
def topic() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L52)

Store topic name

<a id="quixstreams.state.rocksdb.store.RocksDBStore.name"></a>

#### RocksDBStore.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L59)

Store name

<a id="quixstreams.state.rocksdb.store.RocksDBStore.partitions"></a>

#### RocksDBStore.partitions

```python
@property
def partitions() -> Dict[int, RocksDBStorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L66)

Mapping of assigned store partitions

<a id="quixstreams.state.rocksdb.store.RocksDBStore.assign_partition"></a>

#### RocksDBStore.assign\_partition

```python
def assign_partition(partition: int) -> RocksDBStorePartition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L80)

Open and assign store partition.

If the partition is already assigned, it will not re-open it and return
the existing partition instead.

**Arguments**:

- `partition`: partition number

**Returns**:

instance of`RocksDBStorePartition`

<a id="quixstreams.state.rocksdb.store.RocksDBStore.revoke_partition"></a>

#### RocksDBStore.revoke\_partition

```python
def revoke_partition(partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L110)

Revoke and close the assigned store partition.

If the partition is not assigned, it will log the message and return.

**Arguments**:

- `partition`: partition number

<a id="quixstreams.state.rocksdb.store.RocksDBStore.start_partition_transaction"></a>

#### RocksDBStore.start\_partition\_transaction

```python
def start_partition_transaction(partition: int) -> RocksDBPartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L131)

Start a new partition transaction.

`RocksDBPartitionTransaction` is the primary interface for working with data in
the underlying RocksDB.

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `RocksDBPartitionTransaction`

<a id="quixstreams.state.rocksdb.store.RocksDBStore.close"></a>

#### RocksDBStore.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/store.py#L153)

Close the store and revoke all assigned partitions

<a id="quixstreams.state.rocksdb.partition"></a>

## quixstreams.state.rocksdb.partition

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition"></a>

### RocksDBStorePartition

```python
class RocksDBStorePartition(StorePartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L47)

A base class to access state in RocksDB.

It represents a single RocksDB database.

Responsibilities:
 1. Managing access to the RocksDB instance
 2. Creating transactions to interact with data
 3. Flushing WriteBatches to the RocksDB

It opens the RocksDB on `__init__`. If the db is locked by another process,
it will retry according to `open_max_retries` and `open_retry_backoff`.

**Arguments**:

- `path`: an absolute path to the RocksDB folder
- `options`: RocksDB options. If `None`, the default options will be used.
- `open_max_retries`: number of times to retry opening the database
if it's locked by another process. To disable retrying, pass 0.
- `open_retry_backoff`: number of seconds to wait between each retry.

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.begin"></a>

#### RocksDBStorePartition.begin

```python
def begin() -> "RocksDBPartitionTransaction"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L85)

Create a new `RocksDBTransaction` object.

Using `RocksDBTransaction` is a recommended way for accessing the data.

**Returns**:

an instance of `RocksDBTransaction`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.write"></a>

#### RocksDBStorePartition.write

```python
def write(batch: WriteBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L96)

Write `WriteBatch` to RocksDB

**Arguments**:

- `batch`: an instance of `rocksdict.WriteBatch`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get"></a>

#### RocksDBStorePartition.get

```python
def get(key: bytes,
        default: Any = None,
        cf_name: str = "default") -> Union[None, bytes, Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L103)

Get a key from RocksDB.

**Arguments**:

- `key`: a key encoded to `bytes`
- `default`: a default value to return if the key is not found.
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

a value if the key is present in the DB. Otherwise, `default`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.exists"></a>

#### RocksDBStorePartition.exists

```python
def exists(key: bytes, cf_name: str = "default") -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L117)

Check if a key is present in the DB.

**Arguments**:

- `key`: a key encoded to `bytes`.
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

`True` if the key is present, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_processed_offset"></a>

#### RocksDBStorePartition.get\_processed\_offset

```python
def get_processed_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L128)

Get last processed offset for the given partition

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.close"></a>

#### RocksDBStorePartition.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L140)

Close the underlying RocksDB

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.path"></a>

#### RocksDBStorePartition.path

```python
@property
def path() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L153)

Absolute path to RocksDB database folder

**Returns**:

file path

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.destroy"></a>

#### RocksDBStorePartition.destroy

```python
@classmethod
def destroy(cls, path: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L161)

Delete underlying RocksDB database

The database must be closed first.

**Arguments**:

- `path`: an absolute path to the RocksDB folder

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_column_family_handle"></a>

#### RocksDBStorePartition.get\_column\_family\_handle

```python
def get_column_family_handle(cf_name: str) -> ColumnFamily
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L171)

Get a column family handle to pass to it WriteBatch.

This method will cache the CF handle instance to avoid creating them
repeatedly.

**Arguments**:

- `cf_name`: column family name

**Returns**:

instance of `rocksdict.ColumnFamily`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_column_family"></a>

#### RocksDBStorePartition.get\_column\_family

```python
def get_column_family(cf_name: str) -> Rdict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L187)

Get a column family instance.

This method will cache the CF instance to avoid creating them repeatedly.

**Arguments**:

- `cf_name`: column family name

**Returns**:

instance of `rocksdict.Rdict` for the given column family

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction"></a>

### RocksDBPartitionTransaction

```python
class RocksDBPartitionTransaction(PartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L314)

A transaction class to perform simple key-value operations like

"get", "set", "delete" and "exists" on a single RocksDB partition.

Serialization
*************
`RocksDBTransaction` automatically serializes keys and values to bytes.

Prefixing
*********
`RocksDBTransaction` allows to set prefixes for the keys in the given code block
using :meth:`with_prefix()` context manager.
Normally, `StreamingDataFrame` class will use message keys as prefixes
in order to namespace the stored keys across different messages.

Transactional properties
************************
`RocksDBTransaction` uses a combination of in-memory update cache
and RocksDB's WriteBatch in order to accumulate all the state mutations
in a single batch, flush them atomically, and allow the updates be visible
within the transaction before it's flushed (aka "read-your-own-writes" problem).

If any mutation fails during the transaction
(e.g. we failed to write the updates to the RocksDB), the whole transaction
will be marked as failed and cannot be used anymore.
In this case, a new `RocksDBTransaction` should be created.

`RocksDBTransaction` can be used only once.

**Arguments**:

- `partition`: instance of `RocksDBStatePartition` to be used for accessing
the underlying RocksDB
- `dumps`: a function to serialize data to bytes.
- `loads`: a function to deserialize data from bytes.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.with_prefix"></a>

#### RocksDBPartitionTransaction.with\_prefix

```python
@contextlib.contextmanager
def with_prefix(prefix: Any = b"") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L383)

A context manager set the prefix for all keys in the scope.

Normally, it's called by Streaming DataFrames engine to ensure that every
message key is stored separately.

The `with_prefix` calls should not be nested.
Only one prefix can be set at a time.

**Arguments**:

- `prefix`: a prefix string to be used.
Should be either `bytes` or object serializable to `bytes`
by `dumps` function.
The prefix doesn't need to contain the separator, it will be added
automatically between the key and the prefix if the prefix
is not empty.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.get"></a>

#### RocksDBPartitionTransaction.get

```python
@_validate_transaction_state
def get(key: Any,
        default: Any = None,
        cf_name: str = "default") -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L412)

Get a key from the store.

It first looks up the key in the update cache in case it has been updated
but not flushed yet.

It returns `None` if the key is not found and `default` is not provided.

**Arguments**:

- `key`: a key to get from DB
- `default`: value to return if the key is not present in the state.
It can be of any type.
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

value or `default`

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.set"></a>

#### RocksDBPartitionTransaction.set

```python
@_validate_transaction_state
def set(key: Any, value: Any, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L447)

Set a key to the store.

It first updates the key in the update cache.

**Arguments**:

- `key`: key to store in DB
- `value`: value to store in DB
- `cf_name`: rocksdb column family name. Default - "default"

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.delete"></a>

#### RocksDBPartitionTransaction.delete

```python
@_validate_transaction_state
def delete(key: Any, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L472)

Delete a key from the store.

It first deletes the key from the update cache.

**Arguments**:

- `key`: key to delete from DB
- `cf_name`: rocksdb column family name. Default - "default"

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.exists"></a>

#### RocksDBPartitionTransaction.exists

```python
@_validate_transaction_state
def exists(key: Any, cf_name: str = "default") -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L495)

Check if a key exists in the store.

It first looks up the key in the update cache.

**Arguments**:

- `key`: a key to check in DB
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

`True` if the key exists, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.completed"></a>

#### RocksDBPartitionTransaction.completed

```python
@property
def completed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L517)

Check if the transaction is completed.

It doesn't indicate whether transaction is successful or not.
Use `RocksDBTransaction.failed` for that.

The completed transaction should not be re-used.

**Returns**:

`True` if transaction is completed, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.failed"></a>

#### RocksDBPartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L531)

Check if the transaction has failed.

The failed transaction should not be re-used because the update cache
and

**Returns**:

`True` if transaction is failed, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBPartitionTransaction.maybe_flush"></a>

#### RocksDBPartitionTransaction.maybe\_flush

```python
@_validate_transaction_state
def maybe_flush(offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/rocksdb/partition.py#L543)

Flush the recent updates to the database and empty the update cache.

It writes the WriteBatch to RocksDB and marks itself as finished.

If writing fails, the transaction will be also marked as "failed" and
cannot be used anymore.

>***NOTE:*** If no keys have been modified during the transaction
    (i.e. no "set" or "delete" have been called at least once), it will
    not flush ANY data to the database including the offset in order to optimize
    I/O.

**Arguments**:

- `offset`: offset of the last processed message, optional.

<a id="quixstreams.state.rocksdb.metadata"></a>

## quixstreams.state.rocksdb.metadata

<a id="quixstreams.state.rocksdb"></a>

## quixstreams.state.rocksdb

<a id="quixstreams.state.rocksdb.types"></a>

## quixstreams.state.rocksdb.types

<a id="quixstreams.state.rocksdb.exceptions"></a>

## quixstreams.state.rocksdb.exceptions

<a id="quixstreams.state.rocksdb.serialization"></a>

## quixstreams.state.rocksdb.serialization

<a id="quixstreams.state"></a>

## quixstreams.state

<a id="quixstreams.state.types"></a>

## quixstreams.state.types

<a id="quixstreams.state.types.Store"></a>

### Store

```python
class Store(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L9)

Abstract state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

<a id="quixstreams.state.types.Store.topic"></a>

#### Store.topic

```python
@property
def topic() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L18)

Topic name

<a id="quixstreams.state.types.Store.name"></a>

#### Store.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L24)

Store name

<a id="quixstreams.state.types.Store.partitions"></a>

#### Store.partitions

```python
@property
def partitions() -> Dict[int, "StorePartition"]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L30)

Mapping of assigned store partitions

**Returns**:

dict of "{partition: <StorePartition>}"

<a id="quixstreams.state.types.Store.assign_partition"></a>

#### Store.assign\_partition

```python
def assign_partition(partition: int) -> "StorePartition"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L37)

Assign new store partition

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `StorePartition`

<a id="quixstreams.state.types.Store.revoke_partition"></a>

#### Store.revoke\_partition

```python
def revoke_partition(partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L46)

Revoke assigned store partition

**Arguments**:

- `partition`: partition number

<a id="quixstreams.state.types.Store.start_partition_transaction"></a>

#### Store.start\_partition\_transaction

```python
def start_partition_transaction(
        partition: int) -> Optional["PartitionTransaction"]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L55)

Start a new partition transaction.

`PartitionTransaction` is the primary interface for working with data in Stores.

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `PartitionTransaction`

<a id="quixstreams.state.types.Store.close"></a>

#### Store.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L66)

Close store and revoke all store partitions

<a id="quixstreams.state.types.StorePartition"></a>

### StorePartition

```python
class StorePartition(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L78)

A base class to access state in the underlying storage.
It represents a single instance of some storage (e.g. a single database for
the persistent storage).

<a id="quixstreams.state.types.StorePartition.path"></a>

#### StorePartition.path

```python
@property
def path() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L87)

Absolute path to RocksDB database folder

<a id="quixstreams.state.types.StorePartition.begin"></a>

#### StorePartition.begin

```python
def begin() -> "PartitionTransaction"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L93)

State new `PartitionTransaction`

<a id="quixstreams.state.types.State"></a>

### State

```python
class State(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L102)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.types.State.get"></a>

#### State.get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L107)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.State.set"></a>

#### State.set

```python
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L116)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.types.State.delete"></a>

#### State.delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L123)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.types.State.exists"></a>

#### State.exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L131)

Check if the key exists in state.

**Arguments**:

- `key`: key

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.types.PartitionTransaction"></a>

### PartitionTransaction

```python
class PartitionTransaction(State)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L139)

A transaction class to perform simple key-value operations like
"get", "set", "delete" and "exists" on a single storage partition.

<a id="quixstreams.state.types.PartitionTransaction.state"></a>

#### PartitionTransaction.state

```python
@property
def state() -> State
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L146)

An instance of State to be provided to `StreamingDataFrame` functions


<a id="quixstreams.state.types.PartitionTransaction.failed"></a>

#### PartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L153)

Return `True` if transaction failed to update data at some point.

Failed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.PartitionTransaction.completed"></a>

#### PartitionTransaction.completed

```python
@property
def completed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L162)

Return `True` if transaction is completed.

Completed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.PartitionTransaction.with_prefix"></a>

#### PartitionTransaction.with\_prefix

```python
def with_prefix(prefix: Any = b"") -> Iterator[Self]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L171)

A context manager set the prefix for all keys in the scope.

Normally, it's called by `StreamingDataFrame` internals to ensure that every
message key is stored separately.

**Arguments**:

- `prefix`: key prefix

**Returns**:

context maager

<a id="quixstreams.state.types.PartitionTransaction.maybe_flush"></a>

#### PartitionTransaction.maybe\_flush

```python
def maybe_flush(offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L181)

Flush the recent updates and last processed offset to the storage.

**Arguments**:

- `offset`: offset of the last processed message, optional.

<a id="quixstreams.state.types.WindowedState"></a>

### WindowedState

```python
class WindowedState(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L194)

A windowed state to be provided into `StreamingDataFrame` window functions.

<a id="quixstreams.state.types.WindowedState.get_window"></a>

#### WindowedState.get\_window

```python
def get_window(start_ms: int,
               end_ms: int,
               default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L199)

Get the value of the window defined by `start` and `end` timestamps

if the window is present in the state, else default

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.WindowedState.update_window"></a>

#### WindowedState.update\_window

```python
def update_window(start_ms: int, end_ms: int, value: Any, timestamp_ms: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L212)

Set a value for the window.

This method will also update the latest observed timestamp in state partition
using the provided `timestamp`.

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `value`: value of the window
- `timestamp_ms`: current message timestamp in milliseconds

<a id="quixstreams.state.types.WindowedState.get_latest_timestamp"></a>

#### WindowedState.get\_latest\_timestamp

```python
def get_latest_timestamp() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L225)

Get the latest observed timestamp for the current state partition.

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest observed event timestamp in milliseconds

<a id="quixstreams.state.types.WindowedState.expire_windows"></a>

#### WindowedState.expire\_windows

```python
def expire_windows(duration_ms: int, grace_ms: int = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L235)

Get a list of expired windows from RocksDB considering the current

latest timestamp, window duration and grace period.

It also marks the latest found window as expired in the expiration index, so
calling this method multiple times will yield different results for the same
"latest timestamp".

**Arguments**:

- `duration_ms`: duration of the windows in milliseconds
- `grace_ms`: grace period in milliseconds. Default - "0"

<a id="quixstreams.state.types.WindowedPartitionTransaction"></a>

### WindowedPartitionTransaction

```python
class WindowedPartitionTransaction(WindowedState)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L249)

<a id="quixstreams.state.types.WindowedPartitionTransaction.failed"></a>

#### WindowedPartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L255)

Return `True` if transaction failed to update data at some point.

Failed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.WindowedPartitionTransaction.completed"></a>

#### WindowedPartitionTransaction.completed

```python
@property
def completed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L264)

Return `True` if transaction is completed.

Completed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.WindowedPartitionTransaction.with_prefix"></a>

#### WindowedPartitionTransaction.with\_prefix

```python
def with_prefix(prefix: Any = b"") -> Iterator[Self]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L273)

A context manager set the prefix for all keys in the scope.

Normally, it's called by `StreamingDataFrame` internals to ensure that every
message key is stored separately.

**Arguments**:

- `prefix`: key prefix

**Returns**:

context maager

<a id="quixstreams.state.types.WindowedPartitionTransaction.maybe_flush"></a>

#### WindowedPartitionTransaction.maybe\_flush

```python
def maybe_flush(offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/types.py#L283)

Flush the recent updates and last processed offset to the storage.

**Arguments**:

- `offset`: offset of the last processed message, optional.

<a id="quixstreams.state.exceptions"></a>

## quixstreams.state.exceptions

<a id="quixstreams.state.manager"></a>

## quixstreams.state.manager

<a id="quixstreams.state.manager.StateStoreManager"></a>

### StateStoreManager

```python
class StateStoreManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L29)

Class for managing state stores and partitions.

StateStoreManager is responsible for:
 - reacting to rebalance callbacks
 - managing the individual state stores
 - providing access to store transactions

<a id="quixstreams.state.manager.StateStoreManager.stores"></a>

#### StateStoreManager.stores

```python
@property
def stores() -> Dict[str, Dict[str, Store]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L65)

Map of registered state stores

**Returns**:

dict in format {topic: {store_name: store}}

<a id="quixstreams.state.manager.StateStoreManager.get_store"></a>

#### StateStoreManager.get\_store

```python
def get_store(topic: str,
              store_name: str = _DEFAULT_STATE_STORE_NAME) -> Store
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L72)

Get a store for given name and topic

**Arguments**:

- `topic`: topic name
- `store_name`: store name

**Returns**:

instance of `Store`

<a id="quixstreams.state.manager.StateStoreManager.register_store"></a>

#### StateStoreManager.register\_store

```python
def register_store(topic_name: str,
                   store_name: str = _DEFAULT_STATE_STORE_NAME)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L88)

Register a state store to be managed by StateStoreManager.

During processing, the StateStoreManager will react to rebalancing callbacks
and assign/revoke the partitions for registered stores.

Each store can be registered only once for each topic.

**Arguments**:

- `topic_name`: topic name
- `store_name`: store name

<a id="quixstreams.state.manager.StateStoreManager.register_windowed_store"></a>

#### StateStoreManager.register\_windowed\_store

```python
def register_windowed_store(topic_name: str, store_name: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L111)

Register a windowed state store to be managed by StateStoreManager.

During processing, the StateStoreManager will react to rebalancing callbacks
and assign/revoke the partitions for registered stores.

Each window store can be registered only once for each topic.

**Arguments**:

- `topic_name`: topic name
- `store_name`: store name

<a id="quixstreams.state.manager.StateStoreManager.clear_stores"></a>

#### StateStoreManager.clear\_stores

```python
def clear_stores()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L134)

Delete all state stores managed by StateStoreManager.

<a id="quixstreams.state.manager.StateStoreManager.on_partition_assign"></a>

#### StateStoreManager.on\_partition\_assign

```python
def on_partition_assign(tp: TopicPartition) -> List[StorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L149)

Assign store partitions for each registered store for the given `TopicPartition`

and return a list of assigned `StorePartition` objects.

**Arguments**:

- `tp`: `TopicPartition` from Kafka consumer

**Returns**:

list of assigned `StorePartition`

<a id="quixstreams.state.manager.StateStoreManager.on_partition_revoke"></a>

#### StateStoreManager.on\_partition\_revoke

```python
def on_partition_revoke(tp: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L163)

Revoke store partitions for each registered store for the given `TopicPartition`

**Arguments**:

- `tp`: `TopicPartition` from Kafka consumer

<a id="quixstreams.state.manager.StateStoreManager.on_partition_lost"></a>

#### StateStoreManager.on\_partition\_lost

```python
def on_partition_lost(tp: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L172)

Revoke and close store partitions for each registered store for the given

`TopicPartition`

**Arguments**:

- `tp`: `TopicPartition` from Kafka consumer

<a id="quixstreams.state.manager.StateStoreManager.init"></a>

#### StateStoreManager.init

```python
def init()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L182)

Initialize `StateStoreManager` and create a store directory


<a id="quixstreams.state.manager.StateStoreManager.close"></a>

#### StateStoreManager.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L189)

Close all registered stores

<a id="quixstreams.state.manager.StateStoreManager.get_store_transaction"></a>

#### StateStoreManager.get\_store\_transaction

```python
def get_store_transaction(
        store_name: str = _DEFAULT_STATE_STORE_NAME) -> PartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L197)

Get active `PartitionTransaction` for the store

**Arguments**:

- `store_name`: 

<a id="quixstreams.state.manager.StateStoreManager.start_store_transaction"></a>

#### StateStoreManager.start\_store\_transaction

```python
@contextlib.contextmanager
def start_store_transaction(topic: str, partition: int,
                            offset: int) -> Iterator["_MultiStoreTransaction"]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/manager.py#L212)

Starting the multi-store transaction for the Kafka message.

This transaction will keep track of all used stores and flush them in the end.
If any exception is catched during this transaction, none of them
will be flushed as a best effort to keep stores consistent in "at-least-once" setting.

There can be only one active transaction at a time. Starting a new transaction
before the end of the current one will fail.

**Arguments**:

- `topic`: message topic
- `partition`: message partition
- `offset`: message offset

<a id="quixstreams.state.state"></a>

## quixstreams.state.state

<a id="quixstreams.state.state.TransactionState"></a>

### TransactionState

```python
class TransactionState(State)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/state.py#L6)

<a id="quixstreams.state.state.TransactionState.__init__"></a>

#### TransactionState.\_\_init\_\_

```python
def __init__(transaction: PartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/state.py#L9)

Simple key-value state to be provided into `StreamingDataFrame` functions

**Arguments**:

- `transaction`: instance of `PartitionTransaction`

<a id="quixstreams.state.state.TransactionState.get"></a>

#### TransactionState.get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/state.py#L17)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.state.TransactionState.set"></a>

#### TransactionState.set

```python
def set(key: Any, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/state.py#L27)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.state.TransactionState.delete"></a>

#### TransactionState.delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/state.py#L35)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.state.TransactionState.exists"></a>

#### TransactionState.exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/state/state.py#L44)

Check if the key exists in state.

**Arguments**:

- `key`: key

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.exceptions"></a>

## quixstreams.exceptions

<a id="quixstreams.exceptions.assignment"></a>

## quixstreams.exceptions.assignment

<a id="quixstreams.exceptions.assignment.PartitionAssignmentError"></a>

### PartitionAssignmentError

```python
class PartitionAssignmentError(QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/exceptions/assignment.py#L6)

Error happened during partition rebalancing.
Raised from `on_assign`, `on_revoke` and `on_lost` callbacks

<a id="quixstreams.exceptions.base"></a>

## quixstreams.exceptions.base

<a id="quixstreams.context"></a>

## quixstreams.context

<a id="quixstreams.context.set_message_context"></a>

#### set\_message\_context

```python
def set_message_context(context: Optional[MessageContext])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/context.py#L22)

Set a MessageContext for the current message in the given `contextvars.Context`

>***NOTE:*** This is for advanced usage only. If you need to change the message key,
`StreamingDataFrame.to_topic()` has an argument for it.


Example Snippet:

```python
from quixstreams import Application, set_message_context, message_context

# Changes the current sdf value based on what the message partition is.
def alter_context(value):
    context = message_context()
    if value > 1:
        context.headers = context.headers + (b"cool_new_header", value.encode())
        set_message_context(context)

app = Application()
sdf = app.dataframe()
sdf = sdf.update(lambda value: alter_context(value))
```

**Arguments**:

- `context`: instance of `MessageContext`

<a id="quixstreams.context.message_context"></a>

#### message\_context

```python
def message_context() -> MessageContext
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/context.py#L53)

Get a MessageContext for the current message, which houses most of the message

metadata, like:
    - key
    - timestamp
    - partition
    - offset


Example Snippet:

```python
from quixstreams import Application, message_context

# Changes the current sdf value based on what the message partition is.

app = Application()
sdf = app.dataframe()
sdf = sdf.apply(lambda value: 1 if message_context().partition == 2 else 0)
```

**Returns**:

instance of `MessageContext`

<a id="quixstreams.context.message_key"></a>

#### message\_key

```python
def message_key() -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/context.py#L84)

Get the current message's key.

Example Snippet:

```python
from quixstreams import Application, message_key

# Changes the current sdf value based on what the message key is.

app = Application()
sdf = app.dataframe()
sdf = sdf.apply(lambda value: 1 if message_key() == b'1' else 0)
```

**Returns**:

a deserialized message key

<a id="quixstreams.kafka"></a>

## quixstreams.kafka

<a id="quixstreams.kafka.producer"></a>

## quixstreams.kafka.producer

<a id="quixstreams.kafka.producer.Producer"></a>

### Producer

```python
class Producer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/producer.py#L58)

<a id="quixstreams.kafka.producer.Producer.__init__"></a>

#### Producer.\_\_init\_\_

```python
def __init__(broker_address: str,
             partitioner: Partitioner = "murmur2",
             extra_config: dict = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/producer.py#L59)

A wrapper around `confluent_kafka.Producer`.

It initializes `confluent_kafka.Producer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.

**Arguments**:

- `broker_address`: Kafka broker host and port in format `<host>:<port>`.
Passed as `bootstrap.servers` to `confluent_kafka.Producer`.
- `partitioner`: A function to be used to determine the outgoing message
partition.
Available values: "random", "consistent_random", "murmur2", "murmur2_random",
"fnv1a", "fnv1a_random"
Default - "murmur2".
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
Note: values passed as arguments override values in `extra_config`.

<a id="quixstreams.kafka.producer.Producer.produce"></a>

#### Producer.produce

```python
def produce(topic: str,
            value: Union[str, bytes],
            key: Optional[Union[str, bytes]] = None,
            headers: Optional[Headers] = None,
            partition: Optional[int] = None,
            timestamp: Optional[int] = None,
            poll_timeout: float = 5.0,
            buffer_error_max_tries: int = 3)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/producer.py#L98)

Produce message to topic.

It also polls Kafka for callbacks before producing in order to minimize
the probability of `BufferError`.
If `BufferError` still happens, the method will poll Kafka with timeout
to free up the buffer and try again.

**Arguments**:

- `topic`: topic name
- `value`: message value
- `key`: message key
- `headers`: message headers
- `partition`: topic partition
- `timestamp`: message timestamp
- `poll_timeout`: timeout for `poll()` call in case of `BufferError`
- `buffer_error_max_tries`: max retries for `BufferError`.
Pass `0` to not retry after `BufferError`.

<a id="quixstreams.kafka.producer.Producer.poll"></a>

#### Producer.poll

```python
def poll(timeout: float = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/producer.py#L156)

Polls the producer for events and calls `on_delivery` callbacks.

**Arguments**:

- `timeout`: poll timeout seconds

<a id="quixstreams.kafka.producer.Producer.flush"></a>

#### Producer.flush

```python
def flush(timeout: float = None) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/producer.py#L163)

Wait for all messages in the Producer queue to be delivered.

**Arguments**:

- `timeout`: timeout is seconds

**Returns**:

number of messages delivered

<a id="quixstreams.kafka.consumer"></a>

## quixstreams.kafka.consumer

<a id="quixstreams.kafka.consumer.Consumer"></a>

### Consumer

```python
class Consumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L66)

<a id="quixstreams.kafka.consumer.Consumer.__init__"></a>

#### Consumer.\_\_init\_\_

```python
def __init__(broker_address: str,
             consumer_group: Optional[str],
             auto_offset_reset: AutoOffsetReset,
             auto_commit_enable: bool = True,
             assignment_strategy: AssignmentStrategy = "range",
             on_commit: Callable[[Optional[KafkaError], List[TopicPartition]],
                                 None] = None,
             extra_config: Optional[dict] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L67)

A wrapper around `confluent_kafka.Consumer`.

It initializes `confluent_kafka.Consumer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.

**Arguments**:

- `broker_address`: Kafka broker host and port in format `<host>:<port>`.
Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Available values:
- "earliest" - automatically reset the offset to the smallest offset
- "latest" - automatically reset the offset to the largest offset
- "error" - trigger an error (ERR__AUTO_OFFSET_RESET) which is retrieved
  by consuming messages (used for testing)
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `assignment_strategy`: The name of a partition assignment strategy.
Available values: "range", "roundrobin", "cooperative-sticky".
- `on_commit`: Offset commit result propagation callback.
Passed as "offset_commit_cb" to `confluent_kafka.Consumer`.
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
Note: values passed as arguments override values in `extra_config`.

<a id="quixstreams.kafka.consumer.Consumer.poll"></a>

#### Consumer.poll

```python
def poll(timeout: float = None) -> Optional[Message]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L124)

Consumes a single message, calls callbacks and returns events.

The application must check the returned :py:class:`Message`
object's :py:func:`Message.error()` method to distinguish between proper
messages (error() returns None), or an event or error.

Note: Callbacks may be called from this method, such as
``on_assign``, ``on_revoke``, et al.

**Arguments**:

- `timeout` (`float`): Maximum time in seconds to block waiting for message,
event or callback. Default: infinite.

**Raises**:

- `None`: RuntimeError if called on a closed consumer

**Returns**:

A Message object or None on timeout

<a id="quixstreams.kafka.consumer.Consumer.subscribe"></a>

#### Consumer.subscribe

```python
def subscribe(topics: List[str],
              on_assign: Optional[RebalancingCallback] = None,
              on_revoke: Optional[RebalancingCallback] = None,
              on_lost: Optional[RebalancingCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L143)

Set subscription to supplied list of topics

This replaces a previous subscription.

**Arguments**:

- `topics` (`list(str)`): List of topics (strings) to subscribe to.
- `on_assign` (`callable`): callback to provide handling of customized offsets
on completion of a successful partition re-assignment.
- `on_revoke` (`callable`): callback to provide handling of offset commits to
a customized store on the start of a rebalance operation.
- `on_lost` (`callable`): callback to provide handling in the case the partition
assignment has been lost. Partitions that have been lost may already be
owned by other members in the group and therefore committing offsets,
for example, may fail.

**Raises**:

- `KafkaException`: 
- `None`: RuntimeError if called on a closed consumer
.. py:function:: on_assign(consumer, partitions)
.. py:function:: on_revoke(consumer, partitions)
.. py:function:: on_lost(consumer, partitions)

  :param Consumer consumer: Consumer instance.
  :param list(TopicPartition) partitions: Absolute list of partitions being
  assigned or revoked.

<a id="quixstreams.kafka.consumer.Consumer.unsubscribe"></a>

#### Consumer.unsubscribe

```python
def unsubscribe()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L237)

Remove current subscription.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.store_offsets"></a>

#### Consumer.store\_offsets

```python
def store_offsets(message: Optional[Message] = None,
                  offsets: List[TopicPartition] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L245)

.. py:function:: store_offsets([message=None], [offsets=None])

Store offsets for a message or a list of offsets.

``message`` and ``offsets`` are mutually exclusive. The stored offsets
will be committed according to 'auto.commit.interval.ms' or manual
offset-less `commit`.
Note that 'enable.auto.offset.store' must be set to False when using this API.

**Arguments**:

- `message` (`confluent_kafka.Message`): Store message's offset+1.
- `offsets` (`list(TopicPartition)`): List of topic+partitions+offsets to store.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.commit"></a>

#### Consumer.commit

```python
def commit(message: Message = None,
           offsets: List[TopicPartition] = None,
           asynchronous: bool = True) -> Optional[List[TopicPartition]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L279)

Commit a message or a list of offsets.

The ``message`` and ``offsets`` parameters are mutually exclusive.
If neither is set, the current partition assignment's offsets are used instead.
Use this method to commit offsets if you have 'enable.auto.commit' set to False.

**Arguments**:

- `message` (`confluent_kafka.Message`): Commit the message's offset+1.
Note: By convention, committed offsets reflect the next message
to be consumed, **not** the last message consumed.
- `offsets` (`list(TopicPartition)`): List of topic+partitions+offsets to commit.
- `asynchronous` (`bool`): If true, asynchronously commit, returning None
immediately. If False, the commit() call will block until the commit
succeeds or fails and the committed offsets will be returned (on success).
Note that specific partitions may have failed and the .err field of
each partition should be checked for success.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.committed"></a>

#### Consumer.committed

```python
def committed(partitions: List[TopicPartition],
              timeout: float = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L319)

.. py:function:: committed(partitions, [timeout=None])

Retrieve committed offsets for the specified partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to query for stored offsets.
- `timeout` (`float`): Request timeout (seconds).

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`list(TopicPartition)`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.get_watermark_offsets"></a>

#### Consumer.get\_watermark\_offsets

```python
def get_watermark_offsets(partition: TopicPartition,
                          timeout: float = None,
                          cached: bool = False) -> Tuple[int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L339)

Retrieve low and high offsets for the specified partition.

**Arguments**:

- `partition` (`TopicPartition`): Topic+partition to return offsets for.
- `timeout` (`float`): Request timeout (seconds). Ignored if cached=True.
- `cached` (`bool`): Instead of querying the broker, use cached information.
Cached values: The low offset is updated periodically
(if statistics.interval.ms is set) while the high offset is updated on each
message fetched from the broker for this partition.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`tuple(int,int)`: Tuple of (low,high) on success or None on timeout.
The high offset is the offset of the last message + 1.

<a id="quixstreams.kafka.consumer.Consumer.list_topics"></a>

#### Consumer.list\_topics

```python
def list_topics(topic: Optional[str] = None,
                timeout: float = -1) -> ClusterMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L365)

.. py:function:: list_topics([topic=None], [timeout=-1])

Request metadata from the cluster.
This method provides the same information as
listTopics(), describeTopics() and describeCluster() in  the Java Admin client.

**Arguments**:

- `topic` (`str`): If specified, only request information about this topic,
else return results for all topics in cluster.
Warning: If auto.create.topics.enable is set to true on the broker and
an unknown topic is specified, it will be created.
- `timeout` (`float`): The maximum response time before timing out,
or -1 for infinite timeout.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.memberid"></a>

#### Consumer.memberid

```python
def memberid() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L386)

Return this client's broker-assigned group member id.

The member id is assigned by the group coordinator and is propagated to
the consumer during rebalance.

 :returns: Member id string or None
 :rtype: string
 :raises: RuntimeError if called on a closed consumer


<a id="quixstreams.kafka.consumer.Consumer.offsets_for_times"></a>

#### Consumer.offsets\_for\_times

```python
def offsets_for_times(partitions: List[TopicPartition],
                      timeout: float = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L399)

Look up offsets by timestamp for the specified partitions.

The returned offset for each partition is the earliest offset whose
timestamp is greater than or equal to the given timestamp in the
corresponding partition. If the provided timestamp exceeds that of the
last message in the partition, a value of -1 will be returned.

 :param list(TopicPartition) partitions: topic+partitions with timestamps
    in the TopicPartition.offset field.
 :param float timeout: Request timeout (seconds).
 :returns: List of topic+partition with offset field set and possibly error set
 :rtype: list(TopicPartition)
 :raises: KafkaException
 :raises: RuntimeError if called on a closed consumer


<a id="quixstreams.kafka.consumer.Consumer.pause"></a>

#### Consumer.pause

```python
def pause(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L427)

Pause consumption for the provided list of partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to pause.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.resume"></a>

#### Consumer.resume

```python
def resume(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L437)

.. py:function:: resume(partitions)

Resume consumption for the provided list of partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to resume.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.position"></a>

#### Consumer.position

```python
def position(partitions: List[TopicPartition]) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L449)

Retrieve current positions (offsets) for the specified partitions.

**Arguments**:

- `partitions` (`list(TopicPartition)`): List of topic+partitions to return
current offsets for. The current offset is the offset of
the last consumed message + 1.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`list(TopicPartition)`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.seek"></a>

#### Consumer.seek

```python
def seek(partition: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L463)

Set consume position for partition to offset.

The offset may be an absolute (>=0) or a
logical offset (:py:const:`OFFSET_BEGINNING` et.al).

seek() may only be used to update the consume offset of an
actively consumed partition (i.e., after :py:const:`assign()`),
to set the starting offset of partition not being consumed instead
pass the offset in an `assign()` call.

**Arguments**:

- `partition` (`TopicPartition`): Topic+partition+offset to seek to.

**Raises**:

- `None`: KafkaException

<a id="quixstreams.kafka.consumer.Consumer.assignment"></a>

#### Consumer.assignment

```python
def assignment() -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L480)

Returns the current partition assignment.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

**Returns**:

`list(TopicPartition)`: List of assigned topic+partitions.

<a id="quixstreams.kafka.consumer.Consumer.set_sasl_credentials"></a>

#### Consumer.set\_sasl\_credentials

```python
def set_sasl_credentials(username: str, password: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L493)

Sets the SASL credentials used for this client.
These credentials will overwrite the old ones, and will be used the next
time the client needs to authenticate.
This method will not disconnect existing broker connections that have been
established with the old credentials.
This method is applicable only to SASL PLAIN and SCRAM mechanisms.

<a id="quixstreams.kafka.consumer.Consumer.close"></a>

#### Consumer.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/kafka/consumer.py#L505)

Close down and terminate the Kafka Consumer.

Actions performed:

- Stops consuming.
- Commits offsets, unless the consumer property 'enable.auto.commit' is set to False.
- Leaves the consumer group.

Registered callbacks may be called from this method,
see `poll()` for more info.


<a id="quixstreams.app"></a>

## quixstreams.app

<a id="quixstreams.app.Application"></a>

### Application

```python
class Application()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L43)

The main Application class.

Typically, the primary object needed to get a kafka application up and running.

Most functionality is explained the various methods, except for
"column assignment".


What it Does:

- During user setup:
    - Provides defaults or helper methods for commonly needed objects
    - For Quix Platform Users: Configures the app for it
        (see `Application.Quix()`)
- When executed via `.run()` (after setup):
    - Initializes Topics and StreamingDataFrames
    - Facilitates processing of Kafka messages with a `StreamingDataFrame`
    - Handles all Kafka client consumer/producer responsibilities.


Example Snippet:

```python
from quixstreams import Application

# Set up an `app = Application` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value)

app.run(dataframe=df)
```

<a id="quixstreams.app.Application.__init__"></a>

#### Application.\_\_init\_\_

```python
def __init__(broker_address: str,
             consumer_group: str,
             auto_offset_reset: AutoOffsetReset = "latest",
             auto_commit_enable: bool = True,
             assignment_strategy: AssignmentStrategy = "range",
             partitioner: Partitioner = "murmur2",
             consumer_extra_config: Optional[dict] = None,
             producer_extra_config: Optional[dict] = None,
             state_dir: str = "state",
             rocksdb_options: Optional[RocksDBOptionsType] = None,
             on_consumer_error: Optional[ConsumerErrorCallback] = None,
             on_processing_error: Optional[ProcessingErrorCallback] = None,
             on_producer_error: Optional[ProducerErrorCallback] = None,
             on_message_processed: Optional[MessageProcessedCallback] = None,
             consumer_poll_timeout: float = 1.0,
             producer_poll_timeout: float = 0.0,
             loglevel: Optional[LogLevel] = "INFO")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L82)

**Arguments**:

- `broker_address`: Kafka broker host and port in format `<host>:<port>`.
Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
- `auto_offset_reset`: Consumer `auto.offset.reset` setting
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `assignment_strategy`: The name of a partition assignment strategy.
- `partitioner`: A function to be used to determine the outgoing message
partition.
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
- `producer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
- `state_dir`: path to the application state directory.
Default - ".state".
- `rocksdb_options`: RocksDB options.
If `None`, the default options will be used.
- `consumer_poll_timeout`: timeout for `RowConsumer.poll()`. Default - 1.0s
- `producer_poll_timeout`: timeout for `RowProducer.poll()`. Default - 0s.
- `on_message_processed`: a callback triggered when message is successfully
processed.
- `loglevel`: a log level for "quixstreams" logger.
Should be a string or None.
    If `None` is passed, no logging will be configured.
    You may pass `None` and configure "quixstreams" logger
    externally using `logging` library.
    Default - "INFO".

***Error Handlers***

To handle errors, `Application` accepts callbacks triggered when
    exceptions occur on different stages of stream processing. If the callback
    returns `True`, the exception will be ignored. Otherwise, the exception
    will be propagated and the processing will eventually stop.
- `on_consumer_error`: triggered when internal `RowConsumer` fails
to poll Kafka or cannot deserialize a message.
- `on_processing_error`: triggered when exception is raised within
`StreamingDataFrame.process()`.
- `on_producer_error`: triggered when RowProducer fails to serialize
or to produce a message to Kafka.

<a id="quixstreams.app.Application.Quix"></a>

#### Application.Quix

```python
@classmethod
def Quix(cls,
         consumer_group: str,
         auto_offset_reset: AutoOffsetReset = "latest",
         auto_commit_enable: bool = True,
         assignment_strategy: AssignmentStrategy = "range",
         partitioner: Partitioner = "murmur2",
         consumer_extra_config: Optional[dict] = None,
         producer_extra_config: Optional[dict] = None,
         state_dir: str = "state",
         rocksdb_options: Optional[RocksDBOptionsType] = None,
         on_consumer_error: Optional[ConsumerErrorCallback] = None,
         on_processing_error: Optional[ProcessingErrorCallback] = None,
         on_producer_error: Optional[ProducerErrorCallback] = None,
         on_message_processed: Optional[MessageProcessedCallback] = None,
         consumer_poll_timeout: float = 1.0,
         producer_poll_timeout: float = 0.0,
         loglevel: Optional[LogLevel] = "INFO",
         quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
         auto_create_topics: bool = True) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L188)

Initialize an Application to work with Quix platform,

assuming environment is properly configured (by default in the platform).

It takes the credentials from the environment and configures consumer and
producer to properly connect to the Quix platform.

>***NOTE:*** Quix platform requires `consumer_group` and topic names to be
    prefixed with workspace id.
    If the application is created via `Application.Quix()`, the real consumer
    group will be `<workspace_id>-<consumer_group>`,
    and the real topic names will be `<workspace_id>-<topic_name>`.



Example Snippet:

```python
from quixstreams import Application

# Set up an `app = Application.Quix` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything. Also shows off how to
# use the quix-specific serializers and deserializers.

app = Application.Quix()
input_topic = app.topic("topic-in", value_deserializer="quix")
output_topic = app.topic("topic-out", value_serializer="quix_timeseries")
df = app.dataframe(topic_in)
df = df.to_topic(output_topic)

app.run(dataframe=df)
```

**Arguments**:

- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`.
>***NOTE:*** The consumer group will be prefixed by Quix workspace id.
- `auto_offset_reset`: Consumer `auto.offset.reset` setting
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `assignment_strategy`: The name of a partition assignment strategy.
- `partitioner`: A function to be used to determine the outgoing message
partition.
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
- `producer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
- `state_dir`: path to the application state directory.
Default - ".state".
- `rocksdb_options`: RocksDB options.
If `None`, the default options will be used.
- `consumer_poll_timeout`: timeout for `RowConsumer.poll()`. Default - 1.0s
- `producer_poll_timeout`: timeout for `RowProducer.poll()`. Default - 0s.
- `on_message_processed`: a callback triggered when message is successfully
processed.
- `loglevel`: a log level for "quixstreams" logger.
Should be a string or None.
    If `None` is passed, no logging will be configured.
    You may pass `None` and configure "quixstreams" logger
    externally using `logging` library.
    Default - "INFO".

***Error Handlers***

To handle errors, `Application` accepts callbacks triggered when
    exceptions occur on different stages of stream processing. If the callback
    returns `True`, the exception will be ignored. Otherwise, the exception
    will be propagated and the processing will eventually stop.
- `on_consumer_error`: triggered when internal `RowConsumer` fails to poll
Kafka or cannot deserialize a message.
- `on_processing_error`: triggered when exception is raised within
`StreamingDataFrame.process()`.
- `on_producer_error`: triggered when RowProducer fails to serialize
or to produce a message to Kafka.


***Quix-specific Parameters***
- `quix_config_builder`: instance of `QuixKafkaConfigsBuilder` to be used
instead of the default one.
- `auto_create_topics`: Whether to auto-create any topics handed to a
StreamingDataFrame instance (topics_in + topics_out).

**Returns**:

`Application` object

<a id="quixstreams.app.Application.topic"></a>

#### Application.topic

```python
def topic(name: str,
          value_deserializer: DeserializerType = "json",
          key_deserializer: DeserializerType = "bytes",
          value_serializer: SerializerType = "json",
          key_serializer: SerializerType = "bytes",
          creation_configs: Optional[TopicCreationConfigs] = None,
          timestamp_extractor: Optional[TimestampExtractor] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L334)

Create a topic definition.

Allows you to specify serialization that should be used when consuming/producing
to the topic in the form of a string name (i.e. "json" for JSON) or a
serialization class instance directly, like JSONSerializer().


Example Snippet:

```python
from quixstreams import Application

# Specify an input and output topic for a `StreamingDataFrame` instance,
# where the output topic requires adjusting the key serializer.

app = Application()
input_topic = app.topic("input-topic", value_deserializer="json")
output_topic = app.topic(
    "output-topic", key_serializer="str", value_serializer=JSONSerializer()
)
sdf = app.dataframe(input_topic)
sdf.to_topic(output_topic)
```

**Arguments**:

- `name`: topic name
>***NOTE:*** If the application is created via `Quix.Application()`,
the topic name will be prefixed by Quix workspace id, and it will
be `<workspace_id>-<name>`
- `value_deserializer`: a deserializer type for values; default="json"
- `key_deserializer`: a deserializer type for keys; default="bytes"
- `value_serializer`: a serializer type for values; default="json"
- `key_serializer`: a serializer type for keys; default="bytes"
- `creation_configs`: settings for auto topic creation (Quix platform only)
Its name will be overridden by this method's 'name' param.

**Returns**:

`Topic` object

<a id="quixstreams.app.Application.dataframe"></a>

#### Application.dataframe

```python
def dataframe(topic: Topic) -> StreamingDataFrame
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L400)

A simple helper method that generates a `StreamingDataFrame`, which is used

to define your message processing pipeline.

See :class:`quixstreams.dataframe.StreamingDataFrame` for more details.


Example Snippet:

```python
from quixstreams import Application

# Set up an `app = Application` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value)

app.run(dataframe=df)
```

**Arguments**:

- `topic`: a `quixstreams.models.Topic` instance
to be used as an input topic.

**Returns**:

`StreamingDataFrame` object

<a id="quixstreams.app.Application.stop"></a>

#### Application.stop

```python
def stop()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L436)

Stop the internal poll loop and the message processing.

Only necessary when manually managing the lifecycle of the `Application` (
likely through some sort of threading).

To otherwise stop an application, either send a `SIGTERM` to the process
(like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).

<a id="quixstreams.app.Application.get_producer"></a>

#### Application.get\_producer

```python
def get_producer() -> Producer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L448)

Create and return a pre-configured Producer instance.
The Producer is initialized with params passed to Application.

It's useful for producing data to Kafka outside the standard Application processing flow,
(e.g. to produce test data into a topic).
Using this within the StreamingDataFrame functions is not recommended, as it creates a new Producer
instance each time, which is not optimized for repeated use in a streaming pipeline.

Example Snippet:

```python
from quixstreams import Application

app = Application.Quix(...)
topic = app.topic("input")

with app.get_producer() as producer:
    for i in range(100):
        producer.produce(topic=topic.name, key=b"key", value=b"value")
```

<a id="quixstreams.app.Application.get_consumer"></a>

#### Application.get\_consumer

```python
def get_consumer() -> Consumer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L484)

Create and return a pre-configured Consumer instance.
The Consumer is initialized with params passed to Application.

It's useful for consuming data from Kafka outside the standard Application processing flow.
(e.g. to consume test data from a topic).
Using it within the StreamingDataFrame functions is not recommended, as it creates a new Consumer instance
each time, which is not optimized for repeated use in a streaming pipeline.

Note: By default this consumer does not autocommit consumed offsets to allow exactly-once processing.
To store the offset call store_offsets() after processing a message.
If autocommit is necessary set `enable.auto.offset.store` to True in the consumer config when creating the app.

Example Snippet:

```python
from quixstreams import Application

app = Application.Quix(...)
topic = app.topic("input")

with app.get_consumer() as consumer:
    consumer.subscribe([topic.name])
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is not None:
            # Process message
            # Optionally commit the offset
            # consumer.store_offsets(msg)

```

<a id="quixstreams.app.Application.clear_state"></a>

#### Application.clear\_state

```python
def clear_state()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L533)

Clear the state of the application.

<a id="quixstreams.app.Application.run"></a>

#### Application.run

```python
def run(dataframe: StreamingDataFrame)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/app.py#L562)

Start processing data from Kafka using provided `StreamingDataFrame`

One started, can be safely terminated with a `SIGTERM` signal
(like Kubernetes does) or a typical `KeyboardInterrupt` (`Ctrl+C`).


Example Snippet:

```python
from quixstreams import Application

# Set up an `app = Application` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value)

app.run(dataframe=df)
```

**Arguments**:

- `dataframe`: instance of `StreamingDataFrame`

<a id="quixstreams.rowconsumer"></a>

## quixstreams.rowconsumer

<a id="quixstreams.rowconsumer.RowConsumer"></a>

### RowConsumer

```python
class RowConsumer(Consumer, RowConsumerProto)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/rowconsumer.py#L59)

<a id="quixstreams.rowconsumer.RowConsumer.__init__"></a>

#### RowConsumer.\_\_init\_\_

```python
def __init__(broker_address: str,
             consumer_group: str,
             auto_offset_reset: AutoOffsetReset,
             auto_commit_enable: bool = True,
             assignment_strategy: AssignmentStrategy = "range",
             on_commit: Callable[[Optional[KafkaError], List[TopicPartition]],
                                 None] = None,
             extra_config: Optional[dict] = None,
             on_error: Optional[ConsumerErrorCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/rowconsumer.py#L60)

A consumer class that is capable of deserializing Kafka messages to Rows

according to the Topics deserialization settings.

It overrides `.subscribe()` method of Consumer class to accept `Topic`
objects instead of strings.

**Arguments**:

- `broker_address`: Kafka broker host and port in format `<host>:<port>`.
Passed as `bootstrap.servers` to `confluent_kafka.Consumer`.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Available values:
- "earliest" - automatically reset the offset to the smallest offset
- "latest" - automatically reset the offset to the largest offset
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `assignment_strategy`: The name of a partition assignment strategy.
Available values: "range", "roundrobin", "cooperative-sticky".
- `on_commit`: Offset commit result propagation callback.
Passed as "offset_commit_cb" to `confluent_kafka.Consumer`.
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
Note: values passed as arguments override values in `extra_config`.
- `on_error`: a callback triggered when `RowConsumer.poll_row` fails.
If consumer fails and the callback returns `True`, the exception
will be logged but not propagated.
The default callback logs an exception and returns `False`.

<a id="quixstreams.rowconsumer.RowConsumer.subscribe"></a>

#### RowConsumer.subscribe

```python
def subscribe(topics: List[Topic],
              on_assign: Optional[RebalancingCallback] = None,
              on_revoke: Optional[RebalancingCallback] = None,
              on_lost: Optional[RebalancingCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/rowconsumer.py#L115)

Set subscription to supplied list of topics.

This replaces a previous subscription.

This method also updates the internal mapping with topics that is used
to deserialize messages to Rows.

**Arguments**:

- `topics`: list of `Topic` instances to subscribe to.
- `on_assign` (`callable`): callback to provide handling of customized offsets
on completion of a successful partition re-assignment.
- `on_revoke` (`callable`): callback to provide handling of offset commits to
a customized store on the start of a rebalance operation.
- `on_lost` (`callable`): callback to provide handling in the case the partition
assignment has been lost. Partitions that have been lost may already be
owned by other members in the group and therefore committing offsets,
for example, may fail.

<a id="quixstreams.rowconsumer.RowConsumer.poll_row"></a>

#### RowConsumer.poll\_row

```python
def poll_row(timeout: float = None) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/5ea02f7558e6dd73d58eb673f71bad48aaf23282/quixstreams/rowconsumer.py#L149)

Consumes a single message and deserialize it to Row or a list of Rows.

The message is deserialized according to the corresponding Topic.
If deserializer raises `IgnoreValue` exception, this method will return None.
If Kafka returns an error, it will be raised as exception.

**Arguments**:

- `timeout`: poll timeout seconds

**Returns**:

single Row, list of Rows or None

