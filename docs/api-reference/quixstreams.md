<a id="quixstreams"></a>

## quixstreams

<a id="quixstreams.logging"></a>

## quixstreams.logging

<a id="quixstreams.logging.configure_logging"></a>

#### configure\_logging

```python
def configure_logging(loglevel: Optional[LogLevel]) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/logging.py#L26)

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

<a id="quixstreams.platforms.quix.config.strip_workspace_id_prefix"></a>

#### strip\_workspace\_id\_prefix

```python
def strip_workspace_id_prefix(workspace_id: str, s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L31)

Remove the workspace ID from a given string if it starts with it,

typically a topic or consumer group id

**Arguments**:

- `workspace_id`: the workspace id
- `s`: the string to append to

**Returns**:

the string with workspace_id prefix removed

<a id="quixstreams.platforms.quix.config.prepend_workspace_id"></a>

#### prepend\_workspace\_id

```python
def prepend_workspace_id(workspace_id: str, s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L43)

Add the workspace ID as a prefix to a given string if it does not have it,

typically a topic or consumer group it

**Arguments**:

- `workspace_id`: the workspace id
- `s`: the string to append to

**Returns**:

the string with workspace_id prepended

<a id="quixstreams.platforms.quix.config.QuixApplicationConfig"></a>

### QuixApplicationConfig

```python
@dataclasses.dataclass
class QuixApplicationConfig()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L56)

A convenience container class for Quix Application configs.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder"></a>

### QuixKafkaConfigsBuilder

```python
class QuixKafkaConfigsBuilder()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L66)

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
def __init__(quix_sdk_token: Optional[str] = None,
             workspace_id: Optional[str] = None,
             quix_portal_api_service: Optional[QuixPortalApiService] = None,
             timeout: float = 30,
             topic_create_timeout: float = 60)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L82)

**Arguments**:

- `quix_portal_api_service`: A QuixPortalApiService instance (else generated)
- `workspace_id`: A valid Quix Workspace ID (else searched for)

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.strip_workspace_id_prefix"></a>

#### QuixKafkaConfigsBuilder.strip\_workspace\_id\_prefix

```python
def strip_workspace_id_prefix(s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L160)

Remove the workspace ID from a given string if it starts with it,

typically a topic or consumer group id

**Arguments**:

- `s`: the string to append to

**Returns**:

the string with workspace_id prefix removed

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.prepend_workspace_id"></a>

#### QuixKafkaConfigsBuilder.prepend\_workspace\_id

```python
def prepend_workspace_id(s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L170)

Add the workspace ID as a prefix to a given string if it does not have it,

typically a topic or consumer group it

**Arguments**:

- `s`: the string to append to

**Returns**:

the string with workspace_id prepended

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_workspace"></a>

#### QuixKafkaConfigsBuilder.search\_for\_workspace

```python
def search_for_workspace(workspace_name_or_id: Optional[str] = None,
                         timeout: Optional[float] = None) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L180)

Search for a workspace given an expected workspace name or id.

**Arguments**:

- `workspace_name_or_id`: the expected name or id of a workspace
- `timeout`: response timeout (seconds); Default 30

**Returns**:

the workspace data dict if search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_workspace_info"></a>

#### QuixKafkaConfigsBuilder.get\_workspace\_info

```python
def get_workspace_info(known_workspace_topic: Optional[str] = None,
                       timeout: Optional[float] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L222)

Queries for workspace data from the Quix API, regardless of instance cache,

and updates instance attributes from query result.

**Arguments**:

- `known_workspace_topic`: a topic you know to exist in some workspace
- `timeout`: response timeout (seconds); Default 30

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_workspace_for_topic"></a>

#### QuixKafkaConfigsBuilder.search\_workspace\_for\_topic

```python
def search_workspace_for_topic(
        workspace_id: str,
        topic: str,
        timeout: Optional[float] = None) -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L250)

Search through all the topics in the given workspace id to see if there is a

match with the provided topic.

**Arguments**:

- `workspace_id`: the workspace to search in
- `topic`: the topic to search for
- `timeout`: response timeout (seconds); Default 30

**Returns**:

the workspace_id if success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_topic_workspace"></a>

#### QuixKafkaConfigsBuilder.search\_for\_topic\_workspace

```python
def search_for_topic_workspace(topic: str,
                               timeout: Optional[float] = None
                               ) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L271)

Find what workspace a topic belongs to.

If there is only one workspace altogether, it is assumed to be the workspace.
More than one means each workspace will be searched until the first hit.

**Arguments**:

- `topic`: the topic to search for
- `timeout`: response timeout (seconds); Default 30

**Returns**:

workspace data dict if topic search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.create_topics"></a>

#### QuixKafkaConfigsBuilder.create\_topics

```python
def create_topics(topics: List[Topic],
                  timeout: Optional[float] = None,
                  finalize_timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L369)

Create topics in a Quix cluster.

**Arguments**:

- `topics`: a list of `Topic` objects
- `timeout`: response timeout (seconds); Default 30
- `finalize_timeout`: topic finalization timeout (seconds); Default 60
marked as "Ready" (and thus ready to produce to/consume from).

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_topic"></a>

#### QuixKafkaConfigsBuilder.get\_topic

```python
def get_topic(topic_name: str,
              timeout: Optional[float] = None) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L419)

return the topic ID (the actual cluster topic name) if it exists, else None

>***NOTE***: if the name registered in Quix is instead the workspace-prefixed
version, this returns None unless that exact name was created WITHOUT the
Quix API.

**Arguments**:

- `topic_name`: name of the topic
- `timeout`: response timeout (seconds); Default 30

**Returns**:

response dict of the topic info if topic found, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.confirm_topics_exist"></a>

#### QuixKafkaConfigsBuilder.confirm\_topics\_exist

```python
def confirm_topics_exist(topics: Union[List[Topic], List[str]],
                         timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L451)

Confirm whether the desired set of topics exists in the Quix workspace.

**Arguments**:

- `topics`: a list of `Topic` or topic names
- `timeout`: response timeout (seconds); Default 30

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_application_config"></a>

#### QuixKafkaConfigsBuilder.get\_application\_config

```python
def get_application_config(consumer_group_id: str) -> QuixApplicationConfig
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L483)

Get all the necessary attributes for an Application to run on Quix Cloud.

**Arguments**:

- `consumer_group_id`: consumer group id, if needed

**Returns**:

a QuixApplicationConfig instance

<a id="quixstreams.platforms.quix.env"></a>

## quixstreams.platforms.quix.env

<a id="quixstreams.platforms.quix.env.QuixEnvironment"></a>

### QuixEnvironment

```python
class QuixEnvironment()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/env.py#L7)

Class to access various Quix platform environment settings

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_management_enabled"></a>

#### QuixEnvironment.state\_management\_enabled

```python
@property
def state_management_enabled() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/env.py#L19)

Check whether "State management" is enabled for the current deployment

**Returns**:

True if state management is enabled, otherwise False

<a id="quixstreams.platforms.quix.env.QuixEnvironment.deployment_id"></a>

#### QuixEnvironment.deployment\_id

```python
@property
def deployment_id() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/env.py#L27)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/env.py#L39)

Return Quix workspace id if set

**Returns**:

workspace id or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.portal_api"></a>

#### QuixEnvironment.portal\_api

```python
@property
def portal_api() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/env.py#L47)

Return Quix Portal API url if set

**Returns**:

portal API URL or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_dir"></a>

#### QuixEnvironment.state\_dir

```python
@property
def state_dir() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/env.py#L56)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/checks.py#L11)

Check if State Management feature is enabled for the current deployment on
Quix platform.
If it's disabled, the exception will be raised.

<a id="quixstreams.platforms.quix.checks.check_state_dir"></a>

#### check\_state\_dir

```python
def check_state_dir(state_dir: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/checks.py#L28)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/api.py#L19)

A light wrapper around the Quix Portal Api. If used in the Quix Platform, it will
use that workspaces auth token and portal endpoint, else you must provide it.

Function names closely reflect the respective API endpoint,
each starting with the method [GET, POST, etc.] followed by the endpoint path.

Results will be returned in the form of request's Response.json(), unless something
else is required. Non-200's will raise exceptions.

See the swagger documentation for more info about the endpoints.

<a id="quixstreams.platforms.quix.api.QuixPortalApiService.get_workspace_certificate"></a>

#### QuixPortalApiService.get\_workspace\_certificate

```python
def get_workspace_certificate(workspace_id: Optional[str] = None,
                              timeout: float = 30) -> Optional[bytes]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/api.py#L119)

Get a workspace TLS certificate if available.

Returns `None` if certificate is not specified.

**Arguments**:

- `workspace_id`: workspace id, optional
- `timeout`: request timeout; Default 30

**Returns**:

certificate as bytes if present, or None

<a id="quixstreams.platforms.quix.exceptions"></a>

## quixstreams.platforms.quix.exceptions

<a id="quixstreams.platforms.quix.topic_manager"></a>

## quixstreams.platforms.quix.topic\_manager

<a id="quixstreams.platforms.quix.topic_manager.QuixTopicManager"></a>

### QuixTopicManager

```python
class QuixTopicManager(TopicManager)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/topic_manager.py#L9)

The source of all topic management with quixstreams.

This is specifically for Applications using the Quix platform.

Generally initialized and managed automatically by an `Application.Quix`,
but allows a user to work with it directly when needed, such as using it alongside
a plain `Producer` to create its topics.

See methods for details.

<a id="quixstreams.platforms.quix.topic_manager.QuixTopicManager.__init__"></a>

#### QuixTopicManager.\_\_init\_\_

```python
def __init__(topic_admin: TopicAdmin,
             consumer_group: str,
             quix_config_builder: QuixKafkaConfigsBuilder,
             timeout: float = 30,
             create_timeout: float = 60)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/topic_manager.py#L30)

**Arguments**:

- `topic_admin`: an `Admin` instance
- `quix_config_builder`: A QuixKafkaConfigsBuilder instance, else one is
generated for you.
- `timeout`: response timeout (seconds)
- `create_timeout`: timeout for topic creation

<a id="quixstreams.dataframe.dataframe"></a>

## quixstreams.dataframe.dataframe

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame"></a>

### StreamingDataFrame

```python
class StreamingDataFrame(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L86)

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
@_ensure_unlocked
def apply(func: Union[
    ApplyCallback,
    ApplyCallbackStateful,
    ApplyWithMetadataCallback,
    ApplyWithMetadataCallbackStateful,
],
          *,
          stateful: bool = False,
          expand: bool = False,
          metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L203)

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
- `metadata`: if True, the callback will receive key, timestamp and headers
along with the value.
Default - `False`.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.update"></a>

#### StreamingDataFrame.update

```python
@_ensure_unlocked
def update(func: Union[
    UpdateCallback,
    UpdateCallbackStateful,
    UpdateWithMetadataCallback,
    UpdateWithMetadataCallbackStateful,
],
           *,
           stateful: bool = False,
           metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L293)

Apply a function to mutate value in-place or to perform a side effect

(e.g., printing a value to the console).

The result of the function will be ignored, and the original value will be
passed downstream.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).


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
# does not require reassigning
sdf.update(lambda v: v.append(1))
```

**Arguments**:

- `func`: function to update value
- `stateful`: if `True`, the function will be provided with a second argument
of type `State` to perform stateful operations.
- `metadata`: if True, the callback will receive key, timestamp and headers
along with the value.
Default - `False`.

**Returns**:

the updated StreamingDataFrame instance (reassignment NOT required).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.filter"></a>

#### StreamingDataFrame.filter

```python
@_ensure_unlocked
def filter(func: Union[
    FilterCallback,
    FilterCallbackStateful,
    FilterWithMetadataCallback,
    FilterWithMetadataCallbackStateful,
],
           *,
           stateful: bool = False,
           metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L386)

Filter value using provided function.

If the function returns True-like value, the original value will be
passed downstream.

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
- `metadata`: if True, the callback will receive key, timestamp and headers
along with the value.
Default - `False`.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.group_by"></a>

#### StreamingDataFrame.group\_by

```python
@_ensure_unlocked
def group_by(key: Union[str, Callable[[Any], Any]],
             name: Optional[str] = None,
             value_deserializer: Optional[DeserializerType] = "json",
             key_deserializer: Optional[DeserializerType] = "json",
             value_serializer: Optional[SerializerType] = "json",
             key_serializer: Optional[SerializerType] = "json") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L473)

"Groups" messages by re-keying them via the provided group_by operation

on their message values.

This enables things like aggregations on messages with non-matching keys.

You can provide a column name (uses the column's value) or a custom function
to generate this new key.

`.groupby()` can only be performed once per `StreamingDataFrame` instance.

>**NOTE:** group_by generates a topic that copies the original topic's settings.

Example Snippet:

```python
# We have customer purchase events where the message key is the "store_id",
# but we want to calculate sales per customer (by "customer_account_id").

def func(d: dict, state: State):
    current_total = state.get("customer_sum", 0)
    new_total = current_total + d["customer_spent"]
    state.set("customer_sum", new_total)
    d["customer_total"] = new_total
    return d

sdf = StreamingDataframe()
sdf = sdf.group_by("customer_account_id")
sdf = sdf.apply(func, stateful=True)
```

**Arguments**:

- `key`: how the new key should be generated from the message value;
requires a column name (string) or a callable that takes the message value.
- `name`: a name for the op (must be unique per group-by), required if `key`
is a custom callable.
- `value_deserializer`: a deserializer type for values; default - JSON
- `key_deserializer`: a deserializer type for keys; default - JSON
- `value_serializer`: a serializer type for values; default - JSON
- `key_serializer`: a serializer type for keys; default - JSON

**Returns**:

a clone with this operation added (assign to keep its effect).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.contains"></a>

#### StreamingDataFrame.contains

```python
@staticmethod
def contains(key: str) -> StreamingSeries
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L551)

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
@_ensure_unlocked
def to_topic(topic: Topic, key: Optional[Callable[[Any], Any]] = None) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L577)

Produce current value to a topic. You can optionally specify a new key.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

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
# does not require reassigning
sdf.to_topic(output_topic_1, key=lambda data: data["a_field"])
```

**Arguments**:

- `topic`: instance of `Topic`
- `key`: a callable to generate a new message key, optional.
If passed, the return type of this callable must be serializable
by `key_serializer` defined for this Topic object.
By default, the current message key will be used.

**Returns**:

the updated StreamingDataFrame instance (reassignment NOT required).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.set_timestamp"></a>

#### StreamingDataFrame.set\_timestamp

```python
@_ensure_unlocked
def set_timestamp(func: Callable[[Any, Any, int, Any], int]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L623)

Set a new timestamp based on the current message value and its metadata.

The new timestamp will be used in windowed aggregations and when producing
messages to the output topics.

The new timestamp must be in milliseconds to conform Kafka requirements.

Example Snippet:

```python
from quixstreams import Application


app = Application()
input_topic = app.topic("data")

sdf = app.dataframe(input_topic)
# Updating the record's timestamp based on the value
sdf = sdf.set_timestamp(lambda value, key, timestamp, headers: value['new_timestamp'])
```

**Arguments**:

- `func`: callable accepting the current value, key, timestamp, and headers.
It's expected to return a new timestamp as integer in milliseconds.

**Returns**:

a new StreamingDataFrame instance

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.set_headers"></a>

#### StreamingDataFrame.set\_headers

```python
@_ensure_unlocked
def set_headers(
    func: Callable[
        [Any, Any, int, List[Tuple[str, HeaderValue]]],
        Collection[Tuple[str, HeaderValue]],
    ]
) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L665)

Set new message headers based on the current message value and metadata.

The new headers will be used when producing messages to the output topics.

The provided callback must accept value, key, timestamp, and headers,
and return a new collection of (header, value) tuples.

Example Snippet:

```python
from quixstreams import Application


app = Application()
input_topic = app.topic("data")

sdf = app.dataframe(input_topic)
# Updating the record's headers based on the value and metadata
sdf = sdf.set_headers(lambda value, key, timestamp, headers: [('id', value['id'])])
```

**Arguments**:

- `func`: callable accepting the current value, key, timestamp, and headers.
It's expected to return a new set of headers
as a collection of (header, value) tuples.

**Returns**:

a new StreamingDataFrame instance

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.print"></a>

#### StreamingDataFrame.print

```python
@_ensure_unlocked
def print(pretty: bool = True, metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L717)

Print out the current message value (and optionally, the message metadata) to

stdout (console) (like the built-in `print` function).

Can also output a more dict-friendly format with `pretty=True`.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

> NOTE: prints the current (edited) values, not the original values.

Example Snippet:

```python
from quixstreams import Application


app = Application()
input_topic = app.topic("data")

sdf = app.dataframe(input_topic)
sdf["edited_col"] = sdf["orig_col"] + "edited"
# print the updated message value with the newly added column
sdf.print()
```

**Arguments**:

- `pretty`: Whether to use "pprint" formatting, which uses new-lines and
indents for easier console reading (but might be worse for log parsing).
- `metadata`: Whether to additionally print the key, timestamp, and headers

**Returns**:

the updated StreamingDataFrame instance (reassignment NOT required).

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.compose"></a>

#### StreamingDataFrame.compose

```python
def compose(
    sink: Optional[Callable[[Any, Any, int, Any], None]] = None
) -> Dict[str, VoidExecutor]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L759)

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

**Arguments**:

- `sink`: callable to accumulate the results of the execution, optional.

**Returns**:

a function that accepts "value"
and returns a result of StreamingDataFrame

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.test"></a>

#### StreamingDataFrame.test

```python
def test(value: Any,
         key: Any,
         timestamp: int,
         headers: Optional[Any] = None,
         ctx: Optional[MessageContext] = None,
         topic: Optional[Topic] = None) -> List[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L796)

A shorthand to test `StreamingDataFrame` with provided value

and `MessageContext`.

**Arguments**:

- `value`: value to pass through `StreamingDataFrame`
- `key`: key to pass through `StreamingDataFrame`
- `timestamp`: timestamp to pass through `StreamingDataFrame`
- `ctx`: instance of `MessageContext`, optional.
Provide it if the StreamingDataFrame instance calls `to_topic()`,
has stateful functions or windows.
Default - `None`.
- `topic`: optionally, a topic branch to test with

**Returns**:

result of `StreamingDataFrame`

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.tumbling_window"></a>

#### StreamingDataFrame.tumbling\_window

```python
@_ensure_unlocked
def tumbling_window(duration_ms: Union[int, timedelta],
                    grace_ms: Union[int, timedelta] = 0,
                    name: Optional[str] = None) -> TumblingWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L834)

Create a tumbling window transformation on this StreamingDataFrame.

Tumbling windows divide time into fixed-sized, non-overlapping windows.

They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

Notes:

- The timestamp of the aggregation result is set to the window start timestamp.
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.
- The time windows always use the current event time.



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
- `name`: The unique identifier for the window. If not provided, it will be
automatically generated based on the window's properties.

**Returns**:

`TumblingWindowDefinition` instance representing the tumbling window
configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.hopping_window"></a>

#### StreamingDataFrame.hopping\_window

```python
@_ensure_unlocked
def hopping_window(duration_ms: Union[int, timedelta],
                   step_ms: Union[int, timedelta],
                   grace_ms: Union[int, timedelta] = 0,
                   name: Optional[str] = None) -> HoppingWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L911)

Create a hopping window transformation on this StreamingDataFrame.

Hopping windows divide the data stream into overlapping windows based on time.
The overlap is controlled by the `step_ms` parameter.

They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

Notes:

- The timestamp of the aggregation result is set to the window start timestamp.
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.
- The time windows always use the current event time.


Example Snippet:

```python
app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a hopping window of 60s with step 30s and grace period of 10s
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
- `name`: The unique identifier for the window. If not provided, it will be
automatically generated based on the window's properties.

**Returns**:

`HoppingWindowDefinition` instance representing the hopping
window configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. and applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.drop"></a>

#### StreamingDataFrame.drop

```python
@_ensure_unlocked
def drop(columns: Union[str, List[str]],
         errors: Literal["ignore", "raise"] = "raise") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1004)

Drop column(s) from the message value (value must support `del`, like a dict).

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).


Example Snippet:

```python
# Remove columns "x" and "y" from the value.
# This would transform {"x": 1, "y": 2, "z": 3} to {"z": 3}

sdf = StreamingDataframe()
sdf.drop(["x", "y"])
```

**Arguments**:

- `columns`: a single column name or a list of names, where names are `str`
- `errors`: If "ignore", suppress error and only existing labels are dropped.
Default - `"raise"`.

**Returns**:

a new StreamingDataFrame instance

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.sink"></a>

#### StreamingDataFrame.sink

```python
@_ensure_unlocked
def sink(sink: BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1049)

Sink the processed data to the specified destination.

Internally, each processed record is added to a sink, and the sinks are
flushed on each checkpoint.
The offset will be committed only if all the sinks for all topic partitions
are flushed successfully.

Additionally, Sinks may signal the backpressure to the application
(e.g., when the destination is rate-limited).
When this happens, the application will pause the corresponding topic partition
and resume again after the timeout.
The backpressure handling and timeouts are defined by the specific sinks.

Note: `sink()` is a terminal operation, and you cannot add new operations
to the same StreamingDataFrame after it's called.

<a id="quixstreams.dataframe.series"></a>

## quixstreams.dataframe.series

<a id="quixstreams.dataframe.series.StreamingSeries"></a>

### StreamingSeries

```python
class StreamingSeries(BaseStreaming)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L47)

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

<a id="quixstreams.dataframe.series.StreamingSeries.from_apply_callback"></a>

#### StreamingSeries.from\_apply\_callback

```python
@classmethod
def from_apply_callback(cls, func: ApplyWithMetadataCallback) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L107)

Create a StreamingSeries from a function.

The provided function will be wrapped into `Apply`

**Arguments**:

- `func`: a function to apply

**Returns**:

instance of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.apply"></a>

#### StreamingSeries.apply

```python
def apply(func: ApplyCallback) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L121)

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

<a id="quixstreams.dataframe.series.StreamingSeries.compose_returning"></a>

#### StreamingSeries.compose\_returning

```python
def compose_returning() -> ReturningExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L155)

Compose a list of functions from this StreamingSeries and its parents into one

big closure that always returns the transformed record.

This closure is to be used to execute the functions in the stream and to get
the result of the transformations.

Stream may only contain simple "apply" functions to be able to compose itself
into a returning function.

**Returns**:

a callable accepting value, key and timestamp and
returning a tuple "(value, key, timestamp)

<a id="quixstreams.dataframe.series.StreamingSeries.compose"></a>

#### StreamingSeries.compose

```python
def compose(
    sink: Optional[Callable[[Any, Any, int, Any],
                            None]] = None) -> VoidExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L170)

Compose all functions of this StreamingSeries into one big closure.

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

- `sink`: callable to accumulate the results of the execution.

**Raises**:

- `ValueError`: if disallowed functions are present in the tree of
underlying `Stream`.

**Returns**:

a callable accepting value, key and timestamp and
returning None

<a id="quixstreams.dataframe.series.StreamingSeries.test"></a>

#### StreamingSeries.test

```python
def test(value: Any,
         key: Any,
         timestamp: int,
         headers: Optional[Any] = None,
         ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L214)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L269)

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
def contains(other: Union[Self, object]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L296)

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
def is_(other: Union[Self, object]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L321)

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
def isnot(other: Union[Self, object]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L344)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L368)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L391)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L414)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/utils.py#L5)

Convert timedelta to milliseconds.

If the `delta` is not
This function will also round the value to the closest milliseconds in case of
higher precision.

**Arguments**:

- `delta`: `timedelta` object

**Returns**:

timedelta value in milliseconds as `int`

<a id="quixstreams.dataframe.exceptions"></a>

## quixstreams.dataframe.exceptions

<a id="quixstreams.dataframe.windows.definitions"></a>

## quixstreams.dataframe.windows.definitions

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition"></a>

### FixedTimeWindowDefinition

```python
class FixedTimeWindowDefinition(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L20)

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.sum"></a>

#### FixedTimeWindowDefinition.sum

```python
def sum() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L67)

Configure the window to aggregate data by summing up values within

each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to perform sum aggregation.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.count"></a>

#### FixedTimeWindowDefinition.count

```python
def count() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L94)

Configure the window to aggregate data by counting the number of values

within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to perform record count.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.mean"></a>

#### FixedTimeWindowDefinition.mean

```python
def mean() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L121)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L152)

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
This function is used to initialize the aggregation within a window.

**Returns**:

A window configured to perform custom reduce aggregation on the data.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.max"></a>

#### FixedTimeWindowDefinition.max

```python
def max() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L212)

Configure a window to aggregate the maximum value within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to calculate the maximum
value within each window period.

<a id="quixstreams.dataframe.windows.definitions.FixedTimeWindowDefinition.min"></a>

#### FixedTimeWindowDefinition.min

```python
def min() -> "FixedTimeWindow"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L241)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/time_based.py#L32)

<a id="quixstreams.dataframe.windows.time_based.FixedTimeWindow.final"></a>

#### FixedTimeWindow.final

```python
def final() -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/time_based.py#L107)

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

<a id="quixstreams.dataframe.windows.time_based.FixedTimeWindow.current"></a>

#### FixedTimeWindow.current

```python
def current() -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/time_based.py#L145)

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

<a id="quixstreams.dataframe.windows.base"></a>

## quixstreams.dataframe.windows.base

<a id="quixstreams.dataframe.windows.base.get_window_ranges"></a>

#### get\_window\_ranges

```python
def get_window_ranges(timestamp_ms: int,
                      duration_ms: int,
                      step_ms: Optional[int] = None) -> List[Tuple[int, int]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/base.py#L18)

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
class RowProducer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L71)

A producer class that is capable of serializing Rows to bytes and send them to Kafka.

The serialization is performed according to the Topic serialization settings.

**Arguments**:

- `broker_address`: Connection settings for Kafka.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
Note: values passed as arguments override values in `extra_config`.
- `on_error`: a callback triggered when `RowProducer.produce_row()`
or `RowProducer.poll()` fail`.
If producer fails and the callback returns `True`, the exception
will be logged but not propagated.
The default callback logs an exception and returns `False`.
- `flush_timeout`: The time the producer is waiting for all messages to be delivered.
- `transactional`: whether to use Kafka transactions or not.
Note this changes which underlying `Producer` class is used.

<a id="quixstreams.rowproducer.RowProducer.produce_row"></a>

#### RowProducer.produce\_row

```python
def produce_row(row: Row,
                topic: Topic,
                key: Optional[Any] = _KEY_UNSET,
                partition: Optional[int] = None,
                timestamp: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L121)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L161)

Polls the producer for events and calls `on_delivery` callbacks.

If `poll()` fails, it will trigger the provided "on_error" callback

**Arguments**:

- `timeout`: timeout in seconds

<a id="quixstreams.rowproducer.RowProducer.abort_transaction"></a>

#### RowProducer.abort\_transaction

```python
def abort_transaction(timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L232)

Attempt an abort if an active transaction.

Else, skip since it throws an exception if at least
one transaction was successfully completed at some point.

This avoids polluting the stack trace in the case where a transaction was
not active as expected (because of some other exception already raised)
and a cleanup abort is attempted.

NOTE: under normal circumstances a transaction will be open due to how
the Checkpoint inits another immediately after committing.

<a id="quixstreams.core.stream.functions"></a>

## quixstreams.core.stream.functions

<a id="quixstreams.core.stream.functions.StreamFunction"></a>

### StreamFunction

```python
class StreamFunction(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L65)

A base class for all the streaming operations in Quix Streams.

It provides a `get_executor` method to return a closure to be called with the input
values.

<a id="quixstreams.core.stream.functions.StreamFunction.get_executor"></a>

#### StreamFunction.get\_executor

```python
@abc.abstractmethod
def get_executor(child_executor: VoidExecutor) -> VoidExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L79)

Returns a wrapper to be called on a value, key and timestamp.

<a id="quixstreams.core.stream.functions.ApplyFunction"></a>

### ApplyFunction

```python
class ApplyFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L85)

Wrap a function into "Apply" function.

The provided callback is expected to return a new value based on input,
and its result will always be passed downstream.

<a id="quixstreams.core.stream.functions.ApplyWithMetadataFunction"></a>

### ApplyWithMetadataFunction

```python
class ApplyWithMetadataFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L125)

Wrap a function into "Apply" function.

The provided function is expected to accept value, and timestamp and return
a new value based on input,
and its result will always be passed downstream.

<a id="quixstreams.core.stream.functions.FilterFunction"></a>

### FilterFunction

```python
class FilterFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L166)

Wraps a function into a "Filter" function.
The result of a Filter function is interpreted as boolean.
If it's `True`, the input will be return downstream.
If it's `False`, the `Filtered` exception will be raised to signal that the
value is filtered out.

<a id="quixstreams.core.stream.functions.FilterWithMetadataFunction"></a>

### FilterWithMetadataFunction

```python
class FilterWithMetadataFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L187)

Wraps a function into a "Filter" function.

The passed callback must accept value, key, and timestamp, and it's expected to
return a boolean-like result.

If the result is `True`, the input will be passed downstream.
Otherwise, the value will be filtered out.

<a id="quixstreams.core.stream.functions.UpdateFunction"></a>

### UpdateFunction

```python
class UpdateFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L210)

Wrap a function into an "Update" function.

The provided function must accept a value, and it's expected to mutate it
or to perform some side effect.

The result of the callback is always ignored, and the original input is passed
downstream.

<a id="quixstreams.core.stream.functions.UpdateWithMetadataFunction"></a>

### UpdateWithMetadataFunction

```python
class UpdateWithMetadataFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L233)

Wrap a function into an "Update" function.

The provided function must accept a value, a key, and a timestamp.
The callback is expected to mutate the value or to perform some side effect with it.

The result of the callback is always ignored, and the original input is passed
downstream.

<a id="quixstreams.core.stream.functions.TransformFunction"></a>

### TransformFunction

```python
class TransformFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions.py#L256)

Wrap a function into a "Transform" function.

The provided callback must accept a value, a key and a timestamp.
It's expected to return a new value, new key and new timestamp.

This function must be used with caution, because it can technically change the
key.
It's supposed to be used by the library internals and not be a part of the public
API.

The result of the callback will always be passed downstream.

<a id="quixstreams.core.stream"></a>

## quixstreams.core.stream

<a id="quixstreams.core.stream.stream"></a>

## quixstreams.core.stream.stream

<a id="quixstreams.core.stream.stream.Stream"></a>

### Stream

```python
class Stream()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L34)

<a id="quixstreams.core.stream.stream.Stream.__init__"></a>

#### Stream.\_\_init\_\_

```python
def __init__(func: Optional[StreamFunction] = None,
             parent: Optional[Self] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L35)

A base class for all streaming operations.

`Stream` is an abstraction of a function pipeline.
Each Stream has a function and a parent (None by default).
When adding new function to the stream, it creates a new `Stream` object and
sets "parent" to the previous `Stream` to maintain an order of execution.

Streams supports four types of functions:

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
    If it's `False`, the record will be filtered from the stream.
- "Transform" - to transform keys and timestamps along with the values.
    "Transform" functions may change the keys and should be used with caution.
    The result of the Transform function is passed downstream to the next
    functions.
    If "expand=True" is passed and the function returns an `Iterable`,
    each item of it will be treated as a separate value downstream.

To execute the functions on the `Stream`, call `.compose()` method, and
it will return a closure to execute all the functions accumulated in the Stream
and its parents.

**Arguments**:

- `func`: a function to be called on the stream.
It is expected to be wrapped into one of "Apply", "Filter", "Update" or
"Trasform" from `quixstreams.core.stream.functions` package.
Default - "ApplyFunction(lambda value: value)".
- `parent`: a parent `Stream`

<a id="quixstreams.core.stream.stream.Stream.add_filter"></a>

#### Stream.add\_filter

```python
def add_filter(func: Union[FilterCallback, FilterWithMetadataCallback],
               *,
               metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L97)

Add a function to filter values from the Stream.

The return value of the function will be interpreted as `bool`.
If the function returns `False`-like result, the Stream will raise `Filtered`
exception during execution.

**Arguments**:

- `func`: a function to filter values from the stream
- `metadata`: if True, the callback will receive key and timestamp along with
the value.
Default - `False`.

**Returns**:

a new `Stream` derived from the current one

<a id="quixstreams.core.stream.stream.Stream.add_apply"></a>

#### Stream.add\_apply

```python
def add_apply(func: Union[
    ApplyCallback,
    ApplyExpandedCallback,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
],
              *,
              expand: bool = False,
              metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L122)

Add an "apply" function to the Stream.

The function is supposed to return a new value, which will be passed
further during execution.

**Arguments**:

- `func`: a function to generate a new value
- `expand`: if True, expand the returned iterable into individual values
downstream. If returned value is not iterable, `TypeError` will be raised.
Default - `False`.
- `metadata`: if True, the callback will receive key and timestamp along with
the value.
Default - `False`.

**Returns**:

a new `Stream` derived from the current one

<a id="quixstreams.core.stream.stream.Stream.add_update"></a>

#### Stream.add\_update

```python
def add_update(func: Union[UpdateCallback, UpdateWithMetadataCallback],
               *,
               metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L155)

Add an "update" function to the Stream, that will mutate the input value.

The return of this function will be ignored and its input
will be passed downstream.

**Arguments**:

- `func`: a function to mutate the value
- `metadata`: if True, the callback will receive key and timestamp along with
the value.
Default - `False`.

**Returns**:

a new Stream derived from the current one

<a id="quixstreams.core.stream.stream.Stream.add_transform"></a>

#### Stream.add\_transform

```python
def add_transform(func: Union[TransformCallback, TransformExpandedCallback],
                  *,
                  expand: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L179)

Add a "transform" function to the Stream, that will mutate the input value.

The callback must accept a value, a key, and a timestamp.
It's expected to return a new value, new key and new timestamp.

The result of the callback which will be passed downstream
during execution.

**Arguments**:

- `func`: a function to mutate the value
- `expand`: if True, expand the returned iterable into individual items
downstream. If returned value is not iterable, `TypeError` will be raised.
Default - `False`.

**Returns**:

a new Stream derived from the current one

<a id="quixstreams.core.stream.stream.Stream.diff"></a>

#### Stream.diff

```python
def diff(other: "Stream") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L204)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L233)

Return a list of all parent Streams including the node itself.

The tree is ordered from parent to child (current node comes last).

**Returns**:

a list of `Stream` objects

<a id="quixstreams.core.stream.stream.Stream.compose_returning"></a>

#### Stream.compose\_returning

```python
def compose_returning() -> ReturningExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L252)

Compose a list of functions from this `Stream` and its parents into one
big closure that always returns the transformed record.

This closure is to be used to execute the functions in the stream and to get
the result of the transformations.

Stream may only contain simple "apply" functions to be able to compose itself
into a returning function.

<a id="quixstreams.core.stream.stream.Stream.compose"></a>

#### Stream.compose

```python
def compose(
    allow_filters: bool = True,
    allow_updates: bool = True,
    allow_expands: bool = True,
    allow_transforms: bool = True,
    sink: Optional[Callable[[Any, Any, int, Any],
                            None]] = None) -> VoidExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L289)

Compose a list of functions from this `Stream` and its parents into one

big closure using a "composer" function.

This "executor" closure is to be used to execute all functions in the stream for the given
key, value and timestamps.

By default, executor doesn't return the result of the execution.
To accumulate the results, pass the `sink` parameter.

**Arguments**:

- `allow_filters`: If False, this function will fail with `ValueError` if
the stream has filter functions in the tree. Default - True.
- `allow_updates`: If False, this function will fail with `ValueError` if
the stream has update functions in the tree. Default - True.
- `allow_expands`: If False, this function will fail with `ValueError` if
the stream has functions with "expand=True" in the tree. Default - True.
- `allow_transforms`: If False, this function will fail with `ValueError` if
the stream has transform functions in the tree. Default - True.
- `sink`: callable to accumulate the results of the execution, optional.

**Raises**:

- `ValueError`: if disallowed functions are present in the stream tree.

<a id="quixstreams.core"></a>

## quixstreams.core

<a id="quixstreams.processing"></a>

## quixstreams.processing

<a id="quixstreams.processing.context"></a>

## quixstreams.processing.context

<a id="quixstreams.processing.context.ProcessingContext"></a>

### ProcessingContext

```python
@dataclasses.dataclass
class ProcessingContext()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L23)

A class to share processing-related objects
between `Application` and `StreamingDataFrame` instances.

<a id="quixstreams.processing.context.ProcessingContext.store_offset"></a>

#### ProcessingContext.store\_offset

```python
def store_offset(topic: str, partition: int, offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L47)

Store the offset of the processed message to the checkpoint.

**Arguments**:

- `topic`: topic name
- `partition`: partition number
- `offset`: message offset

<a id="quixstreams.processing.context.ProcessingContext.init_checkpoint"></a>

#### ProcessingContext.init\_checkpoint

```python
def init_checkpoint()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L57)

Initialize a new checkpoint

<a id="quixstreams.processing.context.ProcessingContext.commit_checkpoint"></a>

#### ProcessingContext.commit\_checkpoint

```python
def commit_checkpoint(force: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L72)

Attempts finalizing the current Checkpoint only if the Checkpoint is "expired",

or `force=True` is passed, otherwise do nothing.

To finalize: the Checkpoint will be committed if it has any stored offsets,
else just close it. A new Checkpoint is then created.

**Arguments**:

- `force`: if `True`, commit the Checkpoint before its expiration deadline.

<a id="quixstreams.processing.pausing"></a>

## quixstreams.processing.pausing

<a id="quixstreams.processing.pausing.PausingManager"></a>

### PausingManager

```python
class PausingManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/pausing.py#L15)

A class to temporarily pause topic partitions and resume them after
the timeout is elapsed.

<a id="quixstreams.processing.pausing.PausingManager.pause"></a>

#### PausingManager.pause

```python
def pause(topic: str, partition: int, offset_to_seek: int,
          resume_after: float)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/pausing.py#L28)

Pause the topic-partition for a certain period of time.

This method is supposed to be called in case of backpressure from Sinks.

<a id="quixstreams.processing.pausing.PausingManager.is_paused"></a>

#### PausingManager.is\_paused

```python
def is_paused(topic: str, partition: int) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/pausing.py#L68)

Check if the topic-partition is already paused

<a id="quixstreams.processing.pausing.PausingManager.resume_if_ready"></a>

#### PausingManager.resume\_if\_ready

```python
def resume_if_ready()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/pausing.py#L74)

Resume consuming from topic-partitions after the wait period has elapsed.

<a id="quixstreams.processing.pausing.PausingManager.revoke"></a>

#### PausingManager.revoke

```python
def revoke(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/pausing.py#L94)

Remove partition from the list of paused TPs if it's revoked

<a id="quixstreams.sinks.influxdb3"></a>

## quixstreams.sinks.influxdb3

<a id="quixstreams.sinks.influxdb3.InfluxDB3Sink"></a>

### InfluxDB3Sink

```python
class InfluxDB3Sink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/influxdb3.py#L24)

<a id="quixstreams.sinks.influxdb3.InfluxDB3Sink.__init__"></a>

#### InfluxDB3Sink.\_\_init\_\_

```python
def __init__(token: str,
             host: str,
             organization_id: str,
             database: str,
             measurement: str,
             fields_keys: Iterable[str] = (),
             tags_keys: Iterable[str] = (),
             time_key: Optional[str] = None,
             time_precision: WritePrecision = WritePrecision.MS,
             include_metadata_tags: bool = False,
             batch_size: int = 1000,
             enable_gzip: bool = True,
             request_timeout_ms: int = 10_000,
             debug: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/influxdb3.py#L25)

A connector to sink processed data to InfluxDB v3.

It batches the processed records in memory per topic partition, converts
them to the InfluxDB format, and flushes them to InfluxDB at the checkpoint.

The InfluxDB sink transparently handles backpressure if the destination instance
cannot accept more data at the moment
(e.g., when InfluxDB returns an HTTP 429 error with the "retry_after" header set).
When this happens, the sink will notify the Application to pause consuming
from the backpressured topic partition until the "retry_after" timeout elapses.

>***NOTE***: InfluxDB3Sink can accept only dictionaries.
> If the record values are not dicts, you need to convert them to dicts before
> sinking.

**Arguments**:

- `token`: InfluxDB access token
- `host`: InfluxDB host in format "https://<host>"
- `organization_id`: InfluxDB organization_id
- `database`: database name
- `fields_keys`: a list of keys to be used as "fields" when writing to InfluxDB.
If present, it must not overlap with "tags_keys".
If empty, the whole record value will be used.
>***NOTE*** The fields' values can only be strings, floats, integers, or booleans.
Default - `()`.
- `tags_keys`: a list of keys to be used as "tags" when writing to InfluxDB.
If present, it must not overlap with "fields_keys".
These keys will be popped from the value dictionary
automatically because InfluxDB doesn't allow the same keys be
both in tags and fields.
If empty, no tags will be sent.
>***NOTE***: InfluxDB client always converts tag values to strings.
Default - `()`.
- `time_key`: a key to be used as "time" when writing to InfluxDB.
By default, the record timestamp will be used with "ms" time precision.
When using a custom key, you may need to adjust the `time_precision` setting
to match.
- `time_precision`: a time precision to use when writing to InfluxDB.
- `include_metadata_tags`: if True, includes record's key, topic,
and partition as tags.
Default - `False`.
- `batch_size`: how many records to write to InfluxDB in one request.
Note that it only affects the size of one write request, and not the number
of records flushed on each checkpoint.
Default - `1000`.
- `enable_gzip`: if True, enables gzip compression for writes.
Default - `True`.
- `request_timeout_ms`: an HTTP request timeout in milliseconds.
Default - `10000`.
- `debug`: if True, print debug logs from InfluxDB client.
Default - `False`.

<a id="quixstreams.sinks"></a>

## quixstreams.sinks

<a id="quixstreams.sinks.csv"></a>

## quixstreams.sinks.csv

<a id="quixstreams.sinks.csv.CSVSink"></a>

### CSVSink

```python
class CSVSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/csv.py#L9)

<a id="quixstreams.sinks.csv.CSVSink.__init__"></a>

#### CSVSink.\_\_init\_\_

```python
def __init__(path: str,
             dialect: str = "excel",
             key_serializer: Callable[[Any], str] = str,
             value_serializer: Callable[[Any], str] = json.dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/csv.py#L11)

A base CSV sink that writes data from all assigned partitions to a single file.

It's best to be used for local debugging.

Column format:
    (key, value, timestamp, topic, partition, offset)

**Arguments**:

- `path`: a path to CSV file
- `dialect`: a CSV dialect to use. It affects quoting and delimiters.
See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
Default - `"excel"`.
- `key_serializer`: a callable to convert keys to strings.
Default - `str()`.
- `value_serializer`: a callable to convert values to strings.
Default - `json.dumps()`.

<a id="quixstreams.sinks.exceptions"></a>

## quixstreams.sinks.exceptions

<a id="quixstreams.sinks.manager"></a>

## quixstreams.sinks.manager

<a id="quixstreams.sinks.base.sink"></a>

## quixstreams.sinks.base.sink

<a id="quixstreams.sinks.base.sink.BaseSink"></a>

### BaseSink

```python
class BaseSink(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L11)

This is a base class for all sinks.

Subclass it and implement its methods to create your own sink.

Note that Sinks are currently in beta, and their design may change over time.

<a id="quixstreams.sinks.base.sink.BaseSink.flush"></a>

#### BaseSink.flush

```python
@abc.abstractmethod
def flush(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L21)

This method is triggered by the Checkpoint class when it commits.

You can use `flush()` to write the batched data to the destination (in case of
a batching sink), or confirm the delivery of the previously sent messages
(in case of a streaming sink).

If flush() fails, the checkpoint will be aborted.

<a id="quixstreams.sinks.base.sink.BaseSink.add"></a>

#### BaseSink.add

```python
@abc.abstractmethod
def add(value: Any, key: Any, timestamp: int,
        headers: List[Tuple[str, HeaderValue]], topic: str, partition: int,
        offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L33)

This method is triggered on every new processed record being sent to this sink.

You can use it to accumulate batches of data before sending them outside, or
to send results right away in a streaming manner and confirm a delivery later
on flush().

<a id="quixstreams.sinks.base.sink.BaseSink.on_paused"></a>

#### BaseSink.on\_paused

```python
def on_paused(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L51)

This method is triggered when the sink is paused due to backpressure, when
the `SinkBackpressureError` is raised.

Here you can react to the backpressure events.

<a id="quixstreams.sinks.base.sink.BatchingSink"></a>

### BatchingSink

```python
class BatchingSink(BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L60)

A base class for batching sinks, that need to accumulate the data first before
sending it to the external destinatios.

Examples: databases, objects stores, and other destinations where
writing every message is not optimal.

It automatically handles batching, keeping batches in memory per topic-partition.

You may subclass it and override the `write()` method to implement a custom
batching sink.

<a id="quixstreams.sinks.base.sink.BatchingSink.write"></a>

#### BatchingSink.write

```python
@abc.abstractmethod
def write(batch: SinkBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L83)

This method implements actual writing to the external destination.

It may also raise `SinkBackpressureError` if the destination cannot accept new
writes at the moment.
When this happens, the accumulated batch is dropped and the app pauses the
corresponding topic partition.

<a id="quixstreams.sinks.base.sink.BatchingSink.add"></a>

#### BatchingSink.add

```python
def add(value: Any, key: Any, timestamp: int,
        headers: List[Tuple[str, HeaderValue]], topic: str, partition: int,
        offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L93)

Add a new record to in-memory batch.

<a id="quixstreams.sinks.base.sink.BatchingSink.flush"></a>

#### BatchingSink.flush

```python
def flush(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L115)

Flush an accumulated batch to the destination and drop it afterward.

<a id="quixstreams.sinks.base.sink.BatchingSink.on_paused"></a>

#### BatchingSink.on\_paused

```python
def on_paused(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L135)

When the destination is already backpressure, drop the accumulated batch.

<a id="quixstreams.sinks.base.batch"></a>

## quixstreams.sinks.base.batch

<a id="quixstreams.sinks.base.batch.SinkBatch"></a>

### SinkBatch

```python
class SinkBatch()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/batch.py#L11)

<a id="quixstreams.sinks.base.batch.SinkBatch.iter_chunks"></a>

#### SinkBatch.iter\_chunks

```python
def iter_chunks(n: int) -> Iterable[Iterable[SinkItem]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/batch.py#L55)

Iterate over batch data in chunks of length n.
The last batch may be shorter.

<a id="quixstreams.sinks.base"></a>

## quixstreams.sinks.base

<a id="quixstreams.sinks.base.item"></a>

## quixstreams.sinks.base.item

<a id="quixstreams.utils"></a>

## quixstreams.utils

<a id="quixstreams.utils.dicts"></a>

## quixstreams.utils.dicts

<a id="quixstreams.utils.dicts.dict_values"></a>

#### dict\_values

```python
def dict_values(d: object) -> List
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/utils/dicts.py#L4)

Recursively unpacks a set of nested dicts to get a flattened list of leaves,

where "leaves" are the first non-dict item.

i.e {"a": {"b": {"c": 1}, "d": 2}, "e": 3} becomes [1, 2, 3]

**Arguments**:

- `d`: initially, a dict (with potentially nested dicts)

**Returns**:

a list with all the leaves of the various contained dicts

<a id="quixstreams.utils.json"></a>

## quixstreams.utils.json

<a id="quixstreams.utils.json.dumps"></a>

#### dumps

```python
def dumps(value: Any) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/utils/json.py#L8)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/utils/json.py#L18)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/timestamps.py#L8)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/timestamps.py#L14)

Represents a timestamp of incoming Kafka message.

It is made pseudo-immutable (i.e. public attributes don't have setters), and
it should not be mutated during message processing.

<a id="quixstreams.models.timestamps.MessageTimestamp.create"></a>

#### MessageTimestamp.create

```python
@classmethod
def create(cls, timestamp_type: int, milliseconds: int) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/timestamps.py#L41)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/messagecontext.py#L4)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/types.py#L13)

An interface of `confluent_kafka.Message`.

Use it to not depend on exact implementation and simplify testing.

Instances of `confluent_kafka.Message` cannot be directly created from Python,
see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

<a id="quixstreams.models.serializers.avro"></a>

## quixstreams.models.serializers.avro

<a id="quixstreams.models.serializers.avro.AvroSerializer"></a>

### AvroSerializer

```python
class AvroSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L14)

<a id="quixstreams.models.serializers.avro.AvroSerializer.__init__"></a>

#### AvroSerializer.\_\_init\_\_

```python
def __init__(schema: Schema,
             strict: bool = False,
             strict_allow_default: bool = False,
             disable_tuple_notation: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L15)

Serializer that returns data in Avro format.

For more information see fastavro [schemaless_writer](https://fastavro.readthedocs.io/en/latest/writer.html#fastavro._write_py.schemaless_writer) method.

**Arguments**:

- `schema`: The avro schema.
- `strict`: If set to True, an error will be raised if records do not contain exactly the same fields that the schema states.
Default - `False`
- `strict_allow_default`: If set to True, an error will be raised if records do not contain exactly the same fields that the schema states unless it is a missing field that has a default value in the schema.
Default - `False`
- `disable_tuple_notation`: If set to True, tuples will not be treated as a special case. Therefore, using a tuple to indicate the type of a record will not work.
Default - `False`

<a id="quixstreams.models.serializers.avro.AvroDeserializer"></a>

### AvroDeserializer

```python
class AvroDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L59)

<a id="quixstreams.models.serializers.avro.AvroDeserializer.__init__"></a>

#### AvroDeserializer.\_\_init\_\_

```python
def __init__(schema: Schema,
             reader_schema: Optional[Schema] = None,
             return_record_name: bool = False,
             return_record_name_override: bool = False,
             return_named_type: bool = False,
             return_named_type_override: bool = False,
             handle_unicode_errors: str = "strict")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L60)

Deserializer that parses data from Avro.

For more information see fastavro [schemaless_reader](https://fastavro.readthedocs.io/en/latest/reader.html#fastavro._read_py.schemaless_reader) method.

**Arguments**:

- `schema`: The Avro schema.
- `reader_schema`: If the schema has changed since being written then the new schema can be given to allow for schema migration.
Default - `None`
- `return_record_name`: If true, when reading a union of records, the result will be a tuple where the first value is the name of the record and the second value is the record itself.
Default - `False`
- `return_record_name_override`: If true, this will modify the behavior of return_record_name so that the record name is only returned for unions where there is more than one record. For unions that only have one record, this option will make it so that the record is returned by itself, not a tuple with the name.
Default - `False`
- `return_named_type`: If true, when reading a union of named types, the result will be a tuple where the first value is the name of the type and the second value is the record itself NOTE: Using this option will ignore return_record_name and return_record_name_override.
Default - `False`
- `return_named_type_override`: If true, this will modify the behavior of return_named_type so that the named type is only returned for unions where there is more than one named type. For unions that only have one named type, this option will make it so that the named type is returned by itself, not a tuple with the name.
Default - `False`
- `handle_unicode_errors`: Should be set to a valid string that can be used in the errors argument of the string decode() function.
Default - `"strict"`

<a id="quixstreams.models.serializers"></a>

## quixstreams.models.serializers

<a id="quixstreams.models.serializers.exceptions"></a>

## quixstreams.models.serializers.exceptions

<a id="quixstreams.models.serializers.exceptions.IgnoreMessage"></a>

### IgnoreMessage

```python
class IgnoreMessage(exceptions.QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/exceptions.py#L46)

Raise this exception from Deserializer.__call__ in order to ignore the processing
of the particular message.

<a id="quixstreams.models.serializers.quix"></a>

## quixstreams.models.serializers.quix

<a id="quixstreams.models.serializers.quix.QuixDeserializer"></a>

### QuixDeserializer

```python
class QuixDeserializer(JSONDeserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L73)

Handles Deserialization for any Quix-formatted topic.

Parses JSON data from either `TimeseriesData` and `EventData` (ignores the rest).

<a id="quixstreams.models.serializers.quix.QuixDeserializer.__init__"></a>

#### QuixDeserializer.\_\_init\_\_

```python
def __init__(loads: Callable[[Union[bytes, bytearray]], Any] = default_loads)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L80)

**Arguments**:

- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.split_values"></a>

#### QuixDeserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L97)

Each Quix message might contain data for multiple Rows.
This property informs the downstream processors about that, so they can
expect an Iterable instead of Mapping.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.deserialize"></a>

#### QuixDeserializer.deserialize

```python
def deserialize(model_key: str, value: Union[List[Mapping],
                                             Mapping]) -> Iterable[Mapping]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L150)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L271)

<a id="quixstreams.models.serializers.quix.QuixSerializer.__init__"></a>

#### QuixSerializer.\_\_init\_\_

```python
def __init__(as_legacy: bool = True,
             dumps: Callable[[Any], Union[str, bytes]] = default_dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L275)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L318)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L406)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L44)

A deserializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.BytesSerializer"></a>

### BytesSerializer

```python
class BytesSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L53)

A serializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.StringDeserializer"></a>

### StringDeserializer

```python
class StringDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L62)

<a id="quixstreams.models.serializers.simple_types.StringDeserializer.__init__"></a>

#### StringDeserializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L63)

Deserializes bytes to strings using the specified encoding.

**Arguments**:

- `codec`: string encoding
A wrapper around `confluent_kafka.serialization.StringDeserializer`.

<a id="quixstreams.models.serializers.simple_types.IntegerDeserializer"></a>

### IntegerDeserializer

```python
class IntegerDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L81)

Deserializes bytes to integers.

A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.

<a id="quixstreams.models.serializers.simple_types.DoubleDeserializer"></a>

### DoubleDeserializer

```python
class DoubleDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L99)

Deserializes float to IEEE 764 binary64.

A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.

<a id="quixstreams.models.serializers.simple_types.StringSerializer"></a>

### StringSerializer

```python
class StringSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L117)

<a id="quixstreams.models.serializers.simple_types.StringSerializer.__init__"></a>

#### StringSerializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L118)

Serializes strings to bytes using the specified encoding.

**Arguments**:

- `codec`: string encoding

<a id="quixstreams.models.serializers.simple_types.IntegerSerializer"></a>

### IntegerSerializer

```python
class IntegerSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L130)

Serializes integers to bytes

<a id="quixstreams.models.serializers.simple_types.DoubleSerializer"></a>

### DoubleSerializer

```python
class DoubleSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L143)

Serializes floats to bytes

<a id="quixstreams.models.serializers.protobuf"></a>

## quixstreams.models.serializers.protobuf

<a id="quixstreams.models.serializers.protobuf.ProtobufSerializer"></a>

### ProtobufSerializer

```python
class ProtobufSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L12)

<a id="quixstreams.models.serializers.protobuf.ProtobufSerializer.__init__"></a>

#### ProtobufSerializer.\_\_init\_\_

```python
def __init__(msg_type: Message,
             deterministic: bool = False,
             ignore_unknown_fields: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L13)

Serializer that returns data in protobuf format.

Serialisation from a python dictionary can have a significant performance impact. An alternative is to pass the serializer an object of the `msg_type` class.

**Arguments**:

- `msg_type`: protobuf message class.
- `deterministic`: If true, requests deterministic serialization of the protobuf, with predictable ordering of map keys
Default - `False`
- `ignore_unknown_fields`: If True, do not raise errors for unknown fields.
Default - `False`

<a id="quixstreams.models.serializers.protobuf.ProtobufDeserializer"></a>

### ProtobufDeserializer

```python
class ProtobufDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L52)

<a id="quixstreams.models.serializers.protobuf.ProtobufDeserializer.__init__"></a>

#### ProtobufDeserializer.\_\_init\_\_

```python
def __init__(msg_type: Message,
             use_integers_for_enums: bool = False,
             preserving_proto_field_name: bool = False,
             to_dict: bool = True)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L53)

Deserializer that parses protobuf data into a dictionary suitable for a StreamingDataframe.

Deserialisation to a python dictionary can have a significant performance impact. You can disable this behavior using `to_dict`, in that case the protobuf message will be used as the StreamingDataframe row value.

**Arguments**:

- `msg_type`: protobuf message class.
- `use_integers_for_enums`: If true, use integers instead of enum names.
Default - `False`
- `preserving_proto_field_name`: If True, use the original proto field names as
defined in the .proto file. If False, convert the field names to
lowerCamelCase.
Default - `False`
- `to_dict`: If false, return the protobuf message instead of a dict.
Default - `True`

<a id="quixstreams.models.serializers.json"></a>

## quixstreams.models.serializers.json

<a id="quixstreams.models.serializers.json.JSONSerializer"></a>

### JSONSerializer

```python
class JSONSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L15)

<a id="quixstreams.models.serializers.json.JSONSerializer.__init__"></a>

#### JSONSerializer.\_\_init\_\_

```python
def __init__(dumps: Callable[[Any], Union[str, bytes]] = default_dumps,
             schema: Optional[Mapping] = None,
             validator: Optional[Validator] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L16)

Serializer that returns data in json format.

**Arguments**:

- `dumps`: a function to serialize objects to json.
Default - :py:func:`quixstreams.utils.json.dumps`
- `schema`: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/`jsonschema.validators.Draft202012Validator`).
Default - `None`
- `validator`: A jsonschema validator used to validate the data. Takes precedences over the schema.
Default - `None`

<a id="quixstreams.models.serializers.json.JSONDeserializer"></a>

### JSONDeserializer

```python
class JSONDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L58)

<a id="quixstreams.models.serializers.json.JSONDeserializer.__init__"></a>

#### JSONDeserializer.\_\_init\_\_

```python
def __init__(loads: Callable[[Union[bytes, bytearray]], Any] = default_loads,
             schema: Optional[Mapping] = None,
             validator: Optional[Validator] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L59)

Deserializer that parses data from JSON

**Arguments**:

- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.
- `schema`: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/`jsonschema.validators.Draft202012Validator`).
Default - `None`
- `validator`: A jsonschema validator used to validate the data. Takes precedences over the schema.
Default - `None`

<a id="quixstreams.models.serializers.base"></a>

## quixstreams.models.serializers.base

<a id="quixstreams.models.serializers.base.SerializationContext"></a>

### SerializationContext

```python
class SerializationContext(_SerializationContext)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L22)

Provides additional context for message serialization/deserialization.

Every `Serializer` and `Deserializer` receives an instance of `SerializationContext`

<a id="quixstreams.models.serializers.base.Deserializer"></a>

### Deserializer

```python
class Deserializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L42)

<a id="quixstreams.models.serializers.base.Deserializer.__init__"></a>

#### Deserializer.\_\_init\_\_

```python
def __init__(*args, **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L43)

A base class for all Deserializers

<a id="quixstreams.models.serializers.base.Deserializer.split_values"></a>

#### Deserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L49)

Return True if the deserialized message should be considered as Iterable
and each item in it should be processed as a separate message.

<a id="quixstreams.models.serializers.base.Serializer"></a>

### Serializer

```python
class Serializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L60)

A base class for all Serializers

<a id="quixstreams.models.serializers.base.Serializer.extra_headers"></a>

#### Serializer.extra\_headers

```python
@property
def extra_headers() -> MessageHeadersMapping
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L66)

Informs producer to set additional headers

for the message it will be serializing

Must return a dictionary with headers.
Keys must be strings, and values must be strings, bytes or None.

**Returns**:

dict with headers

<a id="quixstreams.models.messages"></a>

## quixstreams.models.messages

<a id="quixstreams.models.rows"></a>

## quixstreams.models.rows

<a id="quixstreams.models.topics"></a>

## quixstreams.models.topics

<a id="quixstreams.models.topics.admin"></a>

## quixstreams.models.topics.admin

<a id="quixstreams.models.topics.admin.convert_topic_list"></a>

#### convert\_topic\_list

```python
def convert_topic_list(topics: List[Topic]) -> List[ConfluentTopic]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L24)

Converts `Topic`s to `ConfluentTopic`s as required for Confluent's

`AdminClient.create_topic()`.

**Arguments**:

- `topics`: list of `Topic`s

**Returns**:

list of confluent_kafka `ConfluentTopic`s

<a id="quixstreams.models.topics.admin.TopicAdmin"></a>

### TopicAdmin

```python
class TopicAdmin()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L47)

For performing "admin"-level operations on a Kafka cluster, mostly around topics.

Primarily used to create and inspect topic configurations.

<a id="quixstreams.models.topics.admin.TopicAdmin.__init__"></a>

#### TopicAdmin.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             logger: logging.Logger = logger,
             extra_config: Optional[Mapping] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L54)

**Arguments**:

- `broker_address`: Connection settings for Kafka.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
- `logger`: a Logger instance to attach librdkafka logging to
- `extra_config`: optional configs (generally accepts producer configs)

<a id="quixstreams.models.topics.admin.TopicAdmin.list_topics"></a>

#### TopicAdmin.list\_topics

```python
def list_topics(timeout: float = -1) -> Dict[str, ConfluentTopicMetadata]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L83)

Get a list of topics and their metadata from a Kafka cluster

**Arguments**:

- `timeout`: response timeout (seconds); Default infinite (-1)

**Returns**:

a dict of topic names and their metadata objects

<a id="quixstreams.models.topics.admin.TopicAdmin.inspect_topics"></a>

#### TopicAdmin.inspect\_topics

```python
def inspect_topics(topic_names: List[str],
                   timeout: float = 30) -> Dict[str, Optional[TopicConfig]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L94)

A simplified way of getting the topic configurations of the provided topics

from the cluster (if they exist).

**Arguments**:

- `topic_names`: a list of topic names
- `timeout`: response timeout (seconds)
>***NOTE***: `timeout` must be >0 here (expects non-neg, and 0 != inf).

**Returns**:

a dict with topic names and their respective `TopicConfig`

<a id="quixstreams.models.topics.admin.TopicAdmin.create_topics"></a>

#### TopicAdmin.create\_topics

```python
def create_topics(topics: List[Topic],
                  timeout: float = 30,
                  finalize_timeout: float = 60)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L176)

Create the given list of topics and confirm they are ready.

Also raises an exception with detailed printout should the creation
fail (it ignores issues for a topic already existing).

**Arguments**:

- `topics`: a list of `Topic`
- `timeout`: creation acknowledge timeout (seconds)
- `finalize_timeout`: topic finalization timeout (seconds)
>***NOTE***: `timeout` must be >0 here (expects non-neg, and 0 != inf).

<a id="quixstreams.models.topics.utils"></a>

## quixstreams.models.topics.utils

<a id="quixstreams.models.topics.utils.merge_headers"></a>

#### merge\_headers

```python
def merge_headers(original: Optional[MessageHeadersTuples],
                  other: MessageHeadersMapping) -> MessageHeadersTuples
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/utils.py#L8)

Merge two sets of Kafka message headers, overwriting headers in "origin"

by the values from "other".

**Arguments**:

- `original`: original headers as a list of (key, value) tuples.
- `other`: headers to merge as a dictionary.

**Returns**:

a list of (key, value) tuples.

<a id="quixstreams.models.topics.topic"></a>

## quixstreams.models.topics.topic

<a id="quixstreams.models.topics.topic.TopicConfig"></a>

### TopicConfig

```python
@dataclasses.dataclass(eq=True)
class TopicConfig()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L43)

Represents all kafka-level configuration for a kafka topic.

Generally used by Topic and any topic creation procedures.

<a id="quixstreams.models.topics.topic.Topic"></a>

### Topic

```python
class Topic()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L84)

A definition of a Kafka topic.

Typically created with an `app = quixstreams.app.Application()` instance via
`app.topic()`, and used by `quixstreams.dataframe.StreamingDataFrame`
instance.

<a id="quixstreams.models.topics.topic.Topic.__init__"></a>

#### Topic.\_\_init\_\_

```python
def __init__(
        name: str,
        config: TopicConfig,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = BytesDeserializer(),
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = BytesSerializer(),
        timestamp_extractor: Optional[TimestampExtractor] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L93)

**Arguments**:

- `name`: topic name
- `config`: topic configs via `TopicConfig` (creation/validation)
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message.

<a id="quixstreams.models.topics.topic.Topic.name"></a>

#### Topic.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L122)

Topic name

<a id="quixstreams.models.topics.topic.Topic.row_serialize"></a>

#### Topic.row\_serialize

```python
def row_serialize(row: Row, key: Any) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L132)

Serialize Row to a Kafka message structure

**Arguments**:

- `row`: Row to serialize
- `key`: message key to serialize

**Returns**:

KafkaMessage object with serialized values

<a id="quixstreams.models.topics.topic.Topic.row_deserialize"></a>

#### Topic.row\_deserialize

```python
def row_deserialize(
        message: ConfluentKafkaMessageProto) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L172)

Deserialize incoming Kafka message to a Row.

**Arguments**:

- `message`: an object with interface of `confluent_kafka.Message`

**Returns**:

Row, list of Rows or None if the message is ignored.

<a id="quixstreams.models.topics.exceptions"></a>

## quixstreams.models.topics.exceptions

<a id="quixstreams.models.topics.manager"></a>

## quixstreams.models.topics.manager

<a id="quixstreams.models.topics.manager.affirm_ready_for_create"></a>

#### affirm\_ready\_for\_create

```python
def affirm_ready_for_create(topics: List[Topic])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L20)

Validate a list of topics is ready for creation attempt

**Arguments**:

- `topics`: list of `Topic`s

<a id="quixstreams.models.topics.manager.TopicManager"></a>

### TopicManager

```python
class TopicManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L30)

The source of all topic management with quixstreams.

Generally initialized and managed automatically by an `Application`,
but allows a user to work with it directly when needed, such as using it alongside
a plain `Producer` to create its topics.

See methods for details.

<a id="quixstreams.models.topics.manager.TopicManager.__init__"></a>

#### TopicManager.\_\_init\_\_

```python
def __init__(topic_admin: TopicAdmin,
             consumer_group: str,
             timeout: float = 30,
             create_timeout: float = 60)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L53)

**Arguments**:

- `topic_admin`: an `Admin` instance (required for some functionality)
- `consumer_group`: the consumer group (of the `Application`)
- `timeout`: response timeout (seconds)
- `create_timeout`: timeout for topic creation

<a id="quixstreams.models.topics.manager.TopicManager.changelog_topics"></a>

#### TopicManager.changelog\_topics

```python
@property
def changelog_topics() -> Dict[str, Dict[str, Topic]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L103)

Note: `Topic`s are the changelogs.

returns: the changelog topic dict, {topic_name: {suffix: Topic}}

<a id="quixstreams.models.topics.manager.TopicManager.all_topics"></a>

#### TopicManager.all\_topics

```python
@property
def all_topics() -> Dict[str, Topic]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L112)

Every registered topic name mapped to its respective `Topic`.

returns: full topic dict, {topic_name: Topic}

<a id="quixstreams.models.topics.manager.TopicManager.topic_config"></a>

#### TopicManager.topic\_config

```python
def topic_config(num_partitions: Optional[int] = None,
                 replication_factor: Optional[int] = None,
                 extra_config: Optional[dict] = None) -> TopicConfig
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L220)

Convenience method for generating a `TopicConfig` with default settings

**Arguments**:

- `num_partitions`: the number of topic partitions
- `replication_factor`: the topic replication factor
- `extra_config`: other optional configuration settings

**Returns**:

a TopicConfig object

<a id="quixstreams.models.topics.manager.TopicManager.topic"></a>

#### TopicManager.topic

```python
def topic(name: str,
          value_deserializer: Optional[DeserializerType] = None,
          key_deserializer: Optional[DeserializerType] = "bytes",
          value_serializer: Optional[SerializerType] = None,
          key_serializer: Optional[SerializerType] = "bytes",
          config: Optional[TopicConfig] = None,
          timestamp_extractor: Optional[TimestampExtractor] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L241)

A convenience method for generating a `Topic`. Will use default config options

as dictated by the TopicManager.

**Arguments**:

- `name`: topic name
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys
- `config`: optional topic configurations (for creation/validation)
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message.

**Returns**:

Topic object with creation configs

<a id="quixstreams.models.topics.manager.TopicManager.repartition_topic"></a>

#### TopicManager.repartition\_topic

```python
def repartition_topic(operation: str,
                      topic_name: str,
                      value_deserializer: Optional[DeserializerType] = "json",
                      key_deserializer: Optional[DeserializerType] = "json",
                      value_serializer: Optional[SerializerType] = "json",
                      key_serializer: Optional[SerializerType] = "json",
                      timeout: Optional[float] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L286)

Create an internal repartition topic.

**Arguments**:

- `operation`: name of the GroupBy operation (column name or user-defined).
- `topic_name`: name of the topic the GroupBy is sourced from.
- `value_deserializer`: a deserializer type for values; default - JSON
- `key_deserializer`: a deserializer type for keys; default - JSON
- `value_serializer`: a serializer type for values; default - JSON
- `key_serializer`: a serializer type for keys; default - JSON
- `timeout`: config lookup timeout (seconds); Default 30

**Returns**:

`Topic` object (which is also stored on the TopicManager)

<a id="quixstreams.models.topics.manager.TopicManager.changelog_topic"></a>

#### TopicManager.changelog\_topic

```python
def changelog_topic(topic_name: str,
                    store_name: str,
                    timeout: Optional[float] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L326)

Performs all the logic necessary to generate a changelog topic based on a

"source topic" (aka input/consumed topic).

Its main goal is to ensure partition counts of the to-be generated changelog
match the source topic, and ensure the changelog topic is compacted. Also
enforces the serialization type. All `Topic` objects generated with this are
stored on the TopicManager.

If source topic already exists, defers to the existing topic settings, else
uses the settings as defined by the `Topic` (and its defaults) as generated
by the `TopicManager`.

In general, users should NOT need this; an Application knows when/how to
generate changelog topics. To turn off changelogs, init an Application with
"use_changelog_topics"=`False`.

**Arguments**:

- `topic_name`: name of consumed topic (app input topic)
> NOTE: normally contain any prefixes added by TopicManager.topic()
- `store_name`: name of the store this changelog belongs to
(default, rolling10s, etc.)
- `timeout`: config lookup timeout (seconds); Default 30

**Returns**:

`Topic` object (which is also stored on the TopicManager)

<a id="quixstreams.models.topics.manager.TopicManager.create_topics"></a>

#### TopicManager.create\_topics

```python
def create_topics(topics: List[Topic],
                  timeout: Optional[float] = None,
                  create_timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L383)

Creates topics via an explicit list of provided `Topics`.

Exists as a way to manually specify what topics to create; otherwise,
`create_all_topics()` is generally simpler.

**Arguments**:

- `topics`: list of `Topic`s
- `timeout`: creation acknowledge timeout (seconds); Default 30
- `create_timeout`: topic finalization timeout (seconds); Default 60

<a id="quixstreams.models.topics.manager.TopicManager.create_all_topics"></a>

#### TopicManager.create\_all\_topics

```python
def create_all_topics(timeout: Optional[float] = None,
                      create_timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L411)

A convenience method to create all Topic objects stored on this TopicManager.

**Arguments**:

- `timeout`: creation acknowledge timeout (seconds); Default 30
- `create_timeout`: topic finalization timeout (seconds); Default 60

<a id="quixstreams.models.topics.manager.TopicManager.validate_all_topics"></a>

#### TopicManager.validate\_all\_topics

```python
def validate_all_topics(timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L424)

Validates all topics exist and changelogs have correct topic and rep factor.

Issues are pooled and raised as an Exception once inspections are complete.

<a id="quixstreams.state.rocksdb.windowed.store"></a>

## quixstreams.state.rocksdb.windowed.store

<a id="quixstreams.state.rocksdb.windowed.store.WindowedRocksDBStore"></a>

### WindowedRocksDBStore

```python
class WindowedRocksDBStore(RocksDBStore)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/store.py#L10)

RocksDB-based windowed state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

<a id="quixstreams.state.rocksdb.windowed.store.WindowedRocksDBStore.__init__"></a>

#### WindowedRocksDBStore.\_\_init\_\_

```python
def __init__(
        name: str,
        topic: str,
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[RocksDBOptionsType] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/store.py#L18)

**Arguments**:

- `name`: a unique store name
- `topic`: a topic name for this store
- `base_dir`: path to a directory with the state
- `changelog_producer_factory`: a ChangelogProducerFactory instance
if using changelogs
- `options`: RocksDB options. If `None`, the default options will be used.

<a id="quixstreams.state.rocksdb.windowed.partition"></a>

## quixstreams.state.rocksdb.windowed.partition

<a id="quixstreams.state.rocksdb.windowed.partition.WindowedRocksDBStorePartition"></a>

### WindowedRocksDBStorePartition

```python
class WindowedRocksDBStorePartition(RocksDBStorePartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/partition.py#L24)

A base class to access windowed state in RocksDB.

It represents a single RocksDB database.

Besides the data, it keeps track of the latest observed timestamp and
stores the expiration index to delete expired windows.

**Arguments**:

- `path`: an absolute path to the RocksDB folder
- `options`: RocksDB options. If `None`, the default options will be used.

<a id="quixstreams.state.rocksdb.windowed.metadata"></a>

## quixstreams.state.rocksdb.windowed.metadata

<a id="quixstreams.state.rocksdb.windowed.transaction"></a>

## quixstreams.state.rocksdb.windowed.transaction

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction"></a>

### WindowedRocksDBPartitionTransaction

```python
class WindowedRocksDBPartitionTransaction(RocksDBPartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/transaction.py#L22)

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction.expire_windows"></a>

#### WindowedRocksDBPartitionTransaction.expire\_windows

```python
def expire_windows(duration_ms: int,
                   prefix: bytes,
                   grace_ms: int = 0) -> List[Tuple[Tuple[int, int], Any]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/transaction.py#L105)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/serialization.py#L12)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/serialization.py#L39)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/serialization.py#L53)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L9)

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.__init__"></a>

#### WindowedTransactionState.\_\_init\_\_

```python
def __init__(transaction: "WindowedRocksDBPartitionTransaction",
             prefix: bytes)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L12)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L23)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L39)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L60)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L72)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L25)

RocksDB database options.

**Arguments**:

- `dumps`: function to dump data to JSON
- `loads`: function to load data from JSON
- `open_max_retries`: number of times to retry opening the database
if it's locked by another process. To disable retrying, pass 0
- `open_retry_backoff`: number of seconds to wait between each retry.
Please see `rocksdict.Options` for a complete description of other options.

<a id="quixstreams.state.rocksdb.options.RocksDBOptions.to_options"></a>

#### RocksDBOptions.to\_options

```python
def to_options() -> rocksdict.Options
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L53)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L19)

RocksDB-based state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

<a id="quixstreams.state.rocksdb.store.RocksDBStore.__init__"></a>

#### RocksDBStore.\_\_init\_\_

```python
def __init__(
        name: str,
        topic: str,
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[options_type] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L29)

**Arguments**:

- `name`: a unique store name
- `topic`: a topic name for this store
- `base_dir`: path to a directory with the state
- `changelog_producer_factory`: a ChangelogProducerFactory instance
if using changelogs
- `options`: RocksDB options. If `None`, the default options will be used.

<a id="quixstreams.state.rocksdb.store.RocksDBStore.topic"></a>

#### RocksDBStore.topic

```python
@property
def topic() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L53)

Store topic name

<a id="quixstreams.state.rocksdb.store.RocksDBStore.name"></a>

#### RocksDBStore.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L60)

Store name

<a id="quixstreams.state.rocksdb.store.RocksDBStore.partitions"></a>

#### RocksDBStore.partitions

```python
@property
def partitions() -> Dict[int, RocksDBStorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L67)

Mapping of assigned store partitions

<a id="quixstreams.state.rocksdb.store.RocksDBStore.assign_partition"></a>

#### RocksDBStore.assign\_partition

```python
def assign_partition(partition: int) -> RocksDBStorePartition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L80)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L117)

Revoke and close the assigned store partition.

If the partition is not assigned, it will log the message and return.

**Arguments**:

- `partition`: partition number

<a id="quixstreams.state.rocksdb.store.RocksDBStore.start_partition_transaction"></a>

#### RocksDBStore.start\_partition\_transaction

```python
def start_partition_transaction(partition: int) -> RocksDBPartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L138)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L160)

Close the store and revoke all assigned partitions

<a id="quixstreams.state.rocksdb.partition"></a>

## quixstreams.state.rocksdb.partition

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition"></a>

### RocksDBStorePartition

```python
class RocksDBStorePartition(StorePartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L40)

A base class to access state in RocksDB.

It represents a single RocksDB database.

Responsibilities:
 1. Managing access to the RocksDB instance
 2. Creating transactions to interact with data
 3. Flushing WriteBatches to the RocksDB

It opens the RocksDB on `__init__`. If the db is locked by another process,
it will retry according to `open_max_retries` and `open_retry_backoff` options.

**Arguments**:

- `path`: an absolute path to the RocksDB folder
- `options`: RocksDB options. If `None`, the default options will be used.

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.begin"></a>

#### RocksDBStorePartition.begin

```python
def begin() -> RocksDBPartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L75)

Create a new `RocksDBTransaction` object.

Using `RocksDBTransaction` is a recommended way for accessing the data.

**Returns**:

an instance of `RocksDBTransaction`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.recover_from_changelog_message"></a>

#### RocksDBStorePartition.recover\_from\_changelog\_message

```python
def recover_from_changelog_message(
        changelog_message: ConfluentKafkaMessageProto, committed_offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L128)

Updates state from a given changelog message.

The actual update may be skipped when both conditions are met:

- The changelog message has headers with the processed message offset.
- This processed offset is larger than the latest committed offset for the same
  topic partition.

This way the state does not apply the state changes for not-yet-committed
messages and improves the state consistency guarantees.

**Arguments**:

- `changelog_message`: A raw Confluent message read from a changelog topic.
- `committed_offset`: latest committed offset for the partition

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.set_changelog_offset"></a>

#### RocksDBStorePartition.set\_changelog\_offset

```python
def set_changelog_offset(changelog_offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L169)

Set the changelog offset based on a message (usually an "offset-only" message).

Used during recovery.

**Arguments**:

- `changelog_offset`: A changelog offset

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.write"></a>

#### RocksDBStorePartition.write

```python
def write(batch: WriteBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L179)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L186)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L200)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L211)

Get last processed offset for the given partition

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_changelog_offset"></a>

#### RocksDBStorePartition.get\_changelog\_offset

```python
def get_changelog_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L223)

Get offset that the changelog is up-to-date with.

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.close"></a>

#### RocksDBStorePartition.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L233)

Close the underlying RocksDB

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.path"></a>

#### RocksDBStorePartition.path

```python
@property
def path() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L246)

Absolute path to RocksDB database folder

**Returns**:

file path

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.destroy"></a>

#### RocksDBStorePartition.destroy

```python
@classmethod
def destroy(cls, path: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L254)

Delete underlying RocksDB database

The database must be closed first.

**Arguments**:

- `path`: an absolute path to the RocksDB folder

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_column_family_handle"></a>

#### RocksDBStorePartition.get\_column\_family\_handle

```python
def get_column_family_handle(cf_name: str) -> ColumnFamily
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L264)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L285)

Get a column family instance.

This method will cache the CF instance to avoid creating them repeatedly.

**Arguments**:

- `cf_name`: column family name

**Returns**:

instance of `rocksdict.Rdict` for the given column family

<a id="quixstreams.state.rocksdb.metadata"></a>

## quixstreams.state.rocksdb.metadata

<a id="quixstreams.state.rocksdb.transaction"></a>

## quixstreams.state.rocksdb.transaction

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction"></a>

### RocksDBPartitionTransaction

```python
class RocksDBPartitionTransaction(PartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L61)

A transaction class to perform simple key-value operations like
"get", "set", "delete" and "exists" on a single RocksDB partition.

Serialization
*************
`RocksDBTransaction` automatically serializes keys and values to bytes.

Prefixing
*********
Methods `get()`, `set()`, `delete()` and `exists()` methods require prefixes for
the keys.
Normally, the Kafka message keys are supposed to be used as prefixes.

Transactional properties
************************
`RocksDBTransaction` uses a combination of in-memory update cache
and RocksDB's WriteBatch in order to accumulate all the state mutations
in a single batch, flush them atomically, and allow the updates be visible
within the transaction before it's flushed (aka "read-your-own-writes" problem).

If any mutation fails during the transaction
(e.g., failed to write the updates to the RocksDB), the whole transaction
will be marked as failed and cannot be used anymore.
In this case, a new `RocksDBTransaction` should be created.

`RocksDBTransaction` can be used only once.

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.__init__"></a>

#### RocksDBPartitionTransaction.\_\_init\_\_

```python
def __init__(partition: "RocksDBStorePartition",
             dumps: DumpsFunc,
             loads: LoadsFunc,
             changelog_producer: Optional[ChangelogProducer] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L100)

**Arguments**:

- `partition`: instance of `RocksDBStatePartition` to be used for accessing
the underlying RocksDB
- `dumps`: a function to serialize data to bytes.
- `loads`: a function to deserialize data from bytes.

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.get"></a>

#### RocksDBPartitionTransaction.get

```python
@_validate_transaction_status(PartitionTransactionStatus.STARTED)
def get(key: Any,
        prefix: bytes,
        default: Any = None,
        cf_name: str = "default") -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L124)

Get a key from the store.

It first looks up the key in the update cache in case it has been updated
but not flushed yet.

It returns `None` if the key is not found and `default` is not provided.

**Arguments**:

- `key`: a key to get from DB
- `prefix`: a key prefix
- `default`: value to return if the key is not present in the state.
It can be of any type.
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

value or `default`

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.set"></a>

#### RocksDBPartitionTransaction.set

```python
@_validate_transaction_status(PartitionTransactionStatus.STARTED)
def set(key: Any, value: Any, prefix: bytes, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L164)

Set a key to the store.

It first updates the key in the update cache.

**Arguments**:

- `key`: key to store in DB
- `prefix`: a key prefix
- `value`: value to store in DB
- `cf_name`: rocksdb column family name. Default - "default"

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.delete"></a>

#### RocksDBPartitionTransaction.delete

```python
@_validate_transaction_status(PartitionTransactionStatus.STARTED)
def delete(key: Any, prefix: bytes, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L187)

Delete a key from the store.

It first deletes the key from the update cache.

**Arguments**:

- `key`: a key to delete from DB
- `prefix`: a key prefix
- `cf_name`: rocksdb column family name. Default - "default"

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.exists"></a>

#### RocksDBPartitionTransaction.exists

```python
@_validate_transaction_status(PartitionTransactionStatus.STARTED)
def exists(key: Any, prefix: bytes, cf_name: str = "default") -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L208)

Check if a key exists in the store.

It first looks up the key in the update cache.

**Arguments**:

- `key`: a key to check in DB
- `prefix`: a key prefix
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

`True` if the key exists, `False` otherwise.

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.prepare"></a>

#### RocksDBPartitionTransaction.prepare

```python
@_validate_transaction_status(PartitionTransactionStatus.STARTED)
def prepare(processed_offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L235)

Produce changelog messages to the changelog topic for all changes accumulated

in this transaction and prepare transaction to flush its state to the state
store.

After successful `prepare()`, the transaction status is changed to PREPARED,
and it cannot receive updates anymore.

If changelog is disabled for this application, no updates will be produced
to the changelog topic.

**Arguments**:

- `processed_offset`: the offset of the latest processed message

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.flush"></a>

#### RocksDBPartitionTransaction.flush

```python
@_validate_transaction_status(PartitionTransactionStatus.STARTED,
                              PartitionTransactionStatus.PREPARED)
def flush(processed_offset: Optional[int] = None,
          changelog_offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L259)

Flush the recent updates to the database.

It writes the WriteBatch to RocksDB and marks itself as finished.

If writing fails, the transaction is marked as failed and
cannot be used anymore.

>***NOTE:*** If no keys have been modified during the transaction
    (i.e. no "set" or "delete" have been called at least once), it will
    not flush ANY data to the database including the offset to optimize
    I/O.

**Arguments**:

- `processed_offset`: offset of the last processed message, optional.
- `changelog_offset`: offset of the last produced changelog message,
optional.

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.completed"></a>

#### RocksDBPartitionTransaction.completed

```python
@property
def completed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L294)

Check if the transaction is completed.

It doesn't indicate whether transaction is successful or not.
Use `RocksDBTransaction.failed` for that.

The completed transaction should not be re-used.

**Returns**:

`True` if transaction is completed, `False` otherwise.

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.prepared"></a>

#### RocksDBPartitionTransaction.prepared

```python
@property
def prepared() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L308)

Check if the transaction is in PREPARED status.

Prepared transaction successfully flushed its changelog and cannot receive
updates anymore, but its state is not yet flushed to the disk

**Returns**:

`True` if transaction is prepared, `False` otherwise.

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.failed"></a>

#### RocksDBPartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L320)

Check if the transaction has failed.

The failed transaction should not be re-used because the update cache
and

**Returns**:

`True` if transaction is failed, `False` otherwise.

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.changelog_topic_partition"></a>

#### RocksDBPartitionTransaction.changelog\_topic\_partition

```python
@property
def changelog_topic_partition() -> Optional[Tuple[str, int]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L332)

Return the changelog topic-partition for the StorePartition of this transaction.

Returns `None` if changelog_producer is not provided.

**Returns**:

(topic, partition) or None

<a id="quixstreams.state.rocksdb.transaction.RocksDBPartitionTransaction.as_state"></a>

#### RocksDBPartitionTransaction.as\_state

```python
def as_state(prefix: Any = DEFAULT_PREFIX) -> TransactionState
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/transaction.py#L346)

Create a one-time use `TransactionState` object with a limited CRUD interface

to be provided to `StreamingDataFrame` operations.

The `TransactionState` will prefix all the keys with the supplied `prefix`
for all underlying operations.

**Arguments**:

- `prefix`: a prefix to be used for all keys

**Returns**:

an instance of `TransactionState`

<a id="quixstreams.state.rocksdb"></a>

## quixstreams.state.rocksdb

<a id="quixstreams.state.rocksdb.types"></a>

## quixstreams.state.rocksdb.types

<a id="quixstreams.state.rocksdb.exceptions"></a>

## quixstreams.state.rocksdb.exceptions

<a id="quixstreams.state.rocksdb.serialization"></a>

## quixstreams.state.rocksdb.serialization

<a id="quixstreams.state.recovery"></a>

## quixstreams.state.recovery

<a id="quixstreams.state.recovery.RecoveryPartition"></a>

### RecoveryPartition

```python
class RecoveryPartition()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L25)

A changelog topic partition mapped to a respective `StorePartition` with helper
methods to determine its current recovery status.

Since `StorePartition`s do recovery directly, it also handles recovery transactions.

<a id="quixstreams.state.recovery.RecoveryPartition.offset"></a>

#### RecoveryPartition.offset

```python
@property
def offset() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L61)

Get the changelog offset from the underlying `StorePartition`.

**Returns**:

changelog offset (int)

<a id="quixstreams.state.recovery.RecoveryPartition.needs_recovery_check"></a>

#### RecoveryPartition.needs\_recovery\_check

```python
@property
def needs_recovery_check() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L77)

Determine whether to attempt recovery for underlying `StorePartition`.

This does NOT mean that anything actually requires recovering.

<a id="quixstreams.state.recovery.RecoveryPartition.has_invalid_offset"></a>

#### RecoveryPartition.has\_invalid\_offset

```python
@property
def has_invalid_offset() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L88)

Determine if the current changelog offset stored in state is invalid.

<a id="quixstreams.state.recovery.RecoveryPartition.recover_from_changelog_message"></a>

#### RecoveryPartition.recover\_from\_changelog\_message

```python
def recover_from_changelog_message(
        changelog_message: ConfluentKafkaMessageProto)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L102)

Recover the StorePartition using a message read from its respective changelog.

**Arguments**:

- `changelog_message`: A confluent kafka message (everything as bytes)

<a id="quixstreams.state.recovery.RecoveryPartition.set_watermarks"></a>

#### RecoveryPartition.set\_watermarks

```python
def set_watermarks(lowwater: int, highwater: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L114)

Set the changelog watermarks as gathered from Consumer.get_watermark_offsets()

**Arguments**:

- `lowwater`: topic partition lowwater
- `highwater`: topic partition highwater

<a id="quixstreams.state.recovery.RecoveryPartition.set_recovery_consume_position"></a>

#### RecoveryPartition.set\_recovery\_consume\_position

```python
def set_recovery_consume_position(offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L124)

Update the recovery partition with the consumer's position (whenever

an empty poll is returned during recovery).

It is possible that it may be set more than once.

**Arguments**:

- `offset`: the consumer's current read position of the changelog

<a id="quixstreams.state.recovery.ChangelogProducerFactory"></a>

### ChangelogProducerFactory

```python
class ChangelogProducerFactory()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L136)

Generates ChangelogProducers, which produce changelog messages to a StorePartition.

<a id="quixstreams.state.recovery.ChangelogProducerFactory.__init__"></a>

#### ChangelogProducerFactory.\_\_init\_\_

```python
def __init__(changelog_name: str, producer: RowProducer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L141)

**Arguments**:

- `changelog_name`: changelog topic name
- `producer`: a RowProducer (not shared with `Application` instance)

**Returns**:

a ChangelogWriter instance

<a id="quixstreams.state.recovery.ChangelogProducerFactory.get_partition_producer"></a>

#### ChangelogProducerFactory.get\_partition\_producer

```python
def get_partition_producer(partition_num) -> "ChangelogProducer"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L151)

Generate a ChangelogProducer for producing to a specific partition number

(and thus StorePartition).

**Arguments**:

- `partition_num`: source topic partition number

<a id="quixstreams.state.recovery.ChangelogProducer"></a>

### ChangelogProducer

```python
class ChangelogProducer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L165)

Generated for a `StorePartition` to produce state changes to its respective
kafka changelog partition.

<a id="quixstreams.state.recovery.ChangelogProducer.__init__"></a>

#### ChangelogProducer.\_\_init\_\_

```python
def __init__(changelog_name: str, partition: int, producer: RowProducer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L171)

**Arguments**:

- `changelog_name`: A changelog topic name
- `partition`: source topic partition number
- `producer`: a RowProducer (not shared with `Application` instance)

<a id="quixstreams.state.recovery.ChangelogProducer.produce"></a>

#### ChangelogProducer.produce

```python
def produce(key: bytes,
            value: Optional[bytes] = None,
            headers: Optional[MessageHeadersMapping] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L194)

Produce a message to a changelog topic partition.

**Arguments**:

- `key`: message key (same as state key, including prefixes)
- `value`: message value (same as state value)
- `headers`: message headers (includes column family info)

<a id="quixstreams.state.recovery.RecoveryManager"></a>

### RecoveryManager

```python
class RecoveryManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L219)

Manages all consumer-related aspects of recovery, including:
    - assigning/revoking, pausing/resuming topic partitions (especially changelogs)
    - consuming changelog messages until state is updated fully.

Also tracks/manages `RecoveryPartitions`, which are assigned/tracked only if
recovery for that changelog partition is required.

Recovery is attempted from the `Application` after any new partition assignment.

<a id="quixstreams.state.recovery.RecoveryManager.partitions"></a>

#### RecoveryManager.partitions

```python
@property
def partitions() -> Dict[int, Dict[str, RecoveryPartition]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L238)

Returns a mapping of assigned RecoveryPartitions in the following format:
{<partition>: {<store_name>: <RecoveryPartition>}}

<a id="quixstreams.state.recovery.RecoveryManager.has_assignments"></a>

#### RecoveryManager.has\_assignments

```python
@property
def has_assignments() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L246)

Whether the Application has assigned RecoveryPartitions

**Returns**:

has assignments, as bool

<a id="quixstreams.state.recovery.RecoveryManager.recovering"></a>

#### RecoveryManager.recovering

```python
@property
def recovering() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L255)

Whether the Application is currently recovering

**Returns**:

is recovering, as bool

<a id="quixstreams.state.recovery.RecoveryManager.register_changelog"></a>

#### RecoveryManager.register\_changelog

```python
def register_changelog(topic_name: str, store_name: str) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L263)

Register a changelog Topic with the TopicManager.

**Arguments**:

- `topic_name`: source topic name
- `store_name`: name of the store

<a id="quixstreams.state.recovery.RecoveryManager.do_recovery"></a>

#### RecoveryManager.do\_recovery

```python
def do_recovery()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L275)

If there are any active RecoveryPartitions, do a recovery procedure.

After, will resume normal `Application` processing.

<a id="quixstreams.state.recovery.RecoveryManager.assign_partition"></a>

#### RecoveryManager.assign\_partition

```python
def assign_partition(topic: str, partition: int, committed_offset: int,
                     store_partitions: Dict[str, StorePartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L328)

Assigns `StorePartition`s (as `RecoveryPartition`s) ONLY IF recovery required.

Pauses active consumer partitions as needed.

<a id="quixstreams.state.recovery.RecoveryManager.revoke_partition"></a>

#### RecoveryManager.revoke\_partition

```python
def revoke_partition(partition_num: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L405)

revoke ALL StorePartitions (across all Stores) for a given partition number

**Arguments**:

- `partition_num`: partition number of source topic

<a id="quixstreams.state"></a>

## quixstreams.state

<a id="quixstreams.state.types"></a>

## quixstreams.state.types

<a id="quixstreams.state.types.Store"></a>

### Store

```python
class Store(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L11)

Abstract state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

<a id="quixstreams.state.types.Store.topic"></a>

#### Store.topic

```python
@property
def topic() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L22)

Topic name

<a id="quixstreams.state.types.Store.name"></a>

#### Store.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L29)

Store name

<a id="quixstreams.state.types.Store.partitions"></a>

#### Store.partitions

```python
@property
def partitions() -> Dict[int, "StorePartition"]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L36)

Mapping of assigned store partitions

**Returns**:

dict of "{partition: <StorePartition>}"

<a id="quixstreams.state.types.Store.assign_partition"></a>

#### Store.assign\_partition

```python
def assign_partition(partition: int) -> "StorePartition"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L43)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L52)

Revoke assigned store partition

**Arguments**:

- `partition`: partition number

<a id="quixstreams.state.types.Store.start_partition_transaction"></a>

#### Store.start\_partition\_transaction

```python
def start_partition_transaction(partition: int) -> "PartitionTransaction"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L60)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L69)

Close store and revoke all store partitions

<a id="quixstreams.state.types.StorePartition"></a>

### StorePartition

```python
class StorePartition(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L80)

A base class to access state in the underlying storage.
It represents a single instance of some storage (e.g. a single database for
the persistent storage).

<a id="quixstreams.state.types.StorePartition.path"></a>

#### StorePartition.path

```python
@property
def path() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L89)

Absolute path to RocksDB database folder

<a id="quixstreams.state.types.StorePartition.begin"></a>

#### StorePartition.begin

```python
def begin() -> "PartitionTransaction"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L95)

State new `PartitionTransaction`

<a id="quixstreams.state.types.StorePartition.recover_from_changelog_message"></a>

#### StorePartition.recover\_from\_changelog\_message

```python
def recover_from_changelog_message(
        changelog_message: ConfluentKafkaMessageProto, committed_offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L100)

Updates state from a given changelog message.

**Arguments**:

- `changelog_message`: A raw Confluent message read from a changelog topic.
- `committed_offset`: latest committed offset for the partition

<a id="quixstreams.state.types.StorePartition.get_processed_offset"></a>

#### StorePartition.get\_processed\_offset

```python
def get_processed_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L111)

Get last processed offset for the given partition

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.types.StorePartition.get_changelog_offset"></a>

#### StorePartition.get\_changelog\_offset

```python
def get_changelog_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L118)

Get offset that the changelog is up-to-date with.

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.types.StorePartition.set_changelog_offset"></a>

#### StorePartition.set\_changelog\_offset

```python
def set_changelog_offset(changelog_offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L125)

Set the changelog offset based on a message (usually an "offset-only" message).

Used during recovery.

**Arguments**:

- `changelog_offset`: A changelog offset

<a id="quixstreams.state.types.State"></a>

### State

```python
class State(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L136)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.types.State.get"></a>

#### State.get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L141)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L151)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.types.State.delete"></a>

#### State.delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L159)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.types.State.exists"></a>

#### State.exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L168)

Check if the key exists in state.

**Arguments**:

- `key`: key

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.types.PartitionTransaction"></a>

### PartitionTransaction

```python
class PartitionTransaction(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L177)

A transaction class to perform simple key-value operations like
"get", "set", "delete" and "exists" on a single storage partition.

<a id="quixstreams.state.types.PartitionTransaction.as_state"></a>

#### PartitionTransaction.as\_state

```python
def as_state(prefix: Any) -> State
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L183)

Create an instance implementing the `State` protocol to be provided

to `StreamingDataFrame` functions.
All operations called on this State object will be prefixed with
the supplied `prefix`.

**Returns**:

an instance implementing the `State` protocol

<a id="quixstreams.state.types.PartitionTransaction.get"></a>

#### PartitionTransaction.get

```python
def get(key: Any, prefix: bytes, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L194)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `prefix`: a key prefix
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.PartitionTransaction.set"></a>

#### PartitionTransaction.set

```python
def set(key: Any, prefix: bytes, value: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L205)

Set value for the key.

**Arguments**:

- `key`: key
- `prefix`: a key prefix
- `value`: value

<a id="quixstreams.state.types.PartitionTransaction.delete"></a>

#### PartitionTransaction.delete

```python
def delete(key: Any, prefix: bytes)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L214)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key
- `prefix`: a key prefix

<a id="quixstreams.state.types.PartitionTransaction.exists"></a>

#### PartitionTransaction.exists

```python
def exists(key: Any, prefix: bytes) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L224)

Check if the key exists in state.

**Arguments**:

- `key`: key
- `prefix`: a key prefix

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.types.PartitionTransaction.failed"></a>

#### PartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L234)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L244)

Return `True` if transaction is successfully completed.

Completed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.PartitionTransaction.prepared"></a>

#### PartitionTransaction.prepared

```python
@property
def prepared() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L254)

Return `True` if transaction is prepared completed.

Prepared transactions cannot receive new updates, but can be flushed.

**Returns**:

bool

<a id="quixstreams.state.types.PartitionTransaction.prepare"></a>

#### PartitionTransaction.prepare

```python
def prepare(processed_offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L263)

Produce changelog messages to the changelog topic for all changes accumulated

in this transaction and prepare transcation to flush its state to the state
store.

After successful `prepare()`, the transaction status is changed to PREPARED,
and it cannot receive updates anymore.

If changelog is disabled for this application, no updates will be produced
to the changelog topic.

**Arguments**:

- `processed_offset`: the offset of the latest processed message

<a id="quixstreams.state.types.PartitionTransaction.changelog_topic_partition"></a>

#### PartitionTransaction.changelog\_topic\_partition

```python
@property
def changelog_topic_partition() -> Optional[Tuple[str, int]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L279)

Return the changelog topic-partition for the StorePartition of this transaction.

Returns `None` if changelog_producer is not provided.

**Returns**:

(topic, partition) or None

<a id="quixstreams.state.types.PartitionTransaction.flush"></a>

#### PartitionTransaction.flush

```python
def flush(processed_offset: Optional[int] = None,
          changelog_offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L288)

Flush the recent updates to the storage.

**Arguments**:

- `processed_offset`: offset of the last processed message, optional.
- `changelog_offset`: offset of the last produced changelog message,
optional.

<a id="quixstreams.state.types.WindowedState"></a>

### WindowedState

```python
class WindowedState(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L306)

A windowed state to be provided into `StreamingDataFrame` window functions.

<a id="quixstreams.state.types.WindowedState.get_window"></a>

#### WindowedState.get\_window

```python
def get_window(start_ms: int,
               end_ms: int,
               default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L311)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L325)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L339)

Get the latest observed timestamp for the current state partition.

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest observed event timestamp in milliseconds

<a id="quixstreams.state.types.WindowedState.expire_windows"></a>

#### WindowedState.expire\_windows

```python
def expire_windows(duration_ms: int,
                   grace_ms: int = 0) -> List[Tuple[Tuple[int, int], Any]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L350)

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
class WindowedPartitionTransaction(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L367)

<a id="quixstreams.state.types.WindowedPartitionTransaction.failed"></a>

#### WindowedPartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L370)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L380)

Return `True` if transaction is successfully completed.

Completed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.types.WindowedPartitionTransaction.prepared"></a>

#### WindowedPartitionTransaction.prepared

```python
@property
def prepared() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L390)

Return `True` if transaction is prepared completed.

Prepared transactions cannot receive new updates, but can be flushed.

**Returns**:

bool

<a id="quixstreams.state.types.WindowedPartitionTransaction.prepare"></a>

#### WindowedPartitionTransaction.prepare

```python
def prepare(processed_offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L399)

Produce changelog messages to the changelog topic for all changes accumulated

in this transaction and prepare transcation to flush its state to the state
store.

After successful `prepare()`, the transaction status is changed to PREPARED,
and it cannot receive updates anymore.

If changelog is disabled for this application, no updates will be produced
to the changelog topic.

**Arguments**:

- `processed_offset`: the offset of the latest processed message

<a id="quixstreams.state.types.WindowedPartitionTransaction.get_window"></a>

#### WindowedPartitionTransaction.get\_window

```python
def get_window(start_ms: int,
               end_ms: int,
               prefix: bytes,
               default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L416)

Get the value of the window defined by `start` and `end` timestamps

if the window is present in the state, else default

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `prefix`: a key prefix
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.WindowedPartitionTransaction.update_window"></a>

#### WindowedPartitionTransaction.update\_window

```python
def update_window(start_ms: int, end_ms: int, value: Any, timestamp_ms: int,
                  prefix: bytes)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L435)

Set a value for the window.

This method will also update the latest observed timestamp in state partition
using the provided `timestamp`.

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `value`: value of the window
- `timestamp_ms`: current message timestamp in milliseconds
- `prefix`: a key prefix

<a id="quixstreams.state.types.WindowedPartitionTransaction.get_latest_timestamp"></a>

#### WindowedPartitionTransaction.get\_latest\_timestamp

```python
def get_latest_timestamp() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L452)

Get the latest observed timestamp for the current state partition.

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest observed event timestamp in milliseconds

<a id="quixstreams.state.types.WindowedPartitionTransaction.expire_windows"></a>

#### WindowedPartitionTransaction.expire\_windows

```python
def expire_windows(duration_ms: int, prefix: bytes, grace_ms: int = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L463)

Get a list of expired windows from RocksDB considering the current

latest timestamp, window duration and grace period.

It also marks the latest found window as expired in the expiration index, so
calling this method multiple times will yield different results for the same
"latest timestamp".

**Arguments**:

- `duration_ms`: duration of the windows in milliseconds
- `prefix`: a key prefix
- `grace_ms`: grace period in milliseconds. Default - "0"

<a id="quixstreams.state.types.WindowedPartitionTransaction.flush"></a>

#### WindowedPartitionTransaction.flush

```python
def flush(processed_offset: Optional[int] = None,
          changelog_offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L478)

Flush the recent updates to the storage.

**Arguments**:

- `processed_offset`: offset of the last processed message, optional.
- `changelog_offset`: offset of the last produced changelog message,
optional.

<a id="quixstreams.state.types.WindowedPartitionTransaction.changelog_topic_partition"></a>

#### WindowedPartitionTransaction.changelog\_topic\_partition

```python
@property
def changelog_topic_partition() -> Optional[Tuple[str, int]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L492)

Return the changelog topic-partition for the StorePartition of this transaction.

Returns `None` if changelog_producer is not provided.

**Returns**:

(topic, partition) or None

<a id="quixstreams.state.types.PartitionRecoveryTransaction"></a>

### PartitionRecoveryTransaction

```python
class PartitionRecoveryTransaction(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L506)

A class for managing recovery for a StorePartition from a changelog message

<a id="quixstreams.state.types.PartitionRecoveryTransaction.flush"></a>

#### PartitionRecoveryTransaction.flush

```python
def flush()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L513)

Flush the recovery update to the storage.

<a id="quixstreams.state.types.PartitionTransactionStatus"></a>

### PartitionTransactionStatus

```python
class PartitionTransactionStatus(enum.Enum)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L520)

<a id="quixstreams.state.types.PartitionTransactionStatus.STARTED"></a>

#### STARTED

Transaction is started and accepts updates

<a id="quixstreams.state.types.PartitionTransactionStatus.PREPARED"></a>

#### PREPARED

Transaction is prepared, it can no longer receive updates

<a id="quixstreams.state.types.PartitionTransactionStatus.COMPLETE"></a>

#### COMPLETE

Transaction is fully completed, it cannot be used anymore

<a id="quixstreams.state.types.PartitionTransactionStatus.FAILED"></a>

#### FAILED

Transaction is failed, it cannot be used anymore

<a id="quixstreams.state.exceptions"></a>

## quixstreams.state.exceptions

<a id="quixstreams.state.manager"></a>

## quixstreams.state.manager

<a id="quixstreams.state.manager.StateStoreManager"></a>

### StateStoreManager

```python
class StateStoreManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L24)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L62)

Map of registered state stores

**Returns**:

dict in format {topic: {store_name: store}}

<a id="quixstreams.state.manager.StateStoreManager.recovery_required"></a>

#### StateStoreManager.recovery\_required

```python
@property
def recovery_required() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L70)

Whether recovery needs to be done.

<a id="quixstreams.state.manager.StateStoreManager.using_changelogs"></a>

#### StateStoreManager.using\_changelogs

```python
@property
def using_changelogs() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L79)

Whether the StateStoreManager is using changelog topics

**Returns**:

using changelogs, as bool

<a id="quixstreams.state.manager.StateStoreManager.do_recovery"></a>

#### StateStoreManager.do\_recovery

```python
def do_recovery()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L87)

Perform a state recovery, if necessary.

<a id="quixstreams.state.manager.StateStoreManager.stop_recovery"></a>

#### StateStoreManager.stop\_recovery

```python
def stop_recovery()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L93)

Stop recovery (called during app shutdown).

<a id="quixstreams.state.manager.StateStoreManager.get_store"></a>

#### StateStoreManager.get\_store

```python
def get_store(topic: str, store_name: str = DEFAULT_STATE_STORE_NAME) -> Store
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L99)

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
                   store_name: str = DEFAULT_STATE_STORE_NAME)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L132)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L157)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L180)

Delete all state stores managed by StateStoreManager.

<a id="quixstreams.state.manager.StateStoreManager.on_partition_assign"></a>

#### StateStoreManager.on\_partition\_assign

```python
def on_partition_assign(topic: str, partition: int,
                        committed_offset: int) -> List[StorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L195)

Assign store partitions for each registered store for the given `TopicPartition`

and return a list of assigned `StorePartition` objects.

**Arguments**:

- `topic`: Kafka topic name
- `partition`: Kafka topic partition
- `committed_offset`: latest committed offset for the partition

**Returns**:

list of assigned `StorePartition`

<a id="quixstreams.state.manager.StateStoreManager.on_partition_revoke"></a>

#### StateStoreManager.on\_partition\_revoke

```python
def on_partition_revoke(topic: str, partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L221)

Revoke store partitions for each registered store for the given `TopicPartition`

**Arguments**:

- `topic`: Kafka topic name
- `partition`: Kafka topic partition

<a id="quixstreams.state.manager.StateStoreManager.init"></a>

#### StateStoreManager.init

```python
def init()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L234)

Initialize `StateStoreManager` and create a store directory


<a id="quixstreams.state.manager.StateStoreManager.close"></a>

#### StateStoreManager.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L241)

Close all registered stores

<a id="quixstreams.state.state"></a>

## quixstreams.state.state

<a id="quixstreams.state.state.TransactionState"></a>

### TransactionState

```python
class TransactionState(State)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/state.py#L6)

<a id="quixstreams.state.state.TransactionState.__init__"></a>

#### TransactionState.\_\_init\_\_

```python
def __init__(prefix: bytes, transaction: PartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/state.py#L12)

Simple key-value state to be provided into `StreamingDataFrame` functions

**Arguments**:

- `transaction`: instance of `PartitionTransaction`

<a id="quixstreams.state.state.TransactionState.get"></a>

#### TransactionState.get

```python
def get(key: Any, default: Any = None) -> Optional[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/state.py#L21)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/state.py#L31)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.state.TransactionState.delete"></a>

#### TransactionState.delete

```python
def delete(key: Any)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/state.py#L39)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.state.TransactionState.exists"></a>

#### TransactionState.exists

```python
def exists(key: Any) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/state.py#L48)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/exceptions/assignment.py#L6)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/context.py#L20)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/context.py#L51)

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

<a id="quixstreams.kafka.configuration"></a>

## quixstreams.kafka.configuration

<a id="quixstreams.kafka.configuration.ConnectionConfig"></a>

### ConnectionConfig

```python
class ConnectionConfig(BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L17)

Provides an interface for all librdkafka connection-based configs.

Allows converting to or from a librdkafka dictionary.

Also obscures secrets and handles any case sensitivity issues.

<a id="quixstreams.kafka.configuration.ConnectionConfig.settings_customise_sources"></a>

#### ConnectionConfig.settings\_customise\_sources

```python
@classmethod
def settings_customise_sources(
    cls, settings_cls: Type[BaseSettings],
    init_settings: PydanticBaseSettingsSource,
    env_settings: PydanticBaseSettingsSource,
    dotenv_settings: PydanticBaseSettingsSource,
    file_secret_settings: PydanticBaseSettingsSource
) -> Tuple[PydanticBaseSettingsSource, ...]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L96)

Included to ignore reading/setting values from the environment

<a id="quixstreams.kafka.configuration.ConnectionConfig.from_librdkafka_dict"></a>

#### ConnectionConfig.from\_librdkafka\_dict

```python
@classmethod
def from_librdkafka_dict(cls,
                         config: dict,
                         ignore_extras: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L110)

Create a `ConnectionConfig` from a librdkafka config dictionary.

**Arguments**:

- `config`: a dict of configs (like {"bootstrap.servers": "url"})
- `ignore_extras`: Ignore non-connection settings (else raise exception)

**Returns**:

a ConnectionConfig

<a id="quixstreams.kafka.configuration.ConnectionConfig.as_librdkafka_dict"></a>

#### ConnectionConfig.as\_librdkafka\_dict

```python
def as_librdkafka_dict(plaintext_secrets=True) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L125)

Dump any non-empty config values as a librdkafka dictionary.

>***NOTE***: All secret values will be dumped in PLAINTEXT by default.

**Arguments**:

- `plaintext_secrets`: whether secret values are plaintext or obscured (***)

**Returns**:

a librdkafka-compatible dictionary

<a id="quixstreams.kafka"></a>

## quixstreams.kafka

<a id="quixstreams.kafka.producer"></a>

## quixstreams.kafka.producer

<a id="quixstreams.kafka.producer.Producer"></a>

### Producer

```python
class Producer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L39)

<a id="quixstreams.kafka.producer.Producer.__init__"></a>

#### Producer.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             logger: logging.Logger = logger,
             error_callback: Callable[[KafkaError], None] = _default_error_cb,
             extra_config: Optional[dict] = None,
             flush_timeout: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L40)

A wrapper around `confluent_kafka.Producer`.

It initializes `confluent_kafka.Producer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.

**Arguments**:

- `broker_address`: Connection settings for Kafka.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
- `logger`: a Logger instance to attach librdkafka logging to
- `error_callback`: callback used for producer errors
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
Note: values passed as arguments override values in `extra_config`.
- `flush_timeout`: The time the producer is waiting for all messages to be delivered.

<a id="quixstreams.kafka.producer.Producer.produce"></a>

#### Producer.produce

```python
def produce(topic: str,
            value: Optional[Union[str, bytes]] = None,
            key: Optional[Union[str, bytes]] = None,
            headers: Optional[Headers] = None,
            partition: Optional[int] = None,
            timestamp: Optional[int] = None,
            poll_timeout: float = 5.0,
            buffer_error_max_tries: int = 3,
            on_delivery: Optional[DeliveryCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L78)

Produce a message to a topic.

It also polls Kafka for callbacks before producing to minimize
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
- `on_delivery`: the delivery callback to be triggered on `poll()`
for the produced message.

<a id="quixstreams.kafka.producer.Producer.poll"></a>

#### Producer.poll

```python
def poll(timeout: float = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L139)

Polls the producer for events and calls `on_delivery` callbacks.

**Arguments**:

- `timeout`: poll timeout seconds; Default: 0 (unlike others)
> NOTE: -1 will hang indefinitely if there are no messages to acknowledge

<a id="quixstreams.kafka.producer.Producer.flush"></a>

#### Producer.flush

```python
def flush(timeout: Optional[float] = None) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L147)

Wait for all messages in the Producer queue to be delivered.

**Arguments**:

- `timeout` (`float`): time to attempt flushing (seconds).
None use producer default or -1 is infinite. Default: None

**Returns**:

number of messages remaining to flush

<a id="quixstreams.kafka.producer.TransactionalProducer"></a>

### TransactionalProducer

```python
class TransactionalProducer(Producer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L178)

A separate producer class used only internally for transactions
(transactions are only needed when using a consumer).

<a id="quixstreams.kafka.consumer"></a>

## quixstreams.kafka.consumer

<a id="quixstreams.kafka.consumer.Consumer"></a>

### Consumer

```python
class Consumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L64)

<a id="quixstreams.kafka.consumer.Consumer.__init__"></a>

#### Consumer.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             consumer_group: Optional[str],
             auto_offset_reset: AutoOffsetReset,
             auto_commit_enable: bool = True,
             logger: logging.Logger = logger,
             error_callback: Callable[[KafkaError], None] = _default_error_cb,
             on_commit: Optional[Callable[
                 [Optional[KafkaError], List[TopicPartition]], None]] = None,
             extra_config: Optional[dict] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L65)

A wrapper around `confluent_kafka.Consumer`.

It initializes `confluent_kafka.Consumer` on demand
avoiding network calls during `__init__`, provides typing info for methods
and some reasonable defaults.

**Arguments**:

- `broker_address`: Connection settings for Kafka.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Available values:
<br>"earliest" - automatically reset the offset to the smallest offset
<br>"latest" - automatically reset the offset to the largest offset
<br>"error" - trigger an error (`ERR__AUTO_OFFSET_RESET`) which is
    retrieved by consuming messages (used for testing)
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
- `logger`: a Logger instance to attach librdkafka logging to
- `error_callback`: callback used for consumer errors
- `on_commit`: Offset commit result propagation callback.
Passed as "offset_commit_cb" to `confluent_kafka.Consumer`.
- `extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
Note: values passed as arguments override values in `extra_config`.

<a id="quixstreams.kafka.consumer.Consumer.poll"></a>

#### Consumer.poll

```python
def poll(timeout: Optional[float] = None) -> Optional[Message]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L128)

Consumes a single message, calls callbacks and returns events.

The application must check the returned :py:class:`Message`
object's :py:func:`Message.error()` method to distinguish between proper
messages (error() returns None), or an event or error.

Note: a `RebalancingCallback` may be called from this method (
`on_assign`, `on_revoke`, or `on_lost`).

**Arguments**:

- `timeout` (`float`): Maximum time in seconds to block waiting for message,
event or callback. None or -1 is infinite. Default: None.

**Raises**:

- `RuntimeError`: if called on a closed consumer

**Returns**:

`Optional[Message]`: A `Message` object or `None` on timeout

<a id="quixstreams.kafka.consumer.Consumer.subscribe"></a>

#### Consumer.subscribe

```python
def subscribe(topics: List[str],
              on_assign: Optional[RebalancingCallback] = None,
              on_revoke: Optional[RebalancingCallback] = None,
              on_lost: Optional[RebalancingCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L147)

Set subscription to supplied list of topics

This replaces a previous subscription.

**Arguments**:

- `topics` (`List[str]`): List of topics (strings) to subscribe to.
- `on_assign` (`Optional[RebalancingCallback]`): callback to provide handling of
customized offsets on completion of a successful partition re-assignment.
- `on_revoke` (`Optional[RebalancingCallback]`): callback to provide handling of
offset commits to a customized store on the start of a rebalance operation.
- `on_lost` (`Optional[RebalancingCallback]`): callback to provide handling in
the case the partition assignment has been lost. Partitions that have been
lost may already be owned by other members in the group and therefore
committing offsets, for example, may fail.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.unsubscribe"></a>

#### Consumer.unsubscribe

```python
def unsubscribe()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L231)

Remove current subscription.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.store_offsets"></a>

#### Consumer.store\_offsets

```python
def store_offsets(message: Optional[Message] = None,
                  offsets: Optional[List[TopicPartition]] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L240)

Store offsets for a message or a list of offsets.

`message` and `offsets` are mutually exclusive. The stored offsets
will be committed according to 'auto.commit.interval.ms' or manual
offset-less `commit`.
Note that 'enable.auto.offset.store' must be set to False when using this API.

**Arguments**:

- `message` (`confluent_kafka.Message`): Store message's offset+1.
- `offsets` (`List[TopicPartition]`): List of topic+partitions+offsets to store.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.commit"></a>

#### Consumer.commit

```python
def commit(message: Optional[Message] = None,
           offsets: Optional[List[TopicPartition]] = None,
           asynchronous: bool = True) -> Optional[List[TopicPartition]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L271)

Commit a message or a list of offsets.

The `message` and `offsets` parameters are mutually exclusive.
If neither is set, the current partition assignment's offsets are used instead.
Use this method to commit offsets if you have 'enable.auto.commit' set to False.

**Arguments**:

- `message` (`Message`): Commit the message's offset+1.
Note: By convention, committed offsets reflect the next message
to be consumed, **not** the last message consumed.
- `offsets` (`List[TopicPartition]`): List of topic+partitions+offsets to commit.
- `asynchronous` (`bool`): If true, asynchronously commit, returning None
immediately. If False, the commit() call will block until the commit
succeeds or fails and the committed offsets will be returned (on success).
Note that specific partitions may have failed and the .err field of
each partition should be checked for success.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.Consumer.committed"></a>

#### Consumer.committed

```python
def committed(partitions: List[TopicPartition],
              timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L311)

Retrieve committed offsets for the specified partitions.

**Arguments**:

- `partitions` (`List[TopicPartition]`): List of topic+partitions to query for stored offsets.
- `timeout` (`float`): Request timeout (seconds).
None or -1 is infinite. Default: None

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

**Returns**:

`List[TopicPartition]`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.get_watermark_offsets"></a>

#### Consumer.get\_watermark\_offsets

```python
def get_watermark_offsets(partition: TopicPartition,
                          timeout: Optional[float] = None,
                          cached: bool = False) -> Tuple[int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L329)

Retrieve low and high offsets for the specified partition.

**Arguments**:

- `partition` (`TopicPartition`): Topic+partition to return offsets for.
- `timeout` (`float`): Request timeout (seconds). None or -1 is infinite.
Ignored if cached=True. Default: None
- `cached` (`bool`): Instead of querying the broker, use cached information.
Cached values: The low offset is updated periodically
(if statistics.interval.ms is set) while the high offset is updated on each
message fetched from the broker for this partition.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

**Returns**:

`Tuple[int, int]`: Tuple of (low,high) on success or None on timeout.
The high offset is the offset of the last message + 1.

<a id="quixstreams.kafka.consumer.Consumer.list_topics"></a>

#### Consumer.list\_topics

```python
def list_topics(topic: Optional[str] = None,
                timeout: Optional[float] = None) -> ClusterMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L355)

Request metadata from the cluster.

This method provides the same information as
listTopics(), describeTopics() and describeCluster() in  the Java Admin client.

**Arguments**:

- `topic` (`str`): If specified, only request information about this topic,
else return results for all topics in cluster.
Warning: If auto.create.topics.enable is set to true on the broker and
an unknown topic is specified, it will be created.
- `timeout` (`float`): The maximum response time before timing out
None or -1 is infinite. Default: None

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.memberid"></a>

#### Consumer.memberid

```python
def memberid() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L376)

Return this client's broker-assigned group member id.

The member id is assigned by the group coordinator and is propagated to
the consumer during rebalance.

**Raises**:

- `RuntimeError`: if called on a closed consumer

**Returns**:

`Optional[string]`: Member id string or None

<a id="quixstreams.kafka.consumer.Consumer.offsets_for_times"></a>

#### Consumer.offsets\_for\_times

```python
def offsets_for_times(partitions: List[TopicPartition],
                      timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L389)

Look up offsets by timestamp for the specified partitions.

The returned offset for each partition is the earliest offset whose
timestamp is greater than or equal to the given timestamp in the
corresponding partition. If the provided timestamp exceeds that of the
last message in the partition, a value of -1 will be returned.

**Arguments**:

- `partitions` (`List[TopicPartition]`): topic+partitions with timestamps
in the TopicPartition.offset field.
- `timeout` (`float`): The maximum response time before timing out.
None or -1 is infinite. Default: None

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

**Returns**:

`List[TopicPartition]`: List of topic+partition with offset field set and possibly error set

<a id="quixstreams.kafka.consumer.Consumer.pause"></a>

#### Consumer.pause

```python
def pause(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L415)

Pause consumption for the provided list of partitions.

Paused partitions must be tracked manually.

Does NOT affect the result of `Consumer.assignment()`.

**Arguments**:

- `partitions` (`List[TopicPartition]`): List of topic+partitions to pause.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.resume"></a>

#### Consumer.resume

```python
def resume(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L428)

Resume consumption for the provided list of partitions.

**Arguments**:

- `partitions` (`List[TopicPartition]`): List of topic+partitions to resume.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.position"></a>

#### Consumer.position

```python
def position(partitions: List[TopicPartition]) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L438)

Retrieve current positions (offsets) for the specified partitions.

**Arguments**:

- `partitions` (`List[TopicPartition]`): List of topic+partitions to return
current offsets for. The current offset is the offset of
the last consumed message + 1.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

**Returns**:

`List[TopicPartition]`: List of topic+partitions with offset and possibly error set.

<a id="quixstreams.kafka.consumer.Consumer.seek"></a>

#### Consumer.seek

```python
def seek(partition: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L452)

Set consume position for partition to offset.

The offset may be an absolute (>=0) or a
logical offset like `OFFSET_BEGINNING`.

`seek()` may only be used to update the consume offset of an
actively consumed partition (i.e., after `Consumer.assign()`),
to set the starting offset of partition not being consumed instead
pass the offset in an `assign()` call.

**Arguments**:

- `partition` (`TopicPartition`): Topic+partition+offset to seek to.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.Consumer.assignment"></a>

#### Consumer.assignment

```python
def assignment() -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L469)

Returns the current partition assignment.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

**Returns**:

`List[TopicPartition]`: List of assigned topic+partitions.

<a id="quixstreams.kafka.consumer.Consumer.set_sasl_credentials"></a>

#### Consumer.set\_sasl\_credentials

```python
def set_sasl_credentials(username: str, password: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L482)

Sets the SASL credentials used for this client.

These credentials will overwrite the old ones, and will be used the next
time the client needs to authenticate.
This method will not disconnect existing broker connections that have been
established with the old credentials.
This method is applicable only to SASL PLAIN and SCRAM mechanisms.

**Arguments**:

- `username` (`str`): your username
- `password` (`str`): your password

<a id="quixstreams.kafka.consumer.Consumer.incremental_assign"></a>

#### Consumer.incremental\_assign

```python
def incremental_assign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L496)

Assign new partitions.

Can be called outside the `Consumer` `on_assign` callback (multiple times).
Partitions immediately show on `Consumer.assignment()`.

Any additional partitions besides the ones passed during the `Consumer`
`on_assign` callback will NOT be associated with the consumer group.

**Arguments**:

- `partitions` (`List[TopicPartition]`): a list of topic partitions

<a id="quixstreams.kafka.consumer.Consumer.incremental_unassign"></a>

#### Consumer.incremental\_unassign

```python
def incremental_unassign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L510)

Revoke partitions.

Can be called outside an on_revoke callback.

**Arguments**:

- `partitions` (`List[TopicPartition]`): a list of topic partitions

<a id="quixstreams.kafka.consumer.Consumer.close"></a>

#### Consumer.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L520)

Close down and terminate the Kafka Consumer.

Actions performed:

- Stops consuming.
- Commits offsets, unless the consumer property 'enable.auto.commit' is set to False.
- Leaves the consumer group.

Registered callbacks may be called from this method,
see `poll()` for more info.

<a id="quixstreams.kafka.consumer.Consumer.consumer_group_metadata"></a>

#### Consumer.consumer\_group\_metadata

```python
def consumer_group_metadata() -> GroupMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L537)

Used by the producer during consumer offset sending for an EOS transaction.

<a id="quixstreams.kafka.exceptions"></a>

## quixstreams.kafka.exceptions

<a id="quixstreams.app"></a>

## quixstreams.app

<a id="quixstreams.app.Application"></a>

### Application

```python
class Application()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L57)

The main Application class.

Typically, the primary object needed to get a kafka application up and running.

Most functionality is explained the various methods, except for
"column assignment".


What it Does:

- On init:
    - Provides defaults or helper methods for commonly needed objects
    - If `quix_sdk_token` is passed, configures the app to use the Quix Cloud.
- When executed via `.run()` (after setup):
    - Initializes Topics and StreamingDataFrames
    - Facilitates processing of Kafka messages with a `StreamingDataFrame`
    - Handles all Kafka client consumer/producer responsibilities.


Example Snippet:

```python
from quixstreams import Application

# Set up an `app = Application` and `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value))

app.run(dataframe=df)
```

<a id="quixstreams.app.Application.__init__"></a>

#### Application.\_\_init\_\_

```python
def __init__(broker_address: Optional[Union[str, ConnectionConfig]] = None,
             quix_sdk_token: Optional[str] = None,
             consumer_group: Optional[str] = None,
             auto_offset_reset: AutoOffsetReset = "latest",
             commit_interval: float = 5.0,
             commit_every: int = 0,
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
             auto_create_topics: bool = True,
             use_changelog_topics: bool = True,
             quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
             topic_manager: Optional[TopicManager] = None,
             request_timeout: float = 30,
             topic_create_timeout: float = 60,
             processing_guarantee: ProcessingGuarantee = "at-least-once")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L95)

**Arguments**:

- `broker_address`: Connection settings for Kafka.
Used by Producer, Consumer, and Admin clients.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
Either this OR `quix_sdk_token` must be set to use `Application` (not both).
Takes priority over quix auto-configuration.
Linked Environment Variable: `Quix__Broker__Address`.
Default: `None`
- `quix_sdk_token`: If using the Quix Cloud, the SDK token to connect with.
Either this OR `broker_address` must be set to use Application (not both).
Linked Environment Variable: `Quix__Sdk__Token`.
Default: None (if not run on Quix Cloud)
  >***NOTE:*** the environment variable is set for you in the Quix Cloud
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`.
Linked Environment Variable: `Quix__Consumer__Group`.
Default - "quixstreams-default" (set during init)
  >***NOTE:*** Quix Applications will prefix it with the Quix workspace id.
- `commit_interval`: How often to commit the processed messages in seconds.
Default - 5.0.
- `commit_every`: Commit the checkpoint after processing N messages.
Use this parameter for more granular control of the commit schedule.
If the value is > 0, the application will commit the checkpoint after
processing the specified number of messages across all the assigned
partitions.
If the value is <= 0, only the `commit_interval` will be considered.
Default - 0.
    >***NOTE:*** Only input offsets are counted, and the application
    > may produce more results than the number of incoming messages.
- `auto_offset_reset`: Consumer `auto.offset.reset` setting
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
- `producer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
- `state_dir`: path to the application state directory.
Default - `".state"`.
- `rocksdb_options`: RocksDB options.
If `None`, the default options will be used.
- `consumer_poll_timeout`: timeout for `RowConsumer.poll()`. Default - `1.0`s
- `producer_poll_timeout`: timeout for `RowProducer.poll()`. Default - `0`s.
- `on_message_processed`: a callback triggered when message is successfully
processed.
- `loglevel`: a log level for "quixstreams" logger.
Should be a string or None.
If `None` is passed, no logging will be configured.
You may pass `None` and configure "quixstreams" logger
externally using `logging` library.
Default - `"INFO"`.
- `auto_create_topics`: Create all `Topic`s made via Application.topic()
Default - `True`
- `use_changelog_topics`: Use changelog topics to back stateful operations
Default - `True`
- `topic_manager`: A `TopicManager` instance
- `request_timeout`: timeout (seconds) for REST-based requests
- `topic_create_timeout`: timeout (seconds) for topic create finalization
- `processing_guarantee`: Use "exactly-once" or "at-least-once" processing.
<br><br>***Error Handlers***<br>
To handle errors, `Application` accepts callbacks triggered when
    exceptions occur on different stages of stream processing. If the callback
    returns `True`, the exception will be ignored. Otherwise, the exception
    will be propagated and the processing will eventually stop.
- `on_consumer_error`: triggered when internal `RowConsumer` fails
to poll Kafka or cannot deserialize a message.
- `on_processing_error`: triggered when exception is raised within
`StreamingDataFrame.process()`.
- `on_producer_error`: triggered when `RowProducer` fails to serialize
or to produce a message to Kafka.
<br><br>***Quix Cloud Parameters***<br>
- `quix_config_builder`: instance of `QuixKafkaConfigsBuilder` to be used
instead of the default one.
> NOTE: It is recommended to just use `quix_sdk_token` instead.

<a id="quixstreams.app.Application.Quix"></a>

#### Application.Quix

```python
@classmethod
def Quix(
    cls,
    consumer_group: Optional[str] = None,
    auto_offset_reset: AutoOffsetReset = "latest",
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
    auto_create_topics: bool = True,
    use_changelog_topics: bool = True,
    topic_manager: Optional[QuixTopicManager] = None,
    request_timeout: float = 30,
    topic_create_timeout: float = 60,
    processing_guarantee: Literal["at-least-once",
                                  "exactly-once"] = "exactly-once"
) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L347)

>***NOTE:*** DEPRECATED: use Application with `quix_sdk_token` argument instead.

Initialize an Application to work with Quix Cloud,
assuming environment is properly configured (by default in Quix Cloud).

It takes the credentials from the environment and configures consumer and
producer to properly connect to the Quix Cloud.

>***NOTE:*** Quix Cloud requires `consumer_group` and topic names to be
    prefixed with workspace id.
    If the application is created via `Application.Quix()`, the real consumer
    group will be `<workspace_id>-<consumer_group>`,
    and the real topic names will be `<workspace_id>-<topic_name>`.



Example Snippet:

```python
from quixstreams import Application

# Set up an `app = Application.Quix` and `sdf = StreamingDataFrame`;
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
Linked Environment Variable: `Quix__Consumer__Group`.
Default - "quixstreams-default" (set during init).
  >***NOTE:*** Quix Applications will prefix it with the Quix workspace id.
- `auto_offset_reset`: Consumer `auto.offset.reset` setting
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
- `producer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Producer` as is.
- `state_dir`: path to the application state directory.
Default - `".state"`.
- `rocksdb_options`: RocksDB options.
If `None`, the default options will be used.
- `consumer_poll_timeout`: timeout for `RowConsumer.poll()`. Default - `1.0`s
- `producer_poll_timeout`: timeout for `RowProducer.poll()`. Default - `0`s.
- `on_message_processed`: a callback triggered when message is successfully
processed.
- `loglevel`: a log level for "quixstreams" logger.
Should be a string or `None`.
If `None` is passed, no logging will be configured.
You may pass `None` and configure "quixstreams" logger
externally using `logging` library.
Default - `"INFO"`.
- `auto_create_topics`: Create all `Topic`s made via `Application.topic()`
Default - `True`
- `use_changelog_topics`: Use changelog topics to back stateful operations
Default - `True`
- `topic_manager`: A `QuixTopicManager` instance
- `request_timeout`: timeout (seconds) for REST-based requests
- `topic_create_timeout`: timeout (seconds) for topic create finalization
- `processing_guarantee`: Use "exactly-once" or "at-least-once" processing.
<br><br>***Error Handlers***<br>
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
<br><br>***Quix Cloud Parameters***<br>
- `quix_config_builder`: instance of `QuixKafkaConfigsBuilder` to be used
instead of the default one.

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
          config: Optional[TopicConfig] = None,
          timestamp_extractor: Optional[TimestampExtractor] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L488)

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
- `config`: optional topic configurations (for creation/validation)
>***NOTE:*** will not create without Application's auto_create_topics set
to True (is True by default)
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message. Default - `None`.

Example Snippet:

```python
app = Application(...)


def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    return value["timestamp"]

topic = app.topic("input-topic", timestamp_extractor=custom_ts_extractor)
```

**Returns**:

`Topic` object

<a id="quixstreams.app.Application.dataframe"></a>

#### Application.dataframe

```python
def dataframe(topic: Topic) -> StreamingDataFrame
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L568)

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
def stop(fail: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L607)

Stop the internal poll loop and the message processing.

Only necessary when manually managing the lifecycle of the `Application` (
likely through some sort of threading).

To otherwise stop an application, either send a `SIGTERM` to the process
(like Kubernetes does) or perform a typical `KeyboardInterrupt` (`Ctrl+C`).

**Arguments**:

- `fail`: if True, signals that application is stopped due
to unhandled exception, and it shouldn't commit the current checkpoint.

<a id="quixstreams.app.Application.get_producer"></a>

#### Application.get\_producer

```python
def get_producer() -> Producer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L630)

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
def get_consumer(auto_commit_enable: bool = True) -> Consumer
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L660)

Create and return a pre-configured Consumer instance.
The Consumer is initialized with params passed to Application.

It's useful for consuming data from Kafka outside the standard
Application processing flow.
(e.g., to consume test data from a topic).
Using it within the StreamingDataFrame functions is not recommended, as it
creates a new Consumer instance
each time, which is not optimized for repeated use in a streaming pipeline.

Note: By default, this consumer does not autocommit the consumed offsets to allow
at-least-once processing.
To store the offset call store_offsets() after processing a message.
If autocommit is necessary set `enable.auto.offset.store` to True in
the consumer config when creating the app.

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L707)

Clear the state of the application.

<a id="quixstreams.app.Application.run"></a>

#### Application.run

```python
def run(dataframe: StreamingDataFrame)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L713)

Start processing data from Kafka using provided `StreamingDataFrame`

Once started, it can be safely terminated with a `SIGTERM` signal
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
class RowConsumer(Consumer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L19)

<a id="quixstreams.rowconsumer.RowConsumer.__init__"></a>

#### RowConsumer.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             consumer_group: str,
             auto_offset_reset: AutoOffsetReset,
             auto_commit_enable: bool = True,
             on_commit: Callable[[Optional[KafkaError], List[TopicPartition]],
                                 None] = None,
             extra_config: Optional[dict] = None,
             on_error: Optional[ConsumerErrorCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L20)

A consumer class that is capable of deserializing Kafka messages to Rows

according to the Topics deserialization settings.

It overrides `.subscribe()` method of Consumer class to accept `Topic`
objects instead of strings.

**Arguments**:

- `broker_address`: Connection settings for Kafka.
Accepts string with Kafka broker host and port formatted as `<host>:<port>`,
or a ConnectionConfig object if authentication is required.
- `consumer_group`: Kafka consumer group.
Passed as `group.id` to `confluent_kafka.Consumer`
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Available values:
- "earliest" - automatically reset the offset to the smallest offset
- "latest" - automatically reset the offset to the largest offset
- `auto_commit_enable`: If true, periodically commit offset of
the last message handed to the application. Default - `True`.
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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L72)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L106)

Consumes a single message and deserialize it to Row or a list of Rows.

The message is deserialized according to the corresponding Topic.
If deserializer raises `IgnoreValue` exception, this method will return None.
If Kafka returns an error, it will be raised as exception.

**Arguments**:

- `timeout`: poll timeout seconds

**Returns**:

single Row, list of Rows or None

<a id="quixstreams.checkpointing.checkpoint"></a>

## quixstreams.checkpointing.checkpoint

<a id="quixstreams.checkpointing.checkpoint.Checkpoint"></a>

### Checkpoint

```python
class Checkpoint()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L27)

Class to keep track of state updates and consumer offsets and to checkpoint these
updates on schedule.

<a id="quixstreams.checkpointing.checkpoint.Checkpoint.expired"></a>

#### Checkpoint.expired

```python
def expired() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L67)

Returns `True` if checkpoint deadline has expired OR
if the total number of processed offsets exceeded the "commit_every" limit
when it's defined.

<a id="quixstreams.checkpointing.checkpoint.Checkpoint.empty"></a>

#### Checkpoint.empty

```python
def empty() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L77)

Returns `True` if checkpoint doesn't have any offsets stored yet.


<a id="quixstreams.checkpointing.checkpoint.Checkpoint.store_offset"></a>

#### Checkpoint.store\_offset

```python
def store_offset(topic: str, partition: int, offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L84)

Store the offset of the processed message to the checkpoint.

**Arguments**:

- `topic`: topic name
- `partition`: partition number
- `offset`: message offset

<a id="quixstreams.checkpointing.checkpoint.Checkpoint.get_store_transaction"></a>

#### Checkpoint.get\_store\_transaction

```python
def get_store_transaction(
        topic: str,
        partition: int,
        store_name: str = DEFAULT_STATE_STORE_NAME) -> PartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L110)

Get a PartitionTransaction for the given store, topic and partition.

It will return already started transaction if there's one.

**Arguments**:

- `topic`: topic name
- `partition`: partition number
- `store_name`: store name

**Returns**:

instance of `PartitionTransaction`

<a id="quixstreams.checkpointing.checkpoint.Checkpoint.close"></a>

#### Checkpoint.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L133)

Perform cleanup (when the checkpoint is empty) instead of committing.

Needed for exactly-once, as Kafka transactions are timeboxed.

<a id="quixstreams.checkpointing.checkpoint.Checkpoint.commit"></a>

#### Checkpoint.commit

```python
def commit()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L142)

Commit the checkpoint.

This method will:
 1. Produce the changelogs for each state store
 2. Flush the producer to ensure everything is delivered.
 3. Commit topic offsets.
 4. Flush each state store partition to the disk.

<a id="quixstreams.checkpointing"></a>

## quixstreams.checkpointing

<a id="quixstreams.checkpointing.exceptions"></a>

## quixstreams.checkpointing.exceptions

