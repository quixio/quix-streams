<a id="quixstreams"></a>

## quixstreams

<a id="quixstreams.logging"></a>

## quixstreams.logging

<a id="quixstreams.logging.configure_logging"></a>

#### configure\_logging

```python
def configure_logging(loglevel: Optional[Union[int, LogLevel]],
                      name: str = LOGGER_NAME,
                      pid: bool = False) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/logging.py#L24)

Configure "quixstreams" logger.

>***NOTE:*** If "quixstreams" logger already has pre-defined handlers
(e.g. logging has already been configured via `logging`, or the function
is called twice), it will skip configuration and return `False`.

**Arguments**:

- `loglevel`: a valid log level as a string or None.
If None passed, this function is no-op and no logging will be configured.
- `name`: the log name included in the output
- `pid`: if True include the process PID in the logs

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L46)

Remove the workspace ID from a given string if it starts with it.

Only used for consumer groups.

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L59)

Add the workspace ID as a prefix to a given string if it does not have it.

Only used for consumer groups.

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L73)

A convenience container class for Quix Application configs.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder"></a>

### QuixKafkaConfigsBuilder

```python
class QuixKafkaConfigsBuilder()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L83)

Retrieves all the necessary information from the Quix API and builds all the
objects required to connect a confluent-kafka client to the Quix Platform.

If not executed within the Quix platform directly, you must provide a Quix
"streaming" (aka "sdk") token, or Personal Access Token.

Ideally you also know your workspace name or id. If not, you can search for it
using a known topic name, but note the search space is limited to the access level
of your token.

It also currently handles the app_auto_create_topics setting for Quix Applications.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.__init__"></a>

#### QuixKafkaConfigsBuilder.\_\_init\_\_

```python
def __init__(quix_sdk_token: Optional[str] = None,
             workspace_id: Optional[str] = None,
             quix_portal_api_service: Optional[QuixPortalApiService] = None,
             timeout: float = 30,
             topic_create_timeout: float = 60)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L99)

**Arguments**:

- `quix_portal_api_service`: A QuixPortalApiService instance (else generated)
- `workspace_id`: A valid Quix Workspace ID (else searched for)

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.convert_topic_response"></a>

#### QuixKafkaConfigsBuilder.convert\_topic\_response

```python
@classmethod
def convert_topic_response(cls, api_response: dict) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L178)

Converts a GET or POST ("create") topic API response to a Topic object

**Arguments**:

- `api_response`: the dict response from a get or create topic call

**Returns**:

a corresponding Topic object

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.strip_workspace_id_prefix"></a>

#### QuixKafkaConfigsBuilder.strip\_workspace\_id\_prefix

```python
def strip_workspace_id_prefix(s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L209)

Remove the workspace ID from a given string if it starts with it.

Only used for consumer groups.

**Arguments**:

- `s`: the string to append to

**Returns**:

the string with workspace_id prefix removed

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.prepend_workspace_id"></a>

#### QuixKafkaConfigsBuilder.prepend\_workspace\_id

```python
def prepend_workspace_id(s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L220)

Add the workspace ID as a prefix to a given string if it does not have it.

Only used for consumer groups.

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L231)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L274)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L303)

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
def search_for_topic_workspace(
        topic: Optional[str],
        timeout: Optional[float] = None) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L326)

Find what workspace a topic belongs to.

If there is only one workspace altogether, it is assumed to be the workspace.
More than one means each workspace will be searched until the first hit.

**Arguments**:

- `topic`: the topic to search for
- `timeout`: response timeout (seconds); Default 30

**Returns**:

workspace data dict if topic search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.create_topic"></a>

#### QuixKafkaConfigsBuilder.create\_topic

```python
def create_topic(topic: Topic, timeout: Optional[float] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L357)

The actual API call to create the topic.

**Arguments**:

- `topic`: a Topic instance
- `timeout`: response timeout (seconds); Default 30

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_or_create_topic"></a>

#### QuixKafkaConfigsBuilder.get\_or\_create\_topic

```python
def get_or_create_topic(topic: Topic, timeout: Optional[float] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L393)

Get or create topics in a Quix cluster as part of initializing the Topic

object to obtain the true topic name.

**Arguments**:

- `topic`: a `Topic` object
- `timeout`: response timeout (seconds); Default 30
marked as "Ready" (and thus ready to produce to/consume from).

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.wait_for_topic_ready_statuses"></a>

#### QuixKafkaConfigsBuilder.wait\_for\_topic\_ready\_statuses

```python
def wait_for_topic_ready_statuses(topics: List[Topic],
                                  timeout: Optional[float] = None,
                                  finalize_timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L421)

After the broker acknowledges topics for creation, they will be in a

"Creating" status; they not usable until they are set to a status of "Ready".

This blocks until all topics are marked as "Ready" or the timeout is hit.

**Arguments**:

- `topics`: a list of `Topic` objects
- `timeout`: response timeout (seconds); Default 30
- `finalize_timeout`: topic finalization timeout (seconds); Default 60
marked as "Ready" (and thus ready to produce to/consume from).

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_topic"></a>

#### QuixKafkaConfigsBuilder.get\_topic

```python
def get_topic(topic: Topic, timeout: Optional[float] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L464)

return the topic ID (the actual cluster topic name) if it exists, else raise

**Arguments**:

- `topic`: The Topic to get
- `timeout`: response timeout (seconds); Default 30

**Raises**:

- `QuixApiRequestFailure`: when topic does not exist

**Returns**:

response dict of the topic info if topic found, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_application_config"></a>

#### QuixKafkaConfigsBuilder.get\_application\_config

```python
def get_application_config(consumer_group_id: str) -> QuixApplicationConfig
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/config.py#L496)

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

<a id="quixstreams.platforms.quix.env.QuixEnvironment.SDK_TOKEN"></a>

#### SDK\_TOKEN

noqa: S105

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

<a id="quixstreams.platforms.quix.checks.is_quix_deployment"></a>

#### is\_quix\_deployment

```python
def is_quix_deployment() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/checks.py#L11)

Check if the current deployment is a Quix deployment.

<a id="quixstreams.platforms.quix.checks.check_state_management_enabled"></a>

#### check\_state\_management\_enabled

```python
def check_state_management_enabled() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/checks.py#L18)

Check if State Management feature is enabled for the current deployment on
Quix platform.

If it's disabled, the warning will be logged.

<a id="quixstreams.platforms.quix.checks.check_state_dir"></a>

#### check\_state\_dir

```python
def check_state_dir(state_dir: Path) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/checks.py#L36)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/api.py#L20)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/api.py#L80)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/topic_manager.py#L15)

The source of all topic management with quixstreams.

This is specifically for Applications using the Quix Cloud.

Generally initialized and managed automatically by a Quix Application,
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
             create_timeout: float = 60,
             auto_create_topics: bool = True)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/platforms/quix/topic_manager.py#L36)

**Arguments**:

- `topic_admin`: an `Admin` instance
- `quix_config_builder`: A QuixKafkaConfigsBuilder instance, else one is
generated for you.
- `timeout`: response timeout (seconds)
- `create_timeout`: timeout for topic creation

<a id="quixstreams.dataframe.registry"></a>

## quixstreams.dataframe.registry

<a id="quixstreams.dataframe.registry.DataframeRegistry"></a>

### DataframeRegistry

```python
class DataframeRegistry()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/registry.py#L16)

Helps manage multiple `StreamingDataFrames` (multi-topic `Applications`)
and their respective repartitions.

`SDF`s are registered by storing their topic and current Stream.

<a id="quixstreams.dataframe.registry.DataframeRegistry.consumer_topics"></a>

#### DataframeRegistry.consumer\_topics

```python
@property
def consumer_topics() -> List[Topic]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/registry.py#L31)

**Returns**:

a list of Topics a consumer should subscribe to.

<a id="quixstreams.dataframe.registry.DataframeRegistry.register_root"></a>

#### DataframeRegistry.register\_root

```python
def register_root(new_sdf: "StreamingDataFrame")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/registry.py#L37)

Register a "root" SDF, or the start of a topic's processing.

**Arguments**:

- `new_sdf`: the new SDF.

<a id="quixstreams.dataframe.registry.DataframeRegistry.register_groupby"></a>

#### DataframeRegistry.register\_groupby

```python
def register_groupby(source_sdf: "StreamingDataFrame",
                     new_sdf: "StreamingDataFrame")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/registry.py#L52)

Register a "groupby" SDF, which is one generated with `SDF.group_by()`.

**Arguments**:

- `source_sdf`: the SDF used by `sdf.group_by()`
- `new_sdf`: the SDF generated by `sdf.group_by()`.

<a id="quixstreams.dataframe.registry.DataframeRegistry.compose_all"></a>

#### DataframeRegistry.compose\_all

```python
def compose_all(
        sink: Optional[VoidExecutor] = None) -> Dict[str, VoidExecutor]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/registry.py#L75)

Composes all the Streams and returns them in a dict, where key is its topic.

**Arguments**:

- `sink`: callable to accumulate the results of the execution, optional.

**Returns**:

a {topic_name: composed} dict, where composed is a callable

<a id="quixstreams.dataframe.dataframe"></a>

## quixstreams.dataframe.dataframe

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame"></a>

### StreamingDataFrame

```python
class StreamingDataFrame()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L80)

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
sdf = StreamingDataFrame()
sdf = sdf.apply(a_func)
sdf = sdf.filter(another_func)
sdf = sdf.to_topic(topic_obj)
```

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.apply"></a>

#### StreamingDataFrame.apply

```python
def apply(func: Union[
    ApplyCallback,
    ApplyExpandedCallback,
    ApplyCallbackStateful,
    ApplyWithMetadataCallback,
    ApplyWithMetadataExpandedCallback,
    ApplyWithMetadataCallbackStateful,
],
          *,
          stateful: bool = False,
          expand: bool = False,
          metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L195)

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

sdf = StreamingDataFrame()
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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L302)

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

sdf = StreamingDataFrame()
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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L408)

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

sdf = StreamingDataFrame()
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
def group_by(key: Union[str, Callable[[Any], Any]],
             name: Optional[str] = None,
             value_deserializer: Optional[DeserializerType] = "json",
             key_deserializer: Optional[DeserializerType] = "json",
             value_serializer: Optional[SerializerType] = "json",
             key_serializer: Optional[SerializerType] = "json") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L496)

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

sdf = StreamingDataFrame()
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
def contains(key: str) -> StreamingSeries
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L574)

Check if the key is present in the Row value.

Example Snippet:

```python
# Add new column 'has_column' which contains a boolean indicating
# the presence of 'column_x'

sdf = StreamingDataFrame()
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
def to_topic(topic: Topic, key: Optional[Callable[[Any], Any]] = None) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L599)

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
def set_timestamp(func: Callable[[Any, Any, int, Any], int]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L644)

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
def set_headers(
        func: Callable[
            [Any, Any, int, HeadersTuples],
            HeadersTuples,
        ]) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L685)

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
def print(pretty: bool = True, metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L736)

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

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.print_table"></a>

#### StreamingDataFrame.print\_table

```python
def print_table(size: int = 5,
                title: Optional[str] = None,
                metadata: bool = True,
                timeout: float = 5.0,
                live: bool = DEFAULT_LIVE,
                live_slowdown: float = DEFAULT_LIVE_SLOWDOWN,
                columns: Optional[List[str]] = None,
                column_widths: Optional[dict[str, int]] = None) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L780)

Print a table with the most recent records.

This feature is experimental and subject to change in future releases.

Creates a live table view that updates in real-time as new records are processed,
showing the most recent N records in a formatted table. When metadata is enabled,
the table includes message metadata columns (_key, _timestamp) along with the
record values.

The table automatically adjusts to show all available columns unless specific
columns are requested. Missing values in any column are displayed as empty cells.
Column widths adjust automatically to fit content unless explicitly specified.

Note: Column overflow is not handled gracefully. If your data has many columns,
the table may become unreadable. Use the `columns` parameter to specify which
columns to display and/or `column_widths` to control column sizes for better
visibility.

Printing Behavior:
- Interactive mode (terminal/console): The table refreshes in-place, with new
  rows appearing at the bottom and old rows being removed from the top when
  the table is full.
- Non-interactive mode (output redirected to file): Collects records until
  either the table is full or the timeout is reached, then prints the complete
  table and starts collecting new records.

Note: This works best in terminal environments. For Jupyter notebooks,
consider using `print()` instead.

Note: The last provided live value will be used for all print_table calls
in the pipeline.

Note: The last provided live_slowdown value will be used for all print_table calls
in the pipeline.

Example Snippet:

```python
sdf = app.dataframe(topic)
# Show last 5 records, update at most every 1 second
sdf.print_table(size=5, title="Live Records", slowdown=1)
```

This will produce a live-updating table like this:

Live Records
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━┳━━━━━┳━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━┓
┃ _key       ┃ _timestamp ┃ active ┃ id  ┃ name    ┃ score ┃ status   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━╇━━━━━╇━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━┩
│ b'53fe8e4' │ 1738685136 │ True   │ 876 │ Charlie │ 27.74 │ pending  │
│ b'91bde51' │ 1738685137 │ True   │ 11  │         │       │ approved │
│ b'6617dfe' │ 1738685138 │        │     │ David   │       │          │
│ b'f47ac93' │ 1738685139 │        │ 133 │         │       │          │
│ b'038e524' │ 1738685140 │ False  │     │         │       │          │
└────────────┴────────────┴────────┴─────┴─────────┴───────┴──────────┘

**Arguments**:

- `size`: Maximum number of records to display in the table. Default: 5
- `title`: Optional title for the table
- `metadata`: Whether to include message metadata (_key, _timestamp) columns.
Default: True
- `timeout`: Time in seconds to wait for table to fill up before printing
an incomplete table. Only relevant for non-interactive environments
(e.g. output redirected to a file). Default: 5.0
- `live`: Whether to print the table in real-time if possible.
If real-time printing is not possible, the table will be printed
in non-interactive mode. Default: True
- `live_slowdown`: Time in seconds to wait between live table updates.
Increase this value if the table updates too quickly.
Default: 0.5 seconds.
- `columns`: Optional list of columns to display. If not provided,
all columns will be displayed. Pass empty list to display only metadata.
- `column_widths`: Optional dictionary mapping column names to their desired
widths in characters. If not provided, column widths will be determined
automatically based on content. Example: {"name": 20, "id": 10}

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.compose"></a>

#### StreamingDataFrame.compose

```python
def compose(sink: Optional[VoidExecutor] = None) -> dict[str, VoidExecutor]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L896)

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
         key: Any = b"key",
         timestamp: int = 0,
         headers: Optional[Any] = None,
         ctx: Optional[MessageContext] = None,
         topic: Optional[Topic] = None) -> List[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L930)

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
def tumbling_window(
    duration_ms: Union[int, timedelta],
    grace_ms: Union[int, timedelta] = 0,
    name: Optional[str] = None,
    on_late: Optional[WindowOnLateCallback] = None
) -> TumblingTimeWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L967)

Create a time-based tumbling window transformation on this StreamingDataFrame.

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
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 60s and grace period of 10s
    sdf.tumbling_window(
        duration_ms=timedelta(seconds=60), grace_ms=timedelta(seconds=10.0)
    )

    # Specify the aggregation function
    .agg(value=agg.Sum())

    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
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
- `on_late`: an optional callback to react on late records in windows and
to configure the logging of such events.
If the callback returns `True`, the message about a late record will be logged
(default behavior).
Otherwise, no message will be logged.

**Returns**:

`TumblingTimeWindowDefinition` instance representing the tumbling window
configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.tumbling_count_window"></a>

#### StreamingDataFrame.tumbling\_count\_window

```python
def tumbling_count_window(
        count: int,
        name: Optional[str] = None) -> TumblingCountWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1056)

Create a count-based tumbling window transformation on this StreamingDataFrame.

Tumbling windows divide messages into fixed-batch, non-overlapping windows.
They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.
Notes:
- The start timestamp of the aggregation result is set to the earliest timestamp.
- The end timestamp of the aggregation result is set to the latest timestamp.
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.


Example Snippet:

```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)
sdf = (
    # Define a tumbling window of 10 messages
    sdf.tumbling_count_window(count=10)
    # Specify the aggregation function
    .agg(value=agg.Sum())
    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```

**Arguments**:

- `count`: The length of each window. The number of messages to include in the window.
- `name`: The unique identifier for the window. If not provided, it will be
automatically generated based on the window's properties.

**Returns**:

`TumblingCountWindowDefinition` instance representing the tumbling window
configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.hopping_window"></a>

#### StreamingDataFrame.hopping\_window

```python
def hopping_window(
    duration_ms: Union[int, timedelta],
    step_ms: Union[int, timedelta],
    grace_ms: Union[int, timedelta] = 0,
    name: Optional[str] = None,
    on_late: Optional[WindowOnLateCallback] = None
) -> HoppingTimeWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1106)

Create a time-based hopping window transformation on this StreamingDataFrame.

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
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

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
    .agg(value=agg.Sum())

    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
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
- `on_late`: an optional callback to react on late records in windows and
to configure the logging of such events.
If the callback returns `True`, the message about a late record will be logged
(default behavior).
Otherwise, no message will be logged.

**Returns**:

`HoppingTimeWindowDefinition` instance representing the hopping
window configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. and applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.hopping_count_window"></a>

#### StreamingDataFrame.hopping\_count\_window

```python
def hopping_count_window(
        count: int,
        step: int,
        name: Optional[str] = None) -> HoppingCountWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1209)

Create a count-based hopping window transformation on this StreamingDataFrame.

Hopping windows divide the data stream into overlapping windows.
The overlap is controlled by the `step` parameter.
They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.
Notes:
- The start timestamp of the aggregation result is set to the earliest timestamp.
- The end timestamp of the aggregation result is set to the latest timestamp.
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.


Example Snippet:

```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)
sdf = (
    # Define a hopping window of 10 messages with a step of 5 messages
    sdf.hopping_count_window(
        count=10,
        step=5,
    )
    # Specify the aggregation function
    .agg(value=agg.Sum())
    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```

**Arguments**:

- `count`: The length of each window. The number of messages to include in the window.
- `step`: The step size for the window. It determines the number of messages between windows.
A  sliding windows is the same as a hopping window with a step of 1 message.
- `name`: The unique identifier for the window. If not provided, it will be
automatically generated based on the window's properties.

**Returns**:

`HoppingCountWindowDefinition` instance representing the hopping
window configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. and applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.sliding_window"></a>

#### StreamingDataFrame.sliding\_window

```python
def sliding_window(
    duration_ms: Union[int, timedelta],
    grace_ms: Union[int, timedelta] = 0,
    name: Optional[str] = None,
    on_late: Optional[WindowOnLateCallback] = None
) -> SlidingTimeWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1266)

Create a time-based sliding window transformation on this StreamingDataFrame.

Sliding windows continuously evaluate the stream with a fixed step of 1 ms
allowing for overlapping, but not redundant windows of a fixed size.

Sliding windows are similar to hopping windows with step_ms set to 1,
but are siginificantly more perforant.

They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.

Notes:

- The timestamp of the aggregation result is set to the window start timestamp.
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.
- The time windows always use the current event time.
- Windows are inclusive on both the start end end time.
- Every window contains a distinct aggregation.

Example Snippet:

```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)

sdf = (
    # Define a sliding window of 60s with a grace period of 10s
    sdf.sliding_window(
        duration_ms=timedelta(seconds=60),
        grace_ms=timedelta(seconds=10)
    )

    # Specify the aggregation function
    .agg(value=agg.Sum())

    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
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
- `on_late`: an optional callback to react on late records in windows and
to configure the logging of such events.
If the callback returns `True`, the message about a late record will be logged
(default behavior).
Otherwise, no message will be logged.

**Returns**:

`SlidingTimeWindowDefinition` instance representing the sliding window
configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.sliding_count_window"></a>

#### StreamingDataFrame.sliding\_count\_window

```python
def sliding_count_window(
        count: int,
        name: Optional[str] = None) -> SlidingCountWindowDefinition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1361)

Create a count-based sliding window transformation on this StreamingDataFrame.

Sliding windows continuously evaluate the stream with a fixed step of 1 message
allowing for overlapping, but not redundant windows of a fixed size.
Sliding windows are similar to hopping windows with step set to 1.
They allow performing stateful aggregations like `sum`, `reduce`, etc.
on top of the data and emit results downstream.
Notes:
- The start timestamp of the aggregation result is set to the earliest timestamp.
- The end timestamp of the aggregation result is set to the latest timestamp.
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.
- Every window contains a distinct aggregation.


Example Snippet:

```python
from quixstreams import Application
import quixstreams.dataframe.windows.aggregations as agg

app = Application()
sdf = app.dataframe(...)
sdf = (
    # Define a sliding window of 10 messages
    sdf.sliding_count_window(count=10)
    # Specify the aggregation function
    .sum(value=agg.Sum())
    # Specify how the results should be emitted downstream.
    # "current()" will emit results as they come for each updated window,
    # possibly producing multiple messages per key-window pair
    # "final()" will emit windows only when they are closed and cannot
    # receive any updates anymore.
    .current()
)
```

**Arguments**:

- `count`: The length of each window. The number of messages to include in the window.
- `name`: The unique identifier for the window. If not provided, it will be
automatically generated based on the window's properties.

**Returns**:

`SlidingCountWindowDefinition` instance representing the sliding window
configuration.
This object can be further configured with aggregation functions
like `sum`, `count`, etc. applied to the StreamingDataFrame.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.fill"></a>

#### StreamingDataFrame.fill

```python
def fill(*columns: str, **mapping: Any) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1414)

Fill missing values in the message value with a constant value.

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).

Example Snippets:

Fill missing values for a single column with a None:
```python
# This would transform {"x": 1} to {"x": 1, "y": None}
sdf.fill("y")
```

Fill missing values for multiple columns with a None:
```python
# This would transform {"x": 1} to {"x": 1, "y": None, "z": None}
sdf.fill("y", "z")
```

Fill missing values in the value with a constant value using a dictionary:
```python
# This would transform {"x": None} to {"x": 1, "y": 2}
sdf.fill(x=1, y=2)
```

Use a combination of positional and keyword arguments:
```python
# This would transform {"y": None} to {"x": None, "y": 2}
sdf.fill("x", y=2)
```

**Arguments**:

- `columns`: a list of column names as strings.
- `mapping`: a dictionary where keys are column names and values are the fill values.

**Returns**:

the original `StreamingDataFrame` instance for chaining.

<a id="quixstreams.dataframe.dataframe.StreamingDataFrame.drop"></a>

#### StreamingDataFrame.drop

```python
def drop(columns: Union[str, List[str]],
         errors: Literal["ignore", "raise"] = "raise") -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1466)

Drop column(s) from the message value (value must support `del`, like a dict).

This operation occurs in-place, meaning reassignment is entirely OPTIONAL: the
original `StreamingDataFrame` is returned for chaining (`sdf.update().print()`).


Example Snippet:

```python
# Remove columns "x" and "y" from the value.
# This would transform {"x": 1, "y": 2, "z": 3} to {"z": 3}

sdf = StreamingDataFrame()
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
def sink(sink: BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/dataframe.py#L1510)

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

Note: `sink()` is a terminal operation - it cannot receive any additional
operations, but branches can still be generated from its originating SDF.

<a id="quixstreams.dataframe.series"></a>

## quixstreams.dataframe.series

<a id="quixstreams.dataframe.series.StreamingSeries"></a>

### StreamingSeries

```python
class StreamingSeries()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L58)

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

sdf = StreamingDataFrame()
sdf = sdf["column_a"].apply(a_func).apply(diff_func, stateful=True)
sdf["my_new_bool_field"] = sdf["column_b"].contains("this_string")
sdf["new_sum_field"] = sdf["column_c"] + sdf["column_d"] + 2
sdf = sdf[["column_a"] & (sdf["new_sum_field"] >= 10)]
```

<a id="quixstreams.dataframe.series.StreamingSeries.from_apply_callback"></a>

#### StreamingSeries.from\_apply\_callback

```python
@classmethod
def from_apply_callback(cls, func: ApplyWithMetadataCallback,
                        sdf_id: int) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L124)

Create a StreamingSeries from a function.

The provided function will be wrapped into `Apply`

**Arguments**:

- `func`: a function to apply
- `sdf_id`: the id of the calling `SDF`.

**Returns**:

instance of `StreamingSeries`

<a id="quixstreams.dataframe.series.StreamingSeries.apply"></a>

#### StreamingSeries.apply

```python
def apply(func: ApplyCallback) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L147)

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

sdf = StreamingDataFrame()
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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L181)

Compose a list of functions from this StreamingSeries and its parents into one

big closure that always returns the transformed record.

This closure is to be used to execute the functions in the stream and to get
the result of the transformations.

Stream may only contain simple "apply" functions to be able to compose itself
into a returning function.

**Returns**:

a callable accepting value, key and timestamp and
returning a tuple "(value, key, timestamp)

<a id="quixstreams.dataframe.series.StreamingSeries.test"></a>

#### StreamingSeries.test

```python
def test(value: Any,
         key: Any,
         timestamp: int,
         headers: Optional[Any] = None,
         ctx: Optional[MessageContext] = None) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L196)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L261)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L292)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L317)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L340)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L364)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L387)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/series.py#L410)

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

<a id="quixstreams.dataframe.windows.sliding"></a>

## quixstreams.dataframe.windows.sliding

<a id="quixstreams.dataframe.windows.sliding.SlidingWindow"></a>

### SlidingWindow

```python
class SlidingWindow(TimeWindow)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/sliding.py#L16)

<a id="quixstreams.dataframe.windows.sliding.SlidingWindow.process_window"></a>

#### SlidingWindow.process\_window

```python
def process_window(
    value: Any, key: Any, timestamp_ms: int,
    transaction: WindowedPartitionTransaction
) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/sliding.py#L33)

The algorithm is based on the concept that each message
is associated with a left and a right window.

Left Window:
- Begins at message timestamp - window size
- Ends at message timestamp

Right Window:
- Begins at message timestamp + 1 ms
- Ends at message timestamp + 1 ms + window size

For example, for a window size of 10 and a message A arriving at timestamp 26:

0        10        20        30        40        50        60
----|---------|---------|---------|---------|---------|---------|--->
A
left window ->    |---------||---------|    <- right window
16      26  27      37

The algorithm scans backward through the window store:
- Starting at: start_time = message timestamp + 1 ms (the right window's start time)
- Ending at: start_time = message timestamp - 2 * window size

During this traversal, the algorithm performs the following actions:

1. Determine if the right window should be created.
If yes, locate the existing aggregation to copy to the new window.
2. Determine if the right window of the previous record should be created.
If yes, locate the existing aggregation and combine it with the incoming message.
3. Locate and update the left window if it exists.
4. If the left window does not exist, create it. Locate the existing
aggregation and combine it with the incoming message.
5. Locate and update all existing windows to which the new message belongs.

**Notes**:

  For collection aggregations (created using .collect()), the behavior is special:
  Windows are persisted with empty values (None) only to preserve their start and
  end times. The actual values are collected separately and combined during
  the window expiration step.

<a id="quixstreams.dataframe.windows.definitions"></a>

## quixstreams.dataframe.windows.definitions

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition"></a>

### WindowDefinition

```python
class WindowDefinition(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L49)

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition.sum"></a>

#### WindowDefinition.sum

```python
def sum() -> "Window"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L70)

Configure the window to aggregate data by summing up values within

each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to perform sum aggregation.

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition.count"></a>

#### WindowDefinition.count

```python
def count() -> "Window"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L83)

Configure the window to aggregate data by counting the number of values

within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to perform record count.

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition.mean"></a>

#### WindowDefinition.mean

```python
def mean() -> "Window"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L96)

Configure the window to aggregate data by calculating the mean of the values

within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to calculate the mean
of the values.

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition.reduce"></a>

#### WindowDefinition.reduce

```python
def reduce(reducer: Callable[[Any, Any], Any],
           initializer: Callable[[Any], Any]) -> "Window"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L110)

Configure the window to perform a custom aggregation using `reducer`

and `initializer` functions.

Example Snippet:
```python
sdf = StreamingDataFrame(...)

# Using "reduce()" to calculate multiple aggregates at once
def reducer(agg: dict, current: int):
    aggregated = {
        'min': min(agg['min'], current),
        'max': max(agg['max'], current),
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

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition.max"></a>

#### WindowDefinition.max

```python
def max() -> "Window"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L154)

Configure a window to aggregate the maximum value within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to calculate the maximum
value within each window period.

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition.min"></a>

#### WindowDefinition.min

```python
def min() -> "Window"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L167)

Configure a window to aggregate the minimum value within each window period.

**Returns**:

an instance of `FixedTimeWindow` configured to calculate the maximum
value within each window period.

<a id="quixstreams.dataframe.windows.definitions.WindowDefinition.collect"></a>

#### WindowDefinition.collect

```python
def collect() -> "Window"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/definitions.py#L180)

Configure the window to collect all values within each window period into a

list, without performing any aggregation.

This method is useful when you need to gather all raw values that fall
within a window period for further processing or analysis.

Example Snippet:
```python
# Collect all values in 1-second windows
window = sdf.tumbling_window(duration_ms=1000).collect()
# Each window will contain a list of all values that occurred
# within that second
```

**Returns**:

an instance of `FixedTimeWindow` configured to collect all values
within each window period.

<a id="quixstreams.dataframe.windows"></a>

## quixstreams.dataframe.windows

<a id="quixstreams.dataframe.windows.time_based"></a>

## quixstreams.dataframe.windows.time\_based

<a id="quixstreams.dataframe.windows.time_based.TimeWindow"></a>

### TimeWindow

```python
class TimeWindow(Window)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/time_based.py#L41)

<a id="quixstreams.dataframe.windows.time_based.TimeWindow.final"></a>

#### TimeWindow.final

```python
def final(
        closing_strategy: ClosingStrategyValues = "key"
) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/time_based.py#L63)

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

**Arguments**:

- `closing_strategy`: the strategy to use when closing windows.
Possible values:
- `"key"` - messages advance time and close windows with the same key.
If some message keys appear irregularly in the stream, the latest windows can remain unprocessed until a message with the same key is received.
- `"partition"` - messages advance time and close windows for the whole partition to which this message key belongs.
If timestamps between keys are not ordered, it may increase the number of discarded late messages.
Default - `"key"`.

<a id="quixstreams.dataframe.windows.time_based.TimeWindow.current"></a>

#### TimeWindow.current

```python
def current(
        closing_strategy: ClosingStrategyValues = "key"
) -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/time_based.py#L95)

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

- `closing_strategy`: the strategy to use when closing windows.
Possible values:
- `"key"` - messages advance time and close windows with the same key.
If some message keys appear irregularly in the stream, the latest windows can remain unprocessed until a message with the same key is received.
- `"partition"` - messages advance time and close windows for the whole partition to which this message key belongs.
If timestamps between keys are not ordered, it may increase the number of discarded late messages.
Default - `"key"`.

<a id="quixstreams.dataframe.windows.count_based"></a>

## quixstreams.dataframe.windows.count\_based

<a id="quixstreams.dataframe.windows.count_based.CountWindow"></a>

### CountWindow

```python
class CountWindow(Window)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/count_based.py#L38)

<a id="quixstreams.dataframe.windows.count_based.CountWindow.process_window"></a>

#### CountWindow.process\_window

```python
def process_window(
    value: Any, key: Any, timestamp_ms: int,
    transaction: WindowedPartitionTransaction[str, CountWindowsData]
) -> tuple[Iterable[WindowKeyResult], Iterable[WindowKeyResult]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/count_based.py#L56)

Count based windows are different from time based windows as we don't
have a clear indicator on when a window starts, it depends on the
previous window. On the other hand there is a clear marker on when it
must end, we can close the window as soon as the window is full.

As count windows can't rely on ordered timestamps, collection need
another way to generate ordered ids. We can use a per-key counter for
that, each incoming message will increment the key counter. Collection
window then only need to know at what id they start, how many messages
to get on completion and how many messages can be safely deleted.

We can further optimise this by removing the global counter and relying
on the previous window state to compute a message id. For example, if
an active window starts at msg id 32 and has a count of 3 it means the
next free msg id is 35 (32 + 3).

For tumbling windows there is no window overlap so we can't rely on that
optimisation. Instead the msg id reset to 0 on every new window.

<a id="quixstreams.dataframe.windows.base"></a>

## quixstreams.dataframe.windows.base

<a id="quixstreams.dataframe.windows.base.Window"></a>

### Window

```python
class Window(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/base.py#L45)

<a id="quixstreams.dataframe.windows.base.Window.final"></a>

#### Window.final

```python
def final() -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/base.py#L95)

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

<a id="quixstreams.dataframe.windows.base.Window.current"></a>

#### Window.current

```python
def current() -> "StreamingDataFrame"
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/base.py#L141)

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

<a id="quixstreams.dataframe.windows.base.SingleAggregationWindowMixin"></a>

### SingleAggregationWindowMixin

```python
class SingleAggregationWindowMixin()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/base.py#L216)

DEPRECATED: Use MultiAggregationWindowMixin instead.

Single aggregation window mixin for windows with a single aggregation or collection.
Store aggregated value directly in the window value.

<a id="quixstreams.dataframe.windows.base.get_window_ranges"></a>

#### get\_window\_ranges

```python
def get_window_ranges(timestamp_ms: int,
                      duration_ms: int,
                      step_ms: Optional[int] = None) -> Deque[tuple[int, int]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/base.py#L435)

Get a list of window ranges for the given timestamp.

**Arguments**:

- `timestamp_ms`: timestamp in milliseconds
- `duration_ms`: window duration in milliseconds
- `step_ms`: window step in milliseconds for hopping windows, optional.

**Returns**:

a list of (<start>, <end>) tuples

<a id="quixstreams.dataframe.windows.aggregations"></a>

## quixstreams.dataframe.windows.aggregations

<a id="quixstreams.dataframe.windows.aggregations.BaseAggregator"></a>

### BaseAggregator

```python
class BaseAggregator(ABC, Generic[S])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L34)

Base class for window aggregation.

Subclass it to implement custom aggregations.

An Aggregator reduce incoming items into a single value or group of values. When the window
is closed the aggregator produce a result based on the reduced value.

To store all incoming items without reducing them use a `Collector`.

<a id="quixstreams.dataframe.windows.aggregations.BaseAggregator.state_suffix"></a>

#### BaseAggregator.state\_suffix

```python
@property
@abstractmethod
def state_suffix() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L48)

The state suffix is used to store the aggregation state in the window.

The complete state key is built using the result column name and this suffix.
If these values change, the state key will also change, and the aggregation state will restart from zero.

Aggregations should change the state suffix when their parameters change to avoid
conflicts with previous state values.

<a id="quixstreams.dataframe.windows.aggregations.BaseAggregator.initialize"></a>

#### BaseAggregator.initialize

```python
@abstractmethod
def initialize() -> S
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L61)

This method is triggered once to build the aggregation starting value.
It should return the initial value for the aggregation.

<a id="quixstreams.dataframe.windows.aggregations.BaseAggregator.agg"></a>

#### BaseAggregator.agg

```python
@abstractmethod
def agg(old: S, new: Any, timestamp: int) -> S
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L69)

This method is trigged when a window is updated with a new value.
It should return the updated aggregated value.

<a id="quixstreams.dataframe.windows.aggregations.BaseAggregator.result"></a>

#### BaseAggregator.result

```python
@abstractmethod
def result(value: S) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L77)

This method is triggered when a window is closed.
It should return the final aggregation result.

<a id="quixstreams.dataframe.windows.aggregations.Aggregator"></a>

### Aggregator

```python
class Aggregator(BaseAggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L85)

Implementation of the `BaseAggregator` interface.

Provides default implementations for the `state_suffix` property.

<a id="quixstreams.dataframe.windows.aggregations.Count"></a>

### Count

```python
class Count(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L102)

Use `Count()` to aggregate the total number of events  within each window period..

<a id="quixstreams.dataframe.windows.aggregations.Sum"></a>

### Sum

```python
class Sum(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L126)

Use `Sum()` to aggregate the sum of the events, or a column of the events, within each window period.

**Arguments**:

- `column`: The column to sum. Use `None` to sum the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.Mean"></a>

### Mean

```python
class Mean(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L150)

Use `Mean()` to aggregate the mean of the events, or a column of the events, within each window period.

**Arguments**:

- `column`: The column to mean. Use `None` to mean the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.Max"></a>

### Max

```python
class Max(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L178)

Use `Max()` to aggregate the max of the events, or a column of the events, within each window period.

**Arguments**:

- `column`: The column to max. Use `None` to max the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.Min"></a>

### Min

```python
class Min(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L203)

Use `Min()` to aggregate the min of the events, or a column of the events, within each window period.

**Arguments**:

- `column`: The column to min. Use `None` to min the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.Earliest"></a>

### Earliest

```python
class Earliest(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L228)

Use `Earliest()` to get the event (or its column) with the smallest timestamp within each window period.

**Arguments**:

- `column`: The column to aggregate. Use `None` to earliest the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.Latest"></a>

### Latest

```python
class Latest(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L259)

Use `Latest()` to get the event (or its column) with the latest timestamp within each window period.

**Arguments**:

- `column`: The column to aggregate. Use `None` to latest the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.First"></a>

### First

```python
class First(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L290)

Use `First()` to get the first event, or a column of the event, within each window period.

This aggregation works based on the processing order.

**Arguments**:

- `column`: The column to aggregate. Use `None` to first the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.Last"></a>

### Last

```python
class Last(Aggregator)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L314)

Use `Last()` to get the last event, or a column of the event, within each window period.

This aggregation works based on the processing order.

**Arguments**:

- `column`: The column to aggregate. Use `None` to last the whole message.
Default - `None`

<a id="quixstreams.dataframe.windows.aggregations.Reduce"></a>

### Reduce

```python
class Reduce(Aggregator, Generic[R])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L341)

`Reduce()` allows you to perform complex aggregations using custom "reducer" and "initializer" functions.

<a id="quixstreams.dataframe.windows.aggregations.BaseCollector"></a>

### BaseCollector

```python
class BaseCollector(ABC, Generic[I])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L367)

Base class for window collections.

Subclass it to implement custom collections.

A Collector store incoming items un-modified in an optimized way.

To reduce incoming items as they come in use an `Aggregator`.

<a id="quixstreams.dataframe.windows.aggregations.BaseCollector.column"></a>

#### BaseCollector.column

```python
@property
@abstractmethod
def column() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L380)

The column to collect.

Use `None` to collect the whole message.

<a id="quixstreams.dataframe.windows.aggregations.BaseCollector.result"></a>

#### BaseCollector.result

```python
@abstractmethod
def result(items: Iterable[I]) -> Any
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L389)

This method is triggered when a window is closed.
It should return the final collection result.

<a id="quixstreams.dataframe.windows.aggregations.Collector"></a>

### Collector

```python
class Collector(BaseCollector)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L397)

Implementation of the `BaseCollector` interface.

Provides a default implementation for the `column` property.

<a id="quixstreams.dataframe.windows.aggregations.Collect"></a>

### Collect

```python
class Collect(Collector)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/dataframe/windows/aggregations.py#L412)

Use `Collect()` to gather all events within each window period. into a list.

**Arguments**:

- `column`: The column to collect. Use `None` to collect the whole message.
Default - `None`

<a id="quixstreams.rowproducer"></a>

## quixstreams.rowproducer

<a id="quixstreams.rowproducer.RowProducer"></a>

### RowProducer

```python
class RowProducer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L77)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L124)

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
def poll(timeout: float = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L164)

Polls the producer for events and calls `on_delivery` callbacks.

If `poll()` fails, it will trigger the provided "on_error" callback

**Arguments**:

- `timeout`: timeout in seconds

<a id="quixstreams.rowproducer.RowProducer.abort_transaction"></a>

#### RowProducer.abort\_transaction

```python
def abort_transaction(timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowproducer.py#L235)

Attempt an abort if an active transaction.

Else, skip since it throws an exception if at least
one transaction was successfully completed at some point.

This avoids polluting the stack trace in the case where a transaction was
not active as expected (because of some other exception already raised)
and a cleanup abort is attempted.

NOTE: under normal circumstances a transaction will be open due to how
the Checkpoint inits another immediately after committing.

<a id="quixstreams.core.stream"></a>

## quixstreams.core.stream

<a id="quixstreams.core.stream.stream"></a>

## quixstreams.core.stream.stream

<a id="quixstreams.core.stream.stream.Stream"></a>

### Stream

```python
class Stream()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L47)

<a id="quixstreams.core.stream.stream.Stream.__init__"></a>

#### Stream.\_\_init\_\_

```python
def __init__(func: Optional[StreamFunction] = None,
             parents: Optional[List[Self]] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L48)

A base class for all streaming operations.

`Stream` is an abstraction of a function pipeline.
Each Stream has a function and an optional list of parents (None by default).
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
- `parents`: an optional list of parent `Stream`s

<a id="quixstreams.core.stream.stream.Stream.add_filter"></a>

#### Stream.add\_filter

```python
def add_filter(func: Union[FilterCallback, FilterWithMetadataCallback],
               *,
               metadata: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L121)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L188)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L229)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L263)

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
def diff(other: Self) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L287)

Takes the difference between Streams `self` and `other` based on their last

common parent, and returns a new, independent `Stream` that includes only
this difference (the start of the "diff" will have no parent).

It's impossible to calculate a diff when:
 - Streams don't have a common parent.
 - When the `self` Stream already includes all the nodes from
    the `other` Stream, and the resulting diff is empty.

**Arguments**:

- `other`: a `Stream` to take a diff from.

**Raises**:

- `ValueError`: if Streams don't have a common parent,
if the diff is empty, or pruning failed.

**Returns**:

a new independent `Stream` instance whose root begins at the diff

<a id="quixstreams.core.stream.stream.Stream.full_tree"></a>

#### Stream.full\_tree

```python
def full_tree() -> List[Self]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L361)

Find every related Stream in the tree across all children and parents and return
them in a topologically sorted order.

<a id="quixstreams.core.stream.stream.Stream.compose"></a>

#### Stream.compose

```python
def compose(allow_filters=True,
            allow_expands=True,
            allow_updates=True,
            allow_transforms=True,
            sink: Optional[VoidExecutor] = None) -> dict[Self, VoidExecutor]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L387)

Generate an "executor" closure by mapping all relatives of this `Stream` and

composing their functions together.

The resulting "executor" can be called with a given
value, key, timestamp, and headers (i.e. a Kafka message).

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

<a id="quixstreams.core.stream.stream.Stream.compose_returning"></a>

#### Stream.compose\_returning

```python
def compose_returning() -> ReturningExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L446)

Compose a list of functions from this `Stream` and its parents into one
big closure that always returns the transformed record.

This closure is to be used to execute the functions in the stream and to get
the result of the transformations.

Stream may only contain simple "apply" functions to be able to compose itself
into a returning function.

<a id="quixstreams.core.stream.stream.Stream.compose_single"></a>

#### Stream.compose\_single

```python
def compose_single(allow_filters=True,
                   allow_expands=True,
                   allow_updates=True,
                   allow_transforms=True,
                   sink: Optional[VoidExecutor] = None) -> VoidExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L484)

A helper function to compose a Stream with a single root.

If there's more than one root in the topology,
it will fail with the `InvalidTopology` error.

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

<a id="quixstreams.core.stream.stream.Stream.root_path"></a>

#### Stream.root\_path

```python
def root_path() -> List[Self]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/stream.py#L525)

Start from self and collect all parents until reaching the root nodes

<a id="quixstreams.core.stream.functions.update"></a>

## quixstreams.core.stream.functions.update

<a id="quixstreams.core.stream.functions.update.UpdateFunction"></a>

### UpdateFunction

```python
class UpdateFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/update.py#L9)

Wrap a function into an "Update" function.

The provided function must accept a value, and it's expected to mutate it
or to perform some side effect.

The result of the callback is always ignored, and the original input is passed
downstream.

<a id="quixstreams.core.stream.functions.update.UpdateWithMetadataFunction"></a>

### UpdateWithMetadataFunction

```python
class UpdateWithMetadataFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/update.py#L37)

Wrap a function into an "Update" function.

The provided function must accept a value, a key, and a timestamp.
The callback is expected to mutate the value or to perform some side effect with it.

The result of the callback is always ignored, and the original input is passed
downstream.

<a id="quixstreams.core.stream.functions"></a>

## quixstreams.core.stream.functions

<a id="quixstreams.core.stream.functions.types"></a>

## quixstreams.core.stream.functions.types

<a id="quixstreams.core.stream.functions.utils"></a>

## quixstreams.core.stream.functions.utils

<a id="quixstreams.core.stream.functions.utils.pickle_copier"></a>

#### pickle\_copier

```python
def pickle_copier(obj: T) -> Callable[[], T]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/utils.py#L12)

A utility function to copy objects using a "pickle" library.

On average, it's faster than "copy.deepcopy".
It accepts an object and returns a callable creating copies of this object.

**Arguments**:

- `obj`: an object to copy

<a id="quixstreams.core.stream.functions.transform"></a>

## quixstreams.core.stream.functions.transform

<a id="quixstreams.core.stream.functions.transform.TransformFunction"></a>

### TransformFunction

```python
class TransformFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/transform.py#L9)

Wrap a function into a "Transform" function.

The provided callback must accept a value, a key and a timestamp.
It's expected to return a new value, new key and new timestamp.

This function must be used with caution, because it can technically change the
key.
It's supposed to be used by the library internals and not be a part of the public
API.

The result of the callback will always be passed downstream.

<a id="quixstreams.core.stream.functions.filter"></a>

## quixstreams.core.stream.functions.filter

<a id="quixstreams.core.stream.functions.filter.FilterFunction"></a>

### FilterFunction

```python
class FilterFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/filter.py#L9)

Wraps a function into a "Filter" function.
The result of a Filter function is interpreted as boolean.
If it's `True`, the input will be return downstream.
If it's `False`, the `Filtered` exception will be raised to signal that the
value is filtered out.

<a id="quixstreams.core.stream.functions.filter.FilterWithMetadataFunction"></a>

### FilterWithMetadataFunction

```python
class FilterWithMetadataFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/filter.py#L39)

Wraps a function into a "Filter" function.

The passed callback must accept value, key, and timestamp, and it's expected to
return a boolean-like result.

If the result is `True`, the input will be passed downstream.
Otherwise, the value will be filtered out.

<a id="quixstreams.core.stream.functions.base"></a>

## quixstreams.core.stream.functions.base

<a id="quixstreams.core.stream.functions.base.StreamFunction"></a>

### StreamFunction

```python
class StreamFunction(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/base.py#L10)

A base class for all the streaming operations in Quix Streams.

It provides a `get_executor` method to return a closure to be called with the input
values.

<a id="quixstreams.core.stream.functions.base.StreamFunction.get_executor"></a>

#### StreamFunction.get\_executor

```python
@abc.abstractmethod
def get_executor(*child_executors: VoidExecutor) -> VoidExecutor
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/base.py#L24)

Returns a wrapper to be called on a value, key, timestamp and headers.

<a id="quixstreams.core.stream.functions.apply"></a>

## quixstreams.core.stream.functions.apply

<a id="quixstreams.core.stream.functions.apply.ApplyFunction"></a>

### ApplyFunction

```python
class ApplyFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/apply.py#L15)

Wrap a function into "Apply" function.

The provided callback is expected to return a new value based on input,
and its result will always be passed downstream.

<a id="quixstreams.core.stream.functions.apply.ApplyWithMetadataFunction"></a>

### ApplyWithMetadataFunction

```python
class ApplyWithMetadataFunction(StreamFunction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/core/stream/functions/apply.py#L72)

Wrap a function into "Apply" function.

The provided function is expected to accept value, and timestamp and return
a new value based on input,
and its result will always be passed downstream.

<a id="quixstreams.core.stream.exceptions"></a>

## quixstreams.core.stream.exceptions

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L24)

A class to share processing-related objects
between `Application` and `StreamingDataFrame` instances.

<a id="quixstreams.processing.context.ProcessingContext.store_offset"></a>

#### ProcessingContext.store\_offset

```python
def store_offset(topic: str, partition: int, offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L49)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L59)

Initialize a new checkpoint

<a id="quixstreams.processing.context.ProcessingContext.commit_checkpoint"></a>

#### ProcessingContext.commit\_checkpoint

```python
def commit_checkpoint(force: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/context.py#L74)

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
def pause(offsets_to_seek: dict[tuple[str, int], int], resume_after: float)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/pausing.py#L28)

Pause all partitions for the certain period of time and seek the partitions
provided in the `offsets_to_seek` dict.

This method is supposed to be called in case of backpressure from Sinks.

<a id="quixstreams.processing.pausing.PausingManager.resume_if_ready"></a>

#### PausingManager.resume\_if\_ready

```python
def resume_if_ready()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/processing/pausing.py#L67)

Resume consuming from assigned data partitions after the wait period has elapsed.

<a id="quixstreams.sinks.core.list"></a>

## quixstreams.sinks.core.list

<a id="quixstreams.sinks.core.list.ListSink"></a>

### ListSink

```python
class ListSink(BaseSink, UserList)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/list.py#L8)

A sink that accumulates data into a list for interactive debugging/inspection.

It behaves just like a list. Messages are appended as they are handled by it.

You can optionally include the message metadata as well.

Intended for debugging with Application.run(timeout=N)

**Example**:

```
from quixstreams import Application
from quixstreams.sinks.core.list import ListSink

app = Application(broker_address="localhost:9092")
topic = app.topic("some-topic")
list_sink = ListSink()  # sink will be a list-like object
sdf = app.dataframe(topic=topic).sink(list_sink)
app.run(timeout=10)  # collect data for 10 seconds

# after running 10s
print(list_sink)    # [1, 2, 3]
list_sink[0]        # 1
```
  
  Metadata Behavior:
  When initialized with metadata=True, each record will be
  enriched with the following metadata fields as a prefix
  to the value dictionary:
  - _key: The message key
  - _timestamp: Message timestamp
  - _headers: String representation of message headers
  - _topic: Source topic name
  - _partition: Source partition number
  - _offset: Message offset in partition
  When metadata=False (default), only the message value is stored.

<a id="quixstreams.sinks.core.influxdb3"></a>

## quixstreams.sinks.core.influxdb3

<a id="quixstreams.sinks.core.influxdb3.InfluxDB3Sink"></a>

### InfluxDB3Sink

```python
class InfluxDB3Sink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/influxdb3.py#L43)

<a id="quixstreams.sinks.core.influxdb3.InfluxDB3Sink.__init__"></a>

#### InfluxDB3Sink.\_\_init\_\_

```python
def __init__(token: str,
             host: str,
             organization_id: str,
             database: str,
             measurement: MeasurementSetter,
             fields_keys: FieldsSetter = (),
             tags_keys: TagsSetter = (),
             time_key: Optional[str] = None,
             time_precision: TimePrecision = "ms",
             allow_missing_fields: bool = False,
             include_metadata_tags: bool = False,
             convert_ints_to_floats: bool = False,
             batch_size: int = 1000,
             enable_gzip: bool = True,
             request_timeout_ms: int = 10_000,
             debug: bool = False,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/influxdb3.py#L51)

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
- `measurement`: measurement name as a string.
Also accepts a single-argument callable that receives the current message
data as a dict and returns a string.
- `fields_keys`: an iterable (list) of strings used as InfluxDB "fields".
Also accepts a single-argument callable that receives the current message
data as a dict and returns an iterable of strings.
- If present, it must not overlap with "tags_keys".
- If empty, the whole record value will be used.
>***NOTE*** The fields' values can only be strings, floats, integers, or booleans.
Default - `()`.
- `tags_keys`: an iterable (list) of strings used as InfluxDB "tags".
Also accepts a single-argument callable that receives the current message
data as a dict and returns an iterable of strings.
- If present, it must not overlap with "fields_keys".
- Given keys are popped from the value dictionary since the same key
cannot be both a tag and field.
- If empty, no tags will be sent.
>***NOTE***: InfluxDB client always converts tag values to strings.
Default - `()`.
- `time_key`: a key to be used as "time" when writing to InfluxDB.
By default, the record timestamp will be used with "ms" time precision.
When using a custom key, you may need to adjust the `time_precision` setting
to match.
- `time_precision`: a time precision to use when writing to InfluxDB.
Possible values: "ms", "ns", "us", "s".
Default - `"ms"`.
- `allow_missing_fields`: if `True`, skip the missing fields keys, else raise `KeyError`.
Default - `False`
- `include_metadata_tags`: if True, includes record's key, topic,
and partition as tags.
Default - `False`.
- `convert_ints_to_floats`: if True, converts all integer values to floats.
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
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sinks.core"></a>

## quixstreams.sinks.core

<a id="quixstreams.sinks.core.csv"></a>

## quixstreams.sinks.core.csv

<a id="quixstreams.sinks.core.csv.CSVSink"></a>

### CSVSink

```python
class CSVSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/csv.py#L9)

<a id="quixstreams.sinks.core.csv.CSVSink.__init__"></a>

#### CSVSink.\_\_init\_\_

```python
def __init__(path: str,
             dialect: str = "excel",
             key_serializer: Callable[[Any], str] = str,
             value_serializer: Callable[[Any], str] = json.dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/core/csv.py#L10)

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
Default - `str`.
- `value_serializer`: a callable to convert values to strings.
Default - `json.dumps`.

<a id="quixstreams.sinks"></a>

## quixstreams.sinks

<a id="quixstreams.sinks.community.postgresql"></a>

## quixstreams.sinks.community.postgresql

<a id="quixstreams.sinks.community.postgresql.PostgreSQLSink"></a>

### PostgreSQLSink

```python
class PostgreSQLSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/postgresql.py#L53)

<a id="quixstreams.sinks.community.postgresql.PostgreSQLSink.__init__"></a>

#### PostgreSQLSink.\_\_init\_\_

```python
def __init__(host: str,
             port: int,
             dbname: str,
             user: str,
             password: str,
             table_name: str,
             schema_auto_update: bool = True,
             connection_timeout_seconds: int = 30,
             statement_timeout_seconds: int = 30,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None,
             **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/postgresql.py#L54)

A connector to sink topic data to PostgreSQL.

**Arguments**:

- `host`: PostgreSQL server address.
- `port`: PostgreSQL server port.
- `dbname`: PostgreSQL database name.
- `user`: Database user name.
- `password`: Database user password.
- `table_name`: PostgreSQL table name.
- `schema_auto_update`: Automatically update the schema when new columns are detected.
- `connection_timeout_seconds`: Timeout for connection.
- `statement_timeout_seconds`: Timeout for DDL operations such as table
creation or schema updates.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.
- `kwargs`: Additional parameters for `psycopg2.connect`.

<a id="quixstreams.sinks.community.elasticsearch"></a>

## quixstreams.sinks.community.elasticsearch

<a id="quixstreams.sinks.community.elasticsearch.ElasticsearchSink"></a>

### ElasticsearchSink

```python
class ElasticsearchSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/elasticsearch.py#L38)

Pushes data to an ElasticSearch index.

By default, uses the kafka message key as the document ID, and dynamically generates
the field types.

You can pass your own type mapping or document ID setter for custom behavior.

<a id="quixstreams.sinks.community.elasticsearch.ElasticsearchSink.__init__"></a>

#### ElasticsearchSink.\_\_init\_\_

```python
def __init__(url: str,
             index: str,
             mapping: Optional[dict] = None,
             document_id_setter: Optional[Callable[
                 [SinkItem], Optional[str]]] = _default_document_id_setter,
             batch_size: int = 500,
             max_bulk_retries: int = 3,
             ignore_bulk_upload_errors: bool = False,
             add_message_metadata: bool = False,
             add_topic_metadata: bool = False,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None,
             **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/elasticsearch.py#L48)

**Arguments**:

- `url`: the ElasticSearch host url
- `index`: the ElasticSearch index name
- `mapping`: a custom mapping; the default dynamically maps all field types
- `document_id_setter`: how to select the document id; the default is the Kafka message key
- `batch_size`: how large each chunk size is with bulk
- `max_bulk_retries`: number of retry attempts for each bulk batch
- `ignore_bulk_upload_errors`: ignore any errors that occur when attempting an upload
- `add_message_metadata`: add key, timestamp, and headers as `__{field}`
- `add_topic_metadata`: add topic, partition, and offset as `__{field}`
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.
- `kwargs`: additional kwargs that are passed to the ElasticSearch client

<a id="quixstreams.sinks.community.file.formats.parquet"></a>

## quixstreams.sinks.community.file.formats.parquet

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat"></a>

### ParquetFormat

```python
class ParquetFormat(Format)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L16)

Serializes batches of messages into Parquet format.

This class provides functionality to serialize a `SinkBatch` into bytes
in Parquet format using PyArrow. It allows setting the file extension
and compression algorithm used for the Parquet files.

This format does not support appending to existing files.

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat.__init__"></a>

#### ParquetFormat.\_\_init\_\_

```python
def __init__(file_extension: str = ".parquet",
             compression: Compression = "snappy") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L29)

Initializes the ParquetFormat.

**Arguments**:

- `file_extension`: The file extension to use for output files.
Defaults to ".parquet".
- `compression`: The compression algorithm to use for Parquet files.
Allowed values are "none", "snappy", "gzip", "brotli", "lz4",
or "zstd". Defaults to "snappy".

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat.file_extension"></a>

#### ParquetFormat.file\_extension

```python
@property
def file_extension() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L47)

Returns the file extension used for output files.

**Returns**:

The file extension as a string.

<a id="quixstreams.sinks.community.file.formats.parquet.ParquetFormat.serialize"></a>

#### ParquetFormat.serialize

```python
def serialize(batch: SinkBatch) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/parquet.py#L55)

Serializes a `SinkBatch` into bytes in Parquet format.

Each item in the batch is converted into a dictionary with "_timestamp",
"_key", and the keys from the message value. If the message key is in
bytes, it is decoded to a string.

Missing fields in messages are filled with `None` to ensure all rows
have the same columns.

**Arguments**:

- `batch`: The `SinkBatch` to serialize.

**Returns**:

The serialized batch as bytes in Parquet format.

<a id="quixstreams.sinks.community.file.formats"></a>

## quixstreams.sinks.community.file.formats

<a id="quixstreams.sinks.community.file.formats.InvalidFormatError"></a>

### InvalidFormatError

```python
class InvalidFormatError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/__init__.py#L17)

Raised when the format is specified incorrectly.

<a id="quixstreams.sinks.community.file.formats.resolve_format"></a>

#### resolve\_format

```python
def resolve_format(format: Union[FormatName, Format]) -> Format
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/__init__.py#L23)

Resolves the format into a `Format` instance.

**Arguments**:

- `format`: The format to resolve, either a format name ("json",
"parquet") or a `Format` instance.

**Raises**:

- `InvalidFormatError`: If the format name is invalid.

**Returns**:

An instance of `Format` corresponding to the specified format.

<a id="quixstreams.sinks.community.file.formats.json"></a>

## quixstreams.sinks.community.file.formats.json

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat"></a>

### JSONFormat

```python
class JSONFormat(Format)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L14)

Serializes batches of messages into JSON Lines format with optional gzip
compression.

This class provides functionality to serialize a `SinkBatch` into bytes
in JSON Lines format. It supports optional gzip compression and allows
for custom JSON serialization through the `dumps` parameter.

This format supports appending to existing files.

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat.__init__"></a>

#### JSONFormat.\_\_init\_\_

```python
def __init__(file_extension: str = ".jsonl",
             compress: bool = False,
             dumps: Optional[Callable[[Any], str]] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L28)

Initializes the JSONFormat.

**Arguments**:

- `file_extension`: The file extension to use for output files.
Defaults to ".jsonl".
- `compress`: If `True`, compresses the output using gzip and
appends ".gz" to the file extension. Defaults to `False`.
- `dumps`: A custom function to serialize objects to JSON-formatted
strings. If provided, the `compact` option is ignored.

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat.file_extension"></a>

#### JSONFormat.file\_extension

```python
@property
def file_extension() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L57)

Returns the file extension used for output files.

**Returns**:

The file extension as a string.

<a id="quixstreams.sinks.community.file.formats.json.JSONFormat.serialize"></a>

#### JSONFormat.serialize

```python
def serialize(batch: SinkBatch) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/json.py#L65)

Serializes a `SinkBatch` into bytes in JSON Lines format.

Each item in the batch is converted into a JSON object with
"_timestamp", "_key", and "_value" fields. If the message key is
in bytes, it is decoded to a string.

**Arguments**:

- `batch`: The `SinkBatch` to serialize.

**Returns**:

The serialized batch in JSON Lines format, optionally
compressed with gzip.

<a id="quixstreams.sinks.community.file.formats.base"></a>

## quixstreams.sinks.community.file.formats.base

<a id="quixstreams.sinks.community.file.formats.base.Format"></a>

### Format

```python
class Format(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L8)

Base class for formatting batches in file sinks.

This abstract base class defines the interface for batch formatting
in file sinks. Subclasses should implement the `file_extension`
property and the `serialize` method to define how batches are
formatted and saved.

<a id="quixstreams.sinks.community.file.formats.base.Format.file_extension"></a>

#### Format.file\_extension

```python
@property
@abstractmethod
def file_extension() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L20)

Returns the file extension used for output files.

**Returns**:

The file extension as a string.

<a id="quixstreams.sinks.community.file.formats.base.Format.supports_append"></a>

#### Format.supports\_append

```python
@property
@abstractmethod
def supports_append() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L30)

Indicates if the format supports appending data to an existing file.

**Returns**:

True if appending is supported, otherwise False.

<a id="quixstreams.sinks.community.file.formats.base.Format.serialize"></a>

#### Format.serialize

```python
@abstractmethod
def serialize(batch: SinkBatch) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/formats/base.py#L39)

Serializes a batch of messages into bytes.

**Arguments**:

- `batch`: The batch of messages to serialize.

**Returns**:

The serialized batch as bytes.

<a id="quixstreams.sinks.community.file.sink"></a>

## quixstreams.sinks.community.file.sink

<a id="quixstreams.sinks.community.file.sink.FileSink"></a>

### FileSink

```python
class FileSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/sink.py#L17)

A sink that writes data batches to files using configurable formats and
destinations.

The sink groups messages by their topic and partition, ensuring data from the
same source is stored together. Each batch is serialized using the specified
format (e.g., JSON, Parquet) before being written to the configured
destination.

The destination determines the storage location and write behavior. By default,
it uses LocalDestination for writing to the local filesystem, but can be
configured to use other storage backends (e.g., cloud storage).

<a id="quixstreams.sinks.community.file.sink.FileSink.__init__"></a>

#### FileSink.\_\_init\_\_

```python
def __init__(
    directory: str = "",
    format: Union[FormatName, Format] = "json",
    destination: Optional[Destination] = None,
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/sink.py#L31)

Initialize the FileSink with the specified configuration.

**Arguments**:

- `directory`: Base directory path for storing files. Defaults to
current directory.
- `format`: Data serialization format, either as a string
("json", "parquet") or a Format instance.
- `destination`: Storage destination handler. Defaults to
LocalDestination if not specified.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sinks.community.file.sink.FileSink.write"></a>

#### FileSink.write

```python
def write(batch: SinkBatch) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/sink.py#L67)

Write a batch of data using the configured format and destination.

The method performs the following steps:
1. Serializes the batch data using the configured format
2. Writes the serialized data to the destination
3. Handles any write failures by raising a backpressure error

**Arguments**:

- `batch`: The batch of data to write.

**Raises**:

- `SinkBackpressureError`: If the write operation fails, indicating
that the sink needs backpressure with a 5-second retry delay.

<a id="quixstreams.sinks.community.file.destinations.local"></a>

## quixstreams.sinks.community.file.destinations.local

<a id="quixstreams.sinks.community.file.destinations.local.LocalDestination"></a>

### LocalDestination

```python
class LocalDestination(Destination)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/local.py#L15)

A destination that writes data to the local filesystem.

Handles writing data to local files with support for both creating new files
and appending to existing ones.

<a id="quixstreams.sinks.community.file.destinations.local.LocalDestination.__init__"></a>

#### LocalDestination.\_\_init\_\_

```python
def __init__(append: bool = False) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/local.py#L22)

Initialize the local destination.

**Arguments**:

- `append`: If True, append to existing files instead of creating new
ones. Defaults to False.

<a id="quixstreams.sinks.community.file.destinations.local.LocalDestination.set_extension"></a>

#### LocalDestination.set\_extension

```python
def set_extension(format: Format) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/local.py#L35)

Set the file extension and validate append mode compatibility.

**Arguments**:

- `format`: The Format instance that defines the file extension.

**Raises**:

- `ValueError`: If append mode is enabled but the format doesn't
support appending.

<a id="quixstreams.sinks.community.file.destinations.local.LocalDestination.write"></a>

#### LocalDestination.write

```python
def write(data: bytes, batch: SinkBatch) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/local.py#L46)

Write data to a local file.

**Arguments**:

- `data`: The serialized data to write.
- `batch`: The batch information containing topic and partition details.

<a id="quixstreams.sinks.community.file.destinations.azure"></a>

## quixstreams.sinks.community.file.destinations.azure

<a id="quixstreams.sinks.community.file.destinations.azure.AzureContainerNotFoundError"></a>

### AzureContainerNotFoundError

```python
class AzureContainerNotFoundError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/azure.py#L24)

Raised when the specified Azure File container does not exist.

<a id="quixstreams.sinks.community.file.destinations.azure.AzureContainerAccessDeniedError"></a>

### AzureContainerAccessDeniedError

```python
class AzureContainerAccessDeniedError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/azure.py#L28)

Raised when the specified Azure File container access is denied.

<a id="quixstreams.sinks.community.file.destinations.azure.AzureFileDestination"></a>

### AzureFileDestination

```python
class AzureFileDestination(Destination)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/azure.py#L32)

A destination that writes data to Microsoft Azure File.

Handles writing data to Azure containers using the Azure Blob SDK. Credentials can
be provided directly or via environment variables.

<a id="quixstreams.sinks.community.file.destinations.azure.AzureFileDestination.__init__"></a>

#### AzureFileDestination.\_\_init\_\_

```python
def __init__(connection_string: str, container: str) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/azure.py#L40)

Initialize the Azure File destination.

**Arguments**:

- `connection_string`: Azure client authentication string.
- `container`: Azure container name.

**Raises**:

- `AzureContainerNotFoundError`: If the specified container doesn't exist.
- `AzureContainerAccessDeniedError`: If access to the container is denied.

<a id="quixstreams.sinks.community.file.destinations.azure.AzureFileDestination.write"></a>

#### AzureFileDestination.write

```python
def write(data: bytes, batch: SinkBatch) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/azure.py#L94)

Write data to Azure.

**Arguments**:

- `data`: The serialized data to write.
- `batch`: The batch information containing topic and partition details.

<a id="quixstreams.sinks.community.file.destinations"></a>

## quixstreams.sinks.community.file.destinations

<a id="quixstreams.sinks.community.file.destinations.s3"></a>

## quixstreams.sinks.community.file.destinations.s3

<a id="quixstreams.sinks.community.file.destinations.s3.S3BucketNotFoundError"></a>

### S3BucketNotFoundError

```python
class S3BucketNotFoundError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/s3.py#L14)

Raised when the specified S3 bucket does not exist.

<a id="quixstreams.sinks.community.file.destinations.s3.S3BucketAccessDeniedError"></a>

### S3BucketAccessDeniedError

```python
class S3BucketAccessDeniedError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/s3.py#L18)

Raised when the specified S3 bucket access is denied.

<a id="quixstreams.sinks.community.file.destinations.s3.S3Destination"></a>

### S3Destination

```python
class S3Destination(Destination)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/s3.py#L22)

A destination that writes data to Amazon S3.

Handles writing data to S3 buckets using the AWS SDK. Credentials can be
provided directly or via environment variables.

<a id="quixstreams.sinks.community.file.destinations.s3.S3Destination.__init__"></a>

#### S3Destination.\_\_init\_\_

```python
def __init__(bucket: str,
             aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
             aws_secret_access_key: Optional[str] = getenv(
                 "AWS_SECRET_ACCESS_KEY"),
             region_name: Optional[str] = getenv("AWS_REGION",
                                                 getenv("AWS_DEFAULT_REGION")),
             endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
             **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/s3.py#L29)

Initialize the S3 destination.

**Arguments**:

- `bucket`: Name of the S3 bucket to write to.
- `aws_access_key_id`: AWS access key ID. Defaults to AWS_ACCESS_KEY_ID
environment variable.
- `aws_secret_access_key`: AWS secret access key. Defaults to
AWS_SECRET_ACCESS_KEY environment variable.
- `region_name`: AWS region name. Defaults to AWS_REGION or
AWS_DEFAULT_REGION environment variable.
- `endpoint_url`: the endpoint URL to use; only required for connecting
to a locally hosted S3.
NOTE: can alternatively set the AWS_ENDPOINT_URL_S3 environment variable
- `kwargs`: Additional keyword arguments passed to boto3.client.

**Raises**:

- `S3BucketNotFoundError`: If the specified bucket doesn't exist.
- `S3BucketAccessDeniedError`: If access to the bucket is denied.

<a id="quixstreams.sinks.community.file.destinations.s3.S3Destination.write"></a>

#### S3Destination.write

```python
def write(data: bytes, batch: SinkBatch) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/s3.py#L89)

Write data to S3.

**Arguments**:

- `data`: The serialized data to write.
- `batch`: The batch information containing topic and partition details.

<a id="quixstreams.sinks.community.file.destinations.base"></a>

## quixstreams.sinks.community.file.destinations.base

<a id="quixstreams.sinks.community.file.destinations.base.Destination"></a>

### Destination

```python
class Destination(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/base.py#L16)

Abstract base class for defining where and how data should be stored.

Destinations handle the storage of serialized data, whether that's to local
disk, cloud storage, or other locations. They manage the physical writing of
data while maintaining a consistent directory/path structure based on topics
and partitions.

<a id="quixstreams.sinks.community.file.destinations.base.Destination.setup"></a>

#### Destination.setup

```python
@abstractmethod
def setup()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/base.py#L29)

Authenticate and validate connection here

<a id="quixstreams.sinks.community.file.destinations.base.Destination.write"></a>

#### Destination.write

```python
@abstractmethod
def write(data: bytes, batch: SinkBatch) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/base.py#L34)

Write the serialized data to storage.

**Arguments**:

- `data`: The serialized data to write.
- `batch`: The batch information containing topic, partition and offset
details.

<a id="quixstreams.sinks.community.file.destinations.base.Destination.set_directory"></a>

#### Destination.set\_directory

```python
def set_directory(directory: str) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/base.py#L43)

Configure the base directory for storing files.

**Arguments**:

- `directory`: The base directory path where files will be stored.

**Raises**:

- `ValueError`: If the directory path contains invalid characters.
Only alphanumeric characters (a-zA-Z0-9), spaces, dots, slashes, and
underscores are allowed.

<a id="quixstreams.sinks.community.file.destinations.base.Destination.set_extension"></a>

#### Destination.set\_extension

```python
def set_extension(format: Format) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/file/destinations/base.py#L64)

Set the file extension based on the format.

**Arguments**:

- `format`: The Format instance that defines the file extension.

<a id="quixstreams.sinks.community.file"></a>

## quixstreams.sinks.community.file

<a id="quixstreams.sinks.community.mongodb"></a>

## quixstreams.sinks.community.mongodb

<a id="quixstreams.sinks.community.mongodb.MongoDBSink"></a>

### MongoDBSink

```python
class MongoDBSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/mongodb.py#L65)

<a id="quixstreams.sinks.community.mongodb.MongoDBSink.__init__"></a>

#### MongoDBSink.\_\_init\_\_

```python
def __init__(url: str,
             db: str,
             collection: str,
             document_matcher: Callable[
                 [SinkItem], MongoQueryFilter] = _default_document_matcher,
             update_method: Literal["UpdateOne", "UpdateMany",
                                    "ReplaceOne"] = "UpdateOne",
             upsert: bool = True,
             add_message_metadata: bool = False,
             add_topic_metadata: bool = False,
             authentication_timeout_ms: int = 15000,
             value_selector: Optional[Callable[[MongoValue],
                                               MongoValue]] = None,
             **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/mongodb.py#L66)

A connector to sink processed data to MongoDB in batches.

**Arguments**:

- `url`: MongoDB url; most commonly `mongodb://username:password@host:port`
- `db`: MongoDB database name
- `collection`: MongoDB collection name
- `document_matcher`: How documents are selected to update.
A callable that accepts a `BatchItem` and returns a MongoDB "query filter".
If no match, will insert if `upsert=True`, where `_id` will be either the
included value if specified, else a random `ObjectId`.
- Default: matches on `_id`, with `_id` assumed to be the kafka key.
- `upsert`: Create documents if no matches with `document_matcher`.
- `update_method`: How documents found with `document_matcher` are updated.
'Update*' options will only update fields included in the kafka message.
'Replace*' option fully replaces the document with the contents of kafka message.
"UpdateOne": Updates the first matching document (usually based on `_id`).
"UpdateMany": Updates ALL matching documents (usually NOT based on `_id`).
"ReplaceOne": Replaces the first matching document (usually based on `_id`).
Default: "UpdateOne".
- `add_message_metadata`: add key, timestamp, and headers as `__{field}`
- `add_topic_metadata`: add topic, partition, and offset as `__{field}`
- `value_selector`: An optional callable that allows final editing of the
outgoing document (right before submitting it).
Largely used when a field is necessary for `document_matcher`,
but not otherwise.
NOTE: metadata is added before this step, so don't accidentally
exclude it here!

<a id="quixstreams.sinks.community.mongodb.MongoDBSink.write"></a>

#### MongoDBSink.write

```python
def write(batch: SinkBatch) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/mongodb.py#L153)

Note: Transactions could be an option here, but then each record requires a
network call, and the transaction has size limits...so `bulk_write` is used
instead, with the downside that duplicate writes may occur if errors arise.

<a id="quixstreams.sinks.community.bigquery"></a>

## quixstreams.sinks.community.bigquery

<a id="quixstreams.sinks.community.bigquery.BigQuerySink"></a>

### BigQuerySink

```python
class BigQuerySink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/bigquery.py#L56)

<a id="quixstreams.sinks.community.bigquery.BigQuerySink.__init__"></a>

#### BigQuerySink.\_\_init\_\_

```python
def __init__(project_id: str,
             location: str,
             dataset_id: str,
             table_name: str,
             service_account_json: Optional[str] = None,
             schema_auto_update: bool = True,
             ddl_timeout: float = 10.0,
             insert_timeout: float = 10.0,
             retry_timeout: float = 30.0,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None,
             **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/bigquery.py#L57)

A connector to sink processed data to Google Cloud BigQuery.

It batches the processed records in memory per topic partition, and flushes them to BigQuery at the checkpoint.

>***NOTE***: BigQuerySink can accept only dictionaries.
> If the record values are not dicts, you need to convert them to dicts before
> sinking.

The column names and types are inferred from individual records.
Each key in the record's dictionary will be inserted as a column to the resulting BigQuery table.

If the column is not present in the schema, the sink will try to add new nullable columns on the fly with types inferred from individual values.
The existing columns will not be affected.
To disable this behavior, pass `schema_auto_update=False` and define the necessary schema upfront.
The minimal schema must define two columns: "timestamp" of type TIMESTAMP, and "__key" with a type of the expected message key.

**Arguments**:

- `project_id`: a Google project id.
- `location`: a BigQuery location.
- `dataset_id`: a BigQuery dataset id.
If the dataset does not exist, the sink will try to create it.
- `table_name`: BigQuery table name.
If the table does not exist, the sink will try to create it with a default schema.
- `service_account_json`: an optional JSON string with service account credentials
to connect to BigQuery.
The internal `google.cloud.bigquery.Client` will use the Application Default Credentials if not provided.
See https://cloud.google.com/docs/authentication/provide-credentials-adc for more info.
Default - `None`.
- `schema_auto_update`: if True, the sink will try to create a dataset and a table if they don't exist.
It will also add missing columns on the fly with types inferred from individual values.
- `ddl_timeout`: a timeout for a single DDL operation (adding tables, columns, etc.).
Default - 10s.
- `insert_timeout`: a timeout for a single INSERT operation.
Default - 10s.
- `retry_timeout`: a total timeout for each request to BigQuery API.
During this timeout, a request can be retried according
to the client's default retrying policy.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.
- `kwargs`: Additional keyword arguments passed to `bigquery.Client`.

<a id="quixstreams.sinks.community.kinesis"></a>

## quixstreams.sinks.community.kinesis

<a id="quixstreams.sinks.community.kinesis.KinesisStreamNotFoundError"></a>

### KinesisStreamNotFoundError

```python
class KinesisStreamNotFoundError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/kinesis.py#L28)

Raised when the specified Kinesis stream does not exist.

<a id="quixstreams.sinks.community.kinesis.KinesisSink"></a>

### KinesisSink

```python
class KinesisSink(BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/kinesis.py#L32)

<a id="quixstreams.sinks.community.kinesis.KinesisSink.__init__"></a>

#### KinesisSink.\_\_init\_\_

```python
def __init__(
        stream_name: str,
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        region_name: Optional[str] = getenv("AWS_REGION",
                                            getenv("AWS_DEFAULT_REGION")),
        aws_endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_KINESIS"),
        value_serializer: Callable[[Any], str] = json.dumps,
        key_serializer: Callable[[Any], str] = bytes.decode,
        on_client_connect_success: Optional[
            ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[
            ClientConnectFailureCallback] = None,
        **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/kinesis.py#L33)

Initialize the KinesisSink.

**Arguments**:

- `stream_name`: Kinesis stream name.
- `aws_access_key_id`: AWS access key ID.
- `aws_secret_access_key`: AWS secret access key.
- `region_name`: AWS region name (e.g., 'us-east-1').
- `value_serializer`: Function to serialize the value to string
(defaults to json.dumps).
- `key_serializer`: Function to serialize the key to string
(defaults to bytes.decode).
- `kwargs`: Additional keyword arguments passed to boto3.client.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sinks.community.kinesis.KinesisSink.add"></a>

#### KinesisSink.add

```python
def add(value: Any, key: Any, timestamp: int, headers: HeadersTuples,
        topic: str, partition: int, offset: int) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/kinesis.py#L103)

Buffer a record for the Kinesis stream.

Records are buffered until the batch size reaches 500, at which point
they are sent immediately. If the batch size is less than 500, records
will be sent when the flush method is called.

<a id="quixstreams.sinks.community.kinesis.KinesisSink.flush"></a>

#### KinesisSink.flush

```python
def flush() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/kinesis.py#L133)

Flush all records bufferred so far.

This method sends any outstanding records that have not yet been sent
because the batch size was less than 500. It waits for all futures to
complete, ensuring that all records are successfully sent to the Kinesis
stream.

<a id="quixstreams.sinks.community.neo4j"></a>

## quixstreams.sinks.community.neo4j

<a id="quixstreams.sinks.community.neo4j.Neo4jSink"></a>

### Neo4jSink

```python
class Neo4jSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/neo4j.py#L32)

<a id="quixstreams.sinks.community.neo4j.Neo4jSink.__init__"></a>

#### Neo4jSink.\_\_init\_\_

```python
def __init__(host: str,
             port: int,
             username: str,
             password: str,
             cypher_query: str,
             chunk_size: int = 10000,
             **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/neo4j.py#L33)

A connector to sink processed data to Neo4j.

**Arguments**:

- `host`: The Neo4j database hostname.
- `port`: The Neo4j database port.
- `username`: The Neo4j database username.
- `password`: The Neo4j database password.
- `cypher_query`: A Cypher Query to execute on each record.
Behavior attempts to match other Neo4j connectors:
- Uses "dot traversal" for (nested) dict key access; ex: "col_x.col_y.col_z"
- Message value is bound to the alias "event"; ex: "event.field_a".
- Message key, value, header and timestamp are bound to "__{attr}"; ex: "__key".
- `chunk_size`: Adjust the size of a Neo4j transactional chunk.
- This does NOT affect how many records can be written/flushed at once.
- The chunks are committed only if ALL of them succeed.
- Larger chunks are generally more efficient, but can encounter size issues.
- This is only necessary to adjust when messages are especially large.
- `kwargs`: Additional keyword arguments passed to the
`neo4j.GraphDatabase.driver` instance.

Example Usage:

```
from quixstreams import Application
from quixstreams.sinks.community.neo4j import Neo4jSink

app = Application(broker_address="localhost:9092")
topic = app.topic("topic-name")

# records structured as:
# {"name": {"first": "John", "last": "Doe"}, "age": 28, "city": "Los Angeles"}

# This assumes the given City nodes exist.
# Notice the use of "event" to reference the message value.
# Could also do things like __key, or __value.name.first.
cypher_query = '''
MERGE (p:Person {first_name: event.name.first, last_name: event.name.last})
SET p.age = event.age
MERGE (c:City {name: event.city})
MERGE (p)-[:LIVES_IN]->(c)
'''

# Configure the sink
neo4j_sink = Neo4jSink(
    host="localhost",
    port=7687,
    username="neo4j",
    password="local_password",
    cypher_query=cypher_query,
)

sdf = app.dataframe(topic=topic)
sdf.sink(neo4j_sink)

if __name__ == "__main__":
    app.run()
```

<a id="quixstreams.sinks.community"></a>

## quixstreams.sinks.community

This module contains Sinks developed and maintained by the members of Quix Streams community.

<a id="quixstreams.sinks.community.redis"></a>

## quixstreams.sinks.community.redis

<a id="quixstreams.sinks.community.redis.RedisSink"></a>

### RedisSink

```python
class RedisSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/redis.py#L26)

<a id="quixstreams.sinks.community.redis.RedisSink.__init__"></a>

#### RedisSink.\_\_init\_\_

```python
def __init__(host: str,
             port: int,
             db: int,
             value_serializer: Callable[[Any], Union[bytes, str]] = json.dumps,
             key_serializer: Optional[Callable[[Any, Any], Union[bytes,
                                                                 str]]] = None,
             password: Optional[str] = None,
             socket_timeout: float = 30.0,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None,
             **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/redis.py#L27)

A connector to sink processed data to Redis.

It batches the processed records in memory per topic partition, and flushes them to Redis at the checkpoint.

**Arguments**:

- `host`: Redis host.
- `port`: Redis port.
- `db`: Redis DB number.
- `value_serializer`: a callable to serialize the value to string or bytes
(defaults to json.dumps).
- `key_serializer`: an optional callable to serialize the key to string or bytes.
If not provided, the Kafka message key will be used as is.
- `password`: Redis password, optional.
- `socket_timeout`: Redis socket timeout.
Default - 30s.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.
- `kwargs`: Additional keyword arguments passed to the `redis.Redis` instance.

<a id="quixstreams.sinks.community.iceberg"></a>

## quixstreams.sinks.community.iceberg

<a id="quixstreams.sinks.community.iceberg.AWSIcebergConfig"></a>

### AWSIcebergConfig

```python
class AWSIcebergConfig(BaseIcebergConfig)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L49)

<a id="quixstreams.sinks.community.iceberg.AWSIcebergConfig.__init__"></a>

#### AWSIcebergConfig.\_\_init\_\_

```python
def __init__(aws_s3_uri: str,
             aws_region: Optional[str] = None,
             aws_access_key_id: Optional[str] = None,
             aws_secret_access_key: Optional[str] = None,
             aws_session_token: Optional[str] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L50)

Configure IcebergSink to work with AWS Glue.

**Arguments**:

- `aws_s3_uri`: The S3 URI where the table data will be stored
(e.g., 's3://your-bucket/warehouse/').
- `aws_region`: The AWS region for the S3 bucket and Glue catalog.
- `aws_access_key_id`: the AWS access key ID.
NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
when using AWS Glue.
- `aws_secret_access_key`: the AWS secret access key.
NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
when using AWS Glue.
- `aws_session_token`: a session token (or will be generated for you).
NOTE: can alternatively set the AWS_SESSION_TOKEN environment variable when
using AWS Glue.

<a id="quixstreams.sinks.community.iceberg.IcebergSink"></a>

### IcebergSink

```python
class IcebergSink(BatchingSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L83)

IcebergSink writes batches of data to an Apache Iceberg table.

The data will by default include the kafka message key, value, and timestamp.

It serializes incoming data batches into Parquet format and appends them to the
Iceberg table, updating the table schema as necessary.

Currently, supports Apache Iceberg hosted in:

- AWS

Supported data catalogs:

- AWS Glue

**Arguments**:

- `table_name`: The name of the Iceberg table.
- `config`: An IcebergConfig with all the various connection parameters.
- `data_catalog_spec`: data cataloger to use (ex. for AWS Glue, "aws_glue").
- `schema`: The Iceberg table schema. If None, a default schema is used.
- `partition_spec`: The partition specification for the table.
If None, a default is used.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
    Callback should accept the raised Exception as an argument.
    Callback must resolve (or propagate/re-raise) the Exception.

Example setup using an AWS-hosted Iceberg with AWS Glue:

```
from quixstreams import Application
from quixstreams.sinks.community.iceberg import IcebergSink, AWSIcebergConfig

# Configure S3 bucket credentials
iceberg_config = AWSIcebergConfig(
    aws_s3_uri="", aws_region="", aws_access_key_id="", aws_secret_access_key=""
)

# Configure the sink to write data to S3 with the AWS Glue catalog spec
iceberg_sink = IcebergSink(
    table_name="glue.sink-test",
    config=iceberg_config,
    data_catalog_spec="aws_glue",
)

app = Application(broker_address='localhost:9092', auto_offset_reset="earliest")
topic = app.topic('sink_topic')

# Do some processing here
sdf = app.dataframe(topic=topic).print(metadata=True)

# Sink results to the IcebergSink
sdf.sink(iceberg_sink)


if __name__ == "__main__":
    # Start the application
    app.run()
```

<a id="quixstreams.sinks.community.iceberg.IcebergSink.write"></a>

#### IcebergSink.write

```python
def write(batch: SinkBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/iceberg.py#L199)

Writes a batch of data to the Iceberg table.

Implements retry logic to handle concurrent write conflicts.

**Arguments**:

- `batch`: The batch of data to write.

<a id="quixstreams.sinks.community.pubsub"></a>

## quixstreams.sinks.community.pubsub

<a id="quixstreams.sinks.community.pubsub.PubSubTopicNotFoundError"></a>

### PubSubTopicNotFoundError

```python
class PubSubTopicNotFoundError(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L30)

Raised when the specified topic does not exist.

<a id="quixstreams.sinks.community.pubsub.PubSubSink"></a>

### PubSubSink

```python
class PubSubSink(BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L34)

A sink that publishes messages to Google Cloud Pub/Sub.

<a id="quixstreams.sinks.community.pubsub.PubSubSink.__init__"></a>

#### PubSubSink.\_\_init\_\_

```python
def __init__(project_id: str,
             topic_id: str,
             service_account_json: Optional[str] = None,
             value_serializer: Callable[[Any], Union[bytes, str]] = json.dumps,
             key_serializer: Callable[[Any], str] = bytes.decode,
             flush_timeout: int = 5,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None,
             **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L37)

Initialize the PubSubSink.

**Arguments**:

- `project_id`: GCP project ID.
- `topic_id`: Pub/Sub topic ID.
- `service_account_json`: an optional JSON string with service account credentials
to connect to Pub/Sub.
The internal `PublisherClient` will use the Application Default Credentials if not provided.
See https://cloud.google.com/docs/authentication/provide-credentials-adc for more info.
Default - `None`.
- `value_serializer`: Function to serialize the value to string or bytes
(defaults to json.dumps).
- `key_serializer`: Function to serialize the key to string
(defaults to bytes.decode).
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.
- `kwargs`: Additional keyword arguments passed to PublisherClient.

<a id="quixstreams.sinks.community.pubsub.PubSubSink.add"></a>

#### PubSubSink.add

```python
def add(value: Any, key: Any, timestamp: int, headers: HeadersTuples,
        topic: str, partition: int, offset: int) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L104)

Publish a message to Pub/Sub.

<a id="quixstreams.sinks.community.pubsub.PubSubSink.flush"></a>

#### PubSubSink.flush

```python
def flush() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/community/pubsub.py#L137)

Wait for all publish operations to complete successfully.

<a id="quixstreams.sinks.base.sink"></a>

## quixstreams.sinks.base.sink

<a id="quixstreams.sinks.base.sink.BaseSink"></a>

### BaseSink

```python
class BaseSink(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L24)

This is a base class for all sinks.

Subclass it and implement its methods to create your own sink.

Note that Sinks are currently in beta, and their design may change over time.

<a id="quixstreams.sinks.base.sink.BaseSink.__init__"></a>

#### BaseSink.\_\_init\_\_

```python
def __init__(on_client_connect_success: Optional[
    ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L33)

**Arguments**:

- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sinks.base.sink.BaseSink.flush"></a>

#### BaseSink.flush

```python
@abc.abstractmethod
def flush()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L54)

This method is triggered by the Checkpoint class when it commits.

You can use `flush()` to write the batched data to the destination (in case of
a batching sink), or confirm the delivery of the previously sent messages
(in case of a streaming sink).

If flush() fails, the checkpoint will be aborted.

<a id="quixstreams.sinks.base.sink.BaseSink.add"></a>

#### BaseSink.add

```python
@abc.abstractmethod
def add(value: Any, key: Any, timestamp: int, headers: HeadersTuples,
        topic: str, partition: int, offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L66)

This method is triggered on every new processed record being sent to this sink.

You can use it to accumulate batches of data before sending them outside, or
to send results right away in a streaming manner and confirm a delivery later
on flush().

<a id="quixstreams.sinks.base.sink.BaseSink.setup"></a>

#### BaseSink.setup

```python
def setup()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L84)

When applicable, set up the client here along with any validation to affirm a
valid/successful authentication/connection.

<a id="quixstreams.sinks.base.sink.BaseSink.start"></a>

#### BaseSink.start

```python
def start()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L90)

Called as part of `Application.run()` to initialize the sink's client.
Allows using a callback pattern around the connection attempt.

<a id="quixstreams.sinks.base.sink.BaseSink.on_paused"></a>

#### BaseSink.on\_paused

```python
def on_paused()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L101)

This method is triggered when the sink is paused due to backpressure, when
the `SinkBackpressureError` is raised.

Here you can react to the backpressure events.

<a id="quixstreams.sinks.base.sink.BatchingSink"></a>

### BatchingSink

```python
class BatchingSink(BaseSink)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L110)

A base class for batching sinks, that need to accumulate the data first before
sending it to the external destinations.

Examples: databases, objects stores, and other destinations where
writing every message is not optimal.

It automatically handles batching, keeping batches in memory per topic-partition.

You may subclass it and override the `write()` method to implement a custom
batching sink.

<a id="quixstreams.sinks.base.sink.BatchingSink.__init__"></a>

#### BatchingSink.\_\_init\_\_

```python
def __init__(on_client_connect_success: Optional[
    ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L126)

**Arguments**:

- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sinks.base.sink.BatchingSink.write"></a>

#### BatchingSink.write

```python
@abc.abstractmethod
def write(batch: SinkBatch)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L149)

This method implements actual writing to the external destination.

It may also raise `SinkBackpressureError` if the destination cannot accept new
writes at the moment.
When this happens, the accumulated batch is dropped and the app pauses the
corresponding topic partition.

<a id="quixstreams.sinks.base.sink.BatchingSink.add"></a>

#### BatchingSink.add

```python
def add(value: Any, key: Any, timestamp: int, headers: HeadersTuples,
        topic: str, partition: int, offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L159)

Add a new record to in-memory batch.

<a id="quixstreams.sinks.base.sink.BatchingSink.flush"></a>

#### BatchingSink.flush

```python
def flush()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L181)

Flush accumulated batches to the destination and drop them afterward.

<a id="quixstreams.sinks.base.sink.BatchingSink.on_paused"></a>

#### BatchingSink.on\_paused

```python
def on_paused()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/sink.py#L199)

When the destination is already backpressured, drop the accumulated batches.

<a id="quixstreams.sinks.base.batch"></a>

## quixstreams.sinks.base.batch

<a id="quixstreams.sinks.base.batch.SinkBatch"></a>

### SinkBatch

```python
class SinkBatch()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/batch.py#L12)

A batch to accumulate processed data by `BatchingSink` between the checkpoints.

Batches are created automatically by the implementations of `BatchingSink`.

**Arguments**:

- `topic`: a topic name
- `partition`: a partition number

<a id="quixstreams.sinks.base.batch.SinkBatch.iter_chunks"></a>

#### SinkBatch.iter\_chunks

```python
def iter_chunks(n: int) -> Iterable[Iterable[SinkItem]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/batch.py#L69)

Iterate over batch data in chunks of length n.
The last batch may be shorter.

<a id="quixstreams.sinks.base"></a>

## quixstreams.sinks.base

<a id="quixstreams.sinks.base.exceptions"></a>

## quixstreams.sinks.base.exceptions

<a id="quixstreams.sinks.base.exceptions.SinkBackpressureError"></a>

### SinkBackpressureError

```python
class SinkBackpressureError(QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sinks/base/exceptions.py#L6)

An exception to be raised by Sinks during flush() call

to signal a backpressure event to the application.

When raised, the app will drop the accumulated sink batches,
pause all assigned topic partitions for
a timeout specified in `retry_after`, and resume them when it's elapsed.

**Arguments**:

- `retry_after`: a timeout in seconds to pause for

<a id="quixstreams.sinks.base.manager"></a>

## quixstreams.sinks.base.manager

<a id="quixstreams.sinks.base.item"></a>

## quixstreams.sinks.base.item

<a id="quixstreams.utils"></a>

## quixstreams.utils

<a id="quixstreams.utils.settings"></a>

## quixstreams.utils.settings

<a id="quixstreams.utils.settings.BaseSettings"></a>

### BaseSettings

```python
class BaseSettings(_BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/utils/settings.py#L10)

<a id="quixstreams.utils.settings.BaseSettings.as_dict"></a>

#### BaseSettings.as\_dict

```python
def as_dict(plaintext_secrets: bool = False,
            include: Optional[Set[str]] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/utils/settings.py#L18)

Dump any non-empty config values as a dictionary.

**Arguments**:

- `plaintext_secrets`: whether secret values are plaintext or obscured (***)
- `include`: optional list of fields to be included in the dictionary

**Returns**:

a dictionary

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

<a id="quixstreams.utils.printing"></a>

## quixstreams.utils.printing

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

<a id="quixstreams.models.types.RawConfluentKafkaMessageProto"></a>

### RawConfluentKafkaMessageProto

```python
class RawConfluentKafkaMessageProto(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/types.py#L18)

An interface of `confluent_kafka.Message`.

Use it to not depend on exact implementation and simplify testing and type hints.

Instances of `confluent_kafka.Message` cannot be directly created from Python,
see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

<a id="quixstreams.models.types.SuccessfulConfluentKafkaMessageProto"></a>

### SuccessfulConfluentKafkaMessageProto

```python
class SuccessfulConfluentKafkaMessageProto(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/types.py#L51)

An interface of `confluent_kafka.Message` for successful message (messages that don't include an error)

Use it to not depend on exact implementation and simplify testing and type hints.

Instances of `confluent_kafka.Message` cannot be directly created from Python,
see https://github.com/confluentinc/confluent-kafka-python/issues/1535.

<a id="quixstreams.models.serializers.avro"></a>

## quixstreams.models.serializers.avro

<a id="quixstreams.models.serializers.avro.AvroSerializer"></a>

### AvroSerializer

```python
class AvroSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L26)

<a id="quixstreams.models.serializers.avro.AvroSerializer.__init__"></a>

#### AvroSerializer.\_\_init\_\_

```python
def __init__(
    schema: Schema,
    strict: bool = False,
    strict_allow_default: bool = False,
    disable_tuple_notation: bool = False,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L27)

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
- `schema_registry_client_config`: If provided, serialization is offloaded to Confluent's AvroSerializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's AvroSerializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.avro.AvroDeserializer"></a>

### AvroDeserializer

```python
class AvroDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L112)

<a id="quixstreams.models.serializers.avro.AvroDeserializer.__init__"></a>

#### AvroDeserializer.\_\_init\_\_

```python
def __init__(
    schema: Optional[Schema] = None,
    reader_schema: Optional[Schema] = None,
    return_record_name: bool = False,
    return_record_name_override: bool = False,
    return_named_type: bool = False,
    return_named_type_override: bool = False,
    handle_unicode_errors: str = "strict",
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None
)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/avro.py#L113)

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
- `schema_registry_client_config`: If provided, deserialization is offloaded to Confluent's AvroDeserializer.
Default - `None`

<a id="quixstreams.models.serializers.schema_registry"></a>

## quixstreams.models.serializers.schema\_registry

<a id="quixstreams.models.serializers.schema_registry.SchemaRegistryClientConfig"></a>

### SchemaRegistryClientConfig

```python
class SchemaRegistryClientConfig(BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/schema_registry.py#L22)

Configuration required to establish the connection with a Schema Registry.

**Arguments**:

- `url`: Schema Registry URL.
- `ssl_ca_location`: Path to CA certificate file used to verify the
Schema Registry's private key.
- `ssl_key_location`: Path to the client's private key (PEM) used for
authentication.
>***NOTE:*** `ssl_certificate_location` must also be set.
- `ssl_certificate_location`: Path to the client's public key (PEM) used
for authentication.
>***NOTE:*** May be set without `ssl_key_location` if the private key is
stored within the PEM as well.
- `basic_auth_user_info`: Client HTTP credentials in the form of
`username:password`.
>***NOTE:*** By default, userinfo is extracted from the URL if present.

<a id="quixstreams.models.serializers.schema_registry.SchemaRegistrySerializationConfig"></a>

### SchemaRegistrySerializationConfig

```python
class SchemaRegistrySerializationConfig(BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/schema_registry.py#L48)

Configuration that instructs Serializer how to handle communication with a

Schema Registry.

**Arguments**:

- `auto_register_schemas`: If True, automatically register the configured schema
with Confluent Schema Registry if it has not previously been associated with the
relevant subject (determined via subject.name.strategy). Defaults to True.
- `normalize_schemas`: Whether to normalize schemas, which will transform schemas
to have a consistent format, including ordering properties and references.
- `use_latest_version`: Whether to use the latest subject version for serialization.
>***NOTE:*** There is no check that the latest schema is backwards compatible with the
object being serialized. Defaults to False.
- `subject_name_strategy`: Callable(SerializationContext, str) -> str
Defines how Schema Registry subject names are constructed. Standard naming
strategies are defined in the confluent_kafka.schema_registry namespace.
Defaults to topic_subject_name_strategy.
- `skip_known_types`: Whether or not to skip known types when resolving
schema dependencies. Defaults to False.
- `reference_subject_name_strategy`: Defines how Schema Registry subject names
for schema references are constructed. Defaults to reference_subject_name_strategy.
- `use_deprecated_format`: Specifies whether the Protobuf serializer should
serialize message indexes without zig-zag encoding. This option must be explicitly
configured as older and newer Protobuf producers are incompatible.
If the consumers of the topic being produced to are using confluent-kafka-python <1.8,
then this property must be set to True until all old consumers have been upgraded.

<a id="quixstreams.models.serializers"></a>

## quixstreams.models.serializers

<a id="quixstreams.models.serializers.exceptions"></a>

## quixstreams.models.serializers.exceptions

<a id="quixstreams.models.serializers.exceptions.IgnoreMessage"></a>

### IgnoreMessage

```python
class IgnoreMessage(exceptions.QuixException)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/exceptions.py#L54)

Raise this exception from Deserializer.__call__ in order to ignore the processing
of the particular message.

<a id="quixstreams.models.serializers.quix"></a>

## quixstreams.models.serializers.quix

<a id="quixstreams.models.serializers.quix.QuixDeserializer"></a>

### QuixDeserializer

```python
class QuixDeserializer(JSONDeserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L76)

Handles Deserialization for any Quix-formatted topic.

Parses JSON data from either `TimeseriesData` and `EventData` (ignores the rest).

<a id="quixstreams.models.serializers.quix.QuixDeserializer.__init__"></a>

#### QuixDeserializer.\_\_init\_\_

```python
def __init__(loads: Callable[[Union[bytes, bytearray]], Any] = default_loads)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L83)

**Arguments**:

- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.split_values"></a>

#### QuixDeserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L100)

Each Quix message might contain data for multiple Rows.
This property informs the downstream processors about that, so they can
expect an Iterable instead of Mapping.

<a id="quixstreams.models.serializers.quix.QuixDeserializer.deserialize"></a>

#### QuixDeserializer.deserialize

```python
def deserialize(model_key: str, value: Union[List[Mapping],
                                             Mapping]) -> Iterable[Mapping]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L153)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L274)

<a id="quixstreams.models.serializers.quix.QuixSerializer.__init__"></a>

#### QuixSerializer.\_\_init\_\_

```python
def __init__(as_legacy: bool = True,
             dumps: Callable[[Any], Union[str, bytes]] = default_dumps)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L278)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L321)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/quix.py#L409)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L56)

A deserializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.BytesSerializer"></a>

### BytesSerializer

```python
class BytesSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L65)

A serializer to bypass bytes without any changes

<a id="quixstreams.models.serializers.simple_types.StringDeserializer"></a>

### StringDeserializer

```python
class StringDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L74)

<a id="quixstreams.models.serializers.simple_types.StringDeserializer.__init__"></a>

#### StringDeserializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L75)

Deserializes bytes to strings using the specified encoding.

**Arguments**:

- `codec`: string encoding
A wrapper around `confluent_kafka.serialization.StringDeserializer`.

<a id="quixstreams.models.serializers.simple_types.IntegerDeserializer"></a>

### IntegerDeserializer

```python
class IntegerDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L93)

Deserializes bytes to integers.

A wrapper around `confluent_kafka.serialization.IntegerDeserializer`.

<a id="quixstreams.models.serializers.simple_types.DoubleDeserializer"></a>

### DoubleDeserializer

```python
class DoubleDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L111)

Deserializes float to IEEE 764 binary64.

A wrapper around `confluent_kafka.serialization.DoubleDeserializer`.

<a id="quixstreams.models.serializers.simple_types.StringSerializer"></a>

### StringSerializer

```python
class StringSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L129)

<a id="quixstreams.models.serializers.simple_types.StringSerializer.__init__"></a>

#### StringSerializer.\_\_init\_\_

```python
def __init__(codec: str = "utf_8")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L130)

Serializes strings to bytes using the specified encoding.

**Arguments**:

- `codec`: string encoding

<a id="quixstreams.models.serializers.simple_types.IntegerSerializer"></a>

### IntegerSerializer

```python
class IntegerSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L142)

Serializes integers to bytes

<a id="quixstreams.models.serializers.simple_types.DoubleSerializer"></a>

### DoubleSerializer

```python
class DoubleSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/simple_types.py#L155)

Serializes floats to bytes

<a id="quixstreams.models.serializers.protobuf"></a>

## quixstreams.models.serializers.protobuf

<a id="quixstreams.models.serializers.protobuf.ProtobufSerializer"></a>

### ProtobufSerializer

```python
class ProtobufSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L24)

<a id="quixstreams.models.serializers.protobuf.ProtobufSerializer.__init__"></a>

#### ProtobufSerializer.\_\_init\_\_

```python
def __init__(
    msg_type: Type[Message],
    deterministic: bool = False,
    ignore_unknown_fields: bool = False,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L25)

Serializer that returns data in protobuf format.

Serialisation from a python dictionary can have a significant performance impact. An alternative is to pass the serializer an object of the `msg_type` class.

**Arguments**:

- `msg_type`: protobuf message class.
- `deterministic`: If true, requests deterministic serialization of the protobuf, with predictable ordering of map keys
Default - `False`
- `ignore_unknown_fields`: If True, do not raise errors for unknown fields.
Default - `False`
- `schema_registry_client_config`: If provided, serialization is offloaded to Confluent's ProtobufSerializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's ProtobufSerializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.protobuf.ProtobufDeserializer"></a>

### ProtobufDeserializer

```python
class ProtobufDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L110)

<a id="quixstreams.models.serializers.protobuf.ProtobufDeserializer.__init__"></a>

#### ProtobufDeserializer.\_\_init\_\_

```python
def __init__(
    msg_type: Type[Message],
    use_integers_for_enums: bool = False,
    preserving_proto_field_name: bool = False,
    to_dict: bool = True,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/protobuf.py#L111)

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
- `schema_registry_client_config`: If provided, deserialization is offloaded to Confluent's ProtobufDeserializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's ProtobufDeserializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.json"></a>

## quixstreams.models.serializers.json

<a id="quixstreams.models.serializers.json.JSONSerializer"></a>

### JSONSerializer

```python
class JSONSerializer(Serializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L34)

<a id="quixstreams.models.serializers.json.JSONSerializer.__init__"></a>

#### JSONSerializer.\_\_init\_\_

```python
def __init__(
    dumps: Callable[[Any], Union[str, bytes]] = default_dumps,
    schema: Optional[Mapping] = None,
    validator: Optional["_Validator"] = None,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None,
    schema_registry_serialization_config: Optional[
        SchemaRegistrySerializationConfig] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L35)

Serializer that returns data in json format.

**Arguments**:

- `dumps`: a function to serialize objects to json.
Default - :py:func:`quixstreams.utils.json.dumps`
- `schema`: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/`jsonschema.validators.Draft202012Validator`).
Default - `None`
- `validator`: A jsonschema validator used to validate the data. Takes precedences over the schema.
Default - `None`
- `schema_registry_client_config`: If provided, serialization is offloaded to Confluent's JSONSerializer.
Default - `None`
- `schema_registry_serialization_config`: Additional configuration for Confluent's JSONSerializer.
Default - `None`
>***NOTE:*** `schema_registry_client_config` must also be set.

<a id="quixstreams.models.serializers.json.JSONDeserializer"></a>

### JSONDeserializer

```python
class JSONDeserializer(Deserializer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L121)

<a id="quixstreams.models.serializers.json.JSONDeserializer.__init__"></a>

#### JSONDeserializer.\_\_init\_\_

```python
def __init__(
    loads: Callable[[Union[bytes, bytearray]], Any] = default_loads,
    schema: Optional[Mapping] = None,
    validator: Optional["_Validator"] = None,
    schema_registry_client_config: Optional[SchemaRegistryClientConfig] = None
)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/json.py#L122)

Deserializer that parses data from JSON

**Arguments**:

- `loads`: function to parse json from bytes.
Default - :py:func:`quixstreams.utils.json.loads`.
- `schema`: A schema used to validate the data using [`jsonschema.Draft202012Validator`](https://python-jsonschema.readthedocs.io/en/stable/api/jsonschema/validators/`jsonschema.validators.Draft202012Validator`).
Default - `None`
- `validator`: A jsonschema validator used to validate the data. Takes precedences over the schema.
Default - `None`
- `schema_registry_client_config`: If provided, deserialization is offloaded to Confluent's JSONDeserializer.
Default - `None`

<a id="quixstreams.models.serializers.base"></a>

## quixstreams.models.serializers.base

<a id="quixstreams.models.serializers.base.SerializationContext"></a>

### SerializationContext

```python
class SerializationContext(_SerializationContext)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L24)

Provides additional context for message serialization/deserialization.

Every `Serializer` and `Deserializer` receives an instance of `SerializationContext`

<a id="quixstreams.models.serializers.base.Deserializer"></a>

### Deserializer

```python
class Deserializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L44)

<a id="quixstreams.models.serializers.base.Deserializer.__init__"></a>

#### Deserializer.\_\_init\_\_

```python
def __init__(*args, **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L45)

A base class for all Deserializers

<a id="quixstreams.models.serializers.base.Deserializer.split_values"></a>

#### Deserializer.split\_values

```python
@property
def split_values() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L51)

Return True if the deserialized message should be considered as Iterable
and each item in it should be processed as a separate message.

<a id="quixstreams.models.serializers.base.Serializer"></a>

### Serializer

```python
class Serializer(abc.ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L62)

A base class for all Serializers

<a id="quixstreams.models.serializers.base.Serializer.extra_headers"></a>

#### Serializer.extra\_headers

```python
@property
def extra_headers() -> HeadersMapping
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/serializers/base.py#L68)

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

<a id="quixstreams.models.topics.admin.TopicAdmin"></a>

### TopicAdmin

```python
class TopicAdmin()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L31)

For performing "admin"-level operations on a Kafka cluster, mostly around topics.

Primarily used to create and inspect topic configurations.

<a id="quixstreams.models.topics.admin.TopicAdmin.__init__"></a>

#### TopicAdmin.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             logger: logging.Logger = logger,
             extra_config: Optional[Mapping] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L38)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L70)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L81)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/admin.py#L185)

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
def merge_headers(original: KafkaHeaders,
                  other: HeadersMapping) -> HeadersTuples
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/utils.py#L13)

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
@dataclasses.dataclass(eq=True, frozen=True)
class TopicConfig()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L44)

Represents all kafka-level configuration for a kafka topic.

Generally used by Topic and any topic creation procedures.

<a id="quixstreams.models.topics.topic.Topic"></a>

### Topic

```python
class Topic()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L85)

A definition of a Kafka topic.

Typically created with an `app = quixstreams.app.Application()` instance via
`app.topic()`, and used by `quixstreams.dataframe.StreamingDataFrame`
instance.

<a id="quixstreams.models.topics.topic.Topic.__init__"></a>

#### Topic.\_\_init\_\_

```python
def __init__(
        name: str,
        create_config: Optional[TopicConfig] = None,
        value_deserializer: Optional[DeserializerType] = None,
        key_deserializer: Optional[DeserializerType] = BytesDeserializer(),
        value_serializer: Optional[SerializerType] = None,
        key_serializer: Optional[SerializerType] = BytesSerializer(),
        timestamp_extractor: Optional[TimestampExtractor] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L94)

**Arguments**:

- `name`: topic name
- `create_config`: a `TopicConfig` to create a new topic if it does not exist
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message.

<a id="quixstreams.models.topics.topic.Topic.create_config"></a>

#### Topic.create\_config

```python
@property
def create_config() -> Optional[TopicConfig]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L139)

A config to create the topic

<a id="quixstreams.models.topics.topic.Topic.broker_config"></a>

#### Topic.broker\_config

```python
@property
def broker_config() -> TopicConfig
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L150)

A topic config obtained from the Kafka broker

<a id="quixstreams.models.topics.topic.Topic.row_serialize"></a>

#### Topic.row\_serialize

```python
def row_serialize(row: Row, key: Any) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L164)

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
    message: SuccessfulConfluentKafkaMessageProto
) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/topic.py#L204)

Deserialize incoming Kafka message to a Row.

**Arguments**:

- `message`: an object with interface of `confluent_kafka.Message`

**Returns**:

Row, list of Rows or None if the message is ignored.

<a id="quixstreams.models.topics.exceptions"></a>

## quixstreams.models.topics.exceptions

<a id="quixstreams.models.topics.manager"></a>

## quixstreams.models.topics.manager

<a id="quixstreams.models.topics.manager.TopicManager"></a>

### TopicManager

```python
class TopicManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L21)

The source of all topic management for a Quix Streams Application.

Intended only for internal use by Application.

To create a Topic, use Application.topic() or generate them directly.

<a id="quixstreams.models.topics.manager.TopicManager.__init__"></a>

#### TopicManager.\_\_init\_\_

```python
def __init__(topic_admin: TopicAdmin,
             consumer_group: str,
             timeout: float = 30,
             create_timeout: float = 60,
             auto_create_topics: bool = True)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L42)

**Arguments**:

- `topic_admin`: an `Admin` instance (required for some functionality)
- `consumer_group`: the consumer group (of the `Application`)
- `timeout`: response timeout (seconds)
- `create_timeout`: timeout for topic creation

<a id="quixstreams.models.topics.manager.TopicManager.changelog_topics"></a>

#### TopicManager.changelog\_topics

```python
@property
def changelog_topics() -> Dict[Optional[str], Dict[str, Topic]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L82)

Note: `Topic`s are the changelogs.

returns: the changelog topic dict, {topic_name: {suffix: Topic}}

<a id="quixstreams.models.topics.manager.TopicManager.changelog_topics_list"></a>

#### TopicManager.changelog\_topics\_list

```python
@property
def changelog_topics_list() -> List[Topic]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L91)

Returns a list of changelog topics

returns: the changelog topic dict, {topic_name: {suffix: Topic}}

<a id="quixstreams.models.topics.manager.TopicManager.non_changelog_topics"></a>

#### TopicManager.non\_changelog\_topics

```python
@property
def non_changelog_topics() -> Dict[str, Topic]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L100)

Returns a dict with normal and repartition topics

<a id="quixstreams.models.topics.manager.TopicManager.all_topics"></a>

#### TopicManager.all\_topics

```python
@property
def all_topics() -> Dict[str, Topic]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L107)

Every registered topic name mapped to its respective `Topic`.

returns: full topic dict, {topic_name: Topic}

<a id="quixstreams.models.topics.manager.TopicManager.topic_config"></a>

#### TopicManager.topic\_config

```python
def topic_config(num_partitions: Optional[int] = None,
                 replication_factor: Optional[int] = None,
                 extra_config: Optional[dict] = None) -> TopicConfig
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L115)

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
          create_config: Optional[TopicConfig] = None,
          timestamp_extractor: Optional[TimestampExtractor] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L137)

A convenience method for generating a `Topic`. Will use default config options

as dictated by the TopicManager.

**Arguments**:

- `name`: topic name
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys
- `create_config`: optional topic configurations (for creation/validation)
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message.

**Returns**:

Topic object with creation configs

<a id="quixstreams.models.topics.manager.TopicManager.register"></a>

#### TopicManager.register

```python
def register(topic: Topic) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L183)

Register an already generated :class:`quixstreams.models.topics.Topic` to the topic manager.

The topic name and config can be updated by the topic manager.

**Arguments**:

- `topic`: The topic to register

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L202)

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
def changelog_topic(topic_name: Optional[str],
                    store_name: str,
                    config: Optional[TopicConfig] = None,
                    timeout: Optional[float] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L242)

Performs all the logic necessary to generate a changelog topic based on an

optional "source topic" (aka input/consumed topic).

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
- `config`: the changelog topic configuration. Default to `topic_name` configuration or TopicManager default
- `timeout`: config lookup timeout (seconds); Default 30

**Returns**:

`Topic` object (which is also stored on the TopicManager)

<a id="quixstreams.models.topics.manager.TopicManager.validate_all_topics"></a>

#### TopicManager.validate\_all\_topics

```python
def validate_all_topics()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/models/topics/manager.py#L311)

Validates that all topics have ".broker_config" set
and changelog topics have correct numbers of partitions and replication factors.

Issues are pooled and raised as an Exception once inspections are complete.

<a id="quixstreams.state.rocksdb.windowed.store"></a>

## quixstreams.state.rocksdb.windowed.store

<a id="quixstreams.state.rocksdb.windowed.store.WindowedRocksDBStore"></a>

### WindowedRocksDBStore

```python
class WindowedRocksDBStore(RocksDBStore)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/store.py#L9)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/store.py#L17)

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

<a id="quixstreams.state.rocksdb.windowed.partition.WindowedRocksDBStorePartition.iter_keys"></a>

#### WindowedRocksDBStorePartition.iter\_keys

```python
def iter_keys(cf_name: str = "default") -> Iterator[bytes]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/partition.py#L63)

Iterate over all keys in the DB.

Addition and deletion of keys during iteration is not supported.

**Arguments**:

- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

An iterable of keys

<a id="quixstreams.state.rocksdb.windowed.metadata"></a>

## quixstreams.state.rocksdb.windowed.metadata

<a id="quixstreams.state.rocksdb.windowed.transaction"></a>

## quixstreams.state.rocksdb.windowed.transaction

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction"></a>

### WindowedRocksDBPartitionTransaction

```python
class WindowedRocksDBPartitionTransaction(PartitionTransaction)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/transaction.py#L60)

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction.expire_windows"></a>

#### WindowedRocksDBPartitionTransaction.expire\_windows

```python
def expire_windows(
        max_start_time: int,
        prefix: bytes,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False) -> Iterable[ExpiredWindowDetail]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/transaction.py#L230)

Get all expired windows with a set prefix from RocksDB up to the specified `max_start_time` timestamp.

This method marks the latest found window as expired in the expiration index,
so consecutive calls may yield different results for the same "latest timestamp".

How it works:
- First, it checks the expiration cache for the start time of the last expired
  window for the current prefix. If found, this value helps reduce the search
  space and prevents returning previously expired windows.
- Next, it iterates over window segments and identifies the windows that should
  be marked as expired.
- Finally, it updates the expiration cache with the start time of the latest
  windows found.

Collection behavior (when collect=True):
- For tumbling and hopping windows (created using .collect()), the window
  value is None and is replaced with the list of collected values.
- For sliding windows, the window value is [max_timestamp, None] where
  None is replaced with the list of collected values.
- Values are collected from a separate column family and obsolete values
  are deleted if delete=True.

**Arguments**:

- `max_start_time`: The timestamp up to which windows are considered expired, inclusive.
- `prefix`: The key prefix for filtering windows.
- `delete`: If True, expired windows will be deleted.
- `collect`: If True, values will be collected into windows.
- `end_inclusive`: If True, the end of the window will be inclusive.
Relevant only together with `collect=True`.

**Returns**:

A sorted list of tuples in the format `((start, end), value)`.

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction.expire_all_windows"></a>

#### WindowedRocksDBPartitionTransaction.expire\_all\_windows

```python
def expire_all_windows(max_end_time: int,
                       step_ms: int,
                       delete: bool = True,
                       collect: bool = False) -> Iterable[ExpiredWindowDetail]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/transaction.py#L323)

Get all expired windows for all prefix from RocksDB up to the specified `max_end_time` timestamp.

**Arguments**:

- `max_end_time`: The timestamp up to which windows are considered expired, inclusive.
- `delete`: If True, expired windows will be deleted.
- `collect`: If True, values will be collected into windows.

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction.delete_windows"></a>

#### WindowedRocksDBPartitionTransaction.delete\_windows

```python
def delete_windows(max_start_time: int, delete_values: bool,
                   prefix: bytes) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/transaction.py#L397)

Delete windows from RocksDB up to the specified `max_start_time` timestamp.

This method removes all window entries that have a start time less than or equal to the given
`max_start_time`. It ensures that expired data is cleaned up efficiently without affecting
unexpired windows.

How it works:
- It retrieves the start time of the last deleted window for the given prefix from the
deletion index. This minimizes redundant scans over already deleted windows.
- It iterates over the windows starting from the last deleted timestamp up to the `max_start_time`.
- Each window within this range is deleted from the database.
- After deletion, it updates the deletion index with the start time of the latest window
that was deleted to keep track of progress.
- Values with timestamps less than max_start_time are considered obsolete and are
deleted if delete_values=True, as they can no longer belong to any active window.

**Arguments**:

- `max_start_time`: The timestamp up to which windows should be deleted, inclusive.
- `delete_values`: If True, obsolete values will be deleted.
- `prefix`: The key prefix used to identify and filter relevant windows.

<a id="quixstreams.state.rocksdb.windowed.transaction.WindowedRocksDBPartitionTransaction.get_windows"></a>

#### WindowedRocksDBPartitionTransaction.get\_windows

```python
def get_windows(start_from_ms: int,
                start_to_ms: int,
                prefix: bytes,
                backwards: bool = False) -> list[WindowDetail]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/transaction.py#L452)

Get all windows within the specified time range.

This method retrieves all window entries that have a start time between
`start_from_ms` (exclusive) and `start_to_ms` (inclusive). The windows can be
retrieved in either forward or reverse chronological order.

How it works:
- It uses `_get_items` to fetch the raw key-value pairs within
  the specified time range.
- For each window, it parses the key to extract start and end timestamps.
- Values are deserialized before being returned.
- Results are returned as tuples of ((start_time, end_time), value).

**Arguments**:

- `start_from_ms`: The lower bound timestamp (exclusive) for window start times.
- `start_to_ms`: The upper bound timestamp (inclusive) for window start times.
- `prefix`: The key prefix used to identify and filter relevant windows.
- `backwards`: If True, returns windows in reverse chronological order.

**Returns**:

A list of tuples in the format ((start_ms, end_ms), value).

<a id="quixstreams.state.rocksdb.windowed"></a>

## quixstreams.state.rocksdb.windowed

<a id="quixstreams.state.rocksdb.windowed.serialization"></a>

## quixstreams.state.rocksdb.windowed.serialization

<a id="quixstreams.state.rocksdb.windowed.serialization.parse_window_key"></a>

#### parse\_window\_key

```python
def parse_window_key(key: bytes) -> tuple[bytes, int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/serialization.py#L20)

Parse the window key from Rocksdb into (message_key, start, end) structure.

Expected window key format:
<message_key>|<start>|<end>

**Arguments**:

- `key`: a key from Rocksdb

**Returns**:

a tuple with message key, start timestamp, end timestamp

<a id="quixstreams.state.rocksdb.windowed.serialization.encode_integer_pair"></a>

#### encode\_integer\_pair

```python
def encode_integer_pair(integer_1: int, integer_2: int) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/serialization.py#L40)

Encode a pair of integers into bytes of the following format:

```<integer_1>|<integer_2>```

Encoding integers this way make them sortable in RocksDB within the same prefix.

**Arguments**:

- `integer_1`: first integer
- `integer_2`: second integer

**Returns**:

integers as bytes

<a id="quixstreams.state.rocksdb.windowed.serialization.append_integer"></a>

#### append\_integer

```python
def append_integer(base_bytes: bytes, integer: int) -> bytes
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/serialization.py#L54)

Append integer to the base bytes

Format:
```<base_bytes>|<integer>```

**Arguments**:

- `base_bytes`: base bytes
- `integer`: integer to append

**Returns**:

bytes

<a id="quixstreams.state.rocksdb.windowed.state"></a>

## quixstreams.state.rocksdb.windowed.state

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState"></a>

### WindowedTransactionState

```python
class WindowedTransactionState(TransactionState, WindowedState)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L10)

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.__init__"></a>

#### WindowedTransactionState.\_\_init\_\_

```python
def __init__(transaction: "WindowedRocksDBPartitionTransaction",
             prefix: bytes)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L13)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L24)

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
def update_window(start_ms: int, end_ms: int, value: Any,
                  timestamp_ms: int) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L40)

Set a value for the window.

This method will also update the latest observed timestamp in state partition
using the provided `timestamp_ms`.

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `value`: value of the window
- `timestamp_ms`: current message timestamp in milliseconds

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.add_to_collection"></a>

#### WindowedTransactionState.add\_to\_collection

```python
def add_to_collection(value: Any, id: Optional[int]) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L66)

Collect a value for collection-type window aggregations.

This method is used internally by collection windows (created using
.collect()) to store individual values. These values are later combined
during window expiration.

**Arguments**:

- `value`: value to be collected
- `timestamp_ms`: current message timestamp in milliseconds

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.get_from_collection"></a>

#### WindowedTransactionState.get\_from\_collection

```python
def get_from_collection(start: int, end: int) -> list[Any]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L83)

Return all values from a collection-type window aggregation.

**Arguments**:

- `start`: starting id of values to fetch (inclusive)
- `end`: end id of values to fetch (exclusive)

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.delete_from_collection"></a>

#### WindowedTransactionState.delete\_from\_collection

```python
def delete_from_collection(end: int, *, start: Optional[int] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L94)

Delete collected values with id less than end.

This method maintains a deletion index to track progress and avoid
re-scanning previously deleted values. It:
1. Retrieves the last deleted id from the cache
2. Scans values from last deleted id up to end
3. Updates the deletion index with the latest deleted id

**Arguments**:

- `end`: Delete values with id less than this value

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.get_latest_timestamp"></a>

#### WindowedTransactionState.get\_latest\_timestamp

```python
def get_latest_timestamp() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L110)

Get the latest observed timestamp for the current message key.

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest observed event timestamp in milliseconds

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.expire_windows"></a>

#### WindowedTransactionState.expire\_windows

```python
def expire_windows(
        max_start_time: int,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False) -> Iterable[ExpiredWindowDetail]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L122)

Get all expired windows from RocksDB up to the specified `max_start_time` timestamp.

This method marks the latest found window as expired in the expiration index,
so consecutive calls may yield different results for the same "latest timestamp".

**Arguments**:

- `max_start_time`: The timestamp up to which windows are considered expired, inclusive.
- `delete`: If True, expired windows will be deleted.
- `collect`: If True, values will be collected into windows.
- `end_inclusive`: If True, the end of the window will be inclusive.
Relevant only together with `collect=True`.

**Returns**:

A sorted list of tuples in the format `((start, end), value)`.

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.get_windows"></a>

#### WindowedTransactionState.get\_windows

```python
def get_windows(start_from_ms: int,
                start_to_ms: int,
                backwards: bool = False) -> list[WindowDetail]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L150)

Get all windows that start between "start_from_ms" and "start_to_ms".

**Arguments**:

- `start_from_ms`: The minimal window start time, exclusive.
- `start_to_ms`: The maximum window start time, inclusive.
- `backwards`: If True, yields windows in reverse order.

**Returns**:

A sorted list of tuples in the format `((start, end), value)`.

<a id="quixstreams.state.rocksdb.windowed.state.WindowedTransactionState.delete_windows"></a>

#### WindowedTransactionState.delete\_windows

```python
def delete_windows(max_start_time: int, delete_values: bool) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/windowed/state.py#L168)

Delete windows from RocksDB up to the specified `max_start_time` timestamp.

This method removes all window entries that have a start time less than or equal
to the given `max_start_time`. It ensures that expired data is cleaned up
efficiently without affecting unexpired windows.

**Arguments**:

- `max_start_time`: The timestamp up to which windows should be deleted, inclusive.
- `delete_values`: If True, values with timestamps less than max_start_time
will be deleted, as they can no longer belong to any active window.

<a id="quixstreams.state.rocksdb.options"></a>

## quixstreams.state.rocksdb.options

<a id="quixstreams.state.rocksdb.options.RocksDBOptions"></a>

### RocksDBOptions

```python
@dataclasses.dataclass(frozen=True)
class RocksDBOptions(RocksDBOptionsType)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L26)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/options.py#L54)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L18)

RocksDB-based state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

<a id="quixstreams.state.rocksdb.store.RocksDBStore.__init__"></a>

#### RocksDBStore.\_\_init\_\_

```python
def __init__(
        name: str,
        topic: Optional[str],
        base_dir: str,
        changelog_producer_factory: Optional[ChangelogProducerFactory] = None,
        options: Optional[RocksDBOptionsType] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/store.py#L26)

**Arguments**:

- `name`: a unique store name
- `topic`: a topic name for this store
- `base_dir`: path to a directory with the state
- `changelog_producer_factory`: a ChangelogProducerFactory instance
if using changelogs
- `options`: RocksDB options. If `None`, the default options will be used.

<a id="quixstreams.state.rocksdb.partition"></a>

## quixstreams.state.rocksdb.partition

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition"></a>

### RocksDBStorePartition

```python
class RocksDBStorePartition(StorePartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L28)

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

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.write"></a>

#### RocksDBStorePartition.write

```python
def write(cache: PartitionTransactionCache,
          changelog_offset: Optional[int],
          batch: Optional[WriteBatch] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L78)

Write data to RocksDB

**Arguments**:

- `cache`: The modified data
- `changelog_offset`: The changelog message offset of the data.
- `batch`: prefilled `rocksdict.WriteBatch`, optional.

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get"></a>

#### RocksDBStorePartition.get

```python
def get(key: bytes,
        cf_name: str = "default") -> Union[bytes, Literal[Marker.UNDEFINED]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L127)

Get a key from RocksDB.

**Arguments**:

- `key`: a key encoded to `bytes`
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

a value if the key is present in the DB. Otherwise, `default`

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.exists"></a>

#### RocksDBStorePartition.exists

```python
def exists(key: bytes, cf_name: str = "default") -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L142)

Check if a key is present in the DB.

**Arguments**:

- `key`: a key encoded to `bytes`.
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

`True` if the key is present, `False` otherwise.

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_changelog_offset"></a>

#### RocksDBStorePartition.get\_changelog\_offset

```python
def get_changelog_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L153)

Get offset that the changelog is up-to-date with.

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.write_changelog_offset"></a>

#### RocksDBStorePartition.write\_changelog\_offset

```python
def write_changelog_offset(offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L165)

Write a new changelog offset to the db.

To be used when we simply need to update the changelog offset without touching
the actual data.

**Arguments**:

- `offset`: new changelog offset

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.close"></a>

#### RocksDBStorePartition.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L178)

Close the underlying RocksDB

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.path"></a>

#### RocksDBStorePartition.path

```python
@property
def path() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L191)

Absolute path to RocksDB database folder

**Returns**:

file path

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.destroy"></a>

#### RocksDBStorePartition.destroy

```python
@classmethod
def destroy(cls, path: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L199)

Delete underlying RocksDB database

The database must be closed first.

**Arguments**:

- `path`: an absolute path to the RocksDB folder

<a id="quixstreams.state.rocksdb.partition.RocksDBStorePartition.get_column_family_handle"></a>

#### RocksDBStorePartition.get\_column\_family\_handle

```python
def get_column_family_handle(cf_name: str) -> ColumnFamily
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L209)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/rocksdb/partition.py#L230)

Get a column family instance.

This method will cache the CF instance to avoid creating them repeatedly.

**Arguments**:

- `cf_name`: column family name

**Returns**:

instance of `rocksdict.Rdict` for the given column family

<a id="quixstreams.state.rocksdb.metadata"></a>

## quixstreams.state.rocksdb.metadata

<a id="quixstreams.state.rocksdb"></a>

## quixstreams.state.rocksdb

<a id="quixstreams.state.rocksdb.types"></a>

## quixstreams.state.rocksdb.types

<a id="quixstreams.state.rocksdb.exceptions"></a>

## quixstreams.state.rocksdb.exceptions

<a id="quixstreams.state.metadata"></a>

## quixstreams.state.metadata

<a id="quixstreams.state.memory.store"></a>

## quixstreams.state.memory.store

<a id="quixstreams.state.memory.store.MemoryStore"></a>

### MemoryStore

```python
class MemoryStore(Store)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/store.py#L14)

In-memory state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

Requires a full state recovery for each partition on assignment.

<a id="quixstreams.state.memory.store.MemoryStore.__init__"></a>

#### MemoryStore.\_\_init\_\_

```python
def __init__(
    name: str,
    topic: Optional[str],
    changelog_producer_factory: Optional[ChangelogProducerFactory] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/store.py#L24)

**Arguments**:

- `name`: a unique store name
- `topic`: a topic name for this store
- `changelog_producer_factory`: a ChangelogProducerFactory instance
if using changelogs topics.

<a id="quixstreams.state.memory.partition"></a>

## quixstreams.state.memory.partition

<a id="quixstreams.state.memory.partition.MemoryStorePartition"></a>

### MemoryStorePartition

```python
class MemoryStorePartition(StorePartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/partition.py#L35)

Class to access in-memory state.

Responsibilities:
 1. Recovering from changelog messages
 2. Creating transaction to interact with data
 3. Track partition state in-memory

<a id="quixstreams.state.memory.partition.MemoryStorePartition.write"></a>

#### MemoryStorePartition.write

```python
@_validate_partition_state()
def write(cache: PartitionTransactionCache,
          changelog_offset: Optional[int]) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/partition.py#L66)

Write data to the state

**Arguments**:

- `cache`: The partition update cache
- `changelog_offset`: The changelog message offset of the data.

<a id="quixstreams.state.memory.partition.MemoryStorePartition.get_changelog_offset"></a>

#### MemoryStorePartition.get\_changelog\_offset

```python
def get_changelog_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/partition.py#L103)

Get offset that the changelog is up-to-date with.

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.memory.partition.MemoryStorePartition.write_changelog_offset"></a>

#### MemoryStorePartition.write\_changelog\_offset

```python
def write_changelog_offset(offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/partition.py#L110)

Write a new changelog offset to the db.

To be used when we simply need to update the changelog offset without touching
the actual data.

**Arguments**:

- `offset`: new changelog offset

<a id="quixstreams.state.memory.partition.MemoryStorePartition.get"></a>

#### MemoryStorePartition.get

```python
@_validate_partition_state()
def get(key: bytes,
        cf_name: str = "default") -> Union[bytes, Literal[Marker.UNDEFINED]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/partition.py#L122)

Get a key from the store

**Arguments**:

- `key`: a key encoded to `bytes`
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

a value if the key is present in the store. Otherwise, `default`

<a id="quixstreams.state.memory.partition.MemoryStorePartition.exists"></a>

#### MemoryStorePartition.exists

```python
@_validate_partition_state()
def exists(key: bytes, cf_name: str = "default") -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/memory/partition.py#L135)

Check if a key is present in the store.

**Arguments**:

- `key`: a key encoded to `bytes`.
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

`True` if the key is present, `False` otherwise.

<a id="quixstreams.state.memory"></a>

## quixstreams.state.memory

<a id="quixstreams.state.recovery"></a>

## quixstreams.state.recovery

<a id="quixstreams.state.recovery.RecoveryPartition"></a>

### RecoveryPartition

```python
class RecoveryPartition()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L38)

A changelog topic partition mapped to a respective `StorePartition` with helper
methods to determine its current recovery status.

Since `StorePartition`s do recovery directly, it also handles recovery transactions.

<a id="quixstreams.state.recovery.RecoveryPartition.offset"></a>

#### RecoveryPartition.offset

```python
@property
def offset() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L78)

Get the changelog offset from the underlying `StorePartition`.

**Returns**:

changelog offset (int)

<a id="quixstreams.state.recovery.RecoveryPartition.needs_recovery_check"></a>

#### RecoveryPartition.needs\_recovery\_check

```python
@property
def needs_recovery_check() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L94)

Determine whether to attempt recovery for underlying `StorePartition`.

This does NOT mean that anything actually requires recovering.

<a id="quixstreams.state.recovery.RecoveryPartition.has_invalid_offset"></a>

#### RecoveryPartition.has\_invalid\_offset

```python
@property
def has_invalid_offset() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L105)

Determine if the current changelog offset stored in state is invalid.

<a id="quixstreams.state.recovery.RecoveryPartition.recover_from_changelog_message"></a>

#### RecoveryPartition.recover\_from\_changelog\_message

```python
def recover_from_changelog_message(
        changelog_message: SuccessfulConfluentKafkaMessageProto)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L122)

Recover the StorePartition using a message read from its respective changelog.

The actual update may be skipped when both conditions are met:

- The changelog message has headers with the processed message offset.
- This processed offsets are larger than the latest committed offsets
    for the same topic-partitions.

This way the state does not apply the state changes for not-yet-committed
messages and improves the state consistency guarantees.

**Arguments**:

- `changelog_message`: An instance of `confluent_kafka.Message`

<a id="quixstreams.state.recovery.RecoveryPartition.set_recovery_consume_position"></a>

#### RecoveryPartition.set\_recovery\_consume\_position

```python
def set_recovery_consume_position(offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L182)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L214)

Generates ChangelogProducers, which produce changelog messages to a StorePartition.

<a id="quixstreams.state.recovery.ChangelogProducerFactory.__init__"></a>

#### ChangelogProducerFactory.\_\_init\_\_

```python
def __init__(changelog_name: str, producer: RowProducer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L219)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L229)

Generate a ChangelogProducer for producing to a specific partition number

(and thus StorePartition).

**Arguments**:

- `partition_num`: source topic partition number

<a id="quixstreams.state.recovery.ChangelogProducer"></a>

### ChangelogProducer

```python
class ChangelogProducer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L243)

Generated for a `StorePartition` to produce state changes to its respective
kafka changelog partition.

<a id="quixstreams.state.recovery.ChangelogProducer.__init__"></a>

#### ChangelogProducer.\_\_init\_\_

```python
def __init__(changelog_name: str, partition: int, producer: RowProducer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L249)

**Arguments**:

- `changelog_name`: A changelog topic name
- `partition`: source topic partition number
- `producer`: a RowProducer (not shared with `Application` instance)

<a id="quixstreams.state.recovery.ChangelogProducer.produce"></a>

#### ChangelogProducer.produce

```python
def produce(key: bytes,
            value: Optional[bytes] = None,
            headers: Optional[Headers] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L272)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L297)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L316)

Returns a mapping of assigned RecoveryPartitions in the following format:
{<partition>: {<store_name>: <RecoveryPartition>}}

<a id="quixstreams.state.recovery.RecoveryManager.has_assignments"></a>

#### RecoveryManager.has\_assignments

```python
@property
def has_assignments() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L324)

Whether the Application has assigned RecoveryPartitions

**Returns**:

has assignments, as bool

<a id="quixstreams.state.recovery.RecoveryManager.recovering"></a>

#### RecoveryManager.recovering

```python
@property
def recovering() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L333)

Whether the Application is currently recovering

**Returns**:

is recovering, as bool

<a id="quixstreams.state.recovery.RecoveryManager.register_changelog"></a>

#### RecoveryManager.register\_changelog

```python
def register_changelog(topic_name: Optional[str],
                       store_name: str,
                       topic_config: Optional[TopicConfig] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L341)

Register a changelog Topic with the TopicManager.

**Arguments**:

- `topic_name`: source topic name
- `store_name`: name of the store
- `topic_config`: a TopicConfig to use, optional.

<a id="quixstreams.state.recovery.RecoveryManager.do_recovery"></a>

#### RecoveryManager.do\_recovery

```python
def do_recovery()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L360)

If there are any active RecoveryPartitions, do a recovery procedure.

After, will resume normal `Application` processing.

<a id="quixstreams.state.recovery.RecoveryManager.assign_partition"></a>

#### RecoveryManager.assign\_partition

```python
def assign_partition(topic: Optional[str], partition: int,
                     committed_offsets: dict[str, int],
                     store_partitions: Dict[str, StorePartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L432)

Assigns `StorePartition`s (as `RecoveryPartition`s) ONLY IF recovery required.

Pauses active consumer partitions as needed.

<a id="quixstreams.state.recovery.RecoveryManager.revoke_partition"></a>

#### RecoveryManager.revoke\_partition

```python
def revoke_partition(partition_num: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/recovery.py#L514)

revoke ALL StorePartitions (across all Stores) for a given partition number

**Arguments**:

- `partition_num`: partition number of source topic

<a id="quixstreams.state"></a>

## quixstreams.state

<a id="quixstreams.state.types"></a>

## quixstreams.state.types

<a id="quixstreams.state.types.WindowDetail"></a>

#### WindowDetail

(start, end), aggregated, key

<a id="quixstreams.state.types.ExpiredWindowDetail"></a>

#### ExpiredWindowDetail

(start, end), aggregated, collected, key

<a id="quixstreams.state.types.WindowedState"></a>

### WindowedState

```python
class WindowedState(Protocol[K, V])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L19)

A windowed state to be provided into `StreamingDataFrame` window functions.

<a id="quixstreams.state.types.WindowedState.get"></a>

#### WindowedState.get

```python
def get(key: K, default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L30)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.types.WindowedState.set"></a>

#### WindowedState.set

```python
def set(key: K, value: V)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L40)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.types.WindowedState.delete"></a>

#### WindowedState.delete

```python
def delete(key: K)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L48)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.types.WindowedState.exists"></a>

#### WindowedState.exists

```python
def exists(key: K) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L57)

Check if the key exists in state.

**Arguments**:

- `key`: key

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.types.WindowedState.get_window"></a>

#### WindowedState.get\_window

```python
def get_window(start_ms: int,
               end_ms: int,
               default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L71)

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
def update_window(start_ms: int, end_ms: int, value: V, timestamp_ms: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L85)

Set a value for the window.

This method will also update the latest observed timestamp in state partition
using the provided `timestamp_ms`.

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `value`: value of the window
- `timestamp_ms`: current message timestamp in milliseconds

<a id="quixstreams.state.types.WindowedState.add_to_collection"></a>

#### WindowedState.add\_to\_collection

```python
def add_to_collection(value: V, id: Optional[int]) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L105)

Collect a value for collection-type window aggregations.

This method is used internally by collection windows (created using
.collect()) to store individual values. These values are later combined
during window expiration.

**Arguments**:

- `value`: value to be collected
- `id`: current message ID, for example timestamp in milliseconds, does not have to be unique.

**Returns**:

the message ID, auto-generated if not provided

<a id="quixstreams.state.types.WindowedState.get_from_collection"></a>

#### WindowedState.get\_from\_collection

```python
def get_from_collection(start: int, end: int) -> list[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L120)

Return all values from a collection-type window aggregation.

**Arguments**:

- `start`: starting id of values to fetch (inclusive)
- `end`: end id of values to fetch (exclusive)

<a id="quixstreams.state.types.WindowedState.delete_from_collection"></a>

#### WindowedState.delete\_from\_collection

```python
def delete_from_collection(end: int, *, start: Optional[int] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L129)

Delete collected values with id less than end.

This method maintains a deletion index to track progress and avoid
re-scanning previously deleted values. It:
1. Retrieves the last deleted id from the cache
2. Scans values from last deleted id up to end
3. Updates the deletion index with the latest deleted id

**Arguments**:

- `end`: Delete values with id less than this value

<a id="quixstreams.state.types.WindowedState.get_latest_timestamp"></a>

#### WindowedState.get\_latest\_timestamp

```python
def get_latest_timestamp() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L143)

Get the latest observed timestamp for the current state partition.

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest observed event timestamp in milliseconds

<a id="quixstreams.state.types.WindowedState.expire_windows"></a>

#### WindowedState.expire\_windows

```python
def expire_windows(
        max_start_time: int,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False) -> Iterable[ExpiredWindowDetail[V]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L154)

Get all expired windows from RocksDB up to the specified `max_start_time` timestamp.

This method marks the latest found window as expired in the expiration index,
so consecutive calls may yield different results for the same "latest timestamp".

**Arguments**:

- `max_start_time`: The timestamp up to which windows are considered expired, inclusive.
- `delete`: If True, expired windows will be deleted.
- `collect`: If True, values will be collected into windows.
- `end_inclusive`: If True, the end of the window will be inclusive.
Relevant only together with `collect=True`.

**Returns**:

A sorted list of tuples in the format `((start, end), value)`.

<a id="quixstreams.state.types.WindowedState.delete_windows"></a>

#### WindowedState.delete\_windows

```python
def delete_windows(max_start_time: int, delete_values: bool) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L176)

Delete windows from RocksDB up to the specified `max_start_time` timestamp.

This method removes all window entries that have a start time less than or equal
to the given `max_start_time`. It ensures that expired data is cleaned up
efficiently without affecting unexpired windows.

**Arguments**:

- `max_start_time`: The timestamp up to which windows should be deleted, inclusive.
- `delete_values`: If True, values with timestamps less than max_start_time
will be deleted, as they can no longer belong to any active window.

<a id="quixstreams.state.types.WindowedState.get_windows"></a>

#### WindowedState.get\_windows

```python
def get_windows(start_from_ms: int,
                start_to_ms: int,
                backwards: bool = False) -> list[WindowDetail[V]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L190)

Get all windows that start between "start_from_ms" and "start_to_ms".

**Arguments**:

- `start_from_ms`: The minimal window start time, exclusive.
- `start_to_ms`: The maximum window start time, inclusive.
- `backwards`: If True, yields windows in reverse order.

**Returns**:

A sorted list of tuples in the format `((start, end), value)`.

<a id="quixstreams.state.types.WindowedPartitionTransaction"></a>

### WindowedPartitionTransaction

```python
class WindowedPartitionTransaction(Protocol[K, V])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L204)

<a id="quixstreams.state.types.WindowedPartitionTransaction.failed"></a>

#### WindowedPartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L206)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L216)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L226)

Return `True` if transaction is prepared completed.

Prepared transactions cannot receive new updates, but can be flushed.

**Returns**:

bool

<a id="quixstreams.state.types.WindowedPartitionTransaction.prepare"></a>

#### WindowedPartitionTransaction.prepare

```python
def prepare(processed_offsets: Optional[dict[str, int]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L235)

Produce changelog messages to the changelog topic for all changes accumulated

in this transaction and prepare transcation to flush its state to the state
store.

After successful `prepare()`, the transaction status is changed to PREPARED,
and it cannot receive updates anymore.

If changelog is disabled for this application, no updates will be produced
to the changelog topic.

**Arguments**:

- `processed_offsets`: the dict with <topic: offset> of
the latest processed message in the current partition

<a id="quixstreams.state.types.WindowedPartitionTransaction.get_window"></a>

#### WindowedPartitionTransaction.get\_window

```python
def get_window(start_ms: int,
               end_ms: int,
               prefix: bytes,
               default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L253)

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
def update_window(start_ms: int, end_ms: int, value: V, timestamp_ms: int,
                  prefix: bytes)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L272)

Set a value for the window.

This method will also update the latest observed timestamp in state partition
using the provided `timestamp`.

**Arguments**:

- `start_ms`: start of the window in milliseconds
- `end_ms`: end of the window in milliseconds
- `value`: value of the window
- `timestamp_ms`: current message timestamp in milliseconds
- `prefix`: a key prefix

<a id="quixstreams.state.types.WindowedPartitionTransaction.add_to_collection"></a>

#### WindowedPartitionTransaction.add\_to\_collection

```python
def add_to_collection(value: V, id: Optional[int]) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L289)

Collect a value for collection-type window aggregations.

This method is used internally by collection windows (created using
.collect()) to store individual values. These values are later combined
during window expiration.

**Arguments**:

- `value`: value to be collected
- `id`: current message ID (for example, timestamp in milliseconds)

**Returns**:

the message ID, auto-generated if not provided

<a id="quixstreams.state.types.WindowedPartitionTransaction.get_from_collection"></a>

#### WindowedPartitionTransaction.get\_from\_collection

```python
def get_from_collection(start: int, end: int) -> list[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L304)

Return all values from a collection-type window aggregation.

**Arguments**:

- `start`: starting id of values to fetch (inclusive)
- `end`: end id of values to fetch (exclusive)

<a id="quixstreams.state.types.WindowedPartitionTransaction.delete_from_collection"></a>

#### WindowedPartitionTransaction.delete\_from\_collection

```python
def delete_from_collection(end: int) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L313)

Delete collected values with id less than end.

This method maintains a deletion index to track progress and avoid
re-scanning previously deleted values. It:
1. Retrieves the last deleted id from the cache
2. Scans values from last deleted id up to end
3. Updates the deletion index with the latest deleted id

**Arguments**:

- `end`: Delete values with id less than this value

<a id="quixstreams.state.types.WindowedPartitionTransaction.get_latest_timestamp"></a>

#### WindowedPartitionTransaction.get\_latest\_timestamp

```python
def get_latest_timestamp(prefix: bytes) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L327)

Get the latest observed timestamp for the current state prefix

(same as message key).

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest observed event timestamp in milliseconds

<a id="quixstreams.state.types.WindowedPartitionTransaction.get_latest_expired"></a>

#### WindowedPartitionTransaction.get\_latest\_expired

```python
def get_latest_expired(prefix: bytes) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L339)

Get the latest expired timestamp for the current state prefix

(same as message key).

Use this timestamp to determine if the arriving event is late and should be
discarded from the processing.

**Returns**:

latest expired timestamp in milliseconds

<a id="quixstreams.state.types.WindowedPartitionTransaction.expire_windows"></a>

#### WindowedPartitionTransaction.expire\_windows

```python
def expire_windows(
        max_start_time: int,
        prefix: bytes,
        delete: bool = True,
        collect: bool = False,
        end_inclusive: bool = False) -> Iterable[ExpiredWindowDetail[V]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L351)

Get all expired windows with a set prefix from RocksDB up to the specified `max_start_time` timestamp.

This method marks the latest found window as expired in the expiration index,
so consecutive calls may yield different results for the same "latest timestamp".

**Arguments**:

- `max_start_time`: The timestamp up to which windows are considered expired, inclusive.
- `prefix`: The key prefix for filtering windows.
- `delete`: If True, expired windows will be deleted.
- `collect`: If True, values will be collected into windows.
- `end_inclusive`: If True, the end of the window will be inclusive.
Relevant only together with `collect=True`.

**Returns**:

A sorted list of tuples in the format `((start, end), value)`.

<a id="quixstreams.state.types.WindowedPartitionTransaction.expire_all_windows"></a>

#### WindowedPartitionTransaction.expire\_all\_windows

```python
def expire_all_windows(
        max_end_time: int,
        step_ms: int,
        delete: bool = True,
        collect: bool = False) -> Iterable[ExpiredWindowDetail[V]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L375)

Get all expired windows for all prefix from RocksDB up to the specified `max_start_time` timestamp.

This method marks the latest found window as expired in the expiration index,
so consecutive calls may yield different results for the same "latest timestamp".

**Arguments**:

- `max_end_time`: The timestamp up to which windows are considered expired, inclusive.
- `delete`: If True, expired windows will be deleted.
- `collect`: If True, values will be collected into windows.

<a id="quixstreams.state.types.WindowedPartitionTransaction.delete_windows"></a>

#### WindowedPartitionTransaction.delete\_windows

```python
def delete_windows(max_start_time: int, delete_values: bool,
                   prefix: bytes) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L394)

Delete windows from RocksDB up to the specified `max_start_time` timestamp.

This method removes all window entries that have a start time less than or equal
to the given `max_start_time`. It ensures that expired data is cleaned up
efficiently without affecting unexpired windows.

**Arguments**:

- `max_start_time`: The timestamp up to which windows should be deleted, inclusive.
- `delete_values`: If True, values with timestamps less than max_start_time
will be deleted, as they can no longer belong to any active window.
- `prefix`: The key prefix used to identify and filter relevant windows.

<a id="quixstreams.state.types.WindowedPartitionTransaction.get_windows"></a>

#### WindowedPartitionTransaction.get\_windows

```python
def get_windows(start_from_ms: int,
                start_to_ms: int,
                prefix: bytes,
                backwards: bool = False) -> list[WindowDetail[V]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L411)

Get all windows that start between "start_from_ms" and "start_to_ms"

within the specified prefix.

**Arguments**:

- `start_from_ms`: The minimal window start time, exclusive.
- `start_to_ms`: The maximum window start time, inclusive.
- `prefix`: The key prefix for filtering windows.
- `backwards`: If True, yields windows in reverse order.

**Returns**:

A sorted list of tuples in the format `((start, end), value)`.

<a id="quixstreams.state.types.WindowedPartitionTransaction.keys"></a>

#### WindowedPartitionTransaction.keys

```python
def keys(cf_name: str = "default") -> Iterable[bytes]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L430)

Iterate over all keys in the store.

Addition and deletion of keys during iteration is not supported.

**Arguments**:

- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

An iterable of keys

<a id="quixstreams.state.types.WindowedPartitionTransaction.flush"></a>

#### WindowedPartitionTransaction.flush

```python
def flush(processed_offset: Optional[int] = None,
          changelog_offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L441)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L455)

Return the changelog topic-partition for the StorePartition of this transaction.

Returns `None` if changelog_producer is not provided.

**Returns**:

(topic, partition) or None

<a id="quixstreams.state.types.PartitionRecoveryTransaction"></a>

### PartitionRecoveryTransaction

```python
class PartitionRecoveryTransaction(Protocol)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L469)

A class for managing recovery for a StorePartition from a changelog message

<a id="quixstreams.state.types.PartitionRecoveryTransaction.flush"></a>

#### PartitionRecoveryTransaction.flush

```python
def flush()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/types.py#L476)

Flush the recovery update to the storage.

<a id="quixstreams.state.exceptions"></a>

## quixstreams.state.exceptions

<a id="quixstreams.state.manager"></a>

## quixstreams.state.manager

<a id="quixstreams.state.manager.StateStoreManager"></a>

### StateStoreManager

```python
class StateStoreManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L30)

Class for managing state stores and partitions.

StateStoreManager is responsible for:
 - reacting to rebalance callbacks
 - managing the individual state stores
 - providing access to store transactions

<a id="quixstreams.state.manager.StateStoreManager.stores"></a>

#### StateStoreManager.stores

```python
@property
def stores() -> Dict[Optional[str], Dict[str, Store]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L79)

Map of registered state stores

**Returns**:

dict in format {topic: {store_name: store}}

<a id="quixstreams.state.manager.StateStoreManager.recovery_required"></a>

#### StateStoreManager.recovery\_required

```python
@property
def recovery_required() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L87)

Whether recovery needs to be done.

<a id="quixstreams.state.manager.StateStoreManager.using_changelogs"></a>

#### StateStoreManager.using\_changelogs

```python
@property
def using_changelogs() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L96)

Whether the StateStoreManager is using changelog topics

**Returns**:

using changelogs, as bool

<a id="quixstreams.state.manager.StateStoreManager.do_recovery"></a>

#### StateStoreManager.do\_recovery

```python
def do_recovery() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L108)

Perform a state recovery, if necessary.

<a id="quixstreams.state.manager.StateStoreManager.stop_recovery"></a>

#### StateStoreManager.stop\_recovery

```python
def stop_recovery() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L117)

Stop recovery (called during app shutdown).

<a id="quixstreams.state.manager.StateStoreManager.get_store"></a>

#### StateStoreManager.get\_store

```python
def get_store(topic: str, store_name: str = DEFAULT_STATE_STORE_NAME) -> Store
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L126)

Get a store for given name and topic

**Arguments**:

- `topic`: topic name
- `store_name`: store name

**Returns**:

instance of `Store`

<a id="quixstreams.state.manager.StateStoreManager.register_store"></a>

#### StateStoreManager.register\_store

```python
def register_store(topic_name: Optional[str],
                   store_name: str = DEFAULT_STATE_STORE_NAME,
                   store_type: Optional[StoreTypes] = None,
                   topic_config: Optional[TopicConfig] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L163)

Register a state store to be managed by StateStoreManager.

During processing, the StateStoreManager will react to rebalancing callbacks
and assign/revoke the partitions for registered stores.

Each store can be registered only once for each topic.

**Arguments**:

- `topic_name`: topic name
- `store_name`: store name
- `store_type`: the storage type used for this store.
Default to StateStoreManager `default_store_type`

<a id="quixstreams.state.manager.StateStoreManager.register_windowed_store"></a>

#### StateStoreManager.register\_windowed\_store

```python
def register_windowed_store(topic_name: str, store_name: str) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L208)

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
def clear_stores() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L234)

Delete all state stores managed by StateStoreManager.

<a id="quixstreams.state.manager.StateStoreManager.on_partition_assign"></a>

#### StateStoreManager.on\_partition\_assign

```python
def on_partition_assign(
        topic: Optional[str], partition: int,
        committed_offsets: dict[str, int]) -> Dict[str, StorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L251)

Assign store partitions for each registered store for the given `TopicPartition`

and return a list of assigned `StorePartition` objects.

**Arguments**:

- `topic`: Kafka topic name
- `partition`: Kafka topic partition
- `committed_offsets`: a dict with latest committed offsets
of all assigned topics for this partition number.

**Returns**:

list of assigned `StorePartition`

<a id="quixstreams.state.manager.StateStoreManager.on_partition_revoke"></a>

#### StateStoreManager.on\_partition\_revoke

```python
def on_partition_revoke(topic: str, partition: int) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L281)

Revoke store partitions for each registered store for the given `TopicPartition`

**Arguments**:

- `topic`: Kafka topic name
- `partition`: Kafka topic partition

<a id="quixstreams.state.manager.StateStoreManager.init"></a>

#### StateStoreManager.init

```python
def init() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L294)

Initialize `StateStoreManager` and create a store directory


<a id="quixstreams.state.manager.StateStoreManager.close"></a>

#### StateStoreManager.close

```python
def close() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/manager.py#L301)

Close all registered stores

<a id="quixstreams.state.serialization"></a>

## quixstreams.state.serialization

<a id="quixstreams.state.base.store"></a>

## quixstreams.state.base.store

<a id="quixstreams.state.base.store.Store"></a>

### Store

```python
class Store(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L15)

Abstract state store.

It keeps track of individual store partitions and provides access to the
partitions' transactions.

<a id="quixstreams.state.base.store.Store.topic"></a>

#### Store.topic

```python
@property
def topic() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L35)

Topic name

<a id="quixstreams.state.base.store.Store.name"></a>

#### Store.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L42)

Store name

<a id="quixstreams.state.base.store.Store.partitions"></a>

#### Store.partitions

```python
@property
def partitions() -> Dict[int, StorePartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L49)

Mapping of assigned store partitions

**Returns**:

dict of "{partition: <StorePartition>}"

<a id="quixstreams.state.base.store.Store.assign_partition"></a>

#### Store.assign\_partition

```python
def assign_partition(partition: int) -> StorePartition
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L56)

Assign new store partition

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `StorePartition`

<a id="quixstreams.state.base.store.Store.revoke_partition"></a>

#### Store.revoke\_partition

```python
def revoke_partition(partition: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L83)

Revoke assigned store partition

**Arguments**:

- `partition`: partition number

<a id="quixstreams.state.base.store.Store.start_partition_transaction"></a>

#### Store.start\_partition\_transaction

```python
def start_partition_transaction(partition: int) -> PartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L101)

Start a new partition transaction.

`PartitionTransaction` is the primary interface for working with data in Stores.

**Arguments**:

- `partition`: partition number

**Returns**:

instance of `PartitionTransaction`

<a id="quixstreams.state.base.store.Store.close"></a>

#### Store.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/store.py#L119)

Close store and revoke all store partitions

<a id="quixstreams.state.base.partition"></a>

## quixstreams.state.base.partition

<a id="quixstreams.state.base.partition.StorePartition"></a>

### StorePartition

```python
class StorePartition(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L21)

A base class to access state in the underlying storage.
It represents a single instance of some storage (e.g. a single database for
the persistent storage).

<a id="quixstreams.state.base.partition.StorePartition.get_changelog_offset"></a>

#### StorePartition.get\_changelog\_offset

```python
@abstractmethod
def get_changelog_offset() -> Optional[int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L43)

Get the changelog offset that the state is up-to-date with.

**Returns**:

offset or `None` if there's no processed offset yet

<a id="quixstreams.state.base.partition.StorePartition.write_changelog_offset"></a>

#### StorePartition.write\_changelog\_offset

```python
@abstractmethod
def write_changelog_offset(offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L51)

Write a new changelog offset to the db.

To be used when we simply need to update the changelog offset without touching
the actual data.

**Arguments**:

- `offset`: new changelog offset

<a id="quixstreams.state.base.partition.StorePartition.write"></a>

#### StorePartition.write

```python
@abstractmethod
def write(cache: PartitionTransactionCache, changelog_offset: Optional[int])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L62)

Update the state with data from the update cache

**Arguments**:

- `cache`: The modified data
- `changelog_offset`: The changelog message offset of the data.

<a id="quixstreams.state.base.partition.StorePartition.get"></a>

#### StorePartition.get

```python
@abstractmethod
def get(key: bytes,
        cf_name: str = "default") -> Union[bytes, Literal[Marker.UNDEFINED]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L75)

Get a key from the store

**Arguments**:

- `key`: a key encoded to `bytes`
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

a value if the key is present in the store. Otherwise, `default`

<a id="quixstreams.state.base.partition.StorePartition.exists"></a>

#### StorePartition.exists

```python
@abstractmethod
def exists(key: bytes, cf_name: str = "default") -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L87)

Check if a key is present in the store.

**Arguments**:

- `key`: a key encoded to `bytes`.
- `cf_name`: rocksdb column family name. Default - "default"

**Returns**:

`True` if the key is present, `False` otherwise.

<a id="quixstreams.state.base.partition.StorePartition.recover_from_changelog_message"></a>

#### StorePartition.recover\_from\_changelog\_message

```python
@abstractmethod
def recover_from_changelog_message(key: bytes, value: Optional[bytes],
                                   cf_name: str, offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L97)

Updates state from a given changelog message.

**Arguments**:

- `key`: changelog message key
- `value`: changelog message value
- `cf_name`: column family name
- `offset`: changelog message offset

<a id="quixstreams.state.base.partition.StorePartition.begin"></a>

#### StorePartition.begin

```python
def begin() -> PartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/partition.py#L109)

Start a new `PartitionTransaction`

Using `PartitionTransaction` is a recommended way for accessing the data.

<a id="quixstreams.state.base.transaction"></a>

## quixstreams.state.base.transaction

<a id="quixstreams.state.base.transaction.PartitionTransactionCache"></a>

### PartitionTransactionCache

```python
class PartitionTransactionCache()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L47)

A cache with the data updated in the current PartitionTransaction.
It is used to read-your-own-writes before the transaction is committed to the Store.

Internally, updates and deletes are separated into two separate structures
to simplify the querying over them.

<a id="quixstreams.state.base.transaction.PartitionTransactionCache.get"></a>

#### PartitionTransactionCache.get

```python
def get(key: bytes,
        prefix: bytes,
        cf_name: str = "default") -> Union[bytes, Marker]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L70)

Get a value for the key.

Returns the key value if it has been updated during the transaction.

If the key has already been deleted, returns "DELETED" sentinel
(we don't need to check the actual store).
If the key is not present in the cache, returns "UNDEFINED sentinel
(we need to check the store).

:param: key: key as bytes
:param: prefix: key prefix as bytes
:param: cf_name: column family name


<a id="quixstreams.state.base.transaction.PartitionTransactionCache.set"></a>

#### PartitionTransactionCache.set

```python
def set(key: bytes, value: bytes, prefix: bytes, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L100)

Set a value for the key.

:param: key: key as bytes
:param: value: value as bytes
:param: prefix: key prefix as bytes
:param: cf_name: column family name


<a id="quixstreams.state.base.transaction.PartitionTransactionCache.delete"></a>

#### PartitionTransactionCache.delete

```python
def delete(key: Any, prefix: bytes, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L113)

Delete a key.

:param: key: key as bytes
:param: prefix: key prefix as bytes
:param: cf_name: column family name


<a id="quixstreams.state.base.transaction.PartitionTransactionCache.is_empty"></a>

#### PartitionTransactionCache.is\_empty

```python
def is_empty() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L125)

Return True if any changes have been made (updates or deletes), otherwise
return False.

<a id="quixstreams.state.base.transaction.PartitionTransactionCache.get_column_families"></a>

#### PartitionTransactionCache.get\_column\_families

```python
def get_column_families() -> Set[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L132)

Get all update column families.

<a id="quixstreams.state.base.transaction.PartitionTransactionCache.get_updates"></a>

#### PartitionTransactionCache.get\_updates

```python
def get_updates(cf_name: str = "default") -> Dict[bytes, Dict[bytes, bytes]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L138)

Get all updated keys (excluding deleted)

in the format "{<prefix>: {<key>: <value>}}".

:param: cf_name: column family name


<a id="quixstreams.state.base.transaction.PartitionTransactionCache.get_deletes"></a>

#### PartitionTransactionCache.get\_deletes

```python
def get_deletes(cf_name: str = "default") -> Set[bytes]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L147)

Get all deleted keys (excluding updated) as a set.

<a id="quixstreams.state.base.transaction.PartitionTransactionStatus"></a>

### PartitionTransactionStatus

```python
class PartitionTransactionStatus(enum.Enum)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L154)

<a id="quixstreams.state.base.transaction.PartitionTransactionStatus.STARTED"></a>

#### STARTED

Transaction is started and accepts updates

<a id="quixstreams.state.base.transaction.PartitionTransactionStatus.PREPARED"></a>

#### PREPARED

Transaction is prepared, it can no longer receive updates

<a id="quixstreams.state.base.transaction.PartitionTransactionStatus.COMPLETE"></a>

#### COMPLETE

Transaction is fully completed, it cannot be used anymore

<a id="quixstreams.state.base.transaction.PartitionTransactionStatus.FAILED"></a>

#### FAILED

Transaction is failed, it cannot be used anymore

<a id="quixstreams.state.base.transaction.validate_transaction_status"></a>

#### validate\_transaction\_status

```python
def validate_transaction_status(*allowed: PartitionTransactionStatus)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L165)

Check that the status of `RocksDBTransaction` is valid before calling a method

<a id="quixstreams.state.base.transaction.PartitionTransaction"></a>

### PartitionTransaction

```python
class PartitionTransaction(ABC, Generic[K, V])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L189)

A transaction class to perform simple key-value operations like
"get", "set", "delete" and "exists" on a single storage partition.

<a id="quixstreams.state.base.transaction.PartitionTransaction.failed"></a>

#### PartitionTransaction.failed

```python
@property
def failed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L221)

Return `True` if transaction failed to update data at some point.

Failed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.base.transaction.PartitionTransaction.completed"></a>

#### PartitionTransaction.completed

```python
@property
def completed() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L231)

Return `True` if transaction is successfully completed.

Completed transactions cannot be re-used.

**Returns**:

bool

<a id="quixstreams.state.base.transaction.PartitionTransaction.prepared"></a>

#### PartitionTransaction.prepared

```python
@property
def prepared() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L241)

Return `True` if transaction is prepared completed.

Prepared transactions cannot receive new updates, but can be flushed.

**Returns**:

bool

<a id="quixstreams.state.base.transaction.PartitionTransaction.changelog_topic_partition"></a>

#### PartitionTransaction.changelog\_topic\_partition

```python
@property
def changelog_topic_partition() -> Optional[Tuple[str, int]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L251)

Return the changelog topic-partition for the StorePartition of this transaction.

Returns `None` if changelog_producer is not provided.

**Returns**:

(topic, partition) or None

<a id="quixstreams.state.base.transaction.PartitionTransaction.as_state"></a>

#### PartitionTransaction.as\_state

```python
def as_state(prefix: Any = DEFAULT_PREFIX) -> State[K, V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L278)

Create an instance implementing the `State` protocol to be provided

to `StreamingDataFrame` functions.
All operations called on this State object will be prefixed with
the supplied `prefix`.

**Returns**:

an instance implementing the `State` protocol

<a id="quixstreams.state.base.transaction.PartitionTransaction.get"></a>

#### PartitionTransaction.get

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED)
def get(key: K,
        prefix: bytes,
        default: Optional[V] = None,
        cf_name: str = "default") -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L303)

Get a key from the store.

It returns `None` if the key is not found and `default` is not provided.

**Arguments**:

- `key`: key
- `prefix`: a key prefix
- `default`: default value to return if the key is not found
- `cf_name`: column family name

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.transaction.PartitionTransaction.set"></a>

#### PartitionTransaction.set

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED)
def set(key: K, value: V, prefix: bytes, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L339)

Set value for the key.

**Arguments**:

- `key`: key
- `prefix`: a key prefix
- `value`: value
- `cf_name`: column family name

<a id="quixstreams.state.base.transaction.PartitionTransaction.delete"></a>

#### PartitionTransaction.delete

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED)
def delete(key: K, prefix: bytes, cf_name: str = "default")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L362)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key
- `prefix`: a key prefix
- `cf_name`: column family name

<a id="quixstreams.state.base.transaction.PartitionTransaction.exists"></a>

#### PartitionTransaction.exists

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED)
def exists(key: K, prefix: bytes, cf_name: str = "default") -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L381)

Check if the key exists in state.

**Arguments**:

- `key`: key
- `prefix`: a key prefix
- `cf_name`: column family name

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.base.transaction.PartitionTransaction.prepare"></a>

#### PartitionTransaction.prepare

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED)
def prepare(processed_offsets: Optional[dict[str, int]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L401)

Produce changelog messages to the changelog topic for all changes accumulated

in this transaction and prepare transaction to flush its state to the state
store.

After successful `prepare()`, the transaction status is changed to PREPARED,
and it cannot receive updates anymore.

If changelog is disabled for this application, no updates will be produced
to the changelog topic.

**Arguments**:

- `processed_offsets`: the dict with <topic: offset> of the latest processed message

<a id="quixstreams.state.base.transaction.PartitionTransaction.flush"></a>

#### PartitionTransaction.flush

```python
@validate_transaction_status(PartitionTransactionStatus.STARTED,
                             PartitionTransactionStatus.PREPARED)
def flush(changelog_offset: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/transaction.py#L461)

Flush the recent updates to the database.

It writes the WriteBatch to RocksDB and marks itself as finished.

If writing fails, the transaction is marked as failed and
cannot be used anymore.

>***NOTE:*** If no keys have been modified during the transaction
    (i.e. no "set" or "delete" have been called at least once), it will
    not flush ANY data to the database including the offset to optimize
    I/O.

**Arguments**:

- `changelog_offset`: offset of the last produced changelog message,
optional.

<a id="quixstreams.state.base"></a>

## quixstreams.state.base

<a id="quixstreams.state.base.state"></a>

## quixstreams.state.base.state

<a id="quixstreams.state.base.state.State"></a>

### State

```python
class State(ABC, Generic[K, V])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L17)

Primary interface for working with key-value state data from `StreamingDataFrame`

<a id="quixstreams.state.base.state.State.get"></a>

#### State.get

```python
@abstractmethod
def get(key: K, default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L29)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.state.State.set"></a>

#### State.set

```python
@abstractmethod
def set(key: K, value: V)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L40)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.base.state.State.delete"></a>

#### State.delete

```python
@abstractmethod
def delete(key: K)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L49)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.base.state.State.exists"></a>

#### State.exists

```python
@abstractmethod
def exists(key: K) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L59)

Check if the key exists in state.

**Arguments**:

- `key`: key

**Returns**:

True if key exists, False otherwise

<a id="quixstreams.state.base.state.TransactionState"></a>

### TransactionState

```python
class TransactionState(State)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L68)

<a id="quixstreams.state.base.state.TransactionState.__init__"></a>

#### TransactionState.\_\_init\_\_

```python
def __init__(prefix: bytes, transaction: "PartitionTransaction")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L74)

Simple key-value state to be provided into `StreamingDataFrame` functions

**Arguments**:

- `transaction`: instance of `PartitionTransaction`

<a id="quixstreams.state.base.state.TransactionState.get"></a>

#### TransactionState.get

```python
def get(key: K, default: Optional[V] = None) -> Optional[V]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L89)

Get the value for key if key is present in the state, else default

**Arguments**:

- `key`: key
- `default`: default value to return if the key is not found

**Returns**:

value or None if the key is not found and `default` is not provided

<a id="quixstreams.state.base.state.TransactionState.set"></a>

#### TransactionState.set

```python
def set(key: K, value: V)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L99)

Set value for the key.

**Arguments**:

- `key`: key
- `value`: value

<a id="quixstreams.state.base.state.TransactionState.delete"></a>

#### TransactionState.delete

```python
def delete(key: K)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L107)

Delete value for the key.

This function always returns `None`, even if value is not found.

**Arguments**:

- `key`: key

<a id="quixstreams.state.base.state.TransactionState.exists"></a>

#### TransactionState.exists

```python
def exists(key: K) -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/state/base/state.py#L116)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/context.py#L22)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/context.py#L53)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L21)

Provides an interface for all librdkafka connection-based configs.

Allows converting to or from a librdkafka dictionary.

Also obscures secrets and handles any case sensitivity issues.

<a id="quixstreams.kafka.configuration.ConnectionConfig.settings_customise_sources"></a>

#### ConnectionConfig.settings\_customise\_sources

```python
@classmethod
def settings_customise_sources(
    cls, settings_cls: Type[PydanticBaseSettings],
    init_settings: PydanticBaseSettingsSource,
    env_settings: PydanticBaseSettingsSource,
    dotenv_settings: PydanticBaseSettingsSource,
    file_secret_settings: PydanticBaseSettingsSource
) -> Tuple[PydanticBaseSettingsSource, ...]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L101)

Included to ignore reading/setting values from the environment

<a id="quixstreams.kafka.configuration.ConnectionConfig.from_librdkafka_dict"></a>

#### ConnectionConfig.from\_librdkafka\_dict

```python
@classmethod
def from_librdkafka_dict(cls,
                         config: dict,
                         ignore_extras: bool = False) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L115)

Create a `ConnectionConfig` from a librdkafka config dictionary.

**Arguments**:

- `config`: a dict of configs (like {"bootstrap.servers": "url"})
- `ignore_extras`: Ignore non-connection settings (else raise exception)

**Returns**:

a ConnectionConfig

<a id="quixstreams.kafka.configuration.ConnectionConfig.as_librdkafka_dict"></a>

#### ConnectionConfig.as\_librdkafka\_dict

```python
def as_librdkafka_dict(plaintext_secrets: bool = True) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/configuration.py#L130)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L45)

<a id="quixstreams.kafka.producer.Producer.__init__"></a>

#### Producer.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             logger: logging.Logger = logger,
             error_callback: Callable[[KafkaError], None] = _default_error_cb,
             extra_config: Optional[dict] = None,
             flush_timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L46)

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
            poll_timeout: float = PRODUCER_POLL_TIMEOUT,
            buffer_error_max_tries: int = PRODUCER_ON_ERROR_RETRIES,
            on_delivery: Optional[DeliveryCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L84)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L145)

Polls the producer for events and calls `on_delivery` callbacks.

**Arguments**:

- `timeout`: poll timeout seconds; Default: 0 (unlike others)
> NOTE: -1 will hang indefinitely if there are no messages to acknowledge

<a id="quixstreams.kafka.producer.Producer.flush"></a>

#### Producer.flush

```python
def flush(timeout: Optional[float] = None) -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L153)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/producer.py#L184)

A separate producer class used only internally for transactions
(transactions are only needed when using a consumer).

<a id="quixstreams.kafka.consumer"></a>

## quixstreams.kafka.consumer

<a id="quixstreams.kafka.consumer.BaseConsumer"></a>

### BaseConsumer

```python
class BaseConsumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L81)

<a id="quixstreams.kafka.consumer.BaseConsumer.__init__"></a>

#### BaseConsumer.\_\_init\_\_

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L82)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.poll"></a>

#### BaseConsumer.poll

```python
def poll(
    timeout: Optional[float] = None
) -> Optional[RawConfluentKafkaMessageProto]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L145)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.unsubscribe"></a>

#### BaseConsumer.unsubscribe

```python
def unsubscribe()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L250)

Remove current subscription.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.BaseConsumer.store_offsets"></a>

#### BaseConsumer.store\_offsets

```python
def store_offsets(message: Optional[Message] = None,
                  offsets: Optional[List[TopicPartition]] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L259)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.commit"></a>

#### BaseConsumer.commit

```python
def commit(message: Optional[Message] = None,
           offsets: Optional[List[TopicPartition]] = None,
           asynchronous: bool = True) -> Optional[List[TopicPartition]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L290)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.committed"></a>

#### BaseConsumer.committed

```python
def committed(partitions: List[TopicPartition],
              timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L331)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.get_watermark_offsets"></a>

#### BaseConsumer.get\_watermark\_offsets

```python
def get_watermark_offsets(partition: TopicPartition,
                          timeout: Optional[float] = None,
                          cached: bool = False) -> Tuple[int, int]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L349)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.list_topics"></a>

#### BaseConsumer.list\_topics

```python
def list_topics(topic: Optional[str] = None,
                timeout: Optional[float] = None) -> ClusterMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L375)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.memberid"></a>

#### BaseConsumer.memberid

```python
def memberid() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L396)

Return this client's broker-assigned group member id.

The member id is assigned by the group coordinator and is propagated to
the consumer during rebalance.

**Raises**:

- `RuntimeError`: if called on a closed consumer

**Returns**:

`Optional[string]`: Member id string or None

<a id="quixstreams.kafka.consumer.BaseConsumer.offsets_for_times"></a>

#### BaseConsumer.offsets\_for\_times

```python
def offsets_for_times(partitions: List[TopicPartition],
                      timeout: Optional[float] = None) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L409)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.pause"></a>

#### BaseConsumer.pause

```python
def pause(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L435)

Pause consumption for the provided list of partitions.

Paused partitions must be tracked manually.

Does NOT affect the result of `Consumer.assignment()`.

**Arguments**:

- `partitions` (`List[TopicPartition]`): List of topic+partitions to pause.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.BaseConsumer.resume"></a>

#### BaseConsumer.resume

```python
def resume(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L448)

Resume consumption for the provided list of partitions.

**Arguments**:

- `partitions` (`List[TopicPartition]`): List of topic+partitions to resume.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs

<a id="quixstreams.kafka.consumer.BaseConsumer.position"></a>

#### BaseConsumer.position

```python
def position(partitions: List[TopicPartition]) -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L458)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.seek"></a>

#### BaseConsumer.seek

```python
def seek(partition: TopicPartition)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L472)

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

<a id="quixstreams.kafka.consumer.BaseConsumer.assignment"></a>

#### BaseConsumer.assignment

```python
def assignment() -> List[TopicPartition]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L489)

Returns the current partition assignment.

**Raises**:

- `KafkaException`: if a Kafka-based error occurs
- `RuntimeError`: if called on a closed consumer

**Returns**:

`List[TopicPartition]`: List of assigned topic+partitions.

<a id="quixstreams.kafka.consumer.BaseConsumer.set_sasl_credentials"></a>

#### BaseConsumer.set\_sasl\_credentials

```python
def set_sasl_credentials(username: str, password: str)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L502)

Sets the SASL credentials used for this client.

These credentials will overwrite the old ones, and will be used the next
time the client needs to authenticate.
This method will not disconnect existing broker connections that have been
established with the old credentials.
This method is applicable only to SASL PLAIN and SCRAM mechanisms.

**Arguments**:

- `username` (`str`): your username
- `password` (`str`): your password

<a id="quixstreams.kafka.consumer.BaseConsumer.incremental_assign"></a>

#### BaseConsumer.incremental\_assign

```python
def incremental_assign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L516)

Assign new partitions.

Can be called outside the `Consumer` `on_assign` callback (multiple times).
Partitions immediately show on `Consumer.assignment()`.

Any additional partitions besides the ones passed during the `Consumer`
`on_assign` callback will NOT be associated with the consumer group.

**Arguments**:

- `partitions` (`List[TopicPartition]`): a list of topic partitions

<a id="quixstreams.kafka.consumer.BaseConsumer.assign"></a>

#### BaseConsumer.assign

```python
def assign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L530)

Set the consumer partition assignment to the provided list of `TopicPartition` and start consuming.

**Arguments**:

- `partitions` (`List[TopicPartition]`): List of topic+partitions and optionally initial offsets to start consuming from.

**Raises**:

- `None`: KafkaException
- `None`: RuntimeError if called on a closed consumer

<a id="quixstreams.kafka.consumer.BaseConsumer.incremental_unassign"></a>

#### BaseConsumer.incremental\_unassign

```python
def incremental_unassign(partitions: List[TopicPartition])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L540)

Revoke partitions.

Can be called outside an on_revoke callback.

**Arguments**:

- `partitions` (`List[TopicPartition]`): a list of topic partitions

<a id="quixstreams.kafka.consumer.BaseConsumer.unassign"></a>

#### BaseConsumer.unassign

```python
def unassign()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L550)

Removes the current partition assignment and stops consuming.

**Raises**:

- `KafkaException`: 
- `RuntimeError`: if called on a closed consumer

<a id="quixstreams.kafka.consumer.BaseConsumer.close"></a>

#### BaseConsumer.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L559)

Close down and terminate the Kafka Consumer.

Actions performed:

- Stops consuming.
- Commits offsets, unless the consumer property 'enable.auto.commit' is set to False.
- Leaves the consumer group.

Registered callbacks may be called from this method,
see `poll()` for more info.

<a id="quixstreams.kafka.consumer.BaseConsumer.consumer_group_metadata"></a>

#### BaseConsumer.consumer\_group\_metadata

```python
def consumer_group_metadata() -> GroupMetadata
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/kafka/consumer.py#L576)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L90)

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

app.run()
```

<a id="quixstreams.app.Application.__init__"></a>

#### Application.\_\_init\_\_

```python
def __init__(broker_address: Optional[Union[str, ConnectionConfig]] = None,
             *,
             quix_sdk_token: Optional[str] = None,
             consumer_group: Optional[str] = None,
             auto_offset_reset: AutoOffsetReset = "latest",
             commit_interval: float = 5.0,
             commit_every: int = 0,
             consumer_extra_config: Optional[dict] = None,
             producer_extra_config: Optional[dict] = None,
             state_dir: Union[None, str, Path] = None,
             rocksdb_options: Optional[RocksDBOptionsType] = None,
             on_consumer_error: Optional[ConsumerErrorCallback] = None,
             on_processing_error: Optional[ProcessingErrorCallback] = None,
             on_producer_error: Optional[ProducerErrorCallback] = None,
             on_message_processed: Optional[MessageProcessedCallback] = None,
             consumer_poll_timeout: float = 1.0,
             producer_poll_timeout: float = 0.0,
             loglevel: Optional[Union[int, LogLevel]] = "INFO",
             auto_create_topics: bool = True,
             use_changelog_topics: bool = True,
             quix_config_builder: Optional[QuixKafkaConfigsBuilder] = None,
             topic_manager: Optional[TopicManager] = None,
             request_timeout: float = 30,
             topic_create_timeout: float = 60,
             processing_guarantee: ProcessingGuarantee = "at-least-once")
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L128)

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
Default - `"state"`.
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
def Quix(cls, *args, **kwargs)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L375)

RAISES EXCEPTION: DEPRECATED.

use Application() with "quix_sdk_token" parameter or set the "Quix__Sdk__Token"
environment variable.

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L407)

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
def dataframe(topic: Optional[Topic] = None,
              source: Optional[BaseSource] = None) -> StreamingDataFrame
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L487)

A simple helper method that generates a `StreamingDataFrame`, which is used

to define your message processing pipeline.

The topic is what the `StreamingDataFrame` will use as its input, unless
a source is provided (`topic` is optional when using a `source`).

If both `topic` AND `source` are provided, the source will write to that topic
instead of its default topic (which the `StreamingDataFrame` then consumes).

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

app.run()
```

**Arguments**:

- `topic`: a `quixstreams.models.Topic` instance
to be used as an input topic.
- `source`: a `quixstreams.sources` "BaseSource" instance

**Returns**:

`StreamingDataFrame` object

<a id="quixstreams.app.Application.stop"></a>

#### Application.stop

```python
def stop(fail: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L543)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L588)

Create and return a pre-configured Producer instance.
The Producer is initialized with params passed to Application.

It's useful for producing data to Kafka outside the standard Application processing flow,
(e.g. to produce test data into a topic).
Using this within the StreamingDataFrame functions is not recommended, as it creates a new Producer
instance each time, which is not optimized for repeated use in a streaming pipeline.

Example Snippet:

```python
from quixstreams import Application

app = Application(...)
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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L643)

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

app = Application(...)
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

**Arguments**:

- `auto_commit_enable`: Enable or disable auto commit
Default - True

<a id="quixstreams.app.Application.clear_state"></a>

#### Application.clear\_state

```python
def clear_state()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L693)

Clear the state of the application.

<a id="quixstreams.app.Application.add_source"></a>

#### Application.add\_source

```python
def add_source(source: BaseSource, topic: Optional[Topic] = None) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L699)

Add a source to the application.

Use when no transformations (which requires a `StreamingDataFrame`) are needed.

See :class:`quixstreams.sources.base.BaseSource` for more details.

**Arguments**:

- `source`: a :class:`quixstreams.sources.BaseSource` instance
- `topic`: the :class:`quixstreams.models.Topic` instance the source will produce to
Default - the topic generated by the `source.default_topic()` method.
Note: the names of default topics are prefixed with "source__".

<a id="quixstreams.app.Application.run"></a>

#### Application.run

```python
def run(dataframe: Optional[StreamingDataFrame] = None,
        timeout: float = 0.0,
        count: int = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L732)

Start processing data from Kafka using provided `StreamingDataFrame`

Once started, it can be safely terminated with a `SIGTERM` signal
(like Kubernetes does) or a typical `KeyboardInterrupt` (`Ctrl+C`).

Alternatively, stop conditions can be set (typically for debugging purposes);
    has the option of stopping after a number of messages, timeout, or both.

Not setting a timeout or count limit will result in the Application running
  indefinitely (expected production behavior).


Stop Condition Details:

A timeout will immediately stop an Application once no new messages have
    been consumed after T seconds (after rebalance and recovery).

A count will process N total records from ANY input/SDF topics (so
  multiple input topics will very likely differ in their consume total!) after
  an initial rebalance and recovery.
THEN, any remaining processes from things such as groupby (which uses internal
  topics) will also be validated to ensure the results of said messages are
  fully processed (this does NOT count towards the process total).
Note that without a timeout, the Application runs until the count is hit.

If timeout and count are used together (which is the recommended pattern for
debugging), either condition will trigger a stop.


Example Snippet:

```python
from quixstreams import Application

# Set up an `app = Application` and  `sdf = StreamingDataFrame`;
# add some operations to `sdf` and then run everything.

app = Application(broker_address='localhost:9092', consumer_group='group')
topic = app.topic('test-topic')
df = app.dataframe(topic)
df.apply(lambda value, context: print('New message', value)

app.run()  # could pass `timeout=5` here, for example
```

**Arguments**:

- `dataframe`: DEPRECATED - do not use; sdfs are now automatically tracked.
- `timeout`: maximum time to wait for a new message.
Default = 0.0 (infinite)
- `count`: how many input topic messages to process before stopping.
Default = 0 (infinite)

<a id="quixstreams.app.Application.setup_topics"></a>

#### Application.setup\_topics

```python
def setup_topics()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L910)

Validate the application topics

<a id="quixstreams.app.ApplicationConfig"></a>

### ApplicationConfig

```python
class ApplicationConfig(BaseSettings)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L1081)

Immutable object holding the application configuration

For details see :class:`quixstreams.Application`

<a id="quixstreams.app.ApplicationConfig.settings_customise_sources"></a>

#### ApplicationConfig.settings\_customise\_sources

```python
@classmethod
def settings_customise_sources(
    cls, settings_cls: Type[PydanticBaseSettings],
    init_settings: PydanticBaseSettingsSource,
    env_settings: PydanticBaseSettingsSource,
    dotenv_settings: PydanticBaseSettingsSource,
    file_secret_settings: PydanticBaseSettingsSource
) -> Tuple[PydanticBaseSettingsSource, ...]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L1116)

Included to ignore reading/setting values from the environment

<a id="quixstreams.app.ApplicationConfig.copy"></a>

#### ApplicationConfig.copy

```python
def copy(**kwargs) -> Self
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/app.py#L1129)

Update the application config and return a copy

<a id="quixstreams.runtracker"></a>

## quixstreams.runtracker

<a id="quixstreams.runtracker.RunTracker"></a>

### RunTracker

```python
class RunTracker()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L14)

<a id="quixstreams.runtracker.RunTracker.__init__"></a>

#### RunTracker.\_\_init\_\_

```python
def __init__(processing_context: ProcessingContext)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L48)

Tracks the runtime status of an Application, along with managing variables
  associated with stopping the app based on a timeout or count.

Though intended for debugging, it is designed to minimize impact on
  normal Application operation.

<a id="quixstreams.runtracker.RunTracker.stop_and_reset"></a>

#### RunTracker.stop\_and\_reset

```python
def stop_and_reset()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L68)

Called when Application is stopped, or self._stop_checker condition is met.
Resets all values required for re-running.

<a id="quixstreams.runtracker.RunTracker.update_status"></a>

#### RunTracker.update\_status

```python
def update_status()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L86)

Trigger stop if any stop conditions are met.

<a id="quixstreams.runtracker.RunTracker.set_topics"></a>

#### RunTracker.set\_topics

```python
def set_topics(primary: list[str], repartition: list[str])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L94)

Sets the primary and repartition topic lists for counting.

<a id="quixstreams.runtracker.RunTracker.set_as_running"></a>

#### RunTracker.set\_as\_running

```python
def set_as_running()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L101)

Called as part of Application.run() to initialize self.running.

<a id="quixstreams.runtracker.RunTracker.set_current_message_tp"></a>

#### RunTracker.set\_current\_message\_tp

```python
def set_current_message_tp(tp: Optional[tuple[str, int]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L110)

Sets the current message topic partition (if one was consumed)

<a id="quixstreams.runtracker.RunTracker.timeout_refresh"></a>

#### RunTracker.timeout\_refresh

```python
def timeout_refresh()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L116)

Timeout is refreshed when:
- Rebalance completes
- Recovery completes
- Any message is consumed (reset during timeout check)

<a id="quixstreams.runtracker.RunTracker.set_stop_condition"></a>

#### RunTracker.set\_stop\_condition

```python
def set_stop_condition(timeout: float = 0.0, count: int = 0)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/runtracker.py#L125)

Called as part of app.run(); this handles the users optional stop conditions.

<a id="quixstreams.sources.core"></a>

## quixstreams.sources.core

<a id="quixstreams.sources.core.kafka.checkpoint"></a>

## quixstreams.sources.core.kafka.checkpoint

<a id="quixstreams.sources.core.kafka.checkpoint.Checkpoint"></a>

### Checkpoint

```python
class Checkpoint(BaseCheckpoint)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/checkpoint.py#L15)

Checkpoint implementation used by the KafkaReplicatorSource

<a id="quixstreams.sources.core.kafka.checkpoint.Checkpoint.close"></a>

#### Checkpoint.close

```python
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/checkpoint.py#L41)

Perform cleanup (when the checkpoint is empty) instead of committing.

Needed for exactly-once, as Kafka transactions are timeboxed.

<a id="quixstreams.sources.core.kafka.checkpoint.Checkpoint.commit"></a>

#### Checkpoint.commit

```python
def commit()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/checkpoint.py#L50)

Commit the checkpoint.

This method will:
 1. Flush the producer to ensure everything is delivered.
 2. Commit topic offsets.

<a id="quixstreams.sources.core.kafka"></a>

## quixstreams.sources.core.kafka

<a id="quixstreams.sources.core.kafka.kafka"></a>

## quixstreams.sources.core.kafka.kafka

<a id="quixstreams.sources.core.kafka.kafka.KafkaReplicatorSource"></a>

### KafkaReplicatorSource

```python
class KafkaReplicatorSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/kafka.py#L27)

Source implementation that replicates a topic from a Kafka broker to your application broker.

Running multiple instances of this source is supported.

Example Snippet:

```python
from quixstreams import Application
from quixstreams.sources.kafka import KafkaReplicatorSource

app = Application(
    consumer_group="group",
)

source = KafkaReplicatorSource(
    name="source-second-kafka",
    app_config=app.config,
    topic="second-kafka-topic",
    broker_address="localhost:9092",
)

sdf = app.dataframe(source=source)
sdf = sdf.print()
app.run()
```

<a id="quixstreams.sources.core.kafka.kafka.KafkaReplicatorSource.__init__"></a>

#### KafkaReplicatorSource.\_\_init\_\_

```python
def __init__(
    name: str,
    app_config: "ApplicationConfig",
    topic: str,
    broker_address: Union[str, ConnectionConfig],
    auto_offset_reset: Optional[AutoOffsetReset] = "latest",
    consumer_extra_config: Optional[dict] = None,
    consumer_poll_timeout: Optional[float] = None,
    shutdown_timeout: float = 10,
    on_consumer_error: ConsumerErrorCallback = default_on_consumer_error,
    value_deserializer: DeserializerType = "json",
    key_deserializer: DeserializerType = "bytes",
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/kafka.py#L56)

**Arguments**:

- `name`: The source unique name.
It is used to generate the default topic name and consumer group name on the source broker.
Running multiple instances of `KafkaReplicatorSource` with the same name connected
to the same broker will make them share the same consumer group.
- `app_config`: The configuration of the application. Used by the source to connect to the application kafka broker.
- `topic`: The topic to replicate.
- `broker_address`: The connection settings for the source Kafka.
- `auto_offset_reset`: Consumer `auto.offset.reset` setting.
Default - Use the Application `auto_offset_reset` setting.
- `consumer_extra_config`: A dictionary with additional options that
will be passed to `confluent_kafka.Consumer` as is.
Default - `None`
- `consumer_poll_timeout`: timeout for `RowConsumer.poll()`
Default - Use the Application `consumer_poll_timeout` setting.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown.
- `on_consumer_error`: Triggered when the source `Consumer` fails to poll Kafka.
- `value_deserializer`: The default topic value deserializer, used by StreamingDataframe connected to the source.
Default - `json`
- `key_deserializer`: The default topic key deserializer, used by StreamingDataframe connected to the source.
Default - `json`
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.core.kafka.quix"></a>

## quixstreams.sources.core.kafka.quix

<a id="quixstreams.sources.core.kafka.quix.QuixEnvironmentSource"></a>

### QuixEnvironmentSource

```python
class QuixEnvironmentSource(KafkaReplicatorSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/quix.py#L23)

Source implementation that replicates a topic from a Quix Cloud environment to your application broker.
It can copy messages for development and testing without risking producing them back or affecting the consumer groups.

Running multiple instances of this source is supported.

Example Snippet:

```python
from quixstreams import Application
from quixstreams.sources.kafka import QuixEnvironmentSource

app = Application(
    consumer_group="group",
)

source = QuixEnvironmentSource(
    name="source-quix",
    app_config=app.config,
    quix_workspace_id="WORKSPACE_ID",
    quix_sdk_token="WORKSPACE_SDK_TOKEN",
    topic="quix-source-topic",
)

sdf = app.dataframe(source=source)
sdf = sdf.print()
app.run()
```

<a id="quixstreams.sources.core.kafka.quix.QuixEnvironmentSource.__init__"></a>

#### QuixEnvironmentSource.\_\_init\_\_

```python
def __init__(
    name: str,
    app_config: "ApplicationConfig",
    topic: str,
    quix_sdk_token: str,
    quix_workspace_id: str,
    quix_portal_api: Optional[str] = None,
    auto_offset_reset: Optional[AutoOffsetReset] = None,
    consumer_extra_config: Optional[dict] = None,
    consumer_poll_timeout: Optional[float] = None,
    shutdown_timeout: float = 10,
    on_consumer_error: ConsumerErrorCallback = default_on_consumer_error,
    value_deserializer: DeserializerType = "json",
    key_deserializer: DeserializerType = "bytes",
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/kafka/quix.py#L54)

**Arguments**:

- `quix_workspace_id`: The Quix workspace ID of the source environment.
- `quix_sdk_token`: Quix cloud sdk token used to connect to the source environment.
- `quix_portal_api`: The Quix portal API URL of the source environment.
Default - `Quix__Portal__Api` environment variable or Quix cloud production URL

For other parameters See `quixstreams.sources.kafka.KafkaReplicatorSource`

<a id="quixstreams.sources.core.csv"></a>

## quixstreams.sources.core.csv

<a id="quixstreams.sources.core.csv.CSVSource"></a>

### CSVSource

```python
class CSVSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/csv.py#L13)

<a id="quixstreams.sources.core.csv.CSVSource.__init__"></a>

#### CSVSource.\_\_init\_\_

```python
def __init__(path: Union[str, Path],
             name: str,
             key_extractor: Optional[Callable[[dict], Union[str,
                                                            bytes]]] = None,
             timestamp_extractor: Optional[Callable[[dict], int]] = None,
             delay: float = 0,
             shutdown_timeout: float = 10,
             dialect: str = "excel") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/core/csv.py#L14)

A base CSV source that reads data from a CSV file and produces rows

to the Kafka topic in JSON format.

**Arguments**:

- `path`: a path to the CSV file.
- `name`: a unique name for the Source.
It is used as a part of the default topic name.
- `key_extractor`: an optional callable to extract the message key from the row.
It must return either `str` or `bytes`.
If empty, the Kafka messages will be produced without keys.
Default - `None`.
- `timestamp_extractor`: an optional callable to extract the message timestamp from the row.
It must return time in milliseconds as `int`.
If empty, the current epoch will be used.
Default - `None`
- `delay`: an optional delay after producing each row for stream simulation.
Default - `0`.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shut down.
- `dialect`: a CSV dialect to use. It affects quoting and delimiters.
See the ["csv" module docs](https://docs.python.org/3/library/csv.html#csv-fmt-params) for more info.
Default - `"excel"`.

<a id="quixstreams.sources"></a>

## quixstreams.sources

<a id="quixstreams.sources.community.kinesis.kinesis"></a>

## quixstreams.sources.community.kinesis.kinesis

<a id="quixstreams.sources.community.kinesis.kinesis.KinesisSource"></a>

### KinesisSource

```python
class KinesisSource(StatefulSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/kinesis.py#L22)

NOTE: Requires `pip install quixstreams[kinesis]` to work.

This source reads data from an Amazon Kinesis stream, dumping it to a
kafka topic using desired `StreamingDataFrame`-based transformations.

Provides "at-least-once" guarantees.

The incoming message value will be in bytes, so transform in your SDF accordingly.

Example Usage:

```python
from quixstreams import Application
from quixstreams.sources.community.kinesis import KinesisSource


kinesis = KinesisSource(
    stream_name="<YOUR STREAM>",
    aws_access_key_id="<YOUR KEY ID>",
    aws_secret_access_key="<YOUR SECRET KEY>",
    aws_region="<YOUR REGION>",
    auto_offset_reset="earliest",  # start from the beginning of the stream (vs end)
)

app = Application(
    broker_address="<YOUR BROKER INFO>",
    consumer_group="<YOUR GROUP>",
)

sdf = app.dataframe(source=kinesis).print(metadata=True)
# YOUR LOGIC HERE!

if __name__ == "__main__":
    app.run()
```

<a id="quixstreams.sources.community.kinesis.kinesis.KinesisSource.__init__"></a>

#### KinesisSource.\_\_init\_\_

```python
def __init__(
        stream_name: str,
        aws_region: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        aws_endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_KINESIS"),
        shutdown_timeout: float = 10,
        auto_offset_reset: AutoOffsetResetType = "latest",
        max_records_per_shard: int = 1000,
        commit_interval: float = 5.0,
        retry_backoff_secs: float = 5.0,
        on_client_connect_success: Optional[
            ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[
            ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/kinesis.py#L61)

**Arguments**:

- `stream_name`: name of the desired Kinesis stream to consume.
- `aws_region`: The AWS region.
NOTE: can alternatively set the AWS_REGION environment variable
- `aws_access_key_id`: the AWS access key ID.
NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
- `aws_secret_access_key`: the AWS secret access key.
NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
- `aws_endpoint_url`: the endpoint URL to use; only required for connecting
to a locally hosted Kinesis.
NOTE: can alternatively set the AWS_ENDPOINT_URL_KINESIS environment variable
- `shutdown_timeout`: 
- `auto_offset_reset`: When no previous offset has been recorded, whether to
start from the beginning ("earliest") or end ("latest") of the stream.
- `max_records_per_shard`: During round-robin consumption, how many records
to consume per shard (partition) per consume (NOT per-commit).
- `commit_interval`: the time between commits
- `retry_backoff_secs`: how long to back off from doing HTTP calls for a
shard when Kinesis consumer encounters handled/expected errors.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.kinesis"></a>

## quixstreams.sources.community.kinesis

<a id="quixstreams.sources.community.kinesis.consumer"></a>

## quixstreams.sources.community.kinesis.consumer

<a id="quixstreams.sources.community.kinesis.consumer.KinesisStreamShardsNotFound"></a>

### KinesisStreamShardsNotFound

```python
class KinesisStreamShardsNotFound(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/consumer.py#L28)

Raised when the Kinesis Stream has no shards

<a id="quixstreams.sources.community.kinesis.consumer.KinesisConsumer"></a>

### KinesisConsumer

```python
class KinesisConsumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/consumer.py#L63)

Consume all shards for a given Kinesis stream in a batched, round-robin fashion.
Also handles checkpointing of said stream (requires a `KinesisCheckpointer`).

<a id="quixstreams.sources.community.kinesis.consumer.KinesisConsumer.process_shards"></a>

#### KinesisConsumer.process\_shards

```python
def process_shards()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/consumer.py#L90)

Process records from the Stream shards one by one and checkpoint their
sequence numbers.

<a id="quixstreams.sources.community.kinesis.consumer.KinesisConsumer.commit"></a>

#### KinesisConsumer.commit

```python
def commit(force: bool = False)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/kinesis/consumer.py#L104)

Commit the checkpoint and save the progress of the

<a id="quixstreams.sources.community.pandas"></a>

## quixstreams.sources.community.pandas

<a id="quixstreams.sources.community.pandas.PandasDataFrameSource"></a>

### PandasDataFrameSource

```python
class PandasDataFrameSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pandas.py#L20)

<a id="quixstreams.sources.community.pandas.PandasDataFrameSource.__init__"></a>

#### PandasDataFrameSource.\_\_init\_\_

```python
def __init__(df: pd.DataFrame,
             key_column: str,
             timestamp_column: str = None,
             delay: float = 0,
             shutdown_timeout: float = 10,
             keep_meta_as_values: bool = True,
             name: str = "pandas-dataframe-source") -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pandas.py#L21)

A source that reads data from a pandas.DataFrame and produces rows to a Kafka topic in JSON format.

**Arguments**:

- `df`: the pandas.DataFrame object to read data from.
- `key_column`: a column name that contains the messages keys.
The values in dataframe[key_column] must be either strings or `None`.
- `timestamp_column`: an optional argument to specify a dataframe column that contains the messages timestamps.
The values in dataframe[timestamp_column] must be time in milliseconds as `int`.
If empty, the current epoch will be used.
Default - `None`
- `name`: a unique name for the Source, used as a part of the default topic name.
Default - `"pandas-dataframe-source"`.
- `delay`: an optional delay after producing each row for stream simulation.
Default - `0`.
- `shutdown_timeout`: Time in seconds the application waits for the source to gracefully shut down.
- `keep_meta_as_values`: Whether to keep metadata (timestamp_column and key_column) as-values data too.
If True, timestamp and key columns are passed both as metadata and values in the message.
If False, timestamp and key columns are passed only as the message's metadata.
Default - `True`.

<a id="quixstreams.sources.community.pandas.PandasDataFrameSource.run"></a>

#### PandasDataFrameSource.run

```python
def run()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pandas.py#L107)

Produces data from the DataFrame row by row.

<a id="quixstreams.sources.community.file.formats.parquet"></a>

## quixstreams.sources.community.file.formats.parquet

<a id="quixstreams.sources.community.file.formats"></a>

## quixstreams.sources.community.file.formats

<a id="quixstreams.sources.community.file.formats.json"></a>

## quixstreams.sources.community.file.formats.json

<a id="quixstreams.sources.community.file.formats.json.JSONFormat"></a>

### JSONFormat

```python
class JSONFormat(Format)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/formats/json.py#L12)

<a id="quixstreams.sources.community.file.formats.json.JSONFormat.__init__"></a>

#### JSONFormat.\_\_init\_\_

```python
def __init__(compression: Optional[CompressionName],
             loads: Optional[Callable[[str], dict]] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/formats/json.py#L13)

Read a JSON-formatted file (along with decompressing it).

**Arguments**:

- `compression`: the compression type used on the file
- `loads`: A custom function to deserialize objects to the expected dict
with {_key: str, _value: dict, _timestamp: int}.

<a id="quixstreams.sources.community.file.formats.base"></a>

## quixstreams.sources.community.file.formats.base

<a id="quixstreams.sources.community.file.formats.base.Format"></a>

### Format

```python
class Format(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/formats/base.py#L13)

Base class for reading files serialized by the Quix Streams File Sink
Connector.

Formats include things like JSON, Parquet, etc.

Also handles different compression types.

<a id="quixstreams.sources.community.file.formats.base.Format.__init__"></a>

#### Format.\_\_init\_\_

```python
@abstractmethod
def __init__(compression: Optional[CompressionName] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/formats/base.py#L24)

super().__init__() this for a usable init.

<a id="quixstreams.sources.community.file.formats.base.Format.deserialize"></a>

#### Format.deserialize

```python
@abstractmethod
def deserialize(filestream: BinaryIO) -> Iterable[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/formats/base.py#L33)

Parse a filelike byte stream into a collection of records

using the designated format's deserialization approach.

The opening, decompression, and closing of the byte stream's origin is handled
automatically.

The iterable should output dicts with the following data/naming structure:
{_key: str, _value: dict, _timestamp: int}.

**Arguments**:

- `filestream`: a filelike byte stream (such as `f` from `f = open(file)`)

<a id="quixstreams.sources.community.file.local"></a>

## quixstreams.sources.community.file.local

<a id="quixstreams.sources.community.file.local.LocalFileSource"></a>

### LocalFileSource

```python
class LocalFileSource(FileSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/local.py#L20)

A source for extracting records stored within files in a local filesystem.

It recursively iterates from the provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic (same topic for all).

<a id="quixstreams.sources.community.file.local.LocalFileSource.__init__"></a>

#### LocalFileSource.\_\_init\_\_

```python
def __init__(filepath: Union[str, Path],
             key_setter: Optional[Callable[[object], object]] = None,
             value_setter: Optional[Callable[[object], object]] = None,
             timestamp_setter: Optional[Callable[[object], int]] = None,
             file_format: Union[Format, FormatName] = "json",
             compression: Optional[CompressionName] = None,
             has_partition_folders: bool = False,
             replay_speed: float = 1.0,
             name: Optional[str] = None,
             shutdown_timeout: float = 30,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/local.py#L29)

**Arguments**:

- `filepath`: folder to recursively iterate from (a file will be used directly).
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.azure"></a>

## quixstreams.sources.community.file.azure

<a id="quixstreams.sources.community.file.azure.AzureFileSource"></a>

### AzureFileSource

```python
class AzureFileSource(FileSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/azure.py#L36)

A source for extracting records stored within files in an Azure Filestore container.

It recursively iterates from the provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic (same topic for all).

<a id="quixstreams.sources.community.file.azure.AzureFileSource.__init__"></a>

#### AzureFileSource.\_\_init\_\_

```python
def __init__(connection_string: str,
             container: str,
             filepath: Union[str, Path],
             key_setter: Optional[Callable[[object], object]] = None,
             value_setter: Optional[Callable[[object], object]] = None,
             timestamp_setter: Optional[Callable[[object], int]] = None,
             file_format: Union[Format, FormatName] = "json",
             compression: Optional[CompressionName] = None,
             has_partition_folders: bool = False,
             replay_speed: float = 1.0,
             name: Optional[str] = None,
             shutdown_timeout: float = 30,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/azure.py#L45)

**Arguments**:

- `connection_string`: Azure client authentication string.
- `container`: Azure container name.
- `filepath`: folder to recursively iterate from (a file will be used directly).
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.azure.AzureFileSource.file_partition_counter"></a>

#### AzureFileSource.file\_partition\_counter

```python
def file_partition_counter() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/azure.py#L135)

This is a simplified version of the recommended way to retrieve folder
names based on the azure SDK docs examples.

<a id="quixstreams.sources.community.file"></a>

## quixstreams.sources.community.file

<a id="quixstreams.sources.community.file.components"></a>

## quixstreams.sources.community.file.components

<a id="quixstreams.sources.community.file.components.file_deserializer"></a>

## quixstreams.sources.community.file.components.file\_deserializer

<a id="quixstreams.sources.community.file.components.file_deserializer.raw_filestream_deserializer"></a>

#### raw\_filestream\_deserializer

```python
def raw_filestream_deserializer(
    formatter: Union[Format,
                     FormatName], compression: Optional[CompressionName]
) -> Callable[[BinaryIO], Iterable[object]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/components/file_deserializer.py#L9)

Returns a Callable that can deserialize a filestream into some form of iterable.

<a id="quixstreams.sources.community.file.components.file_fetcher"></a>

## quixstreams.sources.community.file.components.file\_fetcher

<a id="quixstreams.sources.community.file.components.file_fetcher.FileFetcher"></a>

### FileFetcher

```python
class FileFetcher()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/components/file_fetcher.py#L13)

Serves a file's content as a stream while downloading another in the background.

<a id="quixstreams.sources.community.file.s3"></a>

## quixstreams.sources.community.file.s3

<a id="quixstreams.sources.community.file.s3.S3FileSource"></a>

### S3FileSource

```python
class S3FileSource(FileSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/s3.py#L38)

A source for extracting records stored within files in an S3 bucket location.

It recursively iterates from the provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic (same topic for all).

<a id="quixstreams.sources.community.file.s3.S3FileSource.__init__"></a>

#### S3FileSource.\_\_init\_\_

```python
def __init__(
        filepath: Union[str, Path],
        bucket: str,
        region_name: Optional[str] = getenv("AWS_REGION"),
        aws_access_key_id: Optional[str] = getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key: Optional[str] = getenv("AWS_SECRET_ACCESS_KEY"),
        endpoint_url: Optional[str] = getenv("AWS_ENDPOINT_URL_S3"),
        key_setter: Optional[Callable[[object], object]] = None,
        value_setter: Optional[Callable[[object], object]] = None,
        timestamp_setter: Optional[Callable[[object], int]] = None,
        has_partition_folders: bool = False,
        file_format: Union[Format, FormatName] = "json",
        compression: Optional[CompressionName] = None,
        replay_speed: float = 1.0,
        name: Optional[str] = None,
        shutdown_timeout: float = 30,
        on_client_connect_success: Optional[
            ClientConnectSuccessCallback] = None,
        on_client_connect_failure: Optional[
            ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/s3.py#L47)

**Arguments**:

- `filepath`: folder to recursively iterate from (a file will be used directly).
- `bucket`: The S3 bucket name only (ex: 'your-bucket').
- `region_name`: The AWS region.
NOTE: can alternatively set the AWS_REGION environment variable
- `aws_access_key_id`: the AWS access key ID.
NOTE: can alternatively set the AWS_ACCESS_KEY_ID environment variable
- `aws_secret_access_key`: the AWS secret access key.
NOTE: can alternatively set the AWS_SECRET_ACCESS_KEY environment variable
- `endpoint_url`: the endpoint URL to use; only required for connecting
to a locally hosted S3.
NOTE: can alternatively set the AWS_ENDPOINT_URL_S3 environment variable
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.compressions.gzip"></a>

## quixstreams.sources.community.file.compressions.gzip

<a id="quixstreams.sources.community.file.compressions"></a>

## quixstreams.sources.community.file.compressions

<a id="quixstreams.sources.community.file.compressions.base"></a>

## quixstreams.sources.community.file.compressions.base

<a id="quixstreams.sources.community.file.base"></a>

## quixstreams.sources.community.file.base

<a id="quixstreams.sources.community.file.base.FileSource"></a>

### FileSource

```python
class FileSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L23)

An interface for extracting records using a file-based client.

It recursively iterates from a provided path (file or folder) and
processes all found files by parsing and producing the given records contained
in each file as individual messages to a kafka topic.

Requires defining methods for navigating folders and retrieving/opening raw
files for the respective client.

When these abstract methods are defined, a FileSource will be able to:
1. Prepare a list of files to download, and retrieve them sequentially
2. Retrieve file contents asynchronously by downloading the upcoming one in the
    background
3. Decompress and deserialize the current file to loop through its records
4. Apply a replay delay for each contained record based on previous record
5. Serialize and produce respective messages to Kafka based on provided `setters`

<a id="quixstreams.sources.community.file.base.FileSource.__init__"></a>

#### FileSource.\_\_init\_\_

```python
def __init__(filepath: Union[str, Path],
             key_setter: Optional[Callable[[object], object]] = None,
             value_setter: Optional[Callable[[object], object]] = None,
             timestamp_setter: Optional[Callable[[object], int]] = None,
             file_format: Union[Format, FormatName] = "json",
             compression: Optional[CompressionName] = None,
             has_partition_folders: bool = False,
             replay_speed: float = 1.0,
             name: Optional[str] = None,
             shutdown_timeout: float = 30,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L43)

**Arguments**:

- `filepath`: folder to recursively iterate from (a file will be used directly).
- `key_setter`: sets the kafka message key for a record in the file.
- `value_setter`: sets the kafka message value for a record in the file.
- `timestamp_setter`: sets the kafka message timestamp for a record in the file.
- `file_format`: what format the files are stored as (ex: "json").
- `compression`: what compression was used on the files, if any (ex. "gzip").
- `has_partition_folders`: whether files are nested within partition folders.
If True, FileSource will match the output topic partition count with it.
Set this flag to True if Quix Streams FileSink was used to dump data.
Note: messages will only align with these partitions if original key is used.
Example structure - a 2 partition topic (0, 1):
[/topic/0/file_0.ext, /topic/0/file_1.ext, /topic/1/file_0.ext]
- `replay_speed`: Produce messages with this speed multiplier, which
roughly reflects the time "delay" between the original message producing.
Use any float >= 0, where 0 is no delay, and 1 is the original speed.
NOTE: Time delay will only be accurate per partition, NOT overall.
- `name`: The name of the Source application (Default: last folder name).
- `shutdown_timeout`: Time in seconds the application waits for the source
to gracefully shut down.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.community.file.base.FileSource.get_file_list"></a>

#### FileSource.get\_file\_list

```python
@abstractmethod
def get_file_list(filepath: Path) -> Iterable[Path]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L106)

Find all files/"blobs" starting from a root folder.

Each item in the iterable should be a resolvable filepath.

**Arguments**:

- `filepath`: a starting filepath

**Returns**:

an iterable will all desired files in their desired processing order

<a id="quixstreams.sources.community.file.base.FileSource.read_file"></a>

#### FileSource.read\_file

```python
@abstractmethod
def read_file(filepath: Path) -> BinaryIO
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L117)

Returns a filepath as an unaltered, open filestream.

Result should be ready for deserialization (and/or decompression).

<a id="quixstreams.sources.community.file.base.FileSource.process_record"></a>

#### FileSource.process\_record

```python
def process_record(record: object)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L124)

Applies replay delay, serializes the record, and produces it to Kafka.

<a id="quixstreams.sources.community.file.base.FileSource.file_partition_counter"></a>

#### FileSource.file\_partition\_counter

```python
def file_partition_counter() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L166)

Can optionally define a way of counting folders to intelligently
set the "default_topic" partition count to match partition folder count.

If defined, class flag "has_partition_folders" can then be set to employ it.

It is not required since this operation may not be easy to implement, and the
file structure may not be used outside Quix Streams FileSink.

Example structure with 2 partitions (0,1):
```
topic_name/
├── 0/               # partition 0
│   ├── file_a.ext
│   └── file_b.ext
└── 1/               # partition 1
    ├── file_x.ext
    └── file_y.ext
```

<a id="quixstreams.sources.community.file.base.FileSource.default_topic"></a>

#### FileSource.default\_topic

```python
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/file/base.py#L192)

Optionally allows the file structure to define the partition count for the

internal topic with file_partition_counter (instead of the default of 1).

**Returns**:

the default topic with optionally altered partition count

<a id="quixstreams.sources.community"></a>

## quixstreams.sources.community

This module contains Sources developed and maintained by the members of Quix Streams community.

<a id="quixstreams.sources.community.pubsub"></a>

## quixstreams.sources.community.pubsub

<a id="quixstreams.sources.community.pubsub.consumer"></a>

## quixstreams.sources.community.pubsub.consumer

<a id="quixstreams.sources.community.pubsub.consumer.PubSubSubscriptionNotFound"></a>

### PubSubSubscriptionNotFound

```python
class PubSubSubscriptionNotFound(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/consumer.py#L30)

Raised when an expected subscription does not exist

<a id="quixstreams.sources.community.pubsub.consumer.PubSubConsumer"></a>

### PubSubConsumer

```python
class PubSubConsumer()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/consumer.py#L34)

<a id="quixstreams.sources.community.pubsub.consumer.PubSubConsumer.poll_and_process"></a>

#### PubSubConsumer.poll\_and\_process

```python
def poll_and_process(timeout: Optional[float] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/consumer.py#L105)

This uses the asynchronous puller to retrieve and handle a message with its
assigned callback.

Committing is a separate step.

<a id="quixstreams.sources.community.pubsub.consumer.PubSubConsumer.poll_and_process_batch"></a>

#### PubSubConsumer.poll\_and\_process\_batch

```python
def poll_and_process_batch()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/consumer.py#L122)

Polls and processes until either the max_batch_size or batch_timeout is reached.

<a id="quixstreams.sources.community.pubsub.consumer.PubSubConsumer.subscribe"></a>

#### PubSubConsumer.subscribe

```python
def subscribe()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/consumer.py#L134)

Asynchronous subscribers require subscribing (synchronous do not).

NOTE: This will not detect whether the subscription exists.

<a id="quixstreams.sources.community.pubsub.consumer.PubSubConsumer.handle_subscription"></a>

#### PubSubConsumer.handle\_subscription

```python
def handle_subscription() -> Subscription
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/consumer.py#L144)

Handles subscription management in one place.

Subscriptions work similarly to Kafka consumer groups.

- Each topic can have multiple subscriptions (consumer group ~= subscription).

- A subscription can have multiple subscribers (similar to consumers in a group).

- NOTE: exactly-once adds message methods (ack_with_response) when enabled.

<a id="quixstreams.sources.community.pubsub.pubsub"></a>

## quixstreams.sources.community.pubsub.pubsub

<a id="quixstreams.sources.community.pubsub.pubsub.PubSubSource"></a>

### PubSubSource

```python
class PubSubSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/pubsub.py#L20)

This source enables reading from a Google Cloud Pub/Sub topic,
dumping it to a kafka topic using desired SDF-based transformations.

Provides "at-least-once" guarantees.

Currently, forwarding message keys ("ordered messages" in Pub/Sub) is unsupported.

The incoming message value will be in bytes, so transform in your SDF accordingly.

Example Usage:

```python
from quixstreams import Application
from quixstreams.sources.community.pubsub import PubSubSource
from os import environ

source = PubSubSource(
    # Suggested: pass JSON-formatted credentials from an environment variable.
    service_account_json = environ["PUBSUB_SERVICE_ACCOUNT_JSON"],
    project_id="<project ID>",
    topic_id="<topic ID>",  # NOTE: NOT the full /x/y/z path!
    subscription_id="<subscription ID>",  # NOTE: NOT the full /x/y/z path!
    create_subscription=True,
)
app = Application(
    broker_address="localhost:9092",
    auto_offset_reset="earliest",
    consumer_group="gcp",
    loglevel="INFO"
)
sdf = app.dataframe(source=source).print(metadata=True)

if __name__ == "__main__":
    app.run()
```

<a id="quixstreams.sources.community.pubsub.pubsub.PubSubSource.__init__"></a>

#### PubSubSource.\_\_init\_\_

```python
def __init__(project_id: str,
             topic_id: str,
             subscription_id: str,
             service_account_json: Optional[str] = None,
             commit_every: int = 100,
             commit_interval: float = 5.0,
             create_subscription: bool = False,
             enable_message_ordering: bool = False,
             shutdown_timeout: float = 10.0,
             on_client_connect_success: Optional[
                 ClientConnectSuccessCallback] = None,
             on_client_connect_failure: Optional[
                 ClientConnectFailureCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/community/pubsub/pubsub.py#L59)

**Arguments**:

- `project_id`: a Google Cloud project ID.
- `topic_id`: a Pub/Sub topic ID (NOT the full path).
- `subscription_id`: a Pub/Sub subscription ID (NOT the full path).
- `service_account_json`: a Google Cloud Credentials JSON as a string
Can instead use environment variables (which have different behavior):
- "GOOGLE_APPLICATION_CREDENTIALS" set to a JSON filepath i.e. /x/y/z.json
- "PUBSUB_EMULATOR_HOST" set to a URL if using an emulated Pub/Sub
- `commit_every`: max records allowed to be processed before committing.
- `commit_interval`: max allowed elapsed time between commits.
- `create_subscription`: whether to attempt to create a subscription at
startup; if it already exists, it instead logs its details (DEBUG level).
- `enable_message_ordering`: When creating a Pub/Sub subscription, whether
to allow message ordering. NOTE: does NOT affect existing subscriptions!
- `shutdown_timeout`: How long to wait for a graceful shutdown of the source.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.base"></a>

## quixstreams.sources.base

<a id="quixstreams.sources.base.exceptions"></a>

## quixstreams.sources.base.exceptions

<a id="quixstreams.sources.base.exceptions.SourceException"></a>

### SourceException

```python
class SourceException(Exception)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/exceptions.py#L9)

Raised in the parent process when a source finish with an exception

<a id="quixstreams.sources.base.source"></a>

## quixstreams.sources.base.source

<a id="quixstreams.sources.base.source.BaseSource"></a>

### BaseSource

```python
class BaseSource(ABC)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L28)

This is the base class for all sources.

Sources are executed in a sub-process of the main application.

To create your own source you need to implement:

* `start`
* `stop`
* `default_topic`

`BaseSource` is the most basic interface, and the framework expects every
source to implement it.
Use `Source` to benefit from a base implementation.

You can connect a source to a StreamingDataframe using the Application.

Example snippet:

```python
class RandomNumbersSource(BaseSource):
def __init__(self):
    super().__init__()
    self._running = False

def start(self):
    self._running = True

    while self._running:
        number = random.randint(0, 100)
        serialized = self._producer_topic.serialize(value=number)
        self._producer.produce(
            topic=self._producer_topic.name,
            key=serialized.key,
            value=serialized.value,
        )

def stop(self):
    self._running = False

def default_topic(self) -> Topic:
    return Topic(
        name="topic-name",
        value_deserializer="json",
        value_serializer="json",
    )


def main():
    app = Application(broker_address="localhost:9092")
    source = RandomNumbersSource()

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```

<a id="quixstreams.sources.base.source.BaseSource.configure"></a>

#### BaseSource.configure

```python
def configure(topic: Topic, producer: RowProducer, **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L116)

This method is triggered before the source is started.

It configures the source's Kafka producer, the topic it will produce to and optional dependencies.

<a id="quixstreams.sources.base.source.BaseSource.setup"></a>

#### BaseSource.setup

```python
def setup()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L125)

When applicable, set up the client here along with any validation to affirm a
valid/successful authentication/connection.

<a id="quixstreams.sources.base.source.BaseSource.start"></a>

#### BaseSource.start

```python
@abstractmethod
def start() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L145)

This method is triggered in the subprocess when the source is started.

The subprocess will run as long as the start method executes.
Use it to fetch data and produce it to Kafka.

<a id="quixstreams.sources.base.source.BaseSource.stop"></a>

#### BaseSource.stop

```python
@abstractmethod
def stop() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L154)

This method is triggered when the application is shutting down.

The source must ensure that the `run` method is completed soon.

<a id="quixstreams.sources.base.source.BaseSource.default_topic"></a>

#### BaseSource.default\_topic

```python
@abstractmethod
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L162)

This method is triggered when the topic is not provided to the source.

The source must return a default topic configuration.

Note: if the default topic is used, the Application will prefix its name with "source__".

<a id="quixstreams.sources.base.source.Source"></a>

### Source

```python
class Source(BaseSource)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L172)

A base class for custom Sources that provides a basic implementation of `BaseSource`
interface.
It is recommended to interface to create custom sources.

Subclass it and implement the `run` method to fetch data and produce it to Kafka.

**Example**:

  
```python
import random
import time

from quixstreams import Application
from quixstreams.sources import Source


class RandomNumbersSource(Source):
    def run(self):
        while self.running:
            number = random.randint(0, 100)
            serialized = self._producer_topic.serialize(value=number)
            self.produce(key=str(number), value=serialized.value)
            time.sleep(0.5)


def main():
    app = Application(broker_address="localhost:9092")
    source = RandomNumbersSource(name="random-source")

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```
  
  
  Helper methods and properties:
  
  * `serialize()`
  * `produce()`
  * `flush()`
  * `running`

<a id="quixstreams.sources.base.source.Source.__init__"></a>

#### Source.\_\_init\_\_

```python
def __init__(
    name: str,
    shutdown_timeout: float = 10,
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L222)

**Arguments**:

- `name`: The source unique name. It is used to generate the topic configuration.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.base.source.Source.running"></a>

#### Source.running

```python
@property
def running() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L251)

Property indicating if the source is running.

The `stop` method will set it to `False`. Use it to stop the source gracefully.

<a id="quixstreams.sources.base.source.Source.cleanup"></a>

#### Source.cleanup

```python
def cleanup(failed: bool) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L259)

This method is triggered once the `run` method completes.

Use it to clean up the resources and shut down the source gracefully.

It flushes the producer when `_run` completes successfully.

<a id="quixstreams.sources.base.source.Source.stop"></a>

#### Source.stop

```python
def stop() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L270)

This method is triggered when the application is shutting down.

It sets the `running` property to `False`.

<a id="quixstreams.sources.base.source.Source.start"></a>

#### Source.start

```python
def start() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L278)

This method is triggered in the subprocess when the source is started.

It marks the source as running, execute it's run method and ensure cleanup happens.

<a id="quixstreams.sources.base.source.Source.run"></a>

#### Source.run

```python
@abstractmethod
def run()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L295)

This method is triggered in the subprocess when the source is started.

The subprocess will run as long as the run method executes.
Use it to fetch data and produce it to Kafka.

<a id="quixstreams.sources.base.source.Source.serialize"></a>

#### Source.serialize

```python
def serialize(key: Optional[object] = None,
              value: Optional[object] = None,
              headers: Optional[Headers] = None,
              timestamp_ms: Optional[int] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L303)

Serialize data to bytes using the producer topic serializers and return a `quixstreams.models.messages.KafkaMessage`.

**Returns**:

`quixstreams.models.messages.KafkaMessage`

<a id="quixstreams.sources.base.source.Source.produce"></a>

#### Source.produce

```python
def produce(value: Optional[Union[str, bytes]] = None,
            key: Optional[Union[str, bytes]] = None,
            headers: Optional[Headers] = None,
            partition: Optional[int] = None,
            timestamp: Optional[int] = None,
            poll_timeout: float = PRODUCER_POLL_TIMEOUT,
            buffer_error_max_tries: int = PRODUCER_ON_ERROR_RETRIES) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L319)

Produce a message to the configured source topic in Kafka.

<a id="quixstreams.sources.base.source.Source.flush"></a>

#### Source.flush

```python
def flush(timeout: Optional[float] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L344)

This method flush the producer.

It ensures all messages are successfully delivered to Kafka.

**Arguments**:

- `timeout` (`float`): time to attempt flushing (seconds).
None use producer default or -1 is infinite. Default: None

**Raises**:

- `CheckpointProducerTimeout`: if any message fails to produce before the timeout

<a id="quixstreams.sources.base.source.Source.default_topic"></a>

#### Source.default\_topic

```python
def default_topic() -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L362)

Return a default topic matching the source name.

The default topic will not be used if the topic has already been provided to the source.

Note: if the default topic is used, the Application will prefix its name with "source__".

**Returns**:

`quixstreams.models.topics.Topic`

<a id="quixstreams.sources.base.source.StatefulSource"></a>

### StatefulSource

```python
class StatefulSource(Source)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L381)

A `Source` class for custom Sources that need a state.

Subclasses are responsible for flushing, by calling `flush`, at reasonable intervals.

**Example**:

  
```python
import random
import time

from quixstreams import Application
from quixstreams.sources import StatefulSource


class RandomNumbersSource(StatefulSource):
    def run(self):

        i = 0
        while self.running:
            previous = self.state.get("number", 0)
            current = random.randint(0, 100)
            self.state.set("number", current)

            serialized = self._producer_topic.serialize(value=current + previous)
            self.produce(key=str(current), value=serialized.value)
            time.sleep(0.5)

            # flush the state every 10 messages
            i += 1
            if i % 10 == 0:
                self.flush()


def main():
    app = Application(broker_address="localhost:9092")
    source = RandomNumbersSource(name="random-source")

    sdf = app.dataframe(source=source)
    sdf.print(metadata=True)

    app.run()


if __name__ == "__main__":
    main()
```

<a id="quixstreams.sources.base.source.StatefulSource.__init__"></a>

#### StatefulSource.\_\_init\_\_

```python
def __init__(
    name: str,
    shutdown_timeout: float = 10,
    on_client_connect_success: Optional[ClientConnectSuccessCallback] = None,
    on_client_connect_failure: Optional[ClientConnectFailureCallback] = None
) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L431)

**Arguments**:

- `name`: The source unique name. It is used to generate the topic configuration.
- `shutdown_timeout`: Time in second the application waits for the source to gracefully shutdown.
- `on_client_connect_success`: An optional callback made after successful
client authentication, primarily for additional logging.
- `on_client_connect_failure`: An optional callback made after failed
client authentication (which should raise an Exception).
Callback should accept the raised Exception as an argument.
Callback must resolve (or propagate/re-raise) the Exception.

<a id="quixstreams.sources.base.source.StatefulSource.configure"></a>

#### StatefulSource.configure

```python
def configure(topic: Topic,
              producer: RowProducer,
              *,
              store_partition: Optional[StorePartition] = None,
              **kwargs) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L458)

This method is triggered before the source is started.

It configures the source's Kafka producer, the topic it will produce to and the store partition.

<a id="quixstreams.sources.base.source.StatefulSource.store_partitions_count"></a>

#### StatefulSource.store\_partitions\_count

```python
@property
def store_partitions_count() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L477)

Count of store partitions.

Used to configure the number of partition in the changelog topic.

<a id="quixstreams.sources.base.source.StatefulSource.assigned_store_partition"></a>

#### StatefulSource.assigned\_store\_partition

```python
@property
def assigned_store_partition() -> int
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L486)

The store partition assigned to this instance

<a id="quixstreams.sources.base.source.StatefulSource.store_name"></a>

#### StatefulSource.store\_name

```python
@property
def store_name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L493)

The source store name

<a id="quixstreams.sources.base.source.StatefulSource.state"></a>

#### StatefulSource.state

```python
@property
def state() -> State
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L500)

Access the `State` of the source.

The `State` lifecycle is tied to the store transaction. A transaction is only valid until the next `.flush()` call. If no valid transaction exist, a new transaction is created.

Important: after each `.flush()` call, a previously returned instance is invalidated and cannot be used. The property must be called again.

<a id="quixstreams.sources.base.source.StatefulSource.flush"></a>

#### StatefulSource.flush

```python
def flush(timeout: Optional[float] = None) -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/source.py#L519)

This method commit the state and flush the producer.

It ensures the state is published to the changelog topic and all messages are successfully delivered to Kafka.

**Arguments**:

- `timeout` (`float`): time to attempt flushing (seconds).
None use producer default or -1 is infinite. Default: None

**Raises**:

- `CheckpointProducerTimeout`: if any message fails to produce before the timeout

<a id="quixstreams.sources.base.manager"></a>

## quixstreams.sources.base.manager

<a id="quixstreams.sources.base.manager.SourceProcess"></a>

### SourceProcess

```python
class SourceProcess(process)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L31)

An implementation of the Source subprocess.

It manages a source and its subprocess, handles the communication between the child and parent processes,
lifecycle, and error handling.

Some methods are designed to be used from the parent process, and others from the child process.

<a id="quixstreams.sources.base.manager.SourceProcess.run"></a>

#### SourceProcess.run

```python
def run() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L81)

An entrypoint of the child process.

Responsible for:
    * Configuring the signal handlers to handle shutdown properly
    * Execution of the source `run` method
    * Reporting the source exceptions to the parent process

<a id="quixstreams.sources.base.manager.SourceProcess.raise_for_error"></a>

#### SourceProcess.raise\_for\_error

```python
def raise_for_error() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L205)

Raise a `quixstreams.sources.manager.SourceException`
if the child process was terminated with an exception.

<a id="quixstreams.sources.base.manager.SourceProcess.stop"></a>

#### SourceProcess.stop

```python
def stop()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L229)

Handle shutdown of the source and its subprocess.

First, it tries to shut down gracefully by sending a SIGTERM and waiting up to
`source.shutdown_timeout` seconds for the process to exit. If the process
is still alive, it will kill it with a SIGKILL.

<a id="quixstreams.sources.base.manager.SourceManager"></a>

### SourceManager

```python
class SourceManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L252)

Class managing the sources registered with the app

Sources run in their separate process pay attention about cross-process communication

<a id="quixstreams.sources.base.manager.SourceManager.register"></a>

#### SourceManager.register

```python
def register(source: BaseSource, topic, producer, consumer,
             topic_manager) -> SourceProcess
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L262)

Register a new source in the manager.

Each source need to already be configured, can't reuse a topic and must be unique

<a id="quixstreams.sources.base.manager.SourceManager.raise_for_error"></a>

#### SourceManager.raise\_for\_error

```python
def raise_for_error() -> None
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L313)

Raise an exception if any process has stopped with an exception

<a id="quixstreams.sources.base.manager.SourceManager.is_alive"></a>

#### SourceManager.is\_alive

```python
def is_alive() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/sources/base/manager.py#L320)

Check if any process is alive

**Returns**:

True if at least one process is alive

<a id="quixstreams.sources.base.multiprocessing"></a>

## quixstreams.sources.base.multiprocessing

<a id="quixstreams.rowconsumer"></a>

## quixstreams.rowconsumer

<a id="quixstreams.rowconsumer.RowConsumer"></a>

### RowConsumer

```python
class RowConsumer(BaseConsumer)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L18)

<a id="quixstreams.rowconsumer.RowConsumer.__init__"></a>

#### RowConsumer.\_\_init\_\_

```python
def __init__(broker_address: Union[str, ConnectionConfig],
             consumer_group: str,
             auto_offset_reset: AutoOffsetReset,
             auto_commit_enable: bool = True,
             on_commit: Optional[Callable[
                 [Optional[KafkaError], List[TopicPartition]], None]] = None,
             extra_config: Optional[dict] = None,
             on_error: Optional[ConsumerErrorCallback] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L19)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L71)

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
def poll_row(timeout: Optional[float] = None) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/rowconsumer.py#L109)

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

<a id="quixstreams.checkpointing.checkpoint.BaseCheckpoint"></a>

### BaseCheckpoint

```python
class BaseCheckpoint()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L29)

Base class to keep track of state updates and consumer offsets and to checkpoint these
updates on schedule.

Two implementations exist:
    * one for checkpointing the Application in quixstreams/checkpoint/checkpoint.py
    * one for checkpointing the kafka source in quixstreams/sources/kafka/checkpoint.py

<a id="quixstreams.checkpointing.checkpoint.BaseCheckpoint.expired"></a>

#### BaseCheckpoint.expired

```python
def expired() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L58)

Returns `True` if checkpoint deadline has expired OR
if the total number of processed offsets exceeded the "commit_every" limit
when it's defined.

<a id="quixstreams.checkpointing.checkpoint.BaseCheckpoint.empty"></a>

#### BaseCheckpoint.empty

```python
def empty() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L68)

Returns `True` if checkpoint doesn't have any offsets stored yet.


<a id="quixstreams.checkpointing.checkpoint.BaseCheckpoint.store_offset"></a>

#### BaseCheckpoint.store\_offset

```python
def store_offset(topic: str, partition: int, offset: int)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L75)

Store the offset of the processed message to the checkpoint.

**Arguments**:

- `topic`: topic name
- `partition`: partition number
- `offset`: message offset

<a id="quixstreams.checkpointing.checkpoint.BaseCheckpoint.close"></a>

#### BaseCheckpoint.close

```python
@abstractmethod
def close()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L102)

Perform cleanup (when the checkpoint is empty) instead of committing.

Needed for exactly-once, as Kafka transactions are timeboxed.

<a id="quixstreams.checkpointing.checkpoint.BaseCheckpoint.commit"></a>

#### BaseCheckpoint.commit

```python
@abstractmethod
def commit()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L110)

Commit the checkpoint.

<a id="quixstreams.checkpointing.checkpoint.Checkpoint"></a>

### Checkpoint

```python
class Checkpoint(BaseCheckpoint)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L117)

Checkpoint implementation used by the application

<a id="quixstreams.checkpointing.checkpoint.Checkpoint.get_store_transaction"></a>

#### Checkpoint.get\_store\_transaction

```python
def get_store_transaction(
        topic: str,
        partition: int,
        store_name: str = DEFAULT_STATE_STORE_NAME) -> PartitionTransaction
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L147)

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L170)

Perform cleanup (when the checkpoint is empty) instead of committing.

Needed for exactly-once, as Kafka transactions are timeboxed.

<a id="quixstreams.checkpointing.checkpoint.Checkpoint.commit"></a>

#### Checkpoint.commit

```python
def commit()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/main/quixstreams/checkpointing/checkpoint.py#L179)

Commit the checkpoint.

This method will:
 1. Flush the registered sinks if any
 2. Produce the changelogs for each state store
 3. Flush the producer to ensure everything is delivered.
 4. Commit topic offsets.
 5. Flush each state store partition to the disk.

<a id="quixstreams.checkpointing"></a>

## quixstreams.checkpointing

<a id="quixstreams.checkpointing.exceptions"></a>

## quixstreams.checkpointing.exceptions

