<a id="quixstreams.platforms.quix.api"></a>

## quixstreams.platforms.quix.api

<a id="quixstreams.platforms.quix.api.QuixPortalApiService"></a>

### QuixPortalApiService

```python
class QuixPortalApiService()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/api.py#L14)

A light wrapper around the Quix Portal Api. If used in the Quix Platform, it will
use that workspaces auth token and portal endpoint, else you must provide it.

Function names closely reflect the respective API endpoint,
each starting with the method [GET, POST, etc.] followed by the endpoint path.

Results will be returned in the form of request's Response.json(), unless something
else is required. Non-200's will raise exceptions.

See the swagger documentation for more info about the endpoints.

<a id="quixstreams.platforms.quix.config"></a>

## quixstreams.platforms.quix.config

<a id="quixstreams.platforms.quix.config.TopicCreationConfigs"></a>

### TopicCreationConfigs

```python
@dataclasses.dataclass
class TopicCreationConfigs()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L27)

<a id="quixstreams.platforms.quix.config.TopicCreationConfigs.name"></a>

<br><br>

#### name

Required when not created by a Quix App.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder"></a>

### QuixKafkaConfigsBuilder

```python
class QuixKafkaConfigsBuilder()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L36)

Retrieves all the necessary information from the Quix API and builds all the
objects required to connect a confluent-kafka client to the Quix Platform.

If not executed within the Quix platform directly, you must provide a Quix
"streaming" (aka "sdk") token, or Personal Access Token.

Ideally you also know your workspace name or id. If not, you can search for it
using a known topic name, but note the search space is limited to the access level
of your token.

It also currently handles the app_auto_create_topics setting for Application.Quix.

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.__init__"></a>

<br><br>

#### QuixKafkaConfigsBuilder.\_\_init\_\_

```python
def __init__(quix_portal_api_service: Optional[QuixPortalApiService] = None,
             workspace_id: Optional[str] = None,
             workspace_cert_path: Optional[str] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L52)


<br>
***Arguments:***

- `quix_portal_api_service`: A QuixPortalApiService instance (else generated)
- `workspace_id`: A valid Quix Workspace ID (else searched for)
- `workspace_cert_path`: path to an existing workspace cert (else retrieved)

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.append_workspace_id"></a>

<br><br>

#### QuixKafkaConfigsBuilder.append\_workspace\_id

```python
def append_workspace_id(s: str) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L152)

Add the workspace ID to a given string, typically a topic or consumer group id


<br>
***Arguments:***

- `s`: the string to append to


<br>
***Returns:***

the string with workspace_id appended

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_workspace"></a>

<br><br>

#### QuixKafkaConfigsBuilder.search\_for\_workspace

```python
def search_for_workspace(
        workspace_name_or_id: Optional[str] = None) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L161)

Search for a workspace given an expected workspace name or id.


<br>
***Arguments:***

- `workspace_name_or_id`: the expected name or id of a workspace


<br>
***Returns:***

the workspace data dict if search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_workspace_info"></a>

<br><br>

#### QuixKafkaConfigsBuilder.get\_workspace\_info

```python
def get_workspace_info(known_workspace_topic: Optional[str] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L184)

Queries for workspace data from the Quix API, regardless of instance cache,

and updates instance attributes from query result.


<br>
***Arguments:***

- `known_workspace_topic`: a topic you know to exist in some workspace

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_workspace_for_topic"></a>

<br><br>

#### QuixKafkaConfigsBuilder.search\_workspace\_for\_topic

```python
def search_workspace_for_topic(workspace_id: str, topic: str) -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L211)

Search through all the topics in the given workspace id to see if there is a

match with the provided topic.


<br>
***Arguments:***

- `workspace_id`: the workspace to search in
- `topic`: the topic to search for


<br>
***Returns:***

the workspace_id if success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.search_for_topic_workspace"></a>

<br><br>

#### QuixKafkaConfigsBuilder.search\_for\_topic\_workspace

```python
def search_for_topic_workspace(topic: str) -> Optional[dict]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L227)

Find what workspace a topic belongs to.

If there is only one workspace altogether, it is assumed to be the workspace.
More than one means each workspace will be searched until the first hit.


<br>
***Arguments:***

- `topic`: the topic to search for


<br>
***Returns:***

workspace data dict if topic search success, else None

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_workspace_ssl_cert"></a>

<br><br>

#### QuixKafkaConfigsBuilder.get\_workspace\_ssl\_cert

```python
def get_workspace_ssl_cert(extract_to_folder: Optional[Path] = None) -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L248)

Gets and extracts zipped certificate from the API to provided folder.

If no path was provided, will dump to /tmp. Expects cert named 'ca.cert'.


<br>
***Arguments:***

- `extract_to_folder`: path to folder to dump zipped cert file to


<br>
***Returns:***

full cert filepath as string

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.create_topics"></a>

<br><br>

#### QuixKafkaConfigsBuilder.create\_topics

```python
def create_topics(topics: Iterable[TopicCreationConfigs],
                  finalize_timeout_seconds: Optional[int] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L311)

Create topics in a Quix cluster.


<br>
***Arguments:***

- `topics`: an iterable with TopicCreationConfigs instances
- `finalize_timeout_seconds`: How long to wait for the topics to be
marked as "Ready" (and thus ready to produce to/consume from).

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.confirm_topics_exist"></a>

<br><br>

#### QuixKafkaConfigsBuilder.confirm\_topics\_exist

```python
def confirm_topics_exist(topics: Iterable[Union[Topic, TopicCreationConfigs]])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L357)

Confirm whether the desired set of topics exists in the Quix workspace.


<br>
***Arguments:***

- `topics`: an iterable with Either Topic or TopicCreationConfigs instances

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_confluent_broker_config"></a>

<br><br>

#### QuixKafkaConfigsBuilder.get\_confluent\_broker\_config

```python
def get_confluent_broker_config(known_topic: Optional[str] = None) -> dict
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L397)

Get the full client config dictionary required to authenticate a confluent-kafka

client to a Quix platform broker/workspace.

The returned config can be used directly by any confluent-kafka-python consumer/
producer (add your producer/consumer-specific configs afterward).


<br>
***Arguments:***

- `known_topic`: a topic known to exist in some workspace


<br>
***Returns:***

a dict of confluent-kafka-python client settings (see librdkafka
config for more details)

<a id="quixstreams.platforms.quix.config.QuixKafkaConfigsBuilder.get_confluent_client_configs"></a>

<br><br>

#### QuixKafkaConfigsBuilder.get\_confluent\_client\_configs

```python
def get_confluent_client_configs(
    topics: list,
    consumer_group_id: Optional[str] = None
) -> Tuple[dict, List[str], Optional[str]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/config.py#L431)

Get all the values you need in order to use a confluent_kafka-based client

with a topic on a Quix platform broker/workspace.

The returned config can be used directly by any confluent-kafka-python consumer/
producer (add your producer/consumer-specific configs afterward).

The topics and consumer group are appended with any necessary values.


<br>
***Arguments:***

- `topics`: list of topics
- `consumer_group_id`: consumer group id, if needed


<br>
***Returns:***

a tuple with configs and altered versions of the topics
and consumer group name

<a id="quixstreams.platforms.quix.env"></a>

## quixstreams.platforms.quix.env

<a id="quixstreams.platforms.quix.env.QuixEnvironment"></a>

### QuixEnvironment

```python
class QuixEnvironment()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/env.py#L7)

Class to access various Quix platform environment settings

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_management_enabled"></a>

<br><br>

#### QuixEnvironment.state\_management\_enabled

```python
@property
def state_management_enabled() -> bool
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/env.py#L19)

Check whether "State management" is enabled for the current deployment


<br>
***Returns:***

True if state management is enabled, otherwise False

<a id="quixstreams.platforms.quix.env.QuixEnvironment.deployment_id"></a>

<br><br>

#### QuixEnvironment.deployment\_id

```python
@property
def deployment_id() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/env.py#L27)

Return current Quix deployment id.

This variable is meant to be set only by Quix Platform and only
when the application is deployed.


<br>
***Returns:***

deployment id or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.workspace_id"></a>

<br><br>

#### QuixEnvironment.workspace\_id

```python
@property
def workspace_id() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/env.py#L39)

Return Quix workspace id if set


<br>
***Returns:***

workspace id or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.portal_api"></a>

<br><br>

#### QuixEnvironment.portal\_api

```python
@property
def portal_api() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/env.py#L47)

Return Quix Portal API url if set


<br>
***Returns:***

portal API URL or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.sdk_token"></a>

<br><br>

#### QuixEnvironment.sdk\_token

```python
@property
def sdk_token() -> Optional[str]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/env.py#L56)

Return Quix SDK token if set


<br>
***Returns:***

sdk token or None

<a id="quixstreams.platforms.quix.env.QuixEnvironment.state_dir"></a>

<br><br>

#### QuixEnvironment.state\_dir

```python
@property
def state_dir() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/7bb0b4e47d690ffe285f34c87c3bbe7d39c6b397/quixstreams/platforms/quix/env.py#L64)

Return application state directory on Quix.


<br>
***Returns:***

path to state dir

