<a id="quixstreams.models.topics"></a>

## quixstreams.models.topics

<a id="quixstreams.models.topics.admin"></a>

## quixstreams.models.topics.admin

<a id="quixstreams.models.topics.admin.convert_topic_list"></a>

<br><br>

#### convert\_topic\_list

```python
def convert_topic_list(topics: List[Topic]) -> List[ConfluentTopic]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/admin.py#L23)

Converts `Topic`s to `ConfluentTopic`s as required for Confluent's

`AdminClient.create_topic()`.


<br>
***Arguments:***

- `topics`: list of `Topic`s


<br>
***Returns:***

list of confluent_kafka `ConfluentTopic`s

<a id="quixstreams.models.topics.admin.TopicAdmin"></a>

### TopicAdmin

```python
class TopicAdmin()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/admin.py#L46)

For performing "admin"-level operations on a Kafka cluster, mostly around topics.

Primarily used to create and inspect topic configurations.

<a id="quixstreams.models.topics.admin.TopicAdmin.__init__"></a>

<br><br>

#### TopicAdmin.\_\_init\_\_

```python
def __init__(broker_address: str, extra_config: Optional[Mapping] = None)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/admin.py#L53)


<br>
***Arguments:***

- `broker_address`: the address for the broker
- `extra_config`: optional configs (generally accepts producer configs)

<a id="quixstreams.models.topics.admin.TopicAdmin.list_topics"></a>

<br><br>

#### TopicAdmin.list\_topics

```python
def list_topics() -> Dict[str, ConfluentTopicMetadata]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/admin.py#L74)

Get a list of topics and their metadata from a Kafka cluster


<br>
***Returns:***

a dict of topic names and their metadata objects

<a id="quixstreams.models.topics.admin.TopicAdmin.inspect_topics"></a>

<br><br>

#### TopicAdmin.inspect\_topics

```python
def inspect_topics(topic_names: List[str]) -> Dict[str, Optional[TopicConfig]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/admin.py#L83)

A simplified way of getting the topic configurations of the provided topics

from the cluster (if they exist).


<br>
***Arguments:***

- `topic_names`: a list of topic names


<br>
***Returns:***

a dict with topic names and their respective `TopicConfig`

<a id="quixstreams.models.topics.admin.TopicAdmin.create_topics"></a>

<br><br>

#### TopicAdmin.create\_topics

```python
def create_topics(topics: List[Topic],
                  timeout: int = 10,
                  finalize_timeout: int = 60)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/admin.py#L156)

Create the given list of topics and confirm they are ready.

Also raises an exception with detailed printout should the creation
fail (it ignores issues for a topic already existing).


<br>
***Arguments:***

- `topics`: a list of `Topic`
- `timeout`: timeout of the creation broker request
- `finalize_timeout`: the timeout of the topic finalizing ("ready")

<a id="quixstreams.models.topics.topic"></a>

## quixstreams.models.topics.topic

<a id="quixstreams.models.topics.topic.TopicConfig"></a>

### TopicConfig

```python
@dataclasses.dataclass(eq=True)
class TopicConfig()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/topic.py#L43)

Represents all kafka-level configuration for a kafka topic.

Generally used by Topic and any topic creation procedures.

<a id="quixstreams.models.topics.topic.Topic"></a>

### Topic

```python
class Topic()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/topic.py#L84)

A definition of a Kafka topic.

Typically created with an `app = quixstreams.app.Application()` instance via
`app.topic()`, and used by `quixstreams.dataframe.StreamingDataFrame`
instance.

<a id="quixstreams.models.topics.topic.Topic.__init__"></a>

<br><br>

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/topic.py#L93)


<br>
***Arguments:***

- `name`: topic name
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys
- `config`: optional topic configs via `TopicConfig` (creation/validation)
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message.

<a id="quixstreams.models.topics.topic.Topic.name"></a>

<br><br>

#### Topic.name

```python
@property
def name() -> str
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/topic.py#L122)

Topic name

<a id="quixstreams.models.topics.topic.Topic.row_serialize"></a>

<br><br>

#### Topic.row\_serialize

```python
def row_serialize(row: Row, key: Optional[Any] = None) -> KafkaMessage
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/topic.py#L132)

Serialize Row to a Kafka message structure


<br>
***Arguments:***

- `row`: Row to serialize
- `key`: message key to serialize, optional. Default - current Row key.


<br>
***Returns:***

KafkaMessage object with serialized values

<a id="quixstreams.models.topics.topic.Topic.row_deserialize"></a>

<br><br>

#### Topic.row\_deserialize

```python
def row_deserialize(
        message: ConfluentKafkaMessageProto) -> Union[Row, List[Row], None]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/topic.py#L155)

Deserialize incoming Kafka message to a Row.


<br>
***Arguments:***

- `message`: an object with interface of `confluent_kafka.Message`


<br>
***Returns:***

Row, list of Rows or None if the message is ignored.

<a id="quixstreams.models.topics.manager"></a>

## quixstreams.models.topics.manager

<a id="quixstreams.models.topics.manager.affirm_ready_for_create"></a>

<br><br>

#### affirm\_ready\_for\_create

```python
def affirm_ready_for_create(topics: List[Topic])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L19)

Validate a list of topics is ready for creation attempt


<br>
***Arguments:***

- `topics`: list of `Topic`s

<a id="quixstreams.models.topics.manager.TopicManager"></a>

### TopicManager

```python
class TopicManager()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L29)

The source of all topic management with quixstreams.

Generally initialized and managed automatically by an `Application`,
but allows a user to work with it directly when needed, such as using it alongside
a plain `Producer` to create its topics.

See methods for details.

<a id="quixstreams.models.topics.manager.TopicManager.__init__"></a>

<br><br>

#### TopicManager.\_\_init\_\_

```python
def __init__(topic_admin: TopicAdmin, create_timeout: int = 60)
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L48)


<br>
***Arguments:***

- `topic_admin`: an `Admin` instance (required for some functionality)
- `create_timeout`: timeout for topic creation

<a id="quixstreams.models.topics.manager.TopicManager.changelog_topics"></a>

<br><br>

#### TopicManager.changelog\_topics

```python
@property
def changelog_topics() -> Dict[str, Dict[str, Topic]]
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L71)

Note: `Topic`s are the changelogs.

returns: the changelog topic dict, {topic_name: {suffix: Topic}}

<a id="quixstreams.models.topics.manager.TopicManager.topic_config"></a>

<br><br>

#### TopicManager.topic\_config

```python
def topic_config(num_partitions: Optional[int] = None,
                 replication_factor: Optional[int] = None,
                 extra_config: Optional[dict] = None) -> TopicConfig
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L121)

Convenience method for generating a `TopicConfig` with default settings


<br>
***Arguments:***

- `num_partitions`: the number of topic partitions
- `replication_factor`: the topic replication factor
- `extra_config`: other optional configuration settings


<br>
***Returns:***

a TopicConfig object

<a id="quixstreams.models.topics.manager.TopicManager.topic"></a>

<br><br>

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

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L142)

A convenience method for generating a `Topic`. Will use default config options

as dictated by the TopicManager.


<br>
***Arguments:***

- `name`: topic name
- `value_deserializer`: a deserializer type for values
- `key_deserializer`: a deserializer type for keys
- `value_serializer`: a serializer type for values
- `key_serializer`: a serializer type for keys
- `config`: optional topic configurations (for creation/validation)
- `timestamp_extractor`: a callable that returns a timestamp in
milliseconds from a deserialized message.


<br>
***Returns:***

Topic object with creation configs

<a id="quixstreams.models.topics.manager.TopicManager.changelog_topic"></a>

<br><br>

#### TopicManager.changelog\_topic

```python
def changelog_topic(topic_name: str, store_name: str,
                    consumer_group: str) -> Topic
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L191)

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


<br>
***Arguments:***

- `consumer_group`: name of consumer group (for this app)
- `topic_name`: name of consumed topic (app input topic)
> NOTE: normally contain any prefixes added by TopicManager.topic()
- `store_name`: name of the store this changelog belongs to
(default, rolling10s, etc.)


<br>
***Returns:***

`Topic` object (which is also stored on the TopicManager)

<a id="quixstreams.models.topics.manager.TopicManager.create_topics"></a>

<br><br>

#### TopicManager.create\_topics

```python
def create_topics(topics: List[Topic])
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L262)

Creates topics via an explicit list of provided `Topics`.

Exists as a way to manually specify what topics to create; otherwise,
`create_all_topics()` is generally simpler.


<br>
***Arguments:***

- `topics`: list of `Topic`s

<a id="quixstreams.models.topics.manager.TopicManager.create_all_topics"></a>

<br><br>

#### TopicManager.create\_all\_topics

```python
def create_all_topics()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L277)

A convenience method to create all Topic objects stored on this TopicManager.

<a id="quixstreams.models.topics.manager.TopicManager.validate_all_topics"></a>

<br><br>

#### TopicManager.validate\_all\_topics

```python
def validate_all_topics()
```

[[VIEW SOURCE]](https://github.com/quixio/quix-streams/blob/4fd6ae337f656d6caf022c5329815d7c39ca3466/quixstreams/models/topics/manager.py#L283)

Validates all topics exist and changelogs have correct topic and rep factor.

Issues are pooled and raised as an Exception once inspections are complete.

<a id="quixstreams.models.topics.exceptions"></a>

## quixstreams.models.topics.exceptions

