# Producing Data to Kafka

Quix Streams is a streaming processing library.  
To process the streaming data, the data first needs to be produced to the Kafka topic.

Below we will cover how you can use `Producer` class to produce data to Kafka topics.

## Step 1. Create an Application
To start working with Quix Streams, you first need to create an [`Application`](api-reference/application.md#application) object.

Application is the main entry point, and it provides API to create Producers, Topics, and other necessary objects.
```python
from quixstreams import Application


# Create an Application instance with Kafka config
app = Application(broker_address='localhost:9092')
```

## Step 2. Define a Topic and serialization
When the `Application` is created, you may define the [`Topic`](api-reference/quixstreams.md#topic) object to publish data to it.

The [`Topic`](api-reference/quixstreams.md#topic) is used for:

- Serializing the data.  
- Making the `Application` instance to validate if the topic exists.  
If there is no such topic, by default, the `Application` will try to create it with the default parameters.

To learn more about the Topic objects and available serialization formats, see [Managing Kafka Topics](advanced/topics.md) and [Serialization and Deserialization](advanced/serialization.md) pages.  

```python
# Define a topic "my_topic" with JSON serialization
topic = app.topic(name='my_topic', value_serializer='json')
```


## Step 3. Create a Producer and produce messages
When the `Application` and `Topic` instances are ready, you can create the [`Producer`](api-reference/quixstreams.md#producer) and start producing messages to the topic.

```python
event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}

# Create a Producer instance
with app.get_producer() as producer:
    
    # Serialize an event using the defined Topic 
    message = topic.serialize(key=event["id"], value=event)
    
    # Produce a message into the Kafka topic
    producer.produce(
        topic=topic.name, value=message.value, key=message.key
    )
```


## Complete example
```python
# Create an Application instance with Kafka configs
from quixstreams import Application


app = Application(
    broker_address='localhost:9092', consumer_group='example'
)

# Define a topic "my_topic" with JSON serialization
topic = app.topic(name='my_topic', value_serializer='json')

event = {"id": "1", "text": "Lorem ipsum dolor sit amet"}

# Create a Producer instance
with app.get_producer() as producer:
    
    # Serialize an event using the defined Topic 
    message = topic.serialize(key=event["id"], value=event)
    
    # Produce a message into the Kafka topic
    producer.produce(
        topic=topic.name, value=message.value, key=message.key
    )
```
## Configuration


The Producer configuration is supplied via the [`Application`](api-reference/application.md#application) instance.

The `Producer` is implemented on top of the [`confluent_kafka`](https://github.com/confluentinc/confluent-kafka-python) library, and is configured similarly.

**Main parameters:**

- `broker_address` - the Kafka broker address.
- `partitioner` - the partitioner to be used. Default - `murmur2`. Available values: `"random"`, `"consistent_random"`, `"murmur2"`, `"murmur2_random"`, `"fnv1a"`, `"fnv1a_random"`.
- `producer_poll_timeout` - the timeout to be used when polling Kafka for the producer callbacks. Default - `0.0`. `Producer` polls for callbacks automatically on each `.produce()` call.
- `producer_extra_config` - a dictionary with additional configuration parameters for Producer in the format of `librdkafka`.

The full list of configuration parameters can be found in [the librdkafka documentation](https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md)  
Passing `bootstrap.servers` and `partitioner` within `producer_extra_config` will have no effect because they are already supplied to the `Application` object.
