# Managing Kafka Topics

In this article, we will go over how to manage input and output Kafka topics with Quix Streams.  


To learn more about Changelog topics and how they are managed, please see [Stateful Processing](stateful-processing.md#how-changelog-topics-work)


## Creating Topics Automatically 
Quix Streams provides features to automatically create and configure topics if they don't yet exist in the Kafka broker.

You may use this feature to simplify your development workflow.

To create topics automatically, you need:

1. Make sure the `auto_create_topics=True` configuration parameter is passed to the `Application()` instance.   
Default - `True`.

2. Define a topic using `Application.topic()` method.    
This will let the app track this topic on the Kafka broker

When you have this code in place, the topics will be created on `Application.run()`.

>***NOTE:*** Quix Streams automatically creates topics only if they don't exist in Kafka.   
> It never updates the topic configuration after the topics are already created.

**Example**:

Define input and output topics and create them automatically in Kafka with default configuration.

```python
from quixstreams import Application

# Create an Application and tell it to create topics automatically
app = Application(broker_address='localhost:9092', auto_create_topics=True)

# Define input and output topics
input_topic = app.topic('input')
output_topic = app.topic('output')

# Create a bypass transformation sending messages from the input topic to the output one
sdf = app.dataframe(input_topic).to_topic(output_topic)

# Run the Application. 
# The topics will be validated and created during this function call.
app.run()
```

## Topic Configuration

By default, Quix Streams will create topics with one partition and a replication factor of 1.  

(Note: when working with Quix Cloud, the default replication factor is set by the Quix Cloud platform given the individual broker.)

In production environments, you will probably want to these parameters to be different.

To do that, you can provide a custom configuration to the `Topic` objects.

**Example:**

Define input and output topics and create them automatically in Kafka with custom configs  
by passing them using `TopicConfig`. 

```python
from quixstreams import Application
from quixstreams.models import TopicConfig

# Create an Application and tell it to create topics automatically
app = Application(broker_address='localhost:9092', auto_create_topics=True)

# Define input and output topics with custom configuration using
input_topic = app.topic(
    name='input', config=TopicConfig(replication_factor=3, num_partitions=10),
)

output_topic = app.topic(
    name='output', config=TopicConfig(replication_factor=3, num_partitions=8),
)

# Create a bypass transformation sending messages from the input topic to the output one
sdf = app.dataframe(input_topic).to_topic(output_topic)

# Run the Application. 
# The topics will be validated and created during this function call.
# Note: if the topics already exist, the configs will remain intact.
app.run()
```
