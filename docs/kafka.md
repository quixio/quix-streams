# Kafka and Quix Streams

Quix Streams helps you to leverage Kafka’s powerful features with ease.

## Why this is important

Kafka is a powerful but complex technology. Using Quix Streams, you can leverage the power of Kafka without needing to know its internal details. Quix Streams provides abstractions to make working with Kafka much simpler.

## Concepts

### Topic replica

Each topic can be set to replicate over the Kafka cluster for increased resilience, so a failure of a node will not cause downtime of your processing pipeline. For example, if you set the number of replicas to two, every message you send to the topic will be replicated twice in the cluster.

### Topic retention

Each topic has temporary storage. Every message sent to the topic will remain in Kafka for a configured amount of time or size. That means that a consumer can join the topic later and still consume messages. If your processing pipeline has downtime, no data is lost.

### Topic partitions

Each Kafka topic is created with a number of partitions. You can add more partitions later, but you can’t remove them. Each partition is an independent queue that preserves the order of messages. 

!!! note

  Quix Streams restricts all messages inside one stream to the same single partition. This means that inside one stream, a consumer can rely on the order of messages. 
  
Partitions are spread across your Kafka cluster, over different Kafka nodes, for improved performance.

#### Redistribution of load

Streams are redistributed over available partitions. With an increasing number of streams, each partition will end up with approximately the same number of streams.

!!! warning

	The number of partitions sets the limit for how many replicas of one model can process the topic. For example, a topic with three partitions can be processed with up to three instances of a model. The fourth instance remains idle.

### Consumer group

The **Consumer group** is provided to enable you to horizontally scale your topic processing. Each consumer group has an ID, which you set when opening a connection to the topic:

``` python
topic_consumer = client.get_topic_consumer("{topic}","{your-consumer-group-id}")
```

If you deploy this model with three instances, the partitions are shared across the three instances, to distribute the load.

* If you increase the instance count, some partitions will get reassigned to new instances of the model.

* If you decrease the instance count, partitions left by leaving instances get reassigned to remaining instances in the consumer group.

### Checkpointing

You can think of Kafka temporary storage as a processing queue for each partition. Consumer groups read from this queue and regularly commit offsets to track which messages were already processed. By default, this is done by Quix Streams automatically, but you can override that by manually committing an offset when you are done processing a set of rows:

``` python
from quixstreams import CommitMode

topic_consumer = client.get_topic_consumer("{topic}","{your-consumer-group-id}", commit_settings=CommitMode.Manual)
topic_consumer.commit()
```

The consumer group plays an important role here, as offset commits are associated with the consumer group ID. That means that if you connect to the same topic with a different consumer group ID, the model will start reading from the offset specified, which is latest by default.

!!! note

	When you open a topic you can also choose where to start reading data from. Either read all the data from the start or only read the new data as it arrives. Read more [here](subscribe.md#_open_a_topic_for_reading).

!!! note

  You can use Kafka without specifying a consumer group. If not using a consumer group, you must specify an offset such as `Latest` (default), or `Earliest`, in order to read from Kafka, as Kafka won't have any previously stored value available. Commit and checkpointing features are only available with consumer groups.

!!! tip

	If you want to consume data from the topic locally for debugging purposes, and the model is running elsewhere at the same time, make sure that you change the consumer group ID to prevent clashing with the other deployment.

### Data grouping

Topics group data streams from a single type of source. The rule for maximum performance is to always maintain one schema per topic.

For example:

- For connected car data you could create individual topics to group data from different systems like the engine, transmission, electronics, chassis, infotainment systems.
- For games you might create individual topics to separate player, game and machine data.
- For consumer apps you could create a topic each source i.e one for your IOS app, one for your Android app, and one for your web app.
- For live ML pipelines you’ll want to create a topic for each stage of the pipeline, such as: raw-data-topic → cleaning model → clean-data-topic → ML model → results topic.

### Data governance

Topics are key to good data governance. Use them to organize your data in the following ways:

- Group data streams by type or source.
- Use separate topics for raw, clean, or processed data.
- Create prototyping topics to publish results of models in development.
