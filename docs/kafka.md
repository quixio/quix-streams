# Kafka and Quix SDK

The Quix SDK helps you to leverage Kafka’s powerful features with ease.

### Why this is important

Kafka is a powerful but complex technology to master. Using the Quix
SDK, you can leverage the power of Kafka without worrying about
mastering it. There are just a couple of important concepts to grasp,
the rest is handled in the background by the SDK.

### Concepts

#### Topic replica

Each topic can be set to replicate over the Kafka cluster for increased
resilience, so a failure of a node will not cause downtime of your
processing pipeline. For example, if you set replica to 2, every message
you send to the topic will be replicated twice in the cluster.

!!! tip

	If you set replication to two, data streamed to the cluster is billed twice.

#### Topic retention

Each topic has temporary storage. Every message sent to the topic will
live in Kafka for a configured amount of time or size. That means that a
consumer can join the topic later and still consume messages. If your
processing pipeline has downtime, no data is lost.

#### Topic partitions

Each Kafka topic is created with a number of partitions. You can add
more partitions later, but you can’t remove them. Each partition is an
independent queue that preserves the order of messages. **The Quix SDK
restricts all messages inside one stream to the same single partition.**
That means that inside one stream, a consumer can rely on the order of
messages. Partitions are spread across your Kafka cluster, over
different Kafka nodes, for improved performance.

##### Redistribution of load

Streams are redistributed over available partitions. With an increasing
number of streams, each partition will end up with approximately the
same number of streams.

!!! warning

	The number of partitions sets the limit for how many parallel instances of one model can process the topic. For example: A topic with three partitions can be processed with up to 3 instances of a model. The fourth instance will remain idle.

#### Consumer group

The **Consumer group** is a concept of how to horizontally scale topic
processing. Each consumer group has an ID, which you set when opening a
connection to the topic:

``` python
output_topic = client.open_input_topic("{topic}","{your-consumer-group-id}")
```

If you deploy this model with a replica set to 3, your model will be
deployed in three instances as members of one consumer group. This group
will share partitions between each other and therefore share the load.

  - If you increase the number, some partitions will get reassigned to
    new instances of the model.

  - If you decrease the number, partitions left by leaving instances get
    reassigned to remaining processing instances in the consumer group.

#### Checkpointing

We can think of Kafka temporary storage as a processing queue for each
partition. Consumer groups read from this queue and regularly commit
offsets to track which messages were already processed. By default, this
is done by the Quix SDK automatically, but you can override that by
manually committing an offset when you are done processing a set of
rows.

``` python
input_topic = client.open_input_topic('Telemetry', commit_settings=CommitMode.Manual)
input_topic.commit()
```

The consumer group is playing an important role here as offset commits
are associated with the consumer group ID. That means that if you
connect to the same topic with a different consumer group ID, the model
will start reading from the start of the Kafka queue.

!!! tip

	If you want to consume data from the topic locally for debugging purposes, and the model is deployed in the Quix serverless environment at the same time, make sure that you change consumer group ID to prevent clashing with the cloud deployment.

!!! note

	When you open a topic you can also choose where to start reading data from. Either read all the data from the start or only read the new data as it arrives. Read more [here](read.md#_open_a_topic_for_reading)

#### Data Grouping

Topics group data streams from a single type of source. The golden rule
for maximum performance is to always maintain one schema per topic.

For example:

  - For connected car data you could create individual topics to group
    data from different systems like the engine, transmission,
    electronics, chassis, infotainment systems.

  - For games you might create individual topics to separate player,
    game and machine data.

  - For consumer apps you could create a topic each source i.e one for
    your IOS app, one for your Android app, and one for your web app.

  - For live ML pipelines you’ll want to create a topic for each stage
    of the pipeline ie raw-data-topic → cleaning model →
    clean-data-topic → ML model → results topic

#### Data Governance

Topics are key to good data governance. Use them to organize your data
by:

  - Group data streams by type or source.

  - Use separate topics for raw, clean or processed data.

  - Create prototyping topics to publish results of models in
    development.

#### Scale

Topics automatically scale. We have designed the underlying
infrastructure to automatically stream any amount of data from any
number of sources. With Quix you can connect one source - like a car,
wearable or web app - to do R\&D, then scale your solution to millions
of cars, wearables or apps in production, all on the same topic.

#### Security

Our topics are secured with industry standard SSL data encryption and
SASL ACL authorization and authentication. You can safely send data over
public networks and trust that it is encrypted in-flight.
