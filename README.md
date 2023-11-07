![Quix - React to data, fast](https://github.com/quixio/.github/blob/main/profile/quixstreams-banner.jpg)

[![Quix on Twitter](https://img.shields.io/twitter/url?label=Twitter&style=social&url=https%3A%2F%2Ftwitter.com%2Fquix_io)](https://twitter.com/quix_io)
[![The Stream Community Slack](https://img.shields.io/badge/-The%20Stream%20Slack-blueviolet)](https://quix.io/slack-invite)
[![Linkedin](https://img.shields.io/badge/LinkedIn-0A66C2.svg?logo=linkedin)](https://www.linkedin.com/company/70925173/)
[![Events](https://img.shields.io/badge/-Events-blueviolet)](https://quix.io/community#events)
[![YouTube](https://img.shields.io/badge/YouTube-FF0000.svg?logo=youtube)](https://www.youtube.com/channel/UCrijXvbQg67m9-le28c7rPA)
[![Docs](https://img.shields.io/badge/-Docs-blueviolet)](https://www.quix.io/docs/client-library-intro.html)
[![Roadmap](https://img.shields.io/badge/-Roadmap-red)](https://github.com/orgs/quixio/projects/1)

# Quix Streams 2.0 (alpha)

>***IMPORTANT:*** Quix Streams v2.0 is currently in alpha and will likely experience rapid and 
significant interface changes, feature additions, and bugfixes.
><br>
>Use with discretion!
>
> ***The documentation for Quix Streams <2.0 can be found [here](./README.legacy.md)***

Quix Streams 2.0 is a cloud native library for processing data in Kafka using pure Python. It’s designed to give you the power of a distributed system in a lightweight library by combining the low-level scalability and resiliency features of Kafka with an easy to use Python interface.

Quix Streams has the following benefits:

- No JVM, no orchestrator, no server-side engine.
- Easily integrates with the entire Python ecosystem (pandas, scikit-learn, TensorFlow, PyTorch etc).
- Support for many serialization formats, including JSON (and Quix-specific).
- Support for stateful operations using RocksDB.
- A simple framework with Pandas-like interface to ease newcomers to streaming.
- "At-least-once" Kafka processing guarantees.
- Designed to run and scale resiliently via container orchestration (like Kubernetes).
- Easily runs locally and in Jupyter Notebook for convenient development and debugging.
- Seamless integration with the Quix platform.

Use Quix Streams to build event-driven, machine learning/AI or physics-based applications that depend on real-time data from Kafka.


## Getting started 🏄


### Install Quix Streams

#### Quix Streams 2.0 (currently in alpha)
To install the latest alpha version of Quix Streams 2.0: 

```shell
python -m pip install --pre quixstreams
```

or

```shell
python -m pip install "quixstreams>=2.0a"
```

#### Requirements
Python 3.8+, Apache Kafka 0.10+

See [requirements.txt](./src/StreamingDataFrames/requirements.txt) for the full list of requirements


### Example Application

Here's an example of how to <b>process</b> data from a Kafka Topic with Quix Streams:

```python
from quixstreams import Application, MessageContext, State

# Define an application
app = Application(
   broker_address="localhost:9092",  # Kafka broker address
   consumer_group="consumer-group-name",  # Kafka consumer group
)

# Define the input and output topics. By default, "json" serialization will be used
input_topic = app.topic("my_input_topic")
output_topic = app.topic("my_output_topic")


def add_one(data: dict, ctx: MessageContext):
    for field, value in data.items():
        if isinstance(value, int):
            data[field] += 1

            
def count(data: dict, ctx: MessageContext, state: State):
    # Get a value from state for the current Kafka message key
    total = state.get('total', default=0)
    total += 1
    # Set a value back to the state
    state.set('total', total)
    # Update your message data with a value from the state
    data['total'] = total

# Create a StreamingDataFrame instance
# StreamingDataFrame is a primary interface to define the message processing pipeline
sdf = app.dataframe(topic=input_topic)

# Print the incoming messages
sdf = sdf.apply(lambda value, ctx: print('Received a message:', value))

# Select fields from incoming messages
sdf = sdf[["field_0", "field_2", "field_8"]]

# Filter only messages with "field_0" > 10 and "field_2" != "test"
sdf = sdf[(sdf["field_0"] > 10) & (sdf["field_2"] != "test")]

# Apply custom function to transform the message
sdf = sdf.apply(add_one)

# Apply a stateful function to persist data to the state store
sdf = sdf.apply(count, stateful=True)

# Print the result before producing it
sdf = sdf.apply(lambda value, ctx: print('Producing a message:', value))

# Produce the result to the output topic 
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    # Run the streaming application 
    app.run(sdf)

```


### How It Works
There are two primary components:
- `StreamingDataFrame` - a predefined declarative pipeline to process and transform incoming messages.
- `Application` - to manage the Kafka-related setup & teardown and message lifecycle (consuming, committing). It processes each message with the dataframe you provide it.

Under the hood, the `Application` will:
- Consume a message.
- Deserialize it.
- Process it with your `StreamingDataFrame`.
- Produce it to the output topic.
- Automatically commit the topic offset and state updates after the message is processed.
- React to Kafka rebalancing updates and manage the topic partitions.
- Manage the State store.
- Handle OS signals and gracefully exit the application.

### More Examples
> You may find more examples in the `examples` folder **[here](./src/StreamingDataFrames/examples)**.

### Advanced Usage

For more in-depth description of Quix Streams components, please
follow these links:
- ***[StreamingDataFrame](./src/StreamingDataFrames/docs/streamingdataframe.md)***.
- ***[Serialization](./src/StreamingDataFrames/docs/serialization.md)***.
- ***[Stateful Processing](./src/StreamingDataFrames/docs/stateful-processing.md)***.
- ***[Usage with Quix SaaS Platform](./src/StreamingDataFrames/docs/quix-platform.md)***.
- ***[Upgrading from Quix Streams <2.0](./src/StreamingDataFrames/docs/upgrading-legacy.md)***.


### Using the [Quix Platform](https://quix.io/) - `Application.Quix()`

This library doesn't have any dependency on any commercial products, but if you use it together with Quix SaaS Platform you will get some advantages out of the box during your development process such as:
- Auto-configuration.
- Monitoring.
- Data explorer.
- Data persistence.
- Pipeline visualization.
- Metrics.

and more.

Quix Streams provides a seamless integration with Quix Platform via `Application.Quix()` class.
This class will automatically configure the Application using Quix SDK Token.
<br>
If you are running this within the Quix platform it will be configured 
automatically.
<br>
Otherwise, please see 
[**Quix Platform Configuration**](./src/StreamingDataFrames/docs/quix-platform.md).


### What's Next

This library is being actively developed. 

Here are some of the planned improvements:
- State recovery based on Kafka changelog topics
- Stateful Windowing: tumbling, hopping, and sliding windows
- Group-bys and joins (for merging topics/keys)
- Support for "exactly-once" Kafka processing (aka transactions)
- Other serialization support like Avro and Protobuf
- Schema Registry support


To find out when the next version is ready, make sure you watch this repo 
and join our [Quix Community on Slack](https://quix.io/slack-invite)! 

## Using Quix Streams with the Quix SaaS platform

This library doesn't have any dependency on any commercial product, but if you use it together with [Quix SaaS platform](https://www.quix.io) you will get some advantages out of the box during your development process such as auto-configuration, monitoring, data explorer, data persistence, pipeline visualization, metrics, and more.

## Contribution Guide

Contributing is a great way to learn and we especially welcome those who haven't contributed to an OSS project before.
<br>
We're very open to any feedback or code contributions to this OSS project ❤️. 

Before contributing, please read our [Contributing](https://github.com/quixio/quix-streams/blob/main/CONTRIBUTING.md) file for how you can best give feedback and contribute. 

## Need help?

If you run into any problems, please create an [issue](https://github.com/quixio/quix-streams/issues) or ask in #quix-help in our [Quix Community on Slack](https://quix.io/slack-invite).  

## Community 👭

Join other software engineers in [The Stream](https://quix.io/slack-invite), an online community of people interested in all things data streaming. This is a space to both listen to and share learnings.

🙌  [Join our Slack community!](https://quix.io/slack-invite)

## License

Quix Streams is licensed under the Apache 2.0 license. View a copy of the License file [here](https://github.com/quixio/quix-streams/blob/main/LICENSE).

## Stay in touch 👋

You can follow us on [Twitter](https://twitter.com/quix_io) and [Linkedin](https://www.linkedin.com/company/70925173) where we share our latest tutorials, forthcoming community events and the occasional meme.  

If you have any questions or feedback - write to us at support@quix.io!
