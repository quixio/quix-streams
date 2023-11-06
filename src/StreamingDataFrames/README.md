# Quixstreams v2.0 ALPHA: Streaming DataFrames

***WARNING: v2.0 is currently in alpha and will likely experience rapid and 
significant interface changes, feature additions, and bugfixes. Use with discretion!***

<br>

## A Note to Current Quixstreams Python Users

Quix is sunsetting the previous python interface and replacing it with a new 
`Streaming DataFrames` interface. 

It is also completely independent of the C# library and will be the first to receive new 
features and functionality.

<br>

## Compatibility with `quixstreams<2.0`

`quixstreams v2.0` is currently fully backwards compatible with the
previous versions of the library, though some functionality is not 100% replicated.

Barring very specific circumstances, you should be able to upgrade to the new version
relatively easily.

To see how to use this new library with your existing ecosystem, see 
[**Quix Platform Users**](./documentation/quix_platform.md).

There are also details about format/functional changes at 
[**Upgrading from Legacy Quixstreams**](./documentation/upgrading_legacy.md).

<br>

## Old client code

Should you need the old version of the client still, the code has been moved to a 
separate branch [here](TODO:LINKHERE).


<br><br>


# Installation on `Python>=3.8`


`pip install quixstreams>=2.0alpha`


<br><br>


# Overview

## What is Streaming DataFrames (SDF)?
Streaming DataFrames is a Pandas-like Python application/microservice framework for 
Kafka or Redpanda. 

It uses a similar interface as Pandas for data transformation (ETL), along with 
additional kafka-specific operations or features.

<br>

## How does it work?
There are two primary components: a `StreamingDataFrame` (`dataframe`) and it's handler, 
`Application` (`app`).

Your `dataframe` is a pre-defined (declarative) pipeline that you give to your `app`
to execute. All of your ETL is done here. 

The `app` manages various Kafka-related setup/teardown and message lifecycle 
(consuming, committing). It processes each message with the `dataframe` you provide it.

<br>

## Current Features

- A simple framework with familiar Pandas interface to ease newcomers into streaming.
- Easily integrates with other Python frameworks and ML models.
- Support for basic serialization formats, including JSON (and Quix-specific)
- Support for basic stateful operations using RocksDB
- At-least-once Kafka processing guarantees
- Designed to run and scale resiliently via container orchestration (like Kubernetes)
- Also easily runs locally for development and debugging
- Seamless integration with the Quix platform.

Unfamiliar with the Quix platform? We make building and releasing your applications 
easy by managing an entire infrastructure stack (Kafka, Kubernetes, CI/CD) for you 
under the hood, with production software best practices in mind, and all backed by Git. 

[Be sure to check us out at quix.io](https://quix.io/) to get up and running with 
Kafka, fast!

<br>

## Upcoming Improvements

- Stateful Windowing: tumbling, hopping, and sliding windows
- Group-bys and joins (for merging topics/keys)
- State recovery based on Kafka changelog topics
- Support for exactly-once Kafka processing (aka transactions)
- Other serialization support like Avro and Protobuf support
- Schema Registry support
- Multiple input topic support (for non-stateful applications)

<br>

## Want to get Involved?

We strongly believe in the open source model and would love to build a strong 
community with you, so please join us!

While we are still working on out contribution guidelines, here are some ways you can 
get involved right now:

- [Join our community mailing list](https://share.hsforms.com/1WWc91mC_SqCvk8xeUWibMw4yjw2)
- [Check out our project on GitHub](https://github.com/quixio/quix-streams/)
- [Hang out with us on Slack](https://join.slack.com/t/stream-processing/shared_invite/zt-25lo3wxo7-28hlegDhR2XqyLWjBQynwA)


<br><br>

# The Basics

Here's the minimum you need to know to get started with Streaming DataFrames.

More detailed breakdowns can be found in the [documentation folder](./documentation).

## The `Application` class

This is the handler and entrypoint of any `quixstreams` application.

The `Application` class houses all kafka-related configuration, and can generate all 
necessary objects you will need to get up and running.

At minimum, you'll need to know your Kafka broker address and the consumer group ID you
wish to use.

You can get started via:
```python
from quixstreams import Application

app = Application(
   broker_address="my_broker_url",
   consumer_group="my_consumer_group_name",
)
```

<br>

### Using the Quix Platform? - `Application.Quix()`

If you are using the Quix platform, you will still use the `Application` class, but
instead call it via `Application.Quix()`.

If you are running this within the Quix platform it will be configured 
automatically. Otherwise, see 
[**Quix Platform Configuration**](./documentation/quix_platform.md).

<br>

## Defining Kafka Topics: `Application.topic()`

Any topic you plan to interact with will require a respective `Topic` object. 

To generate a topic, simply call `Application.topic(name)`, like so:

```
input_topic = app.topic("my_input_topic")
output_topic = app.topic("my_output_topic")
```

Hold on to those topic objects, you'll need them later for your `dataframe`!

<br>

## Creating your DataFrame: `Application.dataframe()`

Next is the `StreamingDataFrame`, which the `Application` class can generate for you 
via:

```python
sdf = app.dataframe(topic=input_topic)
```

Then, add processing steps to your `StreamingDataFrame`, like so:

```python
def add_one(data, ctx):
    for field, value in data.items():
        if isinstance(value, int):
            data[field] += 1

sdf = sdf[["field_0", "field_2", "field_8"]]
sdf = sdf.apply(add_one)
sdf = sdf[sdf["field_0" > 10]]
sdf = sdf.to_topic(output_topic)
# etc...
```

<br>

## Running your Application + DataFrame: `Application.run()`

Finally, we can run this as a Kafka client by handing the sdf to the `.run` call:

```
app.run(sdf)
```

That's it! Once running it will, as an endless loop,
- consume a message
- process it with your `StreamingDataFrame`
- commit the message

<br>

## Putting it All Together

Now we have everything we need to get your app running:

```python
from quixstreams import Application

app = Application(
   broker_address="my_broker_url",
   consumer_group="my_consumer_group_name",
)

input_topic = app.topic("my_input_topic")
output_topic = app.topic("my_output_topic")


def add_one(data, ctx):
    for field, value in data.items():
        if isinstance(value, int):
            data[field] += 1

            
sdf = app.dataframe(topic=input_topic)
sdf = sdf[["field_0", "field_2", "field_8"]]
sdf = sdf[sdf["field_0" > 10]]
sdf = sdf.apply(add_one)
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    app.run(sdf)

```


<br>

## Ready to Jump In? Check out some Examples!

[There are some examples available](./examples) to get you started!

The examples outline other important features like changing serializers, using state, 
and showcasing other `dateframe` operations.
