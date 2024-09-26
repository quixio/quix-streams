# Quickstart

Quix Streams is a library for developing real-time streaming applications with Python.

In this quickstart guide, you will learn how to create your first Quix Streams app and run it.


## Demo application
Our demo application will analyze the incoming stream of chat messages, split them into words, and write the results back to Kafka.

In this guide, it's assumed that the Kafka cluster is up and running locally on `localhost:9092`.  

If you don't have Kafka installed yet, just [follow this simple one-liner](./tutorials/README.md#running-kafka-locally) to run it. 

### Step 1. Install Quix Streams

Install Quix Streams with:

```
python -m pip install quixstreams
```

### Step 2. Producing data to Kafka

In order to process events with Quix Streams, they first need to be in Kafka.
Let's create the file `producer.py` to write some test data into a Kafka topic.

```python
from quixstreams import Application

# Create an Application - the main configuration entry point
app = Application(broker_address="localhost:9092", consumer_group="text-splitter-v1")

# Define a topic with chat messages in JSON format
messages_topic = app.topic(name="messages", value_serializer="json")

messages = [
    {"chat_id": "id1", "text": "Lorem ipsum dolor sit amet"},
    {"chat_id": "id2", "text": "Consectetur adipiscing elit sed"},
    {"chat_id": "id1", "text": "Do eiusmod tempor incididunt ut labore et"},
    {"chat_id": "id3", "text": "Mollis nunc sed id semper"},
]


def main():
    with app.get_producer() as producer:
        for message in messages:
            # Serialize chat message to send it to Kafka
            # Use "chat_id" as a Kafka message key
            kafka_msg = messages_topic.serialize(key=message["chat_id"], value=message)

            # Produce chat message to the topic
            print(f'Produce event with key="{kafka_msg.key}" value="{kafka_msg.value}"')
            producer.produce(
                topic=messages_topic.name,
                key=kafka_msg.key,
                value=kafka_msg.value,
            )


if __name__ == "__main__":
    main()
```

### Step 3. Consuming data from Kafka

Let's create the file `consumer.py` to process the data in the topic.
It will start consuming messages from Kafka and apply transformations to them.

```python
from quixstreams import Application

# Create an Application - the main configuration entry point
app = Application(
    broker_address="localhost:9092",
    consumer_group="text-splitter-v1",
    auto_offset_reset="earliest",
)

# Define a topic with chat messages in JSON format
messages_topic = app.topic(name="messages", value_deserializer="json")

# Create a StreamingDataFrame - the stream processing pipeline
# with a Pandas-like interface on streaming data
sdf = app.dataframe(topic=messages_topic)

# Print the input data
sdf = sdf.update(lambda message: print(f"Input:  {message}"))

# Define a transformation to split incoming sentences
# into words using a lambda function
sdf = sdf.apply(
    lambda message: [{"text": word} for word in message["text"].split()],
    expand=True,
)

# Calculate the word length and store the result in the column
sdf["length"] = sdf["text"].apply(lambda word: len(word))

# Print the output result
sdf = sdf.update(lambda row: print(f"Output: {row}"))

# Run the streaming application
if __name__ == "__main__":
    app.run()
```

### Step 4. Running the Producer

Let's run the `producer.py` in a terminal to fill the topic with data.  
If the topic does not exist yet, Quix Streams will create it with the default number of partitions.

```commandline
python producer.py

[2024-02-21 18:26:32,365] [INFO] : Topics required for this application: "messages"
[2024-02-21 18:26:32,379] [INFO] : Validating Kafka topics exist and are configured correctly...
[2024-02-21 18:26:32,462] [INFO] : Kafka topics validation complete
Produce event with key="id1" value="b'{"chat_id":"id1","text":"Lorem ipsum dolor sit amet"}'"
Produce event with key="id2" value="b'{"chat_id":"id2","text":"Consectetur adipiscing elit sed"}'"
Produce event with key="id1" value="b'{"chat_id":"id1","text":"Do eiusmod tempor incididunt ut labore et"}'"
Produce event with key="id3" value="b'{"chat_id":"id3","text":"Mollis nunc sed id semper"}'"
```

### Step 5. Running the Consumer

Now that you have a topic with data, you may start consuming events and process them.  
Let's run the `consumer.py` to see the results.

```commandline
python consumer.py

[2024-02-21 19:57:38,669] [INFO] : Initializing processing of StreamingDataFrame
[2024-02-21 19:57:38,669] [INFO] : Topics required for this application: "messages", "words"
[2024-02-21 19:57:38,699] [INFO] : Validating Kafka topics exist and are configured correctly...
[2024-02-21 19:57:38,718] [INFO] : Kafka topics validation complete
[2024-02-21 19:57:38,718] [INFO] : Initializing state directory at "/app/state/text-splitter-v1"
[2024-02-21 19:57:38,718] [INFO] : Waiting for incoming messages
Input:  {'chat_id': 'id1', 'text': 'Lorem ipsum dolor sit amet'}
Output: {'text': 'Lorem', 'length': 5}
Output: {'text': 'ipsum', 'length': 5}
Output: {'text': 'dolor', 'length': 5}
Output: {'text': 'sit', 'length': 3}
Output: {'text': 'amet', 'length': 4}
Input:  {'chat_id': 'id2', 'text': 'Consectetur adipiscing elit sed'}
...
```

## Next steps

Now that you have a simple Quix Streams application working, you can dive into more advanced features:

- [Producing Data to Kafka](./producer.md)
- [Processing & Transforming Data](./processing.md)
- [Windowing & Aggregations](./windowing.md)

Or check out the tutorials for more in-depth examples:

- [Tutorial - Word Count](tutorials/word-count/tutorial.md)
- [Tutorial - Anomaly Detection](tutorials/anomaly-detection/tutorial.md)
- [Tutorial - Purchase Filtering](tutorials/purchase-filtering/tutorial.md) 

## Getting help

If you run into any problems, please create an [issue](https://github.com/quixio/quix-streams/issues) or ask in `#quix-help` in **[Quix Community on Slack](https://quix.io/slack-invite)**.
