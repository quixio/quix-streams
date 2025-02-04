# MongoDB Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes data to an MongoDB Database, with a few ways to dump data.

By default, it assumes 1:1 relationship with MongoDB's `_id` field and Kafka keys, but 
other update patterns are possible.

## How To Install

To use the MongoDB sink, you need to install the required dependencies:

```bash
pip install quixstreams[mongodb]
```

## How It Works

`MongoDBSink` is a streaming sink that publishes messages to MongoDB in batches.

There are a few different ways to handle/export them with `MongoDBSink`, but the most common
(and default) approach is having a 1:1 correspondence between Kafka message `key` and 
document `_id`.

Regardless of approach, the following is always true:

- The Kafka message `value` fields are dumped as whatever objects they are.
- The sink ensures that the order of messages is preserved within each partition. 
    - This means that messages are sent to MongoDB in the same order they are received from Kafka for each specific partition.


## Export Behavior

How data is dumped with `MongoDBSink` primarily depends on two parameters:
`update_method` and `document_matcher`.

### Default: kafka `key` == MongoDB `_id`

The default `document_matcher` assumes the message `key` corresponds to an equivalently 
named document `_id`, so it attempts to match on that for updating.

If no such `_id` exists (and `MongoDBSink` has `upsert=True`), the document will instead 
be created with that `_id`.

Also by default, only the fields present in the Kafka message will be updated 
 in the document (`update_method="UpdateOne"`). 

However, you can do full document replacement/set by 
setting `update_method="ReplaceOne"`, which will drop any fields that are not present in
the message.

### Using A Custom `_id`

A custom `_id` can be used by simply providing your own 
`document_matcher` to `MongoDBSink` (which should include `{"_id": <YOUR_VALUE>}`).

If no match is found (and assuming `upsert=True`), the document will instead be created
with that `_id`.

If no `document_matcher` or `_id` specification is specified (and `upsert=True`), `MongoDB` will 
create a new document where `_id` will be assigned an `ObjectID` (default MongoDB behavior).


### Alternate behavior: pattern-based updates

The `document_matcher` can alternatively be used to match more than one document at a time; it 
simply has to return any valid MongoDB "filter query" (what is used by MongoDB's `.find()`).

This approach enables updating multiple documents from one message.

To accomplish this, you must additionally set `update_method="UpdateMany"`, otherwise 
only the first encountered match will be updated.

If no match is made, it will instead create a new document with a random `_id` 
(assuming `upsert=True`) with the provided updates.

## How To Use

Create an instance of `MongoDBSink` and pass it to the `StreamingDataFrame.sink()` method:

```python
import os
from quixstreams import Application
from quixstreams.sinks.community.mongodb import MongoDBSink

app = Application(broker_address="localhost:9092")
topic = app.topic("topic-name")

# Message structured as:
# key: "CID_12345"
# value: {"name": {"first": "John", "last": "Doe"}, "age": 28, "city": "Los Angeles"}

# Configure the sink
mongodb_sink = MongoDBSink(
    url="mongodb://localhost:27017",
    db="my_mongodb",
    collection="people",
)

sdf = app.dataframe(topic=topic)
sdf.sink(mongodb_sink)

# MongoDB Document: 
# {"_id": "CID_12345", "name": {"first": "John", "last": "Doe"}, "age": 28, "city": "Los Angeles"}

if __name__ == "__main__":
    app.run()
```

### An `UpdateMany` example

Imagine we get messages that update various product offerings.

```python
# Kafka message
key = "product_updates"
value = {"product_category": "Shirts", "color_options": "blue,black,red"}
```
in this case, we could use `document_matcher` to find all other products that match 
this `product_category` ("Shirts") and update them to have these new `color_options`:

```python
mongodb_sink = MongoDBSink(
    url="mongodb://localhost:27017",
    db="my_mongodb",
    collection="clothing",
    
    # find all other documents with "Shirts" product category
    document_matcher=lambda item: {"product_category": item.value["product_category"]},
    
    # update every document that document_matcher finds
    update_method="UpdateMany",
)
```

## Configuration Options

- `url`: MongoDB url; most commonly `mongodb://username:password@host:port`
- `db`: MongoDB database name
- `document_matcher`: How documents are selected to update.    
    A callable that accepts a `BatchItem` and returns a MongoDB "Filter Query".    
    If no match, will insert if `upsert=True`, where `_id` will be either the 
    included value if specified, else a random `ObjectId`.    
    **Default**: matches on `_id`, with `_id` assumed to be the kafka key.
- `upsert`: Create documents if no matches with `document_matcher`.    
    **Default**: True
- `update_method`: How documents found with `document_matcher` are updated.    
    'Update*' options will only update fields included in the kafka message.    
    'Replace*' option fully replaces the document with the contents of kafka message.    
    - "UpdateOne": Updates the first matching document (usually based on `_id`).    
    - "UpdateMany": Updates ALL matching documents (usually NOT based on `_id`).    
    - "ReplaceOne": Replaces the first matching document (usually based on `_id`).    
    **Default**: "UpdateOne".
- `include_message_metadata`: include key, timestamp, and headers as `__{field}`    
    **Default**: False
- `include_topic_metadata`: include topic, partition, and offset as `__{field}`    
    **Default**: False
- Additional keyword arguments are passed to the `MongoClient`.

## Error Handling and Delivery Guarantees

The sink provides **at-least-once** delivery guarantees, which means:

- Messages are published in batches for better performance
- During checkpointing, the sink waits for all pending publishes to complete
- If any messages fail to publish, a `SinkBackpressureError` is raised
- When `SinkBackpressureError` occurs:
    - The application will retry the entire batch from the last successful offset
    - Some messages that were successfully published in the failed batch may be published again
    - This ensures no messages are lost, but some might be delivered more than once

This behavior makes the sink reliable but the downstream systems must be prepared to handle duplicate messages. If your application requires exactly-once semantics, you'll need to implement deduplication logic in your consumer.

## Testing Locally

You can test your application using a local MongoDB host via Docker:

1. Execute in terminal:

    ```bash
    docker run --rm -d --name mongodb \
    -p 27017:27017 \
    mongodb/mongodb-community-server:latest
    ```

2. Connect using the url:    
   `mongodb://localhost:27017`
