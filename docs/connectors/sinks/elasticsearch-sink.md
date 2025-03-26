# Elasticsearch Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes data to a Elasticsearch Index, with a few ways to dump data.


## How To Install

To use the Elasticsearch sink, you need to install the required dependencies:

```bash
pip install quixstreams[elasticsearch]
```

## How It Works

`ElasticsearchSink` is a streaming sink that publishes messages to Elasticsearch in batches.

You can customize how to handle/export them with `ElasticsearchSink`, but the most common
(and default) approach is having a 1:1 correspondence between Kafka message `key` and 
document `_id`, and field types automatically ("dynamically") inferred.

Also, messages are sent to Elasticsearch in the same order they are received from Kafka
for each specific partition.


## Export Behavior

How data is dumped with `ElasticsearchSink` primarily depends on two parameters: the 
`document_id_setter` and `mapper`.

### Specifying Index Field Types

To specify an Index's field types, you generally pass an Elasticsearch `mapping` dict.

By default, a simple "dynamic" mapping is used, which determines a field's type based 
on the initial type encountered.

> ex: if the first encountered instance of field `quantity` is `5`, then 
that field is assumed to be an `integer` field.

#### Using a Custom Mapping

You can override (or add to) the default behavior by passing your own mapping.

#### Mapping Example

Here we specify that the field `host` should be a `keyword`, otherwise 
keep the default behavior:

```python
custom_mapping = {
    "mappings": {
        "dynamic": "true",  # keeps default behavior
        "properties": {
            "host": {  # enforce type for `host` field
                "type": "keyword"
            }
        },
    },
}
elasticsearch_sink = ElasticsearchSink(
    # ... other args here ...
    mapping=custom_mapping,
)
```

To learn more, check out the [Elasticsearch mappings docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html).


### Document ID setting

The default `document_id_setter` assumes the message `key` corresponds to an equivalently 
named document `_id`, so it attempts to match on that for updating.

If no such `_id` exists (and `ElasticsearchSink` has `upsert=True`), the document will instead 
be created with that `_id`.


#### Using A Custom `_id`

A custom `_id` can be used by simply providing your own 
`document_id_setter` to `ElasticsearchSink` (which should return a string, or `None`).

If no match is found, the document will instead be created with that `_id`.

If no `document_id_setter` or `_id` specification is specified, Elasticsearch will 
create a new document with a random unique `_id`.


#### id setter Example
```python
from quixstreams.sinks.community.elasticsearch import ElasticsearchSink
from quixstreams.sinks.base.item import SinkItem

def get_last_name(batch_item: SinkItem) -> str:
    return batch_item.value["name"]["last"]

sink = ElasticsearchSink(
    ..., # other required stuff
    document_id_setter=get_last_name,
)
```

### Include Message Metadata

You can include `topic` (topic, partition, offset) and `message` 
(key, headers, timestamp) metadata using the flags 
`add_topic_metadata=True` and `add_message_metadata=True` for `ElasticsearchSink`. 

They will be included as
`__{field}` in the document.

#### Example document with `add_message_metadata=True`:
```python
{
    "field_x": "value_a",
    "field_y": "value_b",
    "__key": b"my_key",
    "__headers": {},
    "__timestamp": 1234567890,
}

```

## How To Use

Create an instance of `ElasticsearchSink` and pass it to the `StreamingDataFrame.sink()` method:

```python
import os
from quixstreams import Application
from quixstreams.sinks.community.elasticsearch import ElasticsearchSink

app = Application(broker_address="localhost:9092")
topic = app.topic("topic-name")

# Message structured as:
# key: "CID_12345"
# value: {"name": {"first": "John", "last": "Doe"}, "age": 28, "city": "Los Angeles"}

# Configure the sink
elasticsearch_sink = ElasticsearchSink(
    url="http://localhost:9200",
    index="my_index",
)

sdf = app.dataframe(topic=topic)
sdf.sink(elasticsearch_sink)

# Elasticsearch Document: 
# {"_id": "CID_12345", "name": {"first": "John", "last": "Doe"}, "age": 28, "city": "Los Angeles"}

if __name__ == "__main__":
    app.run()
```

## Configuration Options

- `url`: Elasticsearch url
- `index`: Elasticsearch index name
- `mapping`: a custom mapping  
    **Default**: Dynamically maps all field types
- `batch_size`: how large each chunk size is with bulk  
    **Default**: 500
- `max_bulk_retries`: number of retry attempts for each bulk batch  
    **Default**: 3
- `document_id_setter`: how to select the document id.
    **Default**: `_id` set to the kafka message key.
- `add_message_metadata`: include key, timestamp, and headers as `__{field}`    
    **Default**: False
- `add_topic_metadata`: include topic, partition, and offset as `__{field}`    
    **Default**: False
- `ignore_bulk_upload_errors`: ignore any errors that occur when attempting an upload  
    **Default**: False

Additional keyword arguments are passed to the `Elasticsearch` client.

## Testing Locally

You can test your application using a local Elasticsearch based on the 
[Elasticsearch running locally guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/run-elasticsearch-locally.html):

1. Execute in terminal from a desired folder:

    ```bash
    curl -fsSL https://elastic.co/start-local | sh
    ```

2. Connect using the `api_key` printed by the output from step 1:
    ```python
    from quixstreams.sinks.community.elasticsearch import ElasticsearchSink
   
    elasticsearch_sink = ElasticsearchSink(
        host="http://localhost:9200",
        index="<YOUR INDEX>",
        api_key="<PRINTED IN TERMINAL>",
    )
    ```

3. Optionally, try out the included `Kibana` frontend to see your data.