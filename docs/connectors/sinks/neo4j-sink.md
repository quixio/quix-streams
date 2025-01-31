# Neo4j Sink

!!! info

    This is a **Community** connector. Test it before using in production.

    To learn more about differences between Core and Community connectors, see the [Community and Core Connectors](../community-and-core.md) page.

This sink writes data to a **Neo4j** database using a `Cypher Query`. 

It uses sanctioned Neo4j query aliases for access to the kafka message key, value, 
headers, and timestamp.

## How To Install

To use the Neo4j sink, you need to install the required dependencies:

```bash
pip install quixstreams[neo4j]
```

## How It Works

`Neo4jSink` is a streaming sink that publishes messages to Neo4j in batches with a
transaction and `UNWIND`.

It takes a single-record cypher query (provided by the user), prepends the sanctioned 
aliases (`event` and `__{param}`) and `UNWIND`-ing, and uses that as the query to 
publish messages to Neo4j.


## How To Use

Create an instance of `Neo4jSink` and pass it to the `StreamingDataFrame.sink()` method:

```python
from quixstreams import Application
from quixstreams.sinks.community.neo4j import Neo4jSink

app = Application(broker_address="localhost:9092")
topic = app.topic("topic-name")

# records structured as:
# {"name": {"first": "John", "last": "Doe"}, "age": 28, "city": "Los Angeles"}

# This assumes the given City nodes exist.
# Notice the use of "event" to reference the message value.
# Could also do things like __key, or __value.name.first.
cypher_query = """
MERGE (p:Person {first_name: event.name.first, last_name: event.name.last})
SET p.age = event.age
MERGE (c:City {name: event.city})
MERGE (p)-[:LIVES_IN]->(c)
"""

# Configure the sink
neo4j_sink = Neo4jSink(
    host="localhost",
    port=7687,
    username="neo4j",
    password="local_password",
    cypher_query=cypher_query,
)

sdf = app.dataframe(topic=topic)
sdf.sink(neo4j_sink)

if __name__ == "__main__":
    app.run()
```


## Configuration Options
- `host`: The Neo4j database hostname.
- `port`: The Neo4j database port.
- `username`: The Neo4j database username.
- `password`: The Neo4j database password.
- `cypher_query`: A Cypher Query to execute on each record.    
    Behavior attempts to match other Neo4j connectors:
    - Uses "dot traversal" for (nested) dict key access; ex: `col_x.col_y.col_z`
    - Message value is bound to the alias "event"; ex: `event.field_a`.
    - Message key, value, header and timestamp are bound to `__{attr}`; ex: `__key`.
- Additional keyword arguments are passed to the `neo4j.GraphDatabase.driver`

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

Rather than connect to AWS, you can alternatively test your application using a local Neo4j host via Docker:

1. Execute in terminal:

    ```bash
    docker run --rm -d --name neo4j \
    -p 7474:7474 \
    -p 7687:7687 \
    --env NEO4J_AUTH=neo4j/local_password \
    neo4j:latest
    ```

2. Connect with the following values:
    - `host`: "localhost"
    - `port`: 7687
    - `username`: "neo4j"
    - `password`: "local_password"
