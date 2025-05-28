# Tutorial: Using as-of joins to enrich telemetry data of the solar panels.

In this tutorial, we will use as-of joins to enrich metrics of the solar panels with the weather forecast data.  
The enriched data can be used to optimize battery management and better analyze the performance of the panel (for example, to predict battery discharging during cloudy days).


## What You Will Learn

This example will show how to use a Quix Streams `Application` to enrich data from one Kafka topic using another.  


## Outline of the Problem

For our example, we have a solar battery farm generating electricity. While running, the batteries emit their current power output and internal temperature several times a second.

We want to merge the telemetry data with the current weather forecast in order to better analyze and predict the farm performance, all in real-time.


## Our Example

We will use two [Quix Streams `Sources`](../../connectors/sources/README.md):

- One to generate mock telemetry measurements for 3 solar panels (the "telemetry" topic).
- And another one generating a forecast data once in 30s for the given location (the "forecast" topic).

We need to match the telemetry events with the latest effective forecasts in order to 
have as fresh data as possible.

The data will be processed by the Solar Farm `Application`, which will do the join and send the results to the output topic.


## Before Getting Started

1. You will see links scattered throughout this tutorial.
    - Tutorial code links are marked **>>> LIKE THIS <<<** .
    - ***All other links provided are completely optional***. 
    - They are great ways to learn more about various concepts if you need it!

2. This tutorial uses a [`Source`](../../connectors/sources/README.md) rather than a Kafka [`Topic`]() to ingest data.
    - `Source` connectors enable reading data from a non-Kafka origin (typically to get it into Kafka). 
    - This approach circumvents users having to run a [producer](../../producer.md) alongside the `Application`.
    - A `Source` is easily replaced with an actual Kafka topic (just pass a `Topic` instead of a `Source`).



## TODO: Explain how joins work

## Generating Sample Data

In our [**>>> Enrichment Application <<<**](tutorial_app.py), we use two `Sources`:

- `WeatherForecastGenerator` that generates weather forecast data once in 30s.  
Format:  
`{"timestamp": <float>, "forecast_temp": <float>, "forecast_cloud": <float>}`
<br>
<br>

- `BatteryTelemetryGenerator` that generates battery measurements for three solar panels every second.    
Format:  
`{"timestamp": <float>, "panel_id": <str>, "location_id": <str>, "temperature_C": <float>, "power_watt": <float>}`
<br>
<br>

Each Source will have **a unique name** which is used as a part of the underlying topic name.

Both datasets will use `location_id` as a message key - it is an important part of the join operation.


## Enrichment Application

Now let's go over the `main()` portion of our 
[**>>> Enrichment Application <<<**](tutorial_app.py) in detail!


### Create an Application

Create a [Quix Streams Application](../../configuration.md), which is our constructor for everything! 

We provide it our connection settings, consumer group (ideally unique per Application), 
and where the consumer group should start from on the (internal) Source topic.

!!! TIP

    Once you are more familiar with Kafka, we recommend 
    [learning more about auto_offset_reset](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls).

#### Our Application

```python
import os
from quixstreams import Application

app = Application(
    broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="temperature_alerter",
    auto_offset_reset="earliest",
    # Disable changelog topics for this app, but it's recommended to keep them "on" in production
    use_changelog_topics=False  
)
```



### Specify Topics

`Application.topic()` returns [`Topic`](../../api-reference/topics.md) objects which are used by `StreamingDataFrame`.

Create one for each topic used by your `Application`.

!!! NOTE

    Any missing topics will be automatically created for you upon running an `Application`.


#### Our Topics
We have one output topic, named `"telemetry-with-forecast"`:

```python
output_topic = app.topic(name="telemetry-with-forecast")
```



### The StreamingDataFrames (SDF)

Now we need to define our [StreamingDataFrame](../../processing.md#introduction-to-streamingdataframe), often shorthanded to "SDF".

SDF allows manipulating the message value in a dataframe-like fashion using various operations.

After initializing with either a `Topic` or `Source`, we continue reassigning to the 
same `sdf` variable as we add operations.

!!! NOTE

    A few `StreamingDataFrame` operations are 
    ["in-place"](../../advanced/dataframe-assignments.md#valid-in-place-operations), 
    like `.print()`.

#### Initializing the dataframes

```python
telemetry_sdf = app.dataframe(source=BatteryTelemetryGenerator(name="telemetry"))
forecast_sdf = app.dataframe(source=WeatherForecastGenerator(name="forecast"))
```

First, we initialize our SDF with our `BatteryTelemetryGenerator` and `WeatherForecastGenerator` sources, 
which means we will be consuming data from a non-Kafka origin.


!!! TIP

    You can consume from a Kafka topic instead by passing a `Topic` object
    with `app.dataframe(topic=<Topic>)`.

Let's go over the SDF operations in this example in detail.


### Joining the Dataframes

```python
from datetime import timedelta


def merge_events(telemetry: dict, forecast: dict) -> dict:
    """
    Merge the matching events into a new one
    """
    forecast = {"forecast." + k: v for k, v in forecast.items()}
    return {**telemetry, **forecast}

# Join the telemetry data with the latest effective forecasts (forecast timestamp always <= telemetry timestamp)
# using join_asof().
enriched_sdf = telemetry_sdf.join_asof(
    forecast_sdf,
    how="inner",  # Join using "inner" strategy
    on_merge=merge_events,  # Use a custom function to merge events together because of the overlapping keys
    grace_ms=timedelta(days=7),  # Store forecast updates in state for 7d
)
```

Now we join the telemetry data with the forecasts in "as-of" manner, so each telemetry event is matched with the latest forecast effective at the time
when the telemetry was produced. 

In particular: 

- We use `how="inner"` which means that the results are emitted only when the match is found.  
In can also be set to `how="left"` to emit records even if there is no matching forecast for the telemetry event.

- We also provide a custom `merge_events` function to define how the join result will look like.    
It's an optional step if the column names in both dataframes don't overlap.  
In our case, the `timestamp` column is present on both sides, so we have to resolve it. 

- We set `grace_ms=timedelta(days=7)` to keep the forecast data in the state for at least 7 days.   
This interval can be changed if out-of-order data is expected in the stream (for example, some batteries produce telemetry with a large delay).  


#### How the joining works
Because "join" is a stateful operation, and it requires access to multiple topic partitions within the same process, 
the dataframes must meet certain requirements to be joined:

1. The underlying topics of the dataframes must have the same number of partitions.  
Quix Streams validates that when the `join_asof` is called.  
In this tutorial, both topics have one partition.
<br>
<br>

2. The messages keys in these topics must be distributed across partitions using the same algorithm Messageshe same partitioner.
In our case, messages are produced using the default built-in partitioner.

Under the hood, `join_asof` works like this:

- Records from the right side (`forecast_sdf`) are written to the state store without emitting any updates downstream.
- Records on the left side (`telemetry_sdf`) query the forecasts store for the values with the same **key** and the **timestamp lower or equal to the left record's timestamp**.
  Left side emits data downstream.
  - If the match is found, the two records are merged together into a new one according to the `on_merge` logic.
- The retention of the right store is controlled by the `grace_ms`:
  each "right" record bumps the maximum timestamp for this key in the store, and values with the same keys and timestamps below "<current timestamp> - <grace_ms>" are deleted.


You can find more details on the [Joins](../../joins.md) page.


### Printing the enriched data

```python
# Convert timestamps to strings for readbility
enriched_sdf["timestamp"] = enriched_sdf["timestamp"].apply(timestamp_to_str)
enriched_sdf["forecast.timestamp"] = enriched_sdf["forecast.timestamp"].apply(
    timestamp_to_str
)

# Print the enriched data
enriched_sdf.print_table(live=False)
```

Now we have a joined dataframe, and we want to verify that the data looks as we expect.  

We first convert timestamps from numbers to strings for readability, and the print the 
results as a table.

The output should look like this: 

```
┏━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ _key          ┃ _timestamp    ┃ timestamp                  ┃ panel_id ┃ location_id ┃ temperature_C ┃ power_watt ┃ forecast.timestamp         ┃ forecast.forecast_temp ┃ forecast.forecast_cloud ┃
┡━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ b'location-1' │ 1748444438007 │ 2025-05-28T15:00:38.007725 │ panel-1  │ location-1  │ 21            │ 0.6        │ 2025-05-28T15:00:20.929200 │ 29                     │ 88                      │
│ b'location-1' │ 1748444438007 │ 2025-05-28T15:00:38.007855 │ panel-2  │ location-1  │ 17            │ 1.0        │ 2025-05-28T15:00:20.929200 │ 29                     │ 88                      │
│ b'location-1' │ 1748444438007 │ 2025-05-28T15:00:38.007886 │ panel-3  │ location-1  │ 35            │ 0.6        │ 2025-05-28T15:00:20.929200 │ 29                     │ 88                      │
│ b'location-1' │ 1748444439009 │ 2025-05-28T15:00:39.009509 │ panel-1  │ location-1  │ 20            │ 0.2        │ 2025-05-28T15:00:20.929200 │ 29                     │ 88                      │
│ b'location-1' │ 1748444439009 │ 2025-05-28T15:00:39.009870 │ panel-2  │ location-1  │ 23            │ 0.8        │ 2025-05-28T15:00:20.929200 │ 29                     │ 88                      │
└───────────────┴───────────────┴────────────────────────────┴──────────┴─────────────┴───────────────┴────────────┴────────────────────────────┴────────────────────────┴─────────────────────────┘
```

### Producing the output messages

```python
# Produce results to the output topic
enriched_sdf.to_topic(output_topic)
```

To produce the enriched data, we call [`StreamingDataFrame.to_topic()`](../../processing.md#writing-data-to-kafka-topics) and pass the previously defined `output_topic` to it.


### Running the Application

Running a `Source`-based `Application` requires calling `Application.run()` within a
`if __name__ == "__main__"` block.

#### Our Application Run Block 

Our entire `Application` (and all its spawned objects) resides within a 
`main()` function, executed as required:

```python
if __name__ == "__main__":
    main()
```


## Try it Yourself!

### 1. Run Kafka
First, have a running Kafka cluster. 

To easily run a broker locally with Docker, just [run this simple one-liner](../README.md#running-kafka-locally).

### 2. Download files
- [tutorial_app.py](tutorial_app.py)

### 3. Install Quix Streams
In your desired python environment, execute: `pip install quixstreams`

### 4. Run the application
In your desired python environment, execute: `python tutorial_app.py`.
