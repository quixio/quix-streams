# Tutorial: Anomaly Detection

You will learn how to build a simple anomaly detection system, a common use case of stateful streaming applications. This will show how to use a Quix Streams application to:

- Ingest a non-Kafka data source
- Use stateful windowed operations
- Do simple event alterations
- Do simple event filtering
- Create a Kafka topic 
- Produce results to a Kafka topic



## Outline of the Problem

For our example, we have "machines" performing various tasks. While running, they emit their current temperature several times a second, which we can use to detect issues.

The machines typically rise and fall in temperature under normal use, but having a "prolonged" temperature above 90C is considered malfunctioning.

When this occurs, we want to send alerts as soon as possible so appropriate actions can be taken.




## Our Example

We will use a [Quix Streams `Source`](../../connectors/sources/README.md) to generate mock temperature events for 
3 machines (MACHINE_IDs '0', '1', or '2'); ID's 0 and 1 are functioning normally, 
2 is malfunctioning (overheating).

These events will be processed by our new Anomaly Detector `Application`.

NOTE: our example uses JSON formatting for Kafka message values.



## Alerting Approach (Windowing)

First, let's do an overview of the alerting approach we'll take with our Anomaly Detector.

Basically, we want to look at temperatures for a given MACHINE_ID over a shifting window of time (and as close to "now" as possible) and see if the mean temperature in that window surpasses our allowed threshold of 90C.

This approach is desirable since temperatures fluctuate quickly; it enables more accurate alerting since it: 

- is more tolerant to erroneous sensor readings/spikes

- allows more time for the machine to cool back down (as part of normal operation)



## Before Getting Started

1. You will see links scattered throughout this tutorial.
    - Tutorial code links are marked **>>> LIKE THIS <<<** .
    - ***All other links provided are completely optional***. 
    - They are great ways to learn more about various concepts if you need it!

2. This tutorial uses a [`Source`](../../connectors/sources/README.md) rather than a Kafka [`Topic`]() to ingest data.
    - `Source` connectors enable reading data from a non-Kafka origin (typically to get it into Kafka). 
    - This approach circumvents users having to run a [producer](../../producer.md) alongside the `Application`.
    - A `Source` is easily replaced with an actual Kafka topic (just pass a `Topic` instead of a `Source`).



## Generating Temperature Data

Our [**>>> Anomaly Detector Application <<<**](tutorial_app.py) uses a `Source` called 
`PurchaseGenerator` to generate temperature events.

It cycles through MACHINE_ID's 0-2 (using the ID as the Kafka key), and produces a 
(-1, 0, +1) temperature change for each machine a few times a second, along with the time. 

So the incoming messages look like:

```python
# ...
{kafka_key: '0', kafka_value: {"Temperature_C": 65, "Timestamp": 1710856626905833677}}
{kafka_key: '1', kafka_value: {"Temperature_C": 48, "Timestamp": 1710856627107115587}}
{kafka_key: '2', kafka_value: {"Temperature_C": 82, "Timestamp": 1710856627307969878}}
{kafka_key: '0', kafka_value: {"Temperature_C": 65, "Timestamp": 1710856627508888818}} # +0 C
{kafka_key: '1', kafka_value: {"Temperature_C": 49, "Timestamp": 1710856627710089041}} # +1 C
{kafka_key: '2', kafka_value: {"Temperature_C": 81, "Timestamp": 1710856627910952220}} # -1 C
# etc...
```

It stops producing data when MACHINE_ID 2 hits a temperature of 100C, which should collectively cause at least one alert in our downstream Anomaly Detector.

Feel free to inspect it further, but it's just to get some data flowing. Our focus is on the Anomaly Detector itself.



## Anomaly Detector Application

Now let's go over the `setup_and_run_application()` portion of our 
[**>>> Anomaly Detector Application <<<**](tutorial_app.py) in detail!



### Create an Application

```python
import os
from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="temperature_alerter",
    auto_offset_reset="earliest"
)
```

Create a [Quix Streams Application](../../configuration.md), which is our constructor for everything! 

We provide it our connection settings, consumer group (ideally unique per Application), 
and where the consumer group should start from on the (internal) Source topic.

> [!TIP] 
> Once you are more familiar with Kafka, we recommend [learning more about auto_offset_reset](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls).



### Specify Topics

`Application.topic()` returns [`Topic`](../../api-reference/topics.md) objects which are used by `StreamingDataFrame`.

Create one for each topic used by your `Application`.

> [!NOTE]
> Any missing topics will be automatically created for you upon running the application.

#### Our Topics
We have one output topic, named `price_updates`:

```python
price_updates_topic = app.topic("price_updates")
```



### The StreamingDataFrame (SDF)

```python
sdf = app.dataframe(topic=temperature_readings_topic)
```

Now for the fun part: building our [StreamingDataFrame](../../processing.md#introduction-to-streamingdataframe), often shorthanded to "SDF".  

SDF allows manipulating the message value in a dataframe-like fashion using various operations.

After initializing, we continue re-assigning to the same `sdf` variable as we add operations.

(Also: notice that we pass our input `Topic` from the previous step to it.)



### Prep Data for Windowing

```python
sdf = sdf.apply(lambda data: data["Temperature_C"])
```

To use the built-in windowing functions, our incoming event needs to be transformed: at this point the unaltered event dictionary will look something like (and this should be familiar!):

`>>> {"Temperature_C": 65, "Timestamp": 1710856626905833677}`

But it needs to be just the temperature:

`>>> 65`

So we'll perform a generic SDF transformation using [`SDF.apply(F)`](../../processing.md#streamingdataframeapply), 
(`F` should take your current message value as an argument, and return your new message value): 
our `F` is a simple `lambda`, in this case.



### Windowing

```python
sdf = sdf.hopping_window(duration_ms=5000, step_ms=1000).mean().current()
```

Now we do a (5 second) windowing operation on our temperature value. A few very important notes here:

- You can use either a ["current" or "final" window emission](../../windowing.md#emitting-results). "Current" is often preferred for an "alerting" style.

- Window calculations relate specifically to its message key. 
    - Since we care about temperature readings PER MACHINE, our example producer used MACHINE_ID as the Kafka message key. 
    - If we had instead used a static key, say "my_machines", our Anomaly Detector would have evaluated the machines together as one.

- The event's windowing timestamp comes from the "Timestamp" (case-sensitive!!!) field, which SDF looks for in the message value when first receiving it. If it doesn't exist, the kafka message timestamp is used. [A custom function can also be used](../../windowing.md#extracting-timestamps-from-messages).



### Using Window Result

```python
def should_alert(window_value: int, key, timestamp, headers) -> bool:
    if window_value >= 90:
        print(f'Alerting for MID {key}: Average Temperature {window_value}')
        return True

sdf = sdf.apply(lambda result: round(result['value'], 2)).filter(should_alert, metadata=True)

```

Now we get a window result (mean) along with its start/end timestamp:

`>>> {"value": 67.49478585, "start": 1234567890, "end": 1234567895}`

We don't particularly care about the window itself in our case, just the result...so we extract the "value" with `SDF.apply()` and [`SDF.filter(F)`](../../processing.md#streamingdataframefilter), where `F` is our "should_alert" function. 

For `SDF.filter(F)`, if the (_**boolean**_-ed) return value of `F` is: 

- `True` -> continue processing this event

- `False` -> stop ALL further processing of this event (including produces!)

In our case, this example event would then stop since `bool(None)` is `False`.



### Producing an Alert

```python
sdf = sdf.to_topic(alerts_topic)
```

However, if the value ended up >= 90....we finally finish by producing our alert to our downstream topic via [`SDF.to_topic(T)`](../../processing.md#writing-data-to-kafka-topics), where `T`
is our previously defined `Topic` (not the topic name!).

NOTE: because we use "Current" windowing, we may produce a lot of "duplicate" alerts once triggered...you could solve this in numerous ways downstream. What we care about is alerting as soon as possible!




### Running an Application

Running a `Source`-based `Application` requires calling `Application.run()` within a
`if __name__ == "__main__"` block.

#### Our Application Run Block 

Our entire `Application` (and all its spawned objects) resides within a 
`setup_and_run_application()` function, executed as required:

```python
if __name__ == "__main__":
    setup_and_run_application()
```




## Try it yourself!

### 1. Run Kafka
First, have a running Kafka cluster. 

To easily run a broker locally with Docker, just [run this simple one-liner](../README.md#running-kafka-locally).

### 2. Download files
- [tutorial_app.py](tutorial_app.py)

### 3. Install Quix Streams
In your desired python environment, execute: `pip install quixstreams`

### 4. Run the application
In your desired python environment, execute: `python tutorial_app.py`.

### 5. Check out the results!

Eventually, you should see an alert will look something like: 

`>>> Alerting for MID b'2': Average Temperature 98.57`

NICE!
