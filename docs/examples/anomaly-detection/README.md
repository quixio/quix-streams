Example: Anomaly detection

You will learn how to build a simple anomaly detection system, a common use case of stateful streaming applications. This will show how to use a QuixStreams Application to:

- Create a topic
- Use stateful windowed operations
- Do simple event alterations
- Do simple event filtering
- Produce the result to a topic



## 1. Outline of the Problem

For our example, we have "machines" performing various tasks. While running, they emit their current temperature several times a second, which we can use to detect issues.

The machines typically rise and fall in temperature under normal use, but having a "prolonged" temperature above 90C is considered malfunctioning.

When this occurs, we want to send alerts as soon as possible so appropriate actions can be taken.




## 2. Our example

We will use a producer to generate mock data for 3 machines (MACHINE_IDs '0', '1', or '2'); ID's 0 and 1 are functioning normally, 2 is malfunctioning (overheating).

We will generate temperature data to be processed by our new Anomaly Detector application, which triggers alerts for any malfunctioning machines.

NOTE: our example uses JSON formatting for Kafka message values.



## 3. Alerting Approach (Windowing)

First, let's do an overview of the alerting approach we'll take with our Anomaly Detector.

Basically, we want to look at temperatures for a given MACHINE_ID over a shifting window of time (and as close to "now" as possible) and see if the mean temperature in that window surpasses our allowed threshold of 90C.

This approach is desirable since temperatures fluctuate quickly; it enables more accurate alerting since it: 
- is more tolerant to erroneous sensor readings/spikes
- allows more time for the machine to cool back down (as part of normal operation)




## 4. Generating Temperature Data

Without going into much detail, we have a standalone producer HERE [LINK] to generate temperature readings for us and pair up nicely with our Anomaly Detector.

It cycles through MACHINE_ID's 0-2 (using the ID as the Kafka key), and produces a (-1, 0, +1) temperature change for each machine a few times a second, along with the time. 

So the kafka messages look like:

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

Feel free to inspect it further, but you can basically just run it. Our focus is on the Anomaly Detector itself.





## 5. Anomaly Detector application


Here is what our application will look like in the end:

```python
import os

from quixstreams import Application
from quixstreams.context import message_key


app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="temperature_alerter",
    auto_offset_reset="earliest"
)
temperature_readings_topic = app.topic(name="temperature_readings")
alerts_topic = app.topic(name="alerts")


def should_alert(window_value):
    if window_value >= 90:
        print(f'Alerting for MID {message_key()}: Average Temperature {window_value}')
        return True


sdf = app.dataframe(topic=temperature_readings_topic)
sdf = sdf.apply(lambda data: data["Temperature_C"]).hopping_window(
    duration_ms=5000, step_ms=1000).mean().current()
sdf = sdf.apply(lambda result: round(result['value'], 2)).filter(should_alert)
sdf = sdf.to_topic(alerts_topic)


if __name__ == '__main__':
    app.run(sdf)

```

Let's go over it in detail:

### Create Application

```python
app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="temperature_alerter",
    auto_offset_reset="earliest"
)
```

First, create the quixstreams Application, which is our constructor for everything! We provide it our connection settings, consumer group (ideally unique per Application), and where the consumer group should start from on our topic. 

You can learn more about auto_offset_reset HERE (link to article?), but TL;DR - "earliest" is generally recommended while learning.


### Define Topics

```python
temperature_readings_topic = app.topic(name="temperature_readings")
alerts_topic = app.topic(name="alerts")
```

Next we define our input/output topics, named temperature_readings_topic and alerts_topic, respectively. 

These topics will automatically be created for you when you run the application should they not exist.


### The StreamingDataFrame (SDF)

```python
sdf = app.dataframe(topic=temperature_readings_topic)
```

Now for the fun part: building our StreamingDataFrame, often shorthanded to "SDF".  

We initialize it, and then continue re-assigning to the same variable ("sdf") as we add operations until we are finished with it.


### Prep Data for Windowing (SDF.apply())

```python
sdf = sdf.apply(lambda data: data["Temperature_C"])
```

To use the built-in windowing functions, our incoming event needs to be transformed: at this point the unaltered event dictionary will look something like (and this should be familiar!):

`>>> {"Temperature_C": 65, "Timestamp": 1710856626905833677}`

But it needs to be just the temperature:

`>>> 65`

So we'll perform a generic SDF transformation, using `SDF.apply(F)`, where F is a function that expects one input and returns an output. See HERE for more details!


### Windowing

```python
sdf = sdf.hopping_window(duration_ms=5000, step_ms=1000).mean().current()
```

Now we do a (5 second) windowing operation on our temperature value. A few very important notes here:

- You can use either a "current" or "final" windowing approach. "Current" is often preferred for an "alerting" style. See HERE [LINK] for more details

- Window calculations relate specifically to its message key. Since we care about temperature readings PER MACHINE, our example producer used MACHINE_ID as the Kafka message key. If we had instead used a static key, say "my_machines", our Anomaly Detector would have evaluated EVERY machine together.

- The event's windowing timestamp comes from the "Timestamp" (case-sensitive!!!) field, which SDF looks for in the message value when first recieving it. If it doesn't exist, the kafka message timestamp is used. A custom function can instead be used as described HERE [LINK].


### Using Window Result

```python
def should_alert(window_value):
    if window_value >= 90:
        print(f'Alerting for MID {message_key()}: Average Temperature {window_value}')
        return True

sdf = sdf.apply(lambda result: round(result['value'], 2)).filter(should_alert)

```

Now we get a window result (mean) along with its start/end timestamp:

`>>> {"value": 67.49478585, "start": 1234567890, "end": 1234567895}`

We don't particularly care about the window itself, just the result...so we extract the "value" with SDF.apply() and SDF.filter(F), where F is our "should_alert" function. 

If "F" returns any "True"-like value, the pipeline continues processing that event...otherwise, the event stops here (like in this instance). 


### Producing an Alert

```python
sdf = sdf.to_topic(alerts_topic)
```

However, if the value ended up >= 90....we finally finish by producing our alert to our downstream topic!



## 6. Try it yourself!

### Run kafka
First, go ahead and get a kafka cluster running. To easily follow along with this example, just follow THESE (link) instructions.

### Install quixstreams
Then, install the quixstreams library in your python environment

### Run the Producer and Application
Just call `python producer.py` and `python application.py` in separate windows.

You'll note that the Application does not print any output beyond initialization: it will only print alerts being fired (and thus when it's producing a downstream message).

### Check out the results!

Eventually, you should see an alert will look something like: 

`>>> Alerting for MID b'2': Average Temperature 98.57`

NICE!

NOTE: because we use "Current" windowing, we may produce a lot of "duplicate" alerts once triggered...you could solve this in numerous ways downstream. What we care about is alerting as soon as possible!