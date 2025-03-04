# Windowed Aggregations


## What Are Windows?
In stream processing, windows are used to divide endless streams of events into finite time-based or count-based intervals.

With windows, you can calculate such aggregations as:

- Total of website visitors for every hour
- The average speed of a vehicle over the last 10 minutes
- Maximum temperature of a sensor observed over 30 second ranges
- Give an user a reward after 10 succesful actions 


## Types of Time in Streaming

There are two types of time in streaming systems:

1. **Event time** - the time when the event happened. 
2. **Processing time** - the time when the event is processed by the system


!!! info  

    In Quix Streams, **windows always use event time**.
    The event time is obtained from the timestamps of incoming Kafka messages.

    Every windowed aggregation tracks the event time for each **message key**.  
    It stores the **maximum observed timestamp** for each **message key**, and
    this timestamp is used as a current time in the stream.

    The maximum observed timestamp is used to determine whether the incoming event is late or on-time.  
    See [Lateness and Out-of-Order Processing](#lateness-and-out-of-order-processing) for more info about lateness.
    
 
### Extracting timestamps from messages
By default, Quix Streams uses Kafka message timestamps to determine the time of the event.  

But sometimes the time information may be encoded into the message payload or headers.  
To use a different timestamp in window aggregations, you need to provide a 
**timestamp extractor** to `Application.topic()`.

A timestamp extractor is a callable object accepting these positional arguments:

1. Message value
2. Message headers
3. Kafka message timestamp in milliseconds
4. Kafka timestamp type.

Kafka timestamp type can have these values:

   * `0` - timestamp not available, 
   * `1` - "create time" (specified by a producer) 
   * `2` - "log-append time" (when the broker received a message)

Timestamp extractor must always return timestamp **as an integer in milliseconds**.

Example:

```python
from quixstreams import Application
from quixstreams.models import TimestampType
from typing import Any, Optional, List, Tuple

# Create an Application
app = Application(...)


def custom_ts_extractor(
    value: Any,
    headers: Optional[List[Tuple[str, bytes]]],
    timestamp: float,
    timestamp_type: TimestampType,
) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp from the message payload 
    instead of Kafka timestamp.
    """
    return value["timestamp"]

# Passing the timestamp extractor to the topic.
# The window functions will now use the extracted timestamp instead of the Kafka timestamp.
topic = app.topic("input-topic", timestamp_extractor=custom_ts_extractor)

# Create a StreamingDataFrame and keep processing
sdf = app.dataframe(topic)

if __name__ == '__main__':
    app.run()
```

### Timestamps of the aggregation results

Since version 2.6, all windowed aggregations always set timestamps equal to the start of the window.

**Example:**

```python
from quixstreams import Application
from datetime import timedelta

app = Application(...)

sdf = app.dataframe(...)

# Input:
# value={"temperature" : 9999}, key="sensor_1", timestamp=10001

sdf = (
    # Extract the "temperature" column from the dictionary 
    sdf.apply(lambda value: value['temperature'])
    
    # Define a tumbling window of 10 seconds
    .tumbling_window(timedelta(seconds=10))

    # Calculate the minimum temperature 
    .min()

    # Emit results for every incoming message
    .current()
)
# Output:
# value={
#   'start': 10000, 
#   'end': 20000, 
#   'value': 9999  - minimum temperature
# }, 
# key="sensor_1",
# timestamp=10000 - timestamp equals to the window start timestamp

```


### Message headers of the aggregation results

Currently, windowed aggregations do not store the original headers of the messages.  
The results of the windowed aggregations will have headers set to `None`.  

You may set messages headers by using the `StreamingDataFrame.set_headers()` API, as 
described in [the "Updating Kafka Headers" section](./processing.md#updating-kafka-headers).

## Time-based Tumbling Windows
Tumbling windows slice time into non-overlapping intervals of a fixed size. 

For example, a tumbling window of 1 hour will generate the following intervals:

```
                Tumbling Windows
                
Time    
[00:00, 01:00):     .....
[01:00, 02:00):          .....
[02:00, 03:00):               .....
[03:00, 04:00):                    .....
```
Note that the start of the interval is inclusive and the end is exclusive.

In tumbling windows, each timestamp can be assigned only to a **single** interval.  
For example, a timestamp `00:33:13` will be assigned to an interval 
`00:00:00 - 01:00:00` for a tumbling window of 1 hour.


**Example:**

Imagine you receive temperature readings from sensors, and you need to calculate average temperature for each hour, and produce updates for each incoming message.
The message key is a sensor ID, so the aggregations will be grouped by each sensor.

Input:

(Here the `"timestamp"` column illustrates Kafka message timestamps)
```json
{"temperature": 30, "timestamp": 100}
{"temperature": 29, "timestamp": 200}
{"temperature": 28, "timestamp": 300}
```

Expected output:

```json
{"avg_temperature": 30, "window_start_ms": 0, "window_end_ms": 3600000}
{"avg_temperature": 29.5, "window_start_ms": 0, "window_end_ms": 3600000}
{"avg_temperature": 29, "window_start_ms": 0, "window_end_ms": 3600000}
```

Here is how to do it using tumbling windows: 

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    # Extract "temperature" value from the message
    sdf.apply(lambda value: value["temperature"])

    # Define a tumbling window of 1 hour
    # You can also pass duration_ms as an integer of milliseconds
    .tumbling_window(duration_ms=timedelta(hours=1))

    # Specify the "mean" aggregate function
    .mean()

    # Emit updates for each incoming message
    .current()

    # Unwrap the aggregated result to match the expected output format
    .apply(
        lambda result: {
            "avg_temperature": result["value"],
            "window_start_ms": result["start"],
            "window_end_ms": result["end"],
        }
    )
)

```

## Count-based Tumbling Windows

Count-based Tumbling Windows slice incoming events into batch of a fixed size.

For example, a tumbling window configured with a count of 4 will batch and aggregate message 1 to 4, then 5 to 8, 9 to 12 and so on. 

```
Count       Tumbing Windows
[0, 3]  : ....
[4, 7]  :     ....
[8, 11] :         ....
[12, 15] :            ....
```

In a tumbing window each message is only assigned to a **single** interval.

**Example**

Imagine you have to interact with an external HTTP API. To optimize your data analysis pipeline, you want to batch data before sending it to the external API. In this example, we batch data from 3 messages before sending the request.

Input:

(Here the `"offset"` column illustrates Kafka message offset)
```json
{"data": 100, "timestamp": 121, "offset": 1}
{"data": 50, "timestamp": 165, "offset": 2}
{"data": 200, "timestamp": 583, "offset": 3}
```

Expected window output:

```json
{"data": [100, 50, 200], "window_start_ms": 121, "window_end_ms": 583}
```

Here is how to do it using tumbling windows: 

```python
import time
import json
import urllib.request

from datetime import timedelta
from quixstreams import Application

def external_api(value):
    with urllib.request.urlopen("https://example.com", data=json.dumps(value["data"])) as rep:
        return response.read()

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    # Extract "experience" value from the message
    sdf.apply(lambda value: value["data"])

    # Define a count-based tumbling window of 3 events
    .tumbling_count_window(count=3)

    # Specify the "collect" aggregate function
    .collect()

    # Emit updates once the window is closed
    .final()

    # Unwrap the aggregated result to match the expected output format
    .apply(
        lambda result: {
            "data": result["value"],
            "window_start_ms": result["start"],
            "window_end_ms": result["end"],
        }
    )
)

# Send a request to the external API
# sdf.apply(external_api)

```

## Time-based Hopping Windows
Hopping windows slice time into overlapping intervals of a fixed size and with a fixed step.

For example, a hopping window of 1 hour with a step of 10 minutes will generate the following intervals:

```
               Hopping Windows
               
Time    
[00:00, 01:00):     .....
[00:10, 01:10):       .....
[00:20, 01:20):         .....
[00:30, 01:30):           .....

```
   
Note that the start of the interval is inclusive and the end is exclusive.

In hopping windows, each timestamp can be assigned to multiple intervals, because these
intervals overlap.

For example, a timestamp `00:33:13` will match two intervals for a hopping window of 1 hour with a 30 minute step:

   - `00:00:00 - 01:00:00`
   - `00:30:00 - 01:30:00`

    
**Example:**

Imagine you receive temperature readings from sensors, and you need to calculate average temperature for each hour with 10 minutes hop, and produce updates for each incoming message.
The message key is a sensor ID, so the aggregations will be grouped by each sensor.

Input:  
(Here the `"timestamp"` column illustrates Kafka message timestamps)

```json
{"temperature": 30, "timestamp": 50000}
{"temperature": 29, "timestamp": 60000}
{"temperature": 28, "timestamp": 62000}
```

Expected output:

```json
{"avg_temperature": 30, "window_start_ms": 0, "window_end_ms": 3600000}

{"avg_temperature": 29.5, "window_start_ms": 0, "window_end_ms": 3600000}
{"avg_temperature": 30, "window_start_ms": 60000, "window_end_ms": 4200000}

{"avg_temperature": 29, "window_start_ms": 0, "window_end_ms": 3600000}
{"avg_temperature": 28.5, "window_start_ms": 60000, "window_end_ms": 4200000}
```


```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Extract "temperature" value from the message
    sdf.apply(lambda value: value["temperature"])

    # Define a hopping window of 1h with 10m step
    # You can also pass duration_ms and step_ms as integers of milliseconds
    .hopping_window(duration_ms=timedelta(hours=1), step_ms=timedelta(minutes=10))

    # Specify the "mean" aggregate function
    .mean()

    # Emit updates for each incoming message
    .current()

    # Unwrap the aggregated result to match the expected output format
    .apply(
        lambda result: {
            "avg_temperature": result["value"],
            "window_start_ms": result["start"],
            "window_end_ms": result["end"],
        }
    )
)

```

## Count-based Hopping Windows

Count-based Hopping Windows slice incoming messages into overlapping batch of a fixed size with a fixed step.

For example, a hopping windows of 6 messages with a step of 2 messages will generate the following windows:

```
Count       Hopping Windows
[0, 5]  : ......
[2, 7]  :   ......
[4, 9]  :     ......
[6, 11] :       ......
```

In hopping windows each messages can be assigned to multiple windows because the windows overlap.

## Time-based Sliding Windows
Sliding windows are overlapping time-based windows that advance with each incoming message, rather than at fixed time intervals like hopping windows. They have a fixed 1 ms resolution and perform better and are less resource-intensive than hopping windows with a 1 ms step. Sliding windows do not produce redundant windows; every interval has a distinct aggregation.

Sliding windows provide optimal performance for tasks requiring high-precision real-time monitoring. However, if the task is not time-critical or the data stream is extremely dense, tumbling or hopping windows may perform better.

For example, a sliding window of 1 hour will generate the following intervals as messages A, B, C, and D arrive:

```
                Sliding Windows

Time
[00:00:00.000, 01:00:00.000):  ......A
[00:00:00.001, 01:00:00.001):   ......B
[00:00:00.003, 01:00:00.003):     ......C
[00:00:00.006, 01:00:00.006):        ......D

```
   
Note that both the start and the end of the interval are inclusive.

In sliding windows, each timestamp can be assigned to multiple intervals because these intervals overlap.

For example, a timestamp `01:33:13.000` will match intervals for all messages incoming between `01:33:13.000` and `02:33:13.000`. Borderline windows including this timestamp will be:

   - `00:33:13.000 - 01:33:13.000`
   - `01:33:13.000 - 02:33:13.000`

    
**Example:**

Imagine you receive temperature readings from sensors, and you need to calculate the average temperature for the last hour, producing updates for each incoming message. The message key is a sensor ID, so the aggregations will be grouped by each sensor.

Input:  
(Here the `"timestamp"` column illustrates Kafka message timestamps)

```json
{"temperature": 30, "timestamp": 3600000}
{"temperature": 29, "timestamp": 4800000}
{"temperature": 28, "timestamp": 4800001}
{"temperature": 27, "timestamp": 7200000}
{"temperature": 26, "timestamp": 7200001}
```

Expected output:

```json
{"avg_temperature": 30, "window_start_ms": 0, "window_end_ms": 3600000}
{"avg_temperature": 29.5, "window_start_ms": 1200000, "window_end_ms": 4800000}
{"avg_temperature": 29, "window_start_ms": 1200001, "window_end_ms": 4800001}
{"avg_temperature": 28.5, "window_start_ms": 3600000, "window_end_ms": 7200000}
{"avg_temperature": 27.5, "window_start_ms": 3600001, "window_end_ms": 7200001}  # reading 30 is outside of the window
```


```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Extract "temperature" value from the message
    sdf.apply(lambda value: value["temperature"])

    # Define a sliding window of 1h
    # You can also pass duration_ms as integer of milliseconds
    .sliding_window(duration_ms=timedelta(hours=1))

    # Specify the "mean" aggregate function
    .mean()

    # Emit updates for each incoming message
    .current()

    # Unwrap the aggregated result to match the expected output format
    .apply(
        lambda result: {
            "avg_temperature": result["value"],
            "window_start_ms": result["start"],
            "window_end_ms": result["end"],
        }
    )
)

```

## Count-based Sliding Windows

Sliding windows are overlapping windows that advance with each incoming message. They are equal to count-based hopping windows with a step of 1.

For example a sliding window of 4 messagew will generate the followiwng windows:

```
Count       Sliding Windows
[0, 3]  : ....
[1, 4]  :  ....
[2, 5]  :   ....
[3, 6]  :    ....
```

In sliding windows each message is assigned to multiple windows because the windows overlap.

**Example**

Imagine you receive information about customer purchase in a store and you want to know the average amount of the last 3 purchases.

Input:

(Here the `"offset"` column illustrates Kafka message offset)
```json
{"amount": 100, "timestamp": 121, "offset": 1}
{"amount": 60, "timestamp": 165, "offset": 2}
{"amount": 200, "timestamp": 583, "offset": 3}
{"amount": 40, "timestamp": 723, "offset": 4}
{"amount": 120, "timestamp": 1009, "offset": 5}
{"amount": 80, "timestamp": 1242, "offset": 6}
```

Expected window output:

```json
{"average": 120, "window_start_ms": 121, "window_end_ms": 583}
{"average": 100, "window_start_ms": 165, "window_end_ms": 723}
{"average": 120, "window_start_ms": 583, "window_end_ms": 1009}
{"average": 80, "window_start_ms": 723, "window_end_ms": 1242}

```

Here is how to do it using sliding windows: 

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    # Extract "experience" value from the message
    sdf.apply(lambda value: value["data"])

    # Define a count-based sliding window of 3 events
    .sliding_count_window(count=3)

    # Specify the "mean" aggregate function
    .mean()

    # Emit updates once the window is closed
    .final()

    # Unwrap the aggregated result to match the expected output format
    .apply(
        lambda result: {
            "average": result["value"],
            "window_start_ms": result["start"],
            "window_end_ms": result["end"],
        }
    )
)

```

## Supported Aggregations

Currently, windows support the following aggregation functions:

- [`reduce()`](api-reference/quixstreams.md#fixedtimewindowdefinitionreduce) - to perform custom aggregations using "reducer" and "initializer" functions
- [`collect()`](api-reference/quixstreams.md#fixedtimewindowdefinitioncollect) - to collect all values within a window into a list
- [`min()`](api-reference/quixstreams.md#fixedtimewindowdefinitionmin) - to get a minimum value within a window
- [`max()`](api-reference/quixstreams.md#fixedtimewindowdefinitionmax) - to get a maximum value within a window
- [`mean()`](api-reference/quixstreams.md#fixedtimewindowdefinitionmean) - to get a mean value within a window 
- [`sum()`](api-reference/quixstreams.md#fixedtimewindowdefinitionsum) - to sum values within a window 
- [`count()`](api-reference/quixstreams.md#fixedtimewindowdefinitioncount) - to count the number of values within a window 

We will go over each ot them in more detail below.

### Reduce()

`.reduce()` allows you to perform complex aggregations using custom "reducer" and "initializer" functions:

- The **"initializer"** function receives the **first** value for the given window, and it must return an initial state for this window.  
This state will be later passed to the "reducer" function.  
**It is called only once for each window.**

- The **"reducer"** function receives an aggregated state and a current value, and it must combine them and return a new aggregated state.  
This function should contain the actual aggregation logic.  
It will be called for each message coming into the window, except the first one.

With `reduce()`, you can define a wide range of aggregations, such as:

- Aggregating over multiple message fields at once
- Using multiple message fields to create a single aggregate
- Calculating multiple aggregates for the same value

**Example**:

Assume you receive the temperature data from the sensor, and you need to calculate these aggregates for each 10-minute tumbling window:

- min temperature
- max temperature
- total count of events
- average temperature

Here is how you can do that with `reduce()`:

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)


def initializer(value: dict) -> dict:
    """
    Initialize the state for aggregation when a new window starts.
    
    It will prime the aggregation when the first record arrives 
    in the window.
    """
    return {
        'min_temp': value['temperature'],
        'max_temp': value['temperature'],
        'total_events': 1,
        '_sum_temp': value['temperature'],
        'avg_temp': value['temperature']
    }


def reducer(aggregated: dict, value: dict) -> dict:
    """
    Calculate "min", "max", "total" and "average" over temperature values.
    
    Reducer always receives two arguments:
    - previously aggregated value (the "aggregated" argument)
    - current value (the "value" argument)
    It combines them into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.
    """
    total_events = aggregated['count'] + 1
    sum_temp = aggregated['_sum_temp'] + value
    avg_temp = sum_temp / total_events
    return {
        'min_temp': min(aggregated['min_temp'], value['temperature']),
        'max_temp': max(aggregated['max_temp'], value['temperature']),
        'total_events': total_events,
        'avg_temp': avg_temp,
        '_sum_temp': sum_temp
    }


sdf = (
    
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Create a "reduce" aggregation with "reducer" and "initializer" functions
    .reduce(reducer=reducer, initializer=initializer)

    # Emit results only for closed windows
    .final()
)

# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'value': {'min_temp': 1, 'max_temp': 999, 'total_events': 999, 'avg_temp': 34.32, '_sum_temp': 9999},
# }

```


### Collect()
Use `.collect()` to gather all events in the window into a list. This operation is optimized for collecting values and performs significantly better than using `reduce()` to build a list.

!!! note
    Performance benefit comes at a price: `.collect()` only supports `.final()` mode. Using `.current()` is not supported.

**Example:**

Collect all events over a 10-minute tumbling window into a list.

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Collect events in the window into a list
    .collect()

    # Emit results only for closed windows
    .final()
)
# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'value': [event1, event2, event3, ...] - list of all events in the window
# }
```


### Count()
Use `.count()` to calculate total number of events in the window.

**Example:**

Count all received events over a 10-minute tumbling window.

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Count events in the window 
    .count()

    # Emit results only for closed windows
    .final()
)
# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'value': 9999 - total number of events in the window
# }
```

### Min(), Max(), Mean() and Sum()

Methods `.min()`, `.max()`, `.mean()`, and `.sum()` provide short API to calculate these aggregates over the streaming windows.  


**These methods assume that incoming values are numbers.**

When they are not, extract the numeric values first using `.apply()` function.

**Example:**

Imagine you receive the temperature data from the sensor, and you need to calculate only a minimum temperature for each 10-minute tumbling window.  

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

# Input:
# {"temperature" : 9999}

sdf = (
    # Extract the "temperature" column from the dictionary 
    sdf.apply(lambda value: value['temperature'])
    
    # Define a tumbling window of 10 minutes
    .tumbling_window(timedelta(minutes=10))

    # Calculate the minimum temperature 
    .min()

    # Emit results only for closed windows
    .final()
)
# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'value': 9999  - minimum temperature
# }
```



## Transforming the result of a windowed aggregation
Windowed aggregations return aggregated results in the following format/schema:

```python
{"start": <window start ms>, "end": <window end ms>, "value": <aggregated value>}
```

Since it is rather generic, you may need to transform it into your own schema.  
Here is how you can do that:
 
```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))
    # Specify the "count" aggregation function
    .count()
    # Emit results only for closed windows
    .final()
)

# Input format:
# {"start": <window start ms>, "end": <window end ms>, "value": <aggregated value>
sdf = sdf.apply(
    lambda value: {
        "count": value["value"], 
        "window": (value["start"], value["end"]),
    }
)
# Output format:
# {"count": <aggregated value>, "window": (<window start ms>, <window end ms>)}
```


## Lateness and Out-of-Order Processing
When working with event time, some events may be processed later than they're supposed to.  
Such events are called **"out-of-order"** because they violate the expected order of time in the data stream. 

Example:

- A moving vehicle sends telemetry data, and it suddenly enters a zone without network coverage between 10:00 and 10:20
- At 10:21, the vehicle is back online. It can now send the telemetry for the period of 10:00 - 10:20.
- The events that happened at 10:00 are processed only at 10:21, i.e. ~20 minutes later.


### Processing late events with a "grace period"
To account for late events, windows in Quix Streams employ the concept of a **"grace period"**.

By default, events are dropped (and are not processed) when they arrive after reaching the end of the window.

But you can tell the window to wait for a certain period before closing itself.  
To do that, you must specify a grace period.

Example:

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

# Define a 1 hour tumbling window with a grace period of 10 seconds.
# It will inform the application to wait an additional 10 seconds of event time before considering the 
# particular window closed
sdf.tumbling_window(timedelta(hours=1), grace_ms=timedelta(seconds=10))
```

The appropriate value for a grace period varies depending on the use case.


### Reacting on late events 
!!! info New in v3.8.0

To react on late records coming into time windows, you can pass the `on_late` callbacks to `.tumbling_window()`, `.hopping_window()` and `.sliding_window()` methods.

You can use this callback to customize the logging of such messages or to send them to some dead-letter queue, for example.

**How it works**:

- If the `on_late` callback is not provided (default), the application will simply log the late events.
- The same will happen when the callback returns `True`.
- When the callback returns `False`, no logs will be produced. 


**Example**:

```python
from typing import Any

from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)


def on_late(
    value: Any,         # Record value
    key: Any,           # Record key
    timestamp_ms: int,  # Record timestamp
    late_by_ms: int,    # How late the record is in milliseconds
    start: int,         # Start of the target window
    end: int,           # End of the target window
    name: str,          # Name of the window state store
    topic: str,         # Topic name
    partition: int,     # Topic partition
    offset: int,        # Message offset
) -> bool:
    """
    Define a callback to react on late records coming into windowed aggregations.
    Return `False` to suppress the default logging behavior.
    """
    print(f"Late message is detected at the window {(start, end)}")
    return False

# Define a 1-hour tumbling window and provide the "on_late" callback to it
sdf.tumbling_window(timedelta(hours=1), on_late=on_late)


# Start the application
if __name__ == '__main__':
    app.run()

```



## Emitting results

Quix Streams supports 2 modes of emitting results for time windows:

- For each processed message in the given time window
- Only once, after the time window is closed


### Emitting updates for each message

To emit results for each processed message in the stream, use the following API:

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .current() to emit results immediately
sdf = sdf.tumbling_window(timedelta(seconds=10)).sum().current()

# Results:
# -> Timestamp=100, value=1 -> emit {"start": 0, "end": 10000, "value": 1} 
# -> Timestamp=101, value=1 -> emit {"start": 0, "end": 10000, "value": 2} 
# -> Timestamp=102, value=1 -> emit {"start": 0, "end": 10000, "value": 3} 
```

`.current()` methods instructs the window to return the aggregated result immediately after the message is processed, but the results themselves are not guaranteed to be final for the given interval.

The same window may receive another update in the future, and a new value with the same interval will be emitted.

`current()` mode can be used to react on changes quickly because the application doesn't need to wait until the window is closed. 
But you will likely get duplicated values for each window interval.

### Emitting after the window is closed 

Here is how to emit results only once for each window interval after it's closed:

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .final() to emit results only when the window is complete
sdf = sdf.tumbling_window(timedelta(seconds=10)).sum().final()

# Results:
# -> Timestamp=100, value=1   -> emit nothing (the window is not closed yet) 
# -> Timestamp=101, value=1   -> emit nothing (the window is not closed yet) 
# -> Timestamp=10001, value=1 -> emit {"start": 0, "end": 10000, "value": 2}, because the time has progressed beyond the window end. 
```

`.final()` mode makes the window wait until the maximum observed timestamp for the topic partition passes the window end before emitting.

Emitting final results provides unique and complete values per window interval, but it adds some latency.
Also, specifying a grace period using `grace_ms` will increase the latency, because the window now needs to wait for potential out-of-order events.

You can use `final()` mode when some latency is allowed, but the emitted results must be complete and unique.

## Closing strategies

By default windows use the **key** closing strategy. In this strategy messages advance time and close windows with the **same** message key.

If some message keys appear irregularly in the stream, the latest windows can remain unprocessed until the message with the same key is received.

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .final() to emit results only when the window is complete
sdf = sdf.tumbling_window(timedelta(seconds=10)).sum().final(closing_strategy="key")

# Details:
# -> Timestamp=100, Key="A", value=1   -> emit nothing (the window is not closed yet) 
# -> Timestamp=101, Key="B", value=2   -> emit nothing (the window is not closed yet) 
# -> Timestamp=105, Key="C", value=3   -> emit nothing (the window is not closed yet) 
# -> Timestamp=10100, Key="B", value=2 -> emit one message with key "B" and value {"start": 0, "end": 10000, "value": 2}, the time has progressed beyond the window end for the "B" key only.
# -> Timestamp=8000, Key="A", value=1 -> emit nothing (the window is not closed yet)
# -> Timestamp=10001, Key="A", value=1 -> emit one message with key "A" and value {"start": 0, "end": 10000, "value": 2}, the time has progressed beyond the window end for the "A" key.

# Results:
# (key="B", value={"start": 0, "end": 10000, "value": 2})
# (key="A", value={"start": 0, "end": 10000, "value": 2})
# No message for key "C" as the window is never closed since no messages with key "C" and a timestamp later than 10000 was received 
```

An alternative is to use the **partition** closing strategy.  
In this strategy, messages advance time and close windows for the whole partition to which this key belongs.

If messages aren't ordered accross keys some message can be skipped if the windows are already closed.

```python
from datetime import timedelta
from quixstreams import Application

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .final() to emit results only when the window is complete
sdf = sdf.tumbling_window(timedelta(seconds=10)).sum().final(closing_strategy="partition")

# Details:
# -> Timestamp=100, Key="A", value=1   -> emit nothing (the window is not closed yet) 
# -> Timestamp=101, Key="B", value=2   -> emit nothing (the window is not closed yet)
# -> Timestamp=105, Key="C", value=3   -> emit nothing (the window is not closed yet) 
# -> Timestamp=10100, Key="B", value=1 -> emit three messages, the time has progressed beyond the window end for all the keys in the partition
#                                           1. first one with key "A" and value {"start": 0, "end": 10000, "value": 1}
#                                           2. second one with key "B" and value {"start": 0, "end": 10000, "value": 2}
#                                           3. third one with key "C" and value {"start": 0, "end": 10000, "value": 3}
# -> Timestamp=8000, Key="A", value=1 -> emit nothing and value isn't part of the sum (the window is already closed)
# -> Timestamp=10001, Key="A", value=1 -> emit nothing (the window is not closed yet)

# Results:
# (key="A", value={"start": 0, "end": 10000, "value": 1})
# (key="B", value={"start": 0, "end": 10000, "value": 2})
# (key="C", value={"start": 0, "end": 10000, "value": 3})
```

## Implementation Details

Here are some general concepts about how windowed aggregations are implemented in Quix Streams:

- Only time-based windows are supported. 
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.
- The minimal window unit is a **millisecond**. More fine-grained values (e.g. microseconds) will be rounded towards the closest millisecond number.

## Operational Notes
### How Windows are Stored in State

Each window in a `StreamingDataFrame` creates its own state store. 
This state store will keep the aggregations for each window interval according to the
window specification.

The state store name is auto-generated by default using the following window attributes:

- Window type: `"tumbling"` or `"hopping"`
- Window parameters: `duration_ms` and `step_ms`
- Aggregation function name: `"sum"`, `"count"`, `"reduce"`, etc.

E.g. a store name for `sum` aggregation over a hopping window of 30 seconds with a 5 second step will be  `hopping_window_30000_5000_sum`.

### Updating Window Definitions

Windowed aggregations are stored as aggregates with start and end timestamps for each window interval.

When you change the definition of the window (e.g. its size), the data in the store will most likely become invalid and should not be re-used.

Quix Streams handles some of the situations, like:

- Updating window type (e.g. from tumbling to hopping)
- Updating window period or step 
- Updating an aggregation function (except the `reduce()`)

All of the above will change the name of the underlying state store, and the new window definition will use a different one.

But in some cases, these measures are not enough. For example, updating a code used in `reduce()` will not change the store name, but the data can still become inconsistent.

In this case, you may need to update the `consumer_group` passed to the `Application` class. It will re-create all the state stores from scratch.
