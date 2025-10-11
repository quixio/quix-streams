# Windowed Aggregations


## What Are Windows?
In stream processing, windows are used to divide endless streams of events into finite time-based or count-based intervals.

With windows, you can calculate such aggregations as:

- Total of website visitors for every hour
- The average speed of a vehicle over the last 10 minutes
- Maximum temperature of a sensor observed over 30 second ranges
- Give a user a reward after 10 successful actions
- Track user activity sessions on a website
- Detect fraud patterns in financial transactions 


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
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Min

app = Application(...)

sdf = app.dataframe(...)

# Input:
# value={"temperature" : 9999}, key="sensor_1", timestamp=10001

sdf = (
    # Define a tumbling window of 10 seconds
    .tumbling_window(timedelta(seconds=10))

    # Calculate the minimum temperature 
    .agg(minimum_temperature=Min("temperature"))

    # Emit results for every incoming message
    .current()
)
# Output:
# value={
#   'start': 10000, 
#   'end': 20000, 
#   'minimum_temperature': 9999  - minimum temperature
# }, 
# key="sensor_1",
# timestamp=10000 - timestamp equals to the window start timestamp

```

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
{"avg_temperature": 30, "start": 0, "end": 3600000}
{"avg_temperature": 29.5, "start": 0, "end": 3600000}
{"avg_temperature": 29, "start": 0, "end": 3600000}
```

Here is how to do it using tumbling windows: 

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Mean

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    # Define a tumbling window of 1 hour
    # You can also pass duration_ms as an integer of milliseconds
    .tumbling_window(duration_ms=timedelta(hours=1))

    # Specify the "mean" aggregate function
    .agg(avg_temperature=Mean("temperature"))

    # Emit updates for each incoming message
    .current()
)

```

## Count-based Tumbling Windows

Count-based Tumbling Windows slice incoming events into batch of a fixed size.

For example, a tumbling window configured with a count of 4 will batch and aggregate message 1 to 4, then 5 to 8, 9 to 12 and so on. 

```
Count       Tumbling Windows
[0, 3]  : ....
[4, 7]  :     ....
[8, 11] :         ....
[12, 15] :            ....
```

In a tumbling window each message is only assigned to a **single** interval.

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
{"data": [100, 50, 200], "start": 121, "end": 583}
```

Here is how to do it using tumbling windows: 

```python
import time
import json
import urllib.request

from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Collect

def external_api(value):
    with urllib.request.urlopen("https://example.com", data=json.dumps(value["data"])) as rep:
        return response.read()

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    # Define a count-based tumbling window of 3 events
    .tumbling_count_window(count=3)

    # Specify the "collect" aggregate function
    .agg(data=Collect())

    # Emit updates once the window is closed
    .final()
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
{"avg_temperature": 30, "start": 0, "end": 3600000}

{"avg_temperature": 29.5, "start": 0, "end": 3600000}
{"avg_temperature": 30, "start": 60000, "end": 4200000}

{"avg_temperature": 29, "start": 0, "end": 3600000}
{"avg_temperature": 28.5, "start": 60000, "end": 4200000}
```


```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Mean

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a hopping window of 1h with 10m step
    # You can also pass duration_ms and step_ms as integers of milliseconds
    .hopping_window(duration_ms=timedelta(hours=1), step_ms=timedelta(minutes=10))

    # Specify the "mean" aggregate function
    .agg(avg_temperature=Mean("temperature"))

    # Emit updates for each incoming message
    .current()
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

In hopping windows each message can be assigned to multiple windows because the windows overlap.

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
{"avg_temperature": 30, "start": 0, "end": 3600000}
{"avg_temperature": 29.5, "start": 1200000, "end": 4800000}
{"avg_temperature": 29, "start": 1200001, "end": 4800001}
{"avg_temperature": 28.5, "start": 3600000, "end": 7200000}
{"avg_temperature": 27.5, "start": 3600001, "end": 7200001}  # reading 30 is outside of the window
```


```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Mean

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a sliding window of 1h
    # You can also pass duration_ms as integer of milliseconds
    .sliding_window(duration_ms=timedelta(hours=1))

    # Specify the "mean" aggregate function
    .agg(avg_temperature=Mean("temperature"))

    # Emit updates for each incoming message
    .current()
)

```

## Count-based Sliding Windows

Sliding windows are overlapping windows that advance with each incoming message. They are equal to count-based hopping windows with a step of 1.

For example a sliding window of 4 messages will generate the following windows:

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
{"average": 120, "start": 121, "end": 583}
{"average": 100, "start": 165, "end": 723}
{"average": 120, "start": 583, "end": 1009}
{"average": 80, "start": 723, "end": 1242}

```

Here is how to do it using sliding windows: 

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Mean

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    # Define a count-based sliding window of 3 events
    .sliding_count_window(count=3)

    # Specify the "mean" aggregate function
    .agg(average=Mean("amount"))

    # Emit updates once the window is closed
    .final()
)

```

## Session Windows

Session windows group events that occur within a specified timeout period. Unlike fixed-time windows (tumbling, hopping, sliding), session windows have dynamic durations based on the actual timing of events. This makes them ideal for user activity tracking, fraud detection, and other event-driven scenarios.

A session starts with the first event and extends each time a new event arrives within the timeout period. The session closes after the timeout period with no new events.

Key characteristics of session windows:

- **Dynamic boundaries**: Each session can have different start and end times based on actual events
- **Activity-based**: Sessions extend automatically when events arrive within the timeout period  
- **Event-driven closure**: Sessions close when no events arrive within the timeout period
- **Grace period support**: Late events can still extend sessions if they arrive within the grace period

### How Session Windows Work

```
Time:    0    5    10   15   20   25   30   35   40   45   50
Events:  A         B              C    D              E

Timeout: 10 seconds
Grace:   2 seconds

Session 1: [0, 20] - Events A, B (B extends the session from A)
Session 2: [25, 40] - Events C, D (D extends the session from C)
Session 3: [45, 55] - Event E (session will close at 55 if no more events)
```

In this example:
- Event A starts Session 1 at time 0, session would timeout at time 10
- Event B arrives at time 10, extending Session 1 to timeout at time 20
- Event C arrives at time 25, starting Session 2 (too late for Session 1)
- Event D arrives at time 30, extending Session 2 to timeout at time 40
- Event E arrives at time 45, starting Session 3

### Basic Session Window Example

Imagine you want to track user activity sessions on a website, where a session continues as long as user actions occur within 30 minutes of each other:

Input:
```json
{"user_action": "page_view", "page": "/home", "timestamp": 1000}
{"user_action": "click", "element": "button", "timestamp": 800000}
{"user_action": "page_view", "page": "/products", "timestamp": 1200000}
{"user_action": "purchase", "amount": 50, "timestamp": 2000000}
```

Here's how to track user sessions using session windows:

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Count, Collect

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a session window with 30-minute timeout and 5-minute grace period
    .session_window(
        timeout_ms=timedelta(minutes=30),
        grace_ms=timedelta(minutes=5)
    )

    # Count the number of actions in each session and collect all actions
    .agg(
        action_count=Count(),
        actions=Collect("user_action")
    )

    # Emit results when sessions are complete
    .final()
)

# Expected output (when session expires):
# {
#   "start": 1000,
#   "end": 2000000,  # timestamp of last event
#   "action_count": 4,
#   "actions": ["page_view", "click", "page_view", "purchase"]
# }
```

### Session Window with Current Mode

For real-time monitoring, you can use `.current()` mode to get updates as the session progresses:

Input:
```json
{"amount": 25, "timestamp": 1000}
{"amount": 50, "timestamp": 5000} 
{"amount": 50, "timestamp": 8000}
```

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Sum, Count

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a session window with 10-second timeout
    .session_window(timeout_ms=timedelta(seconds=10))

    # Track total purchase amount and count in each session
    .agg(
        total_amount=Sum("amount"),
        purchase_count=Count()
    )

    # Emit updates for each message (real-time session tracking)
    .current()
)

# Output for each incoming event:
# Event 1: {"start": 1000, "end": 1000, "total_amount": 25, "purchase_count": 1}
# Event 2: {"start": 1000, "end": 5000, "total_amount": 75, "purchase_count": 2}
# Event 3: {"start": 1000, "end": 8000, "total_amount": 125, "purchase_count": 3}
```

### Handling Late Events in Sessions

Session windows support grace periods to handle out-of-order events:

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Count

def on_late_session_event(
    value, key, timestamp_ms, late_by_ms, start, end, name, topic, partition, offset
):
    """Handle late events that couldn't extend any session"""
    print(f"Late event for key {key}: {late_by_ms}ms late")
    print(f"Event would have belonged to session [{start}, {end}]")
    return False  # Suppress default logging

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Session window with 5-minute timeout and 1-minute grace period
    .session_window(
        timeout_ms=timedelta(minutes=5),
        grace_ms=timedelta(minutes=1),
        on_late=on_late_session_event
    )
    .agg(event_count=Count())
    .final()
)
```

### Session Window Use Cases

**1. User Activity Tracking**
```python
# Track user sessions on a website or app
.session_window(timeout_ms=timedelta(minutes=30))
.agg(
    page_views=Count(),
    unique_pages=Count("page_url", unique=True),
    session_duration=Max("timestamp") - Min("timestamp")
)
```

**2. Fraud Detection**
```python
# Detect suspicious transaction patterns
.session_window(timeout_ms=timedelta(minutes=10))
.agg(
    transaction_count=Count(),
    total_amount=Sum("amount"),
    locations=Collect("location")
)
```

**3. IoT Device Monitoring**
```python
# Monitor device activity sessions
.session_window(timeout_ms=timedelta(hours=1))
.agg(
    readings_count=Count(),
    avg_temperature=Mean("temperature"),
    max_pressure=Max("pressure")
)
```

**4. Gaming Analytics**
```python
# Track gaming sessions
.session_window(timeout_ms=timedelta(minutes=20))
.agg(
    actions_performed=Count(),
    points_earned=Sum("points"),
    levels_completed=Count("level_completed")
)
```

### Session Window Parameters

- **`timeout_ms`**: The session timeout period. If no new events arrive within this period, the session will be closed. Can be specified as either an `int` (milliseconds) or a `timedelta` object.

- **`grace_ms`**: The grace period for data arrival. Allows late-arriving data to be included in the session, even if it arrives after the session has theoretically timed out. Can be specified as either an `int` (milliseconds) or a `timedelta` object.

- **`name`**: Optional unique identifier for the window. If not provided, it will be automatically generated based on the window's properties.

- **`on_late`**: Optional callback to react to late records that cannot extend any existing session. Use this to customize logging or route late events to a dead-letter queue.

### Session Window Behavior

**Session Creation**: A new session starts when an event arrives and no existing session can accommodate it (i.e., all existing sessions have timed out).

**Session Extension**: An existing session is extended when an event arrives within `timeout + grace_period` of the session's last activity.

**Session Closure**: A session closes when the current time exceeds `last_event_time + timeout + grace_period`. The session end time in the output represents the timestamp of the last event in the session.

**Out-of-Order Events**: When out-of-order events arrive within the grace period, they extend the session but do not change the end time if they are older than the current latest event. The end time always represents the timestamp of the chronologically latest event in the session.

**Key Grouping**: Like all windows in Quix Streams, sessions are grouped by message key. Each key maintains its own independent sessions.

**Event Time**: Sessions use event time (from Kafka message timestamps) rather than processing time.

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

To react on late records coming into time windows, you can pass the `on_late` callbacks to `.tumbling_window()`, `.hopping_window()`, `.sliding_window()`, and `.session_window()` methods.

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
from quixstreams.dataframe.windows import Sum

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .current() to emit results immediately
sdf = sdf.tumbling_window(timedelta(seconds=10)).agg(value=Sum()).current()

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
from quixstreams.dataframe.windows import Sum

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .final() to emit results only when the window is complete
sdf = sdf.tumbling_window(timedelta(seconds=10)).agg(value=Sum()).final()

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

By default, windows use the **key** closing strategy.  
In this strategy, messages advance time and close only windows with the **same** message key.

If some message keys appear irregularly in the stream, the latest windows can remain unprocessed until the message with the same key is received.

Session windows also support both closing strategies. With **key** strategy, sessions for each key close independently. With **partition** strategy, any message can advance time and close sessions for all keys in the partition.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Sum

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .final() to emit results only when the window is complete
sdf = sdf.tumbling_window(timedelta(seconds=10)).agg(value=Sum()).final(closing_strategy="key")

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

If messages aren't ordered across keys some message can be skipped if the windows are already closed.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Sum

app = Application(...)
sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds 
# and use .final() to emit results only when the window is complete
sdf = sdf.tumbling_window(timedelta(seconds=10)).agg(value=Sum()).final(closing_strategy="partition")

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

## Transforming the result of a windowed aggregation
Windowed aggregations return aggregated results in the following format/schema:

```python
{"start": <window start ms>, "end": <window end ms>, <aggregated result colum>: <aggregated value>}
```

Since it is rather generic, you may need to transform it into your own schema.  
Here is how you can do that:
 
```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Count

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))
    # Specify the "count" aggregation function
    .agg(count=Count())
    # Emit results only for closed windows
    .final()
)

# Input format:
# {"start": <window start ms>, "end": <window end ms>, "count": <aggregated value>
sdf = sdf.apply(
    lambda value: {
        "count": value["count"], 
        "window": (value["start"], value["end"]),
    }
)
# Output format:
# {"count": <aggregated value>, "window": (<window start ms>, <window end ms>)}
```


### Message headers of the aggregation results

Currently, windowed aggregations do not store the original headers of the messages.  
The results of the windowed aggregations will have headers set to `None`.  

You may set messages headers by using the `StreamingDataFrame.set_headers()` API, as 
described in [the "Updating Kafka Headers" section](./processing.md#updating-kafka-headers).


## Implementation Details

Here are some general concepts about how windowed aggregations are implemented in Quix Streams:

- Time-based windows (tumbling, hopping, sliding, session) and count-based windows are supported.
- Every window is grouped by the current Kafka message key.
- Messages with `None` key will be ignored.
- The minimal window unit is a **millisecond**. More fine-grained values (e.g. microseconds) will be rounded towards the closest millisecond number.

## Operational Notes
### How Windows are Stored in State

Each window in a `StreamingDataFrame` creates its own state store. 
This state store will keep the aggregations for each window interval according to the
window specification.

The state store name is auto-generated by default using the following window attributes:

- Window type: `"tumbling"`, `"hopping"`, `"sliding"`, or `"session"`
- Window parameters: `duration_ms` and `step_ms` for time-based windows, `timeout_ms` for session windows

Examples:
- A hopping window of 30 seconds with a 5 second step: `hopping_window_30000_5000`
- A session window with 30 second timeout: `session_window_30000`

### Updating Window Definitions

Windowed aggregations are stored as aggregates with start and end timestamps for each window interval.

When you change the definition of the window (e.g. its size), the data in the store will most likely become invalid and should not be re-used.

Quix Streams handles some of the situations, like:

- Updating window type (e.g. from tumbling to hopping, from hopping to session)
- Updating window period, step, or timeout
- Adding/Removing/Updating an aggregation function (except `Reduce()`)

Updating the window type and parameters will change the name of the underlying state store, and the new window definition will use a different one.

Updating an aggregation parameter will change the aggregation state key and reset the modified aggregation state, other aggregations are not impacted.

But in some cases, these measures are not enough. For example, updating a code used in `Reduce()` will not change the store name, but the data can still become inconsistent.

In this case, you may need to update the `consumer_group` passed to the `Application` class. It will re-create all the state stores from scratch.
