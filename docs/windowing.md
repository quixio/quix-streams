
# Windowed Aggregations


## What Are Windows?
In stream processing, windows are used to divide endless streams of events into finite time-based intervals.

With windows, you can calculate such aggregations as:
- Total of website visitors for every hour
- An average speed of a vehicle over the last 10 minutes
- Maximum temperature of the sensor for each 30s

## Implementation Details

Here are some general concepts about how windowed aggregations are implemented in Quix Streams:

- Only time-based windows are supported. 
- Every window is grouped by the current Kafka message key. 
- The minimal window unit is a **millisecond**.  
More fine-grained values (e.g. microseconds) will be rounded towards the closest millisecond number.


## Types of Time in Streaming

There are two types of time in streaming systems:

1. **Event time** - the time when the event happened. 
2. **Processing time** - the time when the event is processed by the system


In Quix Streams, windows always use event time.  

The event time is obtained from the timestamps of incoming Kafka messages.  

A Quix Streams application keeps its own "clock" for each assigned partition.  
The state store tracks the **maximum observed timestamp** across incoming events within each topic partition, and 
this timestamp is used as a current time in the stream.  
What's important, it can never go backward.

When the application gets an event timestamp for the event, it assigns an interval according to the window definition.


Quix Streams supports two ways of slicing the time:

## Tumbling Windows
   Tumbling windows slice time into non-overlapping intervals of a fixed size. 

   For example, a tumbling window of 1h will generate the following intervals:
    
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

   The following code snippet shows how to define a tumbling window over a `StreamingDataFrame`:

```python
from datetime import timedelta


sdf = app.dataframe(...)

# Calculate the sum over a tumbling window of 1 hour and emit results for each  
# incoming record
sdf = sdf.tumbling_window(duration_ms=timedelta(hours=1)).sum().current()

# Alternative syntax to specify window duration using milliseconds
sdf = sdf.tumbling_window(duration_ms=60 * 60* 1000).sum().current()
```


## Hopping Windows
   Hopping windows slice time into overlapping intervals of a fixed size and with a fixed step.

   For example, a hopping window of 1h with a step of 10m will generate the following intervals:
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

   For example, a timestamp `00:33:13` will match two intervals for a hopping window of 1 hour with a 30-minute step:
   - `00:00:00 - 01:00:00`
   - `00:30:00 - 01:30:00`

    
   The following code snippet shows how to define a hopping window over a `StreamingDataFrame`:
```python
from datetime import timedelta

sdf = app.dataframe(...)

# Create a hopping window of 1 hour with a 10-minute step
sdf = (
    sdf.hopping_window(duration_ms=timedelta(hours=1), step_ms=timedelta(minutes=10))
    .sum()
    .current()
)

# Alternative syntax to specify window duration and step using milliseconds
sdf = (
    sdf.hopping_window(duration_ms=60 * 60 * 1000, step_ms=10 * 60 * 1000)
    .sum()
    .current()
)

```

    


## Lateness and Out-of-Order Processing
When working with event time, some events may be processed later than they're supposed to.  
We call such events **"out-of-order"** because they violate the expected order of time in the data stream. 

Example:
- a moving vehicle sends the telemetry data, and it suddenly enters a zone without network coverage between 10:00 and 10:20
- At 10:21, the vehicle is back online. It can now send the telemetry for the period of 10:00 - 10:20.
- The events that happened at 10:00 are processed only at 10:21, i.e. ~20 minutes later.


### Processing Late Events with a "grace period"
To account for late events, windows in Quix Streams employ the concept of **"grace period"**.

By default, events that arrive after the maximum observed timestamp passes the window end are dropped
from the processing.

But we can tell the window to wait for a certain period before closing itself.  
To do that, you must specify a grace period.

Example:

```python
from datetime import timedelta


sdf = app.dataframe(...)

# Define a 1-hour tumbling window with a grace period of 10 seconds.
# It will inform the application to wait an additional 10 seconds of event time before considering the 
# particular window closed.
sdf.tumbling_window(timedelta(hours=1), grace_ms=timedelta(seconds=10))
```

The appropriate value for a grace period varies depending on the use case.


## Emitting results

Windows in Quix Streams supports 2 modes of emitting results:

1. `current()` - results are emitted for each processed message in the window.

This mode returns the aggregated result immediately after the message is processed, but the results themselves are not guaranteed to be final for the given interval.

The same window may receive another update in the future, and a new value with the same interval will be emitted.

Example:
```python
from datetime import timedelta


sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds and emit results immediately
sdf = sdf.tumbling_window(timedelta(seconds=10)).sum().current()

# -> Timestamp=100, value=1 -> emit {"start": 0, "end": 10000, "value": 1} 
# -> Timestamp=101, value=1 -> emit {"start": 0, "end": 10000, "value": 2} 
# -> Timestamp=102, value=1 -> emit {"start": 0, "end": 10000, "value": 3} 
```

`current()` mode may be used when you need to react to changes quickly because the application doesn't need to wait until the window is closed. 
But you will likely get duplicated values for each window interval


2. `final()` - results are emitted only after the window is expired.

This mode makes the application wait until the maximum observed timestamp for the topic partition passes the window end before emitting.

Example:
```python
from datetime import timedelta


sdf = app.dataframe(...)

# Calculate a sum of values over a window of 10 seconds and emit results only when they are final
sdf = sdf.tumbling_window(timedelta(seconds=10)).sum().current()


# -> Timestamp=100, value=1   -> emit nothing (the window is not closed yet) 
# -> Timestamp=101, value=1   -> emit nothing (the window is not closed yet) 
# -> Timestamp=10001, value=1 -> emit {"start": 0, "end": 10000, "value": 2}, because the time has progressed beyond the window end. 
```

Emitting final results provides unique and complete values per window interval, but it adds some latency.
Also, specifying a grace period via `grace_ms` will increase the latency, because the window now needs to wait for potential out-of-order events.

`final()` mode is suitable for use cases when some latency is allowed, but the emitted events must be complete and unique.

>***NOTE:*** Windows can be closed only by the records with the **same** message key.
> If some message keys appear irregularly in the stream, the latest windows 
> can remain unprocessed until the message with the same key is received.


## Supported Aggregations

Currently, windows support the following aggregation functions:

- [`reduce()`](https://github.com/quixio/quix-streams/blob/main/docs/api-reference/quixstreams.md#fixedtimewindowdefinitionreduce) - to perform custom aggregations using "reducer" and "initializer" functions
- [`min()`](https://github.com/quixio/quix-streams/blob/main/docs/api-reference/quixstreams.md#fixedtimewindowdefinitionmin) - to get a minimum value within a window
- [`max()`](https://github.com/quixio/quix-streams/blob/main/docs/api-reference/quixstreams.md#fixedtimewindowdefinitionmax) -  to get a maximum value within a window
- [`mean()`](https://github.com/quixio/quix-streams/blob/main/docs/api-reference/quixstreams.md#fixedtimewindowdefinitionmean) - to get a mean value within a window 
- [`sum()`](https://github.com/quixio/quix-streams/blob/main/docs/api-reference/quixstreams.md#fixedtimewindowdefinitionsum) - to sum values within a window 
- [`count()`](https://github.com/quixio/quix-streams/blob/main/docs/api-reference/quixstreams.md#fixedtimewindowdefinitioncount) - to count values within a window 


## Examples

### Calculate the sum of a message field over a tumbling window
Summing values over some time interval is probably the most basic aggregation because it can tell a lot about the behavior of the underlying metric.

To calculate a sum of values over a window, you can use the `sum()` aggregation function.  

Note: the `sum()` function does not validate the value type, and it may fail if the value cannot be added.

The example below demonstrates how to extract a value from the record's dictionary and use it in the aggregation:

```python
from datetime import timedelta

sdf = app.dataframe(...)

sdf = (
    # Extract the "total" field from the record
    sdf.apply(lambda value: value["total"])
    
    # Define a tumbling window of 10 minutes
    .tumbling_window(timedelta(minutes=10))
    
    # Specify the "sum" aggregation function
    .sum()
    
    # Emit results only for closed windows
    .final()
)
```


### Calculate multiple aggregations at once using "reduce" 

Aggregation via `reduce` provides a lot of flexibility.  
It allows to define a custom windowed aggregation using two components:
- an **"initializer"** function - to initialize an aggregated window state when the first message comes into the window. **The "initializer" is called only once when the window is created.** 
- a **"reducer"** function - to combine an aggregated window state with new data. **The "reducer" is called only for the second and following incoming values.**  

With `reduce()`, you can define a very wide range of aggregations, such as:
- Aggregating over multiple message fields at once
- Using multiple message fields to create a single aggregate
- Aggregating multiple metrics for the same value


Example:
```python
from datetime import timedelta

sdf = app.dataframe(...)


def initializer(value: dict) -> dict:
    """
    Initialize the state for aggregation when a new window starts.
    
    It will prime the aggregation when the first record arrives 
    to the window.
    """
    return {
        'min': value['total'],
        'max': value['total'],
        'count': 1,
    }


def reducer(aggregated: dict, value: dict) -> dict:
    """
    Calculate "min", "max" and "count" over a set of transactions.
    
    Reducer always receives two arguments:
    - previously aggregated value
    - current value
    It combines them into a new aggregated value and returns it.
    This aggregated value will be also returned as a result of the window.
    """
    return {
        'min': min(aggregated['min'], value['total']),
        'max': max(aggregated['max'], value['total']),
        'count': aggregated['count'] + 1,
    }


sdf = (

    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Create a "reduce" aggregation with "reducer" and "initializer" functions
    .reduce(reducer=reducer, initializer=initializer)

    # Emit results only for closed windows
    .final()
)
```

### Transform a result of a windowed aggregation

```python
sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))
    # Specify the "count" aggregation function
    .count()
    # Emit results only for closed windows
    .final()
)

# Windowed aggregations return aggregation results in the following format:
# {"start": <window start ms>, "end": <window end ms>, "value": <aggregation value>
#
# Let's transform it to a different format:
# {"count": <aggregation value>, "window": (<window start ms>, <window end ms>)}
sdf = sdf.apply(
    lambda value: {"count": value["value"], "window": (value["start"], value["end"])}
)
```


## Operations
### How Windows are Stored in State

Each window in `StreamingDataFrame` creates its own state store. 
This state store will keep the aggregations for each window interval according to the
window specification.

The state store name is auto-generated by default using the following window attributes:
- window type: `"tumbling"` or `"hopping"`
- window parameters: `duration_ms` and `step_ms`
- aggregation function name: `"sum"`, `"count"`, `"reduce"`, etc.

E.g., a store name for `sum` aggregation over a hopping window of 30sec with a 5sec step will be  `hopping_window_30000_5000_sum`.

### Updating Window Definitions

Windowed aggregations are stored as aggregates with start and end timestamps for each window interval.

When you change the definition of the window (e.g. its size), the data in the store will most likely become invalid and should not be re-used.

Quix Streams handles some of the situations, like:
- Updating window type (e.g. from tumbling to hopping)
- Updating window period or step 
- Updating an aggregation function (except the `reduce()`)

All of the above will change the name of the underlying state store, and the new window definition will use a different one.

But in some cases, these measures are not enough.  
For example, updating a code used in `reduce()` will not change the store name, but the data can still become inconsistent.

In this case, you may need to update the `consumer_group` passed to the `Application` class.   
It will re-create all the state stores from scratch.
