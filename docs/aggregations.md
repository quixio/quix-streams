# Aggregating Data

## Aggregation Types
In Quix Streams, aggregation operations are divided into two groups: **Aggregators** and **Collectors**. 

### Aggregators
**Aggregators** incrementally combine the current value and the aggregated data and store the result to the state.   
Use them when the aggregation operation can be performed in incremental way, like counting items. 


### Collectors
**Collectors** accumulate individual values in the state before performing any aggregation.

They can be used to batch items into a collection, or when the aggregation operation needs 
the full dataset, like calculating a median.

**Collectors** are optimized for storing individual values to the state and perform significantly better than **Aggregators** when you need to accumulate values into a list.

!!! note

    Performance benefit comes at a price, **Collectors** only support [`.final()`](windowing.md#emitting-after-the-window-is-closed) mode.
    Using [`.current()`](windowing.md#emitting-updates-for-each-message) is not supported.


## Using Aggregations

!!! info

    Currently, aggregations can be performed only over [windowed](./windowing.md) data.


To calculate an aggregation, you need to define a window.

To learn more about windows, see the [Windowing](./windowing.md) page.

When you have a window, call `.agg()` and pass the configured aggregator or collector as a named parameter.

**Example 1. Count items in the window**

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Count

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Call .agg() and provide an Aggregator or Collector to it.
    # Here we use a built-in aggregator "Count".
    # The parameter name will be used as a part of the aggregated state and returned in the result. 
    .agg(count=Count())

    # Specify how the windowed results are emitted.
    # Here, emit results only for closed windows.
    .final()
)

# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'count': 9999 - total number of events in the window
# }
```

**Example 2. Accumulating items in the window**

Use [`Collect()`](api-reference/quixstreams.md#collect) to gather all events within each window period into a list.  
Collect takes an optional `column` parameter to limit the collection to one column of the input.


```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Collect

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Collect events in the window into a list
    .agg(events=Collect())

    # Emit results only for closed windows
    .final()
)
# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'events': [event1, event2, event3, ..., eventN] - list of all events in the window
# }
```


### Aggregating over a single column

**Aggregators** allow you to select a column using the optional `column` parameter.  

When `column` is passed, the Aggregator will perform aggregation only over this column.
It is assumed that the value is a dictionary. 

Otherwise, it will use the whole message.  

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Min

app = Application(...)
sdf = app.dataframe(...)

# Input:
# {"temperature" : 9999}

sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Calculate the Min aggregation over the "temperature" column 
    .agg(min_temperature=Min(column="temperature"))

    # Emit results only for closed windows
    .final()
)

# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'min_temperature': 9999  - minimum temperature
# }****
```


### Multiple Aggregations

It is possible to calculate several different aggregations and collections over the same window.  

**Collectors** are optimized to store the values only once when shared with other collectors. 

You can define a wide range of aggregations, such as:

- Aggregating over multiple message fields at once
- Calculating multiple aggregates for the same value

**Example**:

Assume you receive the temperature data from the sensor, and you need to calculate these aggregates for each 10-minute tumbling window:

- min temperature
- max temperature
- total count of events
- average temperature

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Min, Max, Count, Mean, Latest

app = Application(...)
sdf = app.dataframe(...)

# Input:
# {"temperature" : 9999, "sensor": "my sensor"}

sdf = (
    
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    .agg(
        min_temp=Min("temperature"),
        max_temp=Max("temperature"),
        avg_temp=Mean("temperature"),
        total_events=Count(),
        sensor=Latest("sensor")  # Propagate the sensor name
    )

    # Emit results only for closed windows
    .final()
)

# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'min_temp': 1,
#   'max_temp': 999,
#   'avg_temp': 34.32,
#   'total_events': 999,
#   'sensor': 'my sensor',
# }
```


## Built-in Aggregators and Collectors

**Aggregators:**

- [`Count()`](api-reference/quixstreams.md#count) - to count the number of values within a window.
- [`Min()`](api-reference/quixstreams.md#min) - to get a minimum value within a window.
- [`Max()`](api-reference/quixstreams.md#max) - to get a maximum value within a window.
- [`Mean()`](api-reference/quixstreams.md#mean) - to get a mean value within a window.
- [`Sum()`](api-reference/quixstreams.md#sum) - to sum values within a window.
- [`Earliest`](api-reference/quixstreams.md#earliest) - to get the earliest value within a window.
- [`Latest`](api-reference/quixstreams.md#latest) - to get the latest value within a window.
- [`First`](api-reference/quixstreams.md#first) - to get the first value within a window.
- [`Last`](api-reference/quixstreams.md#last) - to get the last value within a window.
- [`Reduce()`](api-reference/quixstreams.md#reduce) - to write a custom aggregation (deprecated, use [custom aggregator](#custom-aggregator) instead).

**Collectors:**

- [`Collect()`](api-reference/quixstreams.md#collect) - to collect all values within a window into a list.


## Custom Aggregators


To implement a custom aggregator, subclass the `Aggregator` class and implement 3 methods:

- [`initialize`](api-reference/quixstreams.md#baseaggregatorinitialize): Called when initializing a new window. Starting value of the aggregation.
- [`agg`](api-reference/quixstreams.md#baseaggregatoragg): Called for every item added to the window. It should merge the new value with the aggregated state.
- [`result`](api-reference/quixstreams.md#baseaggregatorresult): Called to generate the result from the aggregated value


By default, the aggregation state key includes the aggregation class name.

If your aggregations accepts parameters, like a column name, you can override the [`state_suffix`](api-reference/quixstreams.md#baseaggregatorstate_suffix) property to include those parameters in the state key.  
Whenever the state key changes, the aggregation's state is reset.


**Example 1. Power sum**

Calculate the sum of the power of incoming data over a 10-minute tumbing window,.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows.aggregations import Aggregator

app = Application(...)
sdf = app.dataframe(...)

class PowerSum(Aggregator):
    def initialize(self):
        return 0

    def agg(self, aggregated, new, timestamp):
        if self.column is not None:
            new = new[self.column]
        return aggregated + (new * new)

    def result(self, aggregated):
        return aggregated

# Input:
# {"amount" : 2}
# {"amount" : 3}

sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Aggregate the custom sum
    .agg(sum=PowerSum())

    # Emit results only for closed windows
    .final()
)
# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'sum': 13
# }
```


**Example 2. Custom aggregation over multiple message fields**


```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Aggregator

class TemperatureAggregator(Aggregator):
    def initialize(self):
        return {
            "min_temp": 0,
            "max_temp": 0,
            "total_events": 0,
            "sum_temp": 0,
        }
    
    def agg(self, old, new, ts):
        if self.column is not None:
            new = new[self.column]

        old["min_temp"] = min(old["min_temp"], new)
        old["max_temp"] = max(old["max_temp"], new)
        old["total_events"] += 1
        old["sum_temp"] += new
        return old

    def result(self, stored):
        return {
            "min_temp": stored["min_temp"],
            "max_temp": stored["max_temp"],
            "total_events": stored["total_events"]
            "avg_temp": stored["sum_temp"] / stored["total_events"]
        }
        

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    .agg(
        value=TemperatureAggregator(column="Temperature")
    )

    # Emit results only for closed windows
    .final()
)

# Output:
# {
#   'start': <window start>, 
#   'end': <window end>,
#   'value': {
#       'min_temp': 1,
#       'max_temp': 999,
#       'avg_temp': 34.32,
#       'total_events': 999,
#   }
# }
```


## Custom Collectors

To implement a custom **Collector**, subclass the [`Collector`](api-reference/quixstreams.md#collector) class and implement the [`result`](api-reference/quixstreams.md#basecollectorresult) method.

It is called when the window is closed with an iterable of all the collected items in this window. 

By default, **Collectors** always store the full message.  

If you only need in a specific column, you can override the [`column`](api-reference/quixstreams.md#basecollectorcolumn) property to specify which column needs to be stored.


**Example:**

Collect all events over a 10-minute tumbling window into a reversed order list.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows.aggregations import Collector

app = Application(...)
sdf = app.dataframe(...)

class ReversedCollect(Collector):
    def result(self, items):
        # items is the list of all collected item during the window
        return list(reversed(items))

sdf = (
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Collect events in the window into a reversed list
    .agg(events=ReversedCollect())

    # Emit results only for closed windows
    .final()
)
# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'events': [eventN, ..., event3, event2, event1] - reversed list of all events in the window
# }
```

## Reduce

!!! warning
    `Reduce` is deprecated. Use [multiple aggregations](aggregations.md#multiple-aggregations) and [custom Aggregators](aggregations.md#custom-aggregators) instead. They provide more control over parameters and better state management. 

[`Reduce()`](api-reference/quixstreams.md#reduce) allows you to perform complex aggregations using custom "reducer" and "initializer" functions:

- The **"initializer"** function receives the **first** value for the given window, and it must return an initial state for this window.  
This state will be later passed to the "reducer" function.  
**It is called only once for each window.**

- The **"reducer"** function receives an aggregated state and a current value, and it must combine them and return a new aggregated state.  
This function should contain the actual aggregation logic.  
It will be called for each message coming into the window, except the first one.
