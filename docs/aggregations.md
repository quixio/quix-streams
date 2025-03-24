# Aggregations and Collections

Currently, [windows](windowing.md) support the following aggregation and collection functions:

- [`Count()`](api-reference/quixstreams.md#count) - to count the number of values within a window 
- [`Min()`](api-reference/quixstreams.md#min) - to get a minimum value within a window
- [`Max()`](api-reference/quixstreams.md#max) - to get a maximum value within a window
- [`Mean()`](api-reference/quixstreams.md#mean) - to get a mean value within a window 
- [`Sum()`](api-reference/quixstreams.md#sum) - to sum values within a window
- [`Reduce()`](api-reference/quixstreams.md#reduce) - to write a custom aggregation (deprecated use [custom aggregator](#custom-aggregator) instead)
- [`Collect()`](api-reference/quixstreams.md#collect) - to collect all values within a window into a list

You can also create your own **custom aggregator and collector**. We will go over each of them in more detail below.

## Aggregators

### Count
Use [`Count()`](api-reference/quixstreams.md#count) to calculate total number of events within each window period.

**Example:**

Count all received events over a 10-minute tumbling window.

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Count

app = Application(...)
sdf = app.dataframe(...)


sdf = (
    
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    # Count events in the window 
    .agg(value=Count())

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

### Min, Max, Mean and Sum

[`Min()`](api-reference/quixstreams.md#min), [`Max()`](api-reference/quixstreams.md#max), [`Mean()`](api-reference/quixstreams.md#mean), and [`Sum()`](api-reference/quixstreams.md#sum) aggregators provide short API to calculate these aggregates over the streaming windows. They assume the incoming values are numbers.

**These methods allow you to select a column using the `column` optional parameter**

**Example:**

Imagine you receive the temperature data from the sensor, and you need to calculate only a minimum temperature for each 10-minute tumbling window.  

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
    .tumbling_window(timedelta(minutes=10))

    # Calculate the minimum temperature 
    .agg(value=Min(column="temperature"))

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

### Custom Aggregator

Custom aggregator can be implemented by subclassing the `Aggregator` class and implementing 3 methods.

- [`initialize`](api-reference/quixstreams.md#baseaggregatorinitialize): Called when initializing a new window. Starting value of the aggregation.
- [`agg`](api-reference/quixstreams.md#baseaggregatoragg): Called for every item added to the window. It should merge the new value with the aggregated state.
- [`result`](api-reference/quixstreams.md#baseaggregatorresult): Called to generate the result from the aggregated value

**Example**

Calculate the sum of the power of incoming data over a 10-minute tumbing window

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows.aggregations import Aggregator

app = Application(...)
sdf = app.dataframe(...)

class PowerSum(Aggregator):
    def initialize(self):
        return 0

    def agg(self, aggregated, new):
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

By default the aggregation state key is build using the aggregation class name. If your aggregations takes parameters, like a column name, you can override the [`state_suffix`](api-reference/quixstreams.md#baseaggregatorstate_suffix) property to include those parameters in the state key. Whenever the state key change the aggregation state is reset.

An implementation of `PowerSum` that calculate the sum of a column.

```python
class PowerSum(Aggregator):
    def __init__(self, column: str):
        self._column = column

    @property
    def state_suffix(self) -> str:
        return f"{self.__class__.__name__}/{self.column}"

    def initialize(self):
        return 0

    def agg(self, aggregated, new):
        value = new[self._column]
        return aggregated + (value * value)

    def result(self, aggregated):
        return aggregated
```

### Reduce

!!! warning
    `Reduce` is deprecated. Use [multiple aggregations](aggregations.md#multiple-aggregations) and [custom aggregators](aggregations.md#custom-aggregator) instead. They provide more control over parameters and better state management. 

[`Reduce()`](api-reference/quixstreams.md#reduce) allows you to perform complex aggregations using custom "reducer" and "initializer" functions:

- The **"initializer"** function receives the **first** value for the given window, and it must return an initial state for this window.  
This state will be later passed to the "reducer" function.  
**It is called only once for each window.**

- The **"reducer"** function receives an aggregated state and a current value, and it must combine them and return a new aggregated state.  
This function should contain the actual aggregation logic.  
It will be called for each message coming into the window, except the first one.

## Collectors

Collectors are used to gather all input into a list and perform an operation on the complete list of values when the window is closed.
Collectors are optimized for collecting values and performs significantly better than using [`Reduce`](aggregations.md#reduce) to build a list.

!!! note
    Performance benefit comes at a price, collections only supports [`.final()`](windowing.md#emitting-after-the-window-is-closed) mode. Using [`.current()`](windowing.md#emitting-updates-for-each-message) is not supported.

### Collect

Use [`Collect()`](api-reference/quixstreams.md#collect) to gather all events within each window period. into a list. Collect takes an optional `column` parameter to limit the collection to one column of the input.

**Collect allow you to select a column using the column optional parameter**

**Example:**

Collect all events over a 10-minute tumbling window into a list.

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

### Custom Collector

Custom collector can be implemented by subclassing the [`Collector`](api-reference/quixstreams.md#collector) class and implementing the [`result`](api-reference/quixstreams.md#basecollectorresult) method. It is called, whenever the window is closed, with an iterable of all the collected items to generate the collector result. 

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

By default a collector will collect the full message. Sometime that's unecessary if you are only interested in a specific column. A custom collector can override the [`column`](api-reference/quixstreams.md#basecollectorcolumn) property to tell quixstreams which column it needs.

An implementation of `ReversedCollect` taking an optinal `column` parameter

```python
class ReversedCollect(Collector):
    def result(self, items):
        # items is the list of all collected item during the window
        return list(reversed(items))
```

## Multiple aggregations

Quixstreams windows support multiple aggregations and collections on the same window. Collectors are optimized to store the values only once when shared with other collectors. 

You can define a wide range of aggregations, such as:

- Aggregating over multiple message fields at once
- Using multiple message fields to create a single aggregate
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
from quixstreams.dataframe.windows import Min, Max, Count, Mean

app = Application(...)
sdf = app.dataframe(...)

sdf = (
    
    # Define a tumbling window of 10 minutes
    sdf.tumbling_window(timedelta(minutes=10))

    .agg(
        min_temp=Min("temperature"),
        max_temp=Max("temperature"),
        avg_temp=Mean("temperature"),
        total_events=Count(),
    )

    # Emit results only for closed windows
    .final()
)

# Output:
# {
#   'start': <window start>, 
#   'end': <window end>, 
#   'min_temp': 1
#   'max_temp': 999
#   'avg_temp': 34.32
#   'total_events': 999
# }
```

Here is how you can do that with `Reduce()`:

```python
from datetime import timedelta
from quixstreams import Application
from quixstreams.dataframe.windows import Reduce

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
    .agg(value=Reduce(reducer=reducer, initializer=initializer))

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
#       'total_events': 999,
#       'avg_temp': 34.32,
#       '_sum_temp': 9999
#   },
# }
```