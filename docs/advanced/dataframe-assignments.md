# StreamingDataFrame Assignment Rules

This section aggregates some of the more nuanced rules behind `StreamingDataFrame`
operation assignments that are mentioned in various places.


## Recommendation: Always Assign Operations!

_**You can safely assign any `StreamingDataFrame` operation to a variable, regardless
of it being [in-place](#valid-in-place-operations) or not.**_

```python
# NOTE: this is an incomplete stub just to show usage
from quixstreams import Application

sdf = Application().dataframe()
sdf = sdf.apply()
sdf = sdf.update()  # in-place with assigning!
sdf = sdf.apply().to_topic()
```

So whenever in doubt, simply assign operations to a variable, even if it won't 
be referenced afterward.

Once you grow more comfortable with how the various `StreamingDataFrame`operations work, 
feel free to skip assignments where applicable.

The only time you should _not_ do assignments is [very special edge cases with 
intermediate operation referencing](#avoid-intermediate-operation-referencing).

## Avoid: Intermediate Operation Referencing

Intermediate operation referencing corresponds to a specific set of `StreamingDataFrame` 
operations that are assigned as a variable in one assignment step with the intention to
being used in future ones (often more than once), especially with branching. 

It will NOT work as expected, so avoid it!

It applies only to a few specific use cases:

1. Using a previous `SDF.apply()` as a filter in a later step:
    - **Recommended**:
        ```python
        # `.apply()` in the same assignment step as its filtering use!
        sdf = sdf[sdf.apply(f)]
        ```
    - **Avoid**:
        ```python
        my_filter = sdf.apply(f)
        sdf = sdf[my_filter]
        ```

2. Using column-based manipulations later (`SDF["col"]` references):
    - **Recommended**:
        ```python
        # column manipulations and assignment on same assignment step!
        sdf["z"] = sdf["x"] + sdf["y"]
        ```
    - **Avoid**:
        ```python
        my_sum = sdf["x"] + sdf["y"]
        sdf["z"] = my_sum
        ```

**Intermediate operation references will not raise exceptions**, so be careful!

## Assignment vs "in-place" Patterns

For `StreamingDataFrames`, the general expected pattern is to assign it to a variable 
and reassign it as operations are added:

```python
# NOTE: this is an incomplete stub just to show usage
from quixstreams import Application

sdf = Application().dataframe()
sdf = sdf.apply()  # reassign with new operation
sdf = sdf.apply().apply()  # reassign with chaining
```

This is in contrast to an "in-place" (or "discard", "side effect") pattern, where the 
operation is not assigned:

```python
# NOTE: this is an incomplete stub just to show usage
from quixstreams import Application

sdf = Application().dataframe()
sdf.print()  # no assignment
sdf.update().print()  # no assignment with chaining
```

### Valid In-Place Operations

There are a few operations that are the exception: they are added to the 
"current branch" as expected _without assignment_:

- `.to_topic()`
- `.print()`
- `.update()`
- `.drop()`

that said, **they can still be safely assigned** and function as expected, because they
return the `StreamingDataFrame`, which is why the recommended pattern is to 
just assign everything.

#### Sink

The `.sink()` operation is a special case: it is an in-place operation with no return, 
so **DO NOT assign it** if you wish to branch from its `StreamingDataFrame afterward.
See [sinks](../../quix-connectors/quix-streams/sinks/index.html#sinks-are-terminal-operations) for details.

## The Impact of Branching

With the introduction of branching, assignment is no longer technically required 
for the "assignment" operation to be added: it instead generates a 
[terminal branch](../branching.md#terminal-branches-no-assignment).

In general, it is still recommended to follow both the 
[recommended assignment patterns](#assignment-vs-in-place-patterns) and
[recommended branching patterns](../branching.md#branching-fundamentals).

## Before Quix Streams v3.0.0 (branching)

> For those who never used anything <v3.0, ignore this!

Prior to Quix Streams 3.0, "reassignment" was a required pattern for practically all
but a few operations; if something like `.apply()` was called without assigning it, 
the function would be skipped entirely, like it never existed 
(because it was never actually added!).
