# StreamingDataFrame Assignment Rules

This section aggregates some of the more nuanced rules behind `StreamingDataFrame`
operation assignments that are mentioned in various places.

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

This is in contrast to an "inplace" (or "discard", "side effect") pattern, where the 
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
See [sinks](../connectors/sinks/README.md#sinks-are-terminal-operations) for details.

## The Impact of Branching

With the introduction of branching, assignment is no longer technically required 
for the "assignment" operation to be added: it instead generates a 
[terminal branch](branching.md#terminal-branches-no-assignment).

In general, it is still recommended to follow both the 
[recommended assignment patterns](#assignment-vs-in-place-patterns) and
[recommended branching patterns](branching.md#branching-fundamentals).

## Before Quix Streams v3.0.0 (branching)

> For those who never used anything <v3.0, ignore this!

Prior to Quix Streams 3.0, "reassignment" was a required pattern for practically all
but a few operations; if something like `.apply()` was called without assigning it, 
the function would be skipped entirely, like it never existed 
(because it was never actually added!).
