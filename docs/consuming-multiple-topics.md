# Consuming Multiple Topics with `Applications`

`Applications` now support consuming multiple topics by initializing multiple 
`StreamingDataFrames` (`SDF`). This may also be referred to as a multi-`SDF` `Application`.

## Multi-Topic Use Cases

The benefits of consuming from multiple topics in one Application are a little more 
nuanced, but the main benefits are: 

### Consolidating Applications

It may help to consolidate two or more `Applications` that share similar 
operational contexts.

### Code Sharing

It's now much easier to share/use code that applies to multiple topics by having it 
all in one `Application`.

### Joining Topics (coming soon)

[Joins](#joins) will vastly simplify many problems that require handling data from 
multiple topics at once.

## Using Multiple Topics

Initialize an `Application` and all topics as normal with `Application.topic()`.

Then, for each consumer topic `T`, initialize a `SDF` as normal with 
`sdf_T = Application.dataframe(T)`, stored as a unique variable (ex: `sdf_T` here).

The `Application` will track all `SDF`s generated this way and will execute all of 
them when `Application.run()` is called.

Note that you cannot use the same topic across multiple `SDF`s.

### Branching vs Multi-SDFs

[Branching](branching.md) is independent of multi-`SDF`;
branches can be used in each of the `SDF`s from multiple topics, but they cannot
interact with one another in any way.


### `StreamingDataFrame` Usage

For each `SDF`, add operations as normal. Each topic's messages will be processed by
its respective `SDF`.


#### Limitations
There are no additional restrictions for `SDF`'s when used with multiple topics.

_However_, each `SDF` should be treated like the others do not exist: they cannot 
interact or share any operations with one another in any way. 

#### State
Each `SDF`'s state used in a multi-`SDF` implementation is entirely independent
(including all `stateful=True` operations), meaning `SDF`s cannot access or manipulate
the state of another `SDF`.

As a reminder, state is ultimately tied to a given topic (and thus its `SDF`).

[See here to learn more about stateful processing](./advanced/stateful-processing.md).

### Multiple Topics: NOT parallel

Though multiple `SDF`s are involved with multiple topics, they do not run in parallel: 
it simply subscribes the `Application`s underlying consumer to all the `SDF`'s topics.

So, more topics being processed will directly affect the processing time of each given 
topic.


### Simple Example

```python
from quixstreams import Application

app = Application("localhost:9092")
input_topic_a = app.topic("input_a")
input_topic_b = app.topic("input_b")
output_topic = app.topic("output")

sdf_a = app.dataframe(input_topic_a)
sdf_a = sdf_a.apply(func_x).to_topic(output_topic)

sdf_b = app.dataframe(input_topic_b)
sdf_b.update(func_y).to_topic(output_topic)

app.run()
```

## Upcoming Features 

### Joins

Joins are a way of combining two topics together into one
data stream using various options and conditions.

They are on the immediate roadmap.