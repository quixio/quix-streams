# Consuming Multiple Topics with `Applications`

`Applications` now support consuming multiple topics by initializing multiple 
`StreamingDataFrames` (`SDF`). This may also be referred to as a multi-`SDF` `Application`.


# Using Multiple Topics

Initialize an `Application` and all topics as normal with `Application.topic()`.

Then, for each consumer topic `T`, initialize a `SDF` as normal with 
`sdf_T = Application.dataframe(T)`, stored as a unique variable (ex: `sdf_T` here).

The `Application` will track all `SDF`s generated this way and will execute all of 
them when `Application.run()` is called.

Note that you cannot use the same topic in multiple `SDF`s.

## `StreamingDatFrame` Usage

For each `SDF`, add operations as normal. Each topic's messages will be processed by
it's respective `SDF`. There are no additional restrictions for multiple `SDF`s.

However, each `SDF` should be treated like the others do not exist: they cannot 
interact or share any operations with one another in any way, and their states are 
also fully independent.


## Multiple Topics: NOT parallel

Though multiple `SDF`s are involved with multiple topics, they do not run in parallel: 
it simply subscribes the `Application`s underlying consumer to all the `SDF`'s topics.

So, more topics being processed will directly affect the processing time of each given 
topic.


## Simple Example

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