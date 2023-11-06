# Stateful Applications

Currently, Streaming DataFrames utilizes a basic state-store with RocksDB.

This allows you to do things like compare a record to a previous version of it, or
do some aggregate calculations. Here, we will outline how stateful processing works.

<br>

## How State Relates to Kafka Keys

The most important concept to understand with state is that it depends on the message 
key due to how kafka topic partitioning works.

What does this mean for you?

**Every Kafka key's state is independent and _inaccessible_ from all others; it is
accessible only while it is the currently active message key**. 

Be sure to take this into consideration when making decisions around what your 
Kafka message keys should be.

The good news? _The library manages this aspect for you_, so you don't need to 
handle that complexity yourself! You do need to understand the limitations, however.

### Example: 

I have two messages with two new message keys, `KEY_A` and `KEY_B`. 

I consume and process `KEY_A`, storing a value for it, `{"important_value": 5}`. Done!

Next, I read the message with `KEY_B`. 

While processing `KEY_B`, I would love to know what happened with `KEY_A` to decide 
what to do, but I cannot access `KEY_A`'s state, because `KEY_A` (and thus its 
state store) effectively _does not exist_ according to `KEY_B`: only `KEY_A` 
can access `KEY_A`'s state!

<br>

## Using State

To have an `Application` be stateful, simply use stateful function calls with 
`StreamingDataFrame`. 

Currently, this relates only to `StreamingDataFrame.apply()`, so
see that section for details.


<br>

## Changing the State FilePath

Optionally, you can specify a filepath where the `Application` will store your files 
(defaults to `"./state"`) via `Application(state_dir="folder/path/here")`


<br>

## State Guarantees

Because we currently handle messages with "At Least Once" guarantees, it is possible
for the state to become slightly out of sync with a topic in-between shutdowns and
rebalances. 

While the impact of this is generally minimal and only for a small amount of messages,
be aware this could cause side effects where the same message may be re-processed 
differently if it depended on certain state conditionals.

Exactly Once Semantics avoids this, and it is currently on our roadmap.


<br>

## Recovery

Currently, if the state store becomes corrupted, the state must start from scratch.

An appropriate recovery process is currently under development.
