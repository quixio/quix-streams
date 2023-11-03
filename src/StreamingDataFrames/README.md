# Quixstreams v2.0.0a: Streaming DataFrames

***WARNING: This library is in alpha and will experience rapid and significant 
interface changes, feature additions, and bugfixes. Use with discretion!***


## A Note to Current Quixstreams Python Users

Quix is sunsetting the previous `quixstreams` python client and replacing it with a new 
`streamingdataframes` interface. 

It is now completely independent of the C# library and will be the first to receive new 
features and functionality.


## Compatibility with `quixstreams<2.0.0a`

`streamingdataframes` is currently fully backwards compatible with the
previous versions of the library, though some functionality is not 100% replicated.
Barring very specific circumstances, you should be able to upgrade to the new version
relatively easily.

To see how to use this new library with your existing ecoystem, check out the section
**Migrating from Legacy Quixstreams (<2.0.0a)**


<br>

## Old client code

Should you need the old version of the client still, the code has been moved to a 
separate branch [here](TODO:LINKHERE).



# Installation

`pip install quixstreams>=2.0.0a`

<br>


# Overview

## What is Streaming DataFrames (SDF)?
Streaming DataFrames is a Pandas-like Kafka client for Python. It uses a similar 
interface as Pandas where possible for data transformation steps, along with adding 
kafka-specific operations or features.

It additionally provides simple state storage (based on message keys).

In the future, it will offer more complex stateful aggregations like windowed 
calculations and exactly once semantics.


## How does it work?
Unlike Pandas, your `dataframe` is a pre-defined pipeline that is executed at runtime, 
so you do not interact with it in real time. You define a handler that manages other 
kafka-related setup and runs the `dataframe`.

Your `dataframe` will be receiving kafka messages one at a time, so imagine each message 
is equivalent to a single-rowed `dataframe`.

With the `dataframe` itself, you can do typical Pandas column-wise ETL manipulations, or
more direct work with `.apply()`, manipulating the row like it were a dictionary. 

You can also "filter" a row by doing Pandas-like conditional checks, which ignores all 
additional processing steps for that row if successful (including producing).

See below for a more detailed breakdown of functionality.







<br><br>

# The Basics of Streaming DataFrames

Here's the minimum you need to know to get started with Streaming DataFrames.


## The `Application` class

This is the handler and entrypoint of Quixstreams application, and thus a good place 
to start!

You can use the `Application` class to help define all of kafka-related configuration,
and it can generate all necessary objects you will need to get up and running.

At minimum, you'll need to know your Kafka broker address and the consumer group ID you
wish to use.

You can get started via:
```python
from streamingdataframes import Application

app = Application(
   broker_address="my_broker_url",
   consumer_group="my_consumer_group_name",
)
```

<br>

### Using the Quix Platform? - `Application.Quix()`

If you are using the Quix platform, you will still use the `Application` class, but
instead call it via `Application.Quix()`.

If you are running this within the Quix platform it will be configured 
automatically. Otherwise, see the **Quix Platform Configuration** section.

<br>

## Defining Kafka Topics - `Application.topic()`

Any topic you plan to interact with will require a respective `Topic` object. 

To generate a topic, simply call `Application.topic(name)`, like so:

```
input_topic = app.topic("my_input_topic")
output_topic = app.topic("my_output_topic")
```

Hold on to those topic objects, you'll need them later for your `dataframe`!


## Creating your DataFrame - `Application.dataframe()`

You'll need a `StreamingDataFrame` object to work with; the `Application` class can 
generate that for you via:

```
sdf = app.dataframe(topics_in=[input_topic])
```

From here, you can add processing steps to your `StreamingDataFrame`, like so:

```
sdf = sdf[["field_a", "field_b"]]
sdf = sdf[sdf["field_a" > 10]]
sdf = sdf.to_topic(output_topic)
# etc...
```

More details on the features and usage of `StreamingDataFrame` will be in the 
next section.


## Running your Application + DataFrame - `Application.run()`

Finally, we can run this as a Kafka client by handing the sdf to the `.run` call:

```
app.run(sdf)
```

That's it! Once running it will, as an endless loop,
- consume a message
- process it with your `StreamingDataFrame`
- commit the message


## Ready to Jump In? Try out the Examples!

There are some examples available in the `./examples` folder of the library to get
you started!

Otherwise, proceed onward to learn more about the libraries features in detail.



<br><br><br>

# `StreamingDataFrame` - Detailed Overview

Now that we've outlined how to get an `Application` up and running, what exactly can you
do with a `StreamingDataFrame`?

<br>

For example purposes, assume the record we are manipulating looks like this:

```python
row = {
    "field_a": "my_str", 
    "field_b": "my_other_str",
    "field_c": [1, 2, 3],
    "field_d": "DELETE ME",
}
```

Along with a `StreamingDataFrame` instance, as generated by an `Application`:

```python
sdf = Application().dataframe()
```


## Interacting with `Rows`

Under the hood, `StreamingDataFrame` is manipulating kafka messages via `Row` objects.

Simplified, a `Row` is effectively a dictionary of the Kafka message 
value, with each key equivalent to a dataframe column name. 

Our `StreamingDataFrame` interacts with `Row` objects via the Pandas dataframe
interface, unless specified otherwise (i.e. the `.apply()` feature).

As a user, their existence should largely go unnoticed.


## Accessing Fields/Columns

In typical Pandas dataframe fashion, you can access a column:

```python
sdf["field_a"]  # "my_str"
```

Typically, this is done in combination with other operations.

You can also access nested objects (dicts, lists, etc):

```python
sdf["field_c"][2]  # 3
```


## Performing Operations with Columns

In typical Pandas dataframe fashion, you can do almost any basic operations or 
comparisons with columns, assuming validity:

```python
sdf["field_a"] + sdf["field_b"]
sdf["field_a"] or sdf["field_b"]
sdf["field_a"] & sdf["field_b"]
sdf["field_a"] is not None
sdf["field_a"] != "woo"
```


## Assigning New Columns

In typical Pandas fashion, you can add new columns from the results of numerous other
operations:

```python
sdf["a_new_int_field"] = 5 
sdf["a_new_str_field"] = sdf["field_a"] + sdf["field_b"]
sdf["another_new_field"] = sdf["a_new_str_field"].apply(lambda value: value + "another")
```

See the `.apply()` section for more information on how that works.


## Subsetting/Slicing Columns

In typical Pandas fashion, you can take a subset of columns:

```python
# remove "field_d"
sdf = sdf[["field_a", "field_b", "field_c"]]
```


## Filtering rows

"Filtering" is a very specific concept and operation with `StreamingDataFrames`.

In practice, it functions similarly to how you might filter rows with Pandas DataFrames
with conditionals.

Basically, when a "column" reference is actually another operation, it will be treated 
as a "filter". If that result is empty or None, the row is now "filtered".

When filtered, ALL additional SDF-defined processes for that row are now skipped,
_including kafka-related operations like producing_.

```python
# This would continue onward
sdf = sdf[sdf["field_a"] == "my_str"]

# This would filter the row, skipping further functions
sdf = sdf[(sdf["field_a"] != "woo") and (sdf["field_c"][0] > 100)]
```

<br>

## User Defined Functions - `.apply()`

Should you need more advanced transformations, `.apply()` allows you
to use any python function to operate on your row.

When used on a `StreamingDataFrame`, your function must accept 2 ordered arguments, 
first is the row data (as a dictionary), and the other is a special "context" object
that allows you to access other message metadata (key, partition, etc).

Consequently, your function **MUST either** _alter this dict in-place_ 
**OR** _return a dictionary_ to directly replace the current data with.

For example:

```python
# in place example
def in_place(row, ctx):
    for k in list(row.keys()):
        if isinstance(row[k], str):
            del row[k]     
            
sdf = sdf.apply(in_place)


# replacement example
def new_data(row, ctx):
    return {col: val for col, val in row.items() if isinstance(val, str)}

sdf = sdf.apply(new_data)
```

<br>

The `.apply()` function is also valid for columns, but rather than providing a 
dictionary, it instead uses the column value, and the function must return a value.

```python
sdf["new_field"] = sdf["field_a"].apply(lambda value: value + "-add_me")
```

NOTE: Every `.apply()` is a _temporary_ state change, but the result can be assigned. 
So, in the above example, `field_a` remains `my_str`, but `new_field == my_str-add_me` 
as desired.

<br>

### Stateful Processing with `.apply()`

If you are using stateful processing, you can access the state for a given row via
a keyword argument `stateful=True`, and your function should accept a third object as
an argument (you can just call it something like `state`).

When your function has access to state, it will receive a `State` object, which can do:
- `.get(key)`
- `.set(key, value)`
- `.delete(key)`
- `.exists(key)`

`Key` and value can be anything, and you can have any number of keys.

NOTE: `key` is unrelated to the Kafka message key, which is handled behind the scenes.

```python
def edit_data(row, ctx, state):
    msg_max = len(row["field_c"])
    current_max = state.get("current_len_max")
    if current_max < msg_max:
        state.set("current_len_max", msg_max)
        current_max = msg_max
    row["len_max"] = current_max
    return row


sdf = sdf.apply(edit_data, stateful=True)
```

For more information about stateful processing in general, see the 
**Stateful Processing** section.



## Producing to Topics - `.to_topic()`

To send the current state of the `StreamingDataFrame` to a topic, simply call 
`to_topic` with a `Topic` instance (like one generated from `Application.topic()`) 
as an argument.

To change the outgoing message key (which defaults to the current consumed key), 
you can optionally provide a key function, which operates similarly to the `.apply()` 
function with a `row` (dict) and `ctx` argument, and returns a desired 
(serializable) key.

```python
output_topic = Application().topic("my_output_topic")
other_output_topic = Application().topic("my_other_output_topic")

def key_generator(row, ctx):
    # do stuff
    return "my_new_key"


### previous sdf stuff here
sdf = sdf.to_topic(output_topic, key=key_generator)

### additional sdf stuff here
sdf = sdf.to_topic(other_output_topic)
```






<br>

# Serialization and Deserialization (SERDES)

SERDES simply refers to how you pack (serialize) or unpack (deserialize) your data 
when publishing to or reading from a topic. With our `Application.topic()`, we use the `JSON`
format by default.

There are numerous ways to SERDES your data, and we provide some plain formats for you 
to select from, including `bytes`, `string`, `integer`, etc.

We also plan on including other popular ones like `PROTOBUF` in the near future.

## Using a SERDES

SERDES are used by providing the appropriate SERDES class to a `Topic` object
(or, with `Application.topic()` which forwards all the same arguments to `Topic`)

You can select them like so:

```python
from streamingdataframes.models.serializers import (
    JSONSerializer, JSONDeserializer
)
from streamingdataframes.app import Application

app = Application()
topic_in = app.topic(
    "my_input_topic", value_deserializer=JSONDeserializer(),
)
topic_out = app.topic(
    "my_output_topic", value_serializer=JSONSerializer(),
)
```


## Picking a SERDES

You can find all available serializers in `streamingdataframes.models.serializers`.


## Message Key and Value SERDES

Most people refer to serializing the message _value_ when discussing SERDES. However,
you can also SERDES message _keys_, but you probably won't need to.

Should you need it, they use the same SERDES'es classes:

```python
topic = app.topic("my_topic", key_serializer=JSONSerializer())
```




<br>

# Stateful Applications

Currently, Streaming DataFrames utilizes a basic state-store with RocksDB.

This allows you to do things like compare a record to a previous version of it, or
do some aggregate calculations. Here, we will outline how stateful processing works.


## Single Topic Consumption Only

Due to limitations outlined below, you can only consume from 1 topic for a stateful
application.


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


## Changing the State FilePath

Optionally, you can specify a filepath where the `Application` will store your files 
(defaults to `"./state"`) via `Application(state_dir="folder/path/here")`


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



<br><br>

# Quix Platform Users - `Application.Quix()`

For those using the Quix platform directly (that is, using the client in the web 
browser), all you need for ensuring everything connects as expected is to use the 
`Application.Quix()` instance.

There aren't many features unique to the `Quix` version other than allowing topics to
auto create via `auto_create_topics=True`, which is the default (Quix requires a special
API to create topics).

<br>

## Using `Application.Quix()` externally

If you decide to connect to the Quix Platform from external sources (i.e. running an 
app connected to the platform directly via your local machine), you will need to set 
the following environment variables:

```
Quix__Sdk__Token
Quix__Portal__Api
```

You can find these values on the platform in your given workspace settings.


<br><br>

## Migrating from Legacy Quixstreams (<2.0.0a)





# Configuration

<br>

## Producers and Consumers

<br>

## Other options?