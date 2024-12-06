# Tutorial: Word Counts

We will build a simple word counter, which is a great introduction to Quix Streams and Kafka!


## What You Will Learn

This example will show how to use a Quix Streams `Application` to:

- Ingest a non-Kafka data source
- Do simple event alterations
- Generate multiple events from a single event
- Filter any undesired events
- Create a Kafka topic 
- Produce results, with new Kafka keys, to a topic



## Outline of the Problem

Imagine we are a company with various products.

We want to process text reviews for our various products in real time 
and see what words are the most popular across all of them.

We need an application that will split reviews into individual words, and then send
the counts of each individually downstream for further processing.



## Our Example

We will use a [Quix Streams `Source`](../../connectors/sources/README.md) to generate text to be processed by our 
new Word Counter `Application`.


!!! NOTE

    Our example uses `JSON` formatting for Kafka message values.



## Event Expansion
 
The most important concept we want to explore here is how you can "expand" a single
event/message into multiple new ones.

More than that: each new event generated via expansion is processed individually
through the remainder of your pipeline, allowing you to write ALL your operations 
like they are handling a single event...because they are!

NOTE: Expanding often includes adjusting outgoing Kafka keys as well, so we additionally
showcase that.



## Before Getting Started

1. You will see links scattered throughout this tutorial.
    - Tutorial code links are marked **>>> LIKE THIS <<<** .
    - ***All other links provided are completely optional***. 
    - They are great ways to learn more about various concepts if you need it!

2. This tutorial uses a [`Source`](../../connectors/sources/README.md) rather than a Kafka [`Topic`]() to ingest data.
    - `Source` connectors enable reading data from a non-Kafka origin (typically to get it into Kafka). 
    - This approach circumvents users having to run a [producer](../../producer.md) alongside the `Application`.
    - A `Source` is easily replaced with an actual Kafka topic (just pass a `Topic` instead of a `Source`).



## Generating Text Data

Our [**>>> Word Counter Application <<<**](tutorial_app.py) uses a `Source` called  `ReviewGenerator` 
that generates a static set of "reviews", which are simply strings, where the key is the product name.

The incoming Kafka messages look like:

```python
# ...
{kafka_key: 'product_one', kafka_value: "WOW I love product_one. It is the best."}
{kafka_key: 'product_two', kafka_value: "Fire the creator of product_two please."}
# etc...
```


## Word Counter Application

Now let's go over the `setup_and_run_application()` portion of 
our [**>>> Word Counter Application <<<**](tutorial_app.py) in detail!



### Create an Application

Create a [Quix Streams Application](../../configuration.md), which is our constructor for everything! 

We provide it our connection settings, consumer group (ideally unique per Application), 
and where the consumer group should start from on the (internal) Source topic.

!!! TIP

    Once you are more familiar with Kafka, we recommend 
    [learning more about auto_offset_reset](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls).

#### Our Application

```python
import os
from quixstreams import Application

app = Application(
    broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="product_review_word_counter",
    auto_offset_reset="earliest"
)
```


### Specify Topics

`Application.topic()` returns [`Topic`](../../api-reference/topics.md) objects which are used by `StreamingDataFrame`.

Create one for each topic used by your `Application`.

!!! NOTE

    Any missing topics will be automatically created for you upon running an `Application`.


#### Our Topics
We have one output topic, named `product_review_word_counts`:

```python
word_counts_topic = app.topic(name="product_review_word_counts")
```



### The StreamingDataFrame (SDF)

Now for the fun part: building our [StreamingDataFrame](../../processing.md#introduction-to-streamingdataframe), often shorthanded to "SDF".

SDF allows manipulating the message value in a dataframe-like fashion using various operations.

After initializing with either a `Topic` or `Source`, we continue reassigning to the 
same `sdf` variable as we add operations.

!!! NOTE

    A few `StreamingDataFrame` operations are 
    ["in-place"](../../advanced/dataframe-assignments.md#valid-in-place-operations), 
    like `.print()`.

#### Initializing our SDF

```python
sdf = app.dataframe(source=ReviewGenerator())
```

First, we initialize our SDF with our `ReviewGenerator` `Source`, 
which means we will be consuming data from a non-Kafka origin.


!!! TIP

    You can consume from a Kafka topic instead by passing a `Topic` object
    with app.dataframe(topic=<Topic>)

Let's go over the SDF operations in this example in detail.



### Tokenizing Text

```python
from collections import Counter

def tokenize_and_count(text):
    words = Counter(text.lower().replace(".", " ").split()).items()
    return words

sdf = sdf.apply(tokenize_and_count, expand=True)
```

This is where most of the magic happens! 

We alter our text data with [`SDF.apply(F)`](../../processing.md#streamingdataframeapply) (`F` should take your current 
message value as an argument, and return your new message value):
our `F` here is `tokenize_and_count`.

Basically we do some fairly typical string normalization and count the words, resulting in (word, count) pairs.


This effectively turns this:

`>>> "Bob likes bananas and Frank likes apples."`

to this:

`>>> [('bob', 1), ('likes', 2), ('bananas', 1), ('and', 1), ('frank', 1), ('apples', 1)]`


!!! INFO 

    Two VERY important and related points around the `expand=True` argument:

    1. It tells SDF "hey, this .apply() returns _**multiple independent**_ events!"

    2. Our `F` returns a `list` (or a non-dict iterable of some kind), hence the "expand"!




### Filtering Expanded Results

```python
def should_skip(word_count_pair):
    word, count = word_count_pair
    return word not in ['i', 'a', 'we', 'it', 'is', 'and', 'or', 'the']

sdf = sdf.filter(should_skip)
```

Now we filter out some "filler" words using [`SDF.filter(F)`](../../processing.md#streamingdataframefilter), where `F` is our `should_skip` function. 

For `SDF.filter(F)`, if the (_**boolean**_-ed) return value of `F` is: 

- `True` -> continue processing this event

- `False` -> stop ALL further processing of this event (including produces!)

Remember that each word is an independent event now due to our previous expand, so our
`F` expects only a single word count pair.

With this filter applied, our "and" event is removed:

`>>> [('bob', 1), ('likes', 2), ('bananas', 1), ('frank', 1), ('apples', 1)]`




### Producing Events With New Keys

```python
sdf = sdf.to_topic(word_counts_topic, key=lambda word_count_pair: word_count_pair[0])
```

Finally, we produce each event downstream (they will be independent messages) 
via [`SDF.to_topic(T)`](../../processing.md#writing-data-to-kafka-topics), where `T` is our previously defined `Topic` (not the topic name!).

Notice here the optional `key` argument, which allows you to provide a [custom key generator](../../processing.md#changing-message-key-before-producing).

While it's fairly common to maintain the input event's key (SDF's default behavior), 
there are also many reasons why you might adjust it, so we showcase an example of that 
here!

!!! QUESTION

    What would be the benefit of changing the key to the counted word, in this case?

    This key change would enable calculating _total word counts over time_ from this 
    topic without additional data transformations (a more advanced operation). 

    Even though we won't do that in this example, you can imagine doing so in a 
    downstream `Application`!

In the end we would produce 5 messages in total, like so:

```python
# two shown here...
{kafka_key: "bob", kafka_value: ["bob", 1]}
{kafka_key: "likes", kafka_value: ["likes", 2]}
# etc...
```

!!! NOTE

    This is a user-friendly representation of how a message key/value in the Kafka topic 
    `product_review_word_counts` would appear.





### Running the Application

Running a `Source`-based `Application` requires calling `Application.run()` within a
`if __name__ == "__main__"` block.

#### Our Application Run Block 

Our entire `Application` (and all its spawned objects) resides within a 
`setup_and_run_application()` function, executed as required:

```python
if __name__ == "__main__":
    setup_and_run_application()
```



## Try it Yourself!

### 1. Run Kafka
First, have a running Kafka cluster. 

To easily run a broker locally with Docker, just [run this simple one-liner](../README.md#running-kafka-locally).

### 2. Download files
- [tutorial_app.py](tutorial_app.py)

### 3. Install Quix Streams
In your desired python environment, execute: `pip install quixstreams`

### 4. Run the application
In your desired python environment, execute: `python tutorial_app.py`.

### 5. Check out the results!

...but wait, I don't see any `Application` processing output...Is it working???

One thing to keep in mind is that the Quix Streams does not log/print any message processing
operations by default.

To get visual outputs around message processing, you can either:

- use [recommended way of printing/logging with SDF](../../processing.md#debugging)

- use `DEBUG` mode via `Application(loglevel="DEBUG")`


    !!! DANGER

        you should NOT run your applications in `DEBUG` mode in production.





## Related topics - Data Aggregation

If you were interested in learning how to aggregate across events as we hinted
at with our key changes, check out how easy it is with either SDF's [stateful functions](../../processing.md#using-state-store)
or [windowing](../../windowing.md), depending on your use case!
