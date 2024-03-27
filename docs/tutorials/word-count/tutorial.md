# Tutorial: Word Counts

We will build a simple word counter, which is a great introduction to quixstreams and Kafka!

You'll learn how to:

- Create a topic
- Do simple event alterations
- Generate multiple events from a single event
- Filter any undesired events
- Produce events, with new Kafka keys, to a topic



## 1. Outline of the Problem

Imagine we are a company with various products. 

We want to process text reviews for our various products in real time 
and see what words are the most popular across all of them.

We need an application that will split reviews into individual words, and then send
the counts of each individually downstream for further processing.

## 2. Our Example

We will use a simple producer to generate text to be processed by our 
new Word Counter application.

NOTE: our example uses JSON formatting for Kafka message values.



## 3. Event Expansion
 
The most important concept we want to explore here is how you can "expand" a single
event into multiple new ones.

More than that: each new event generated via expansion is processed individually
through the remainder of your pipeline, allowing you to write ALL your operations 
like they are handling a single event...because they are!

This can often require adjusting their respective outgoing Kafka keys as well, so we additionally
showcase that.

## 4. Generating Text Data

We have a standalone producer HERE [LINK] that generates a static set of "reviews", 
which are simply strings, where the key is the product name.

The Kafka message looks like:

```python
# ...
{kafka_key: 'product_one', kafka_value: "WOW I love product_one. It is the best."}
{kafka_key: 'product_two', kafka_value: "Fire the creator of product_two please."}
# etc...
```


## 5. Word Counter application

Now let's go over our [**Word Counter Application**](application.py) line-by-line!

### Create Application

```python
app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="product_review_word_counter",
    auto_offset_reset="earliest"
)
```

First, create the [Quix Streams Application](../../configuration.md), which is our constructor for everything! We provide it our connection settings, consumer group (ideally unique per Application), and where the consumer group should start from on our topic. 

Once you are more familiar with Kafka, we definitely recommend [learning more about auto_offset_reset](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls).

### Define Topics

```python
product_reviews_topic = app.topic(name="product_reviews")
word_counts_topic = app.topic(name="product_review_word_counts")
```

Next we define our input/output topics, named `product_reviews` and `product_review_word_counts`, respectively. 

They each return `Topic` objects, used later on.

NOTE: the topics will automatically be created for you in Kafka when you run the application should they not exist.


### The StreamingDataFrame (SDF)

```python
sdf = app.dataframe(topic=product_reviews_topic)
```

Now for the fun part: building our [StreamingDataFrame](../../processing.md#introduction-to-streamingdataframe), often shorthanded to "SDF".  

We initialize it, and then continue re-assigning to the same variable (`sdf`) as we add operations until we are finished with it.

Also note how we pass our input topic object (from the previous step) to it.

### Tokenizing Text

```python
def tokenize_and_count(text):
    words = Counter(text.lower().replace(".", " ").split()).items()
    return words

sdf = sdf.apply(tokenize_and_count, expand=True)
```

This is where most of the magic happens! We alter our text data with [SDF.apply(F)](../../processing.md#streamingdataframeapply) (`F` should take your data as an 
argument, and return something back): our `F` here is `tokenize_and_count`.

Basically we do some fairly typical string normalization and count the words, resulting in (word, count) pairs.


This effectively turns this:

`>>> "Bob likes bananas and Frank likes apples."`

to this:

`>>> [('bob', 1), ('likes', 2), ('bananas', 1), ('and', 1), ('frank', 1), ('apples', 1)]`


NOTE: two VERY important and related points around the `expand=True` argument:
1. it tells SDF "hey, this .apply() returns _**multiple independent**_ events!"
2. Our `F` returns a `list` (or a non-dict iterable of some kind), hence the "expand"!


### Filtering Expanded Results

```python
def should_skip(word_count_pair):
    word, count = word_count_pair
    return word not in ['i', 'a', 'we', 'it', 'is', 'and', 'or', 'the']

sdf = sdf.filter(should_skip)
```

Now we filter out some "filler" words using [SDF.filter(F)](../../processing.md#streamingdataframefilter), where `F` is our `should_skip` function. 

For `SDF.filter(F)`, if the (_**boolean**_-ed) return value of `F` is: 
- `True` -> continue processing this event
- `False` -> stop ALL further processing of this event (including produces!)

Remember that each word is now an independent event now due to our previous expand, so our
`F` expects only a single word count pair.

With this filter applied, our "and" event is removed:

`>>> [('bob', 1), ('likes', 2), ('bananas', 1), ('frank', 1), ('apples', 1)]`


### Producing Events With New Keys

```python
sdf = sdf.to_topic(word_counts_topic, key=lambda word_count_pair: word_count_pair[0])
```

Finally, we produce each event downstream (they will be independent messages) 
via [SDF.to_topic(T)](../../processing.md#writing-data-to-kafka-topics), where `T` is our previously defined `Topic` (not the topic name!).

Notice here the optional `key` argument, which allows you to provide a [custom key generator](../../processing.md#changing-message-key-before-producing).

While it's fairly common to maintain the input event's key (SDF's default behavior), 
there are many reasons why you might adjust it.

In our case, we are changing the message key to the word so
if something is totaling word counts over time, the data is now set up to do so.

So we would produce 5 messages in total, like so:

```python
# two shown here...
{kafka_key: "bob", kafka_value: ["bob", 1]}
{kafka_key: "likes", kafka_value: ["likes", 2]}
# etc...
```

## 6. Try it yourself!

### Run Kafka
First, have a running Kafka cluster. 

To conveniently follow along with this tutorial, just [run this simple one-liner](../tutorials-overview.md#running-kafka-locally).

### Install Quix Streams
In your python environment, run `pip install quixstreams`

### Run the Producer and Application
Just call `python producer.py` and `python application.py` in separate windows.

### Check out the results!

Look at all those counted works, beautiful!

### Related topics - Data Aggregation

If you were interested in learning how to aggregate across events as we hinted
at with our key changes, check out how easy it is with either SDF's [stateful functions](../../processing.md#using-state-store)
or [windowing](../../windowing.md), depending on your use case!
