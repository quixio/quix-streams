# Example: Word Counts

We will build a simple word counter, which is a great introduction to quixstreams and Kafka!

You'll learn how to:

- Create a topic
- Do simple event alterations
- Generate multiple events from a single event
- Filter any of these events you don't need
- Produce remaining events to a topic using a new key for each



## 1. Outline of the Problem

Imagine we are a company with various products. 

We want to process text reviews for our various products in real time 
and see what words are the most popular across all of them.

We need an application that will split reviews into individual words, and then send
the counts of each individually downstream to be further processed as needed.

## 2. Our Example

We will use a very simple producer to generate text to be processed by our 
new Word Counter application.

NOTE: our example uses JSON formatting for Kafka message values.



## 3. Event Expansion
 
The most important concept we want to explore here is how you can "expand" a single
event into multiple new ones.

More than that: each new event generated via expansion is processed individually
through the remainder of your pipeline, allowing you to write ALL your operations 
like they are handling a single event...because they are!

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


Here is what our application will look like in the end:

```python
import os
from collections import Counter

from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="product_review_word_counter",
    auto_offset_reset="earliest",
)
product_reviews_topic = app.topic(name="product_reviews")
word_counts_topic = app.topic(name="product_review_word_counts")


def tokenize_and_count(text):
    return list(Counter(text.lower().replace(".", " ").split()).items())


def should_skip(word_count_pair):
    word, count = word_count_pair
    return word in ['i', 'a', 'we', 'it', 'is', 'and', 'or', 'the']


sdf = app.dataframe(topic=product_reviews_topic)
sdf = sdf.apply(tokenize_and_count, expand=True)
sdf = sdf.filter(should_skip)
sdf = sdf.to_topic(word_counts_topic, key=lambda word_count_pair: word_count_pair[0])


if __name__ == '__main__':
    app.run(sdf)


```

Let's go over it in detail:

### Create Application

```python
app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="product_review_word_counter",
    auto_offset_reset="earliest"
)
```

First, create the quixstreams Application, which is our constructor for everything! We provide it our connection settings, consumer group (ideally unique per Application), and where the consumer group should start from on our topic. 

You can learn more about auto_offset_reset HERE (link to article?), but TL;DR - "earliest" is generally recommended while learning.


### Define Topics

```python
product_reviews_topic = app.topic(name="product_reviews")
word_counts_topic = app.topic(name="product_review_word_counts")
```

Next we define our input/output topics, named product_reviews and product_review_word_counts, respectively. 

These topics will automatically be created for you when you run the application should they not exist.


### The StreamingDataFrame (SDF)

```python
sdf = app.dataframe(topic=product_reviews_topic)
```

Now for the fun part: building our StreamingDataFrame, often shorthanded to "SDF".  

We initialize it, and then continue re-assigning to the same variable ("sdf") as we add operations until we are finished with it.


### Tokenizing Text - SDF.apply(F, expand=True)

```python
def tokenize_and_count(text):
    words = Counter(text.lower().replace(".", " ").split()).items()
    return words

sdf = sdf.apply(tokenize_and_count, expand=True)
```

This is where most of the magic happens! Using SDF.apply(F), we can apply any custom
function F to alter our incoming text (F should take the data as an argument, and return data).

So, with "tokenize_and_count", we do some fairly typical string normalization and finish with counting, resulting in (word, count) pairs.


This effectively turns this:

`>>> "Bob likes bananas and Frank likes apples."`

to this:

`>>> [('bob', 1), ('likes', 2), ('bananas', 1), ('and', 1), ('frank', 1), ('apples', 1)]`


Two important and related points here:
1. Note the `expand=True` argument for SDF.apply(F), which tells SDF "hey, this .apply() returns multiple independent events!"
2. Our F returns a list (must be a non-dict iterable of some kind), hence the "expand".


### Filtering Expanded Results

```python
def should_skip(word_count_pair):
    word, count = word_count_pair
    return word not in ['i', 'a', 'we', 'it', 'is', 'and', 'or', 'the']

sdf = sdf.filter(should_skip)
```

Now we filter out some "filler" words using SDF.filter(F), where F is our "should_skip" function. 

If F returns any "True"-like value, the pipeline continues processing that event...otherwise, the event stops here. 

Remember that each word is treated like an independent event now due to the previous expand, so our
function expects only one word count pair at a time.

With this filter applied, our "and" event is removed:

`>>> [('bob', 1), ('likes', 2), ('bananas', 1), ('frank', 1), ('apples', 1)]`


### Producing Events With New Keys

```python
sdf = sdf.to_topic(word_counts_topic, key=lambda word_count_pair: word_count_pair[0])
```

Finally, we produce each event downstream (they will be independent messages).

Notice here the optional `key` argument, which allows you to provide a custom key generator.

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

### Run kafka
First, go ahead and get a kafka cluster running. To easily follow along with this example, just follow THESE (link) instructions.

### Install quixstreams
Then, install the quixstreams library in your python environment

### Run the Producer and Application
Just call `python producer.py` and `python application.py` in separate windows.

### Check out the results!

Look at all those counted works, beautiful!

### Related topics - Data Aggregation

If you were interested in learning how to aggregate across events as we hinted
at with our key changes, check out how easy it is with SDF's stateful functions here! [LINK]
