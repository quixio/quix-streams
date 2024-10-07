# Tutorial: Purchase Filtering

We will build a simple Purchase Filtering app to showcase some common Quix Streams 
dataframe-like operations with dictionary/JSON data (a format frequently used).

You'll learn how to:

- Create a topic
- Assign a value to a new column
- Use `SDF.apply()` with additional operations
- Filter with inequalities combined with and/or (`&`, `|`)
- Get a subset/selection of columns
- Produce resulting output to a topic


## Outline of the Problem

Imagine we are a company who sells goods to members only: they can have a 
"Bronze", "Silver", or "Gold" membership.

We always send a coupon to our Silver and Gold members who spend at least $100 in one
visit.

We need an application that will filter for the applicable customers and send only the
necessary information downstream.



## Our Example

We will use a simple producer to generate some mock purchase data to be processed by our 
new Purchase Filtering application.



## Important Takeaways
 
The primary lesson: learning how you can use common pandas-like 
operations on dictionary/JSON data to perform various transformations
as if it were a dataframe.



## Before Getting Started

- You will see links scattered throughout this tutorial.
    - Tutorial code links are marked **>>> LIKE THIS <<<** .
    - ***All other links provided are completely optional***. 
    - They are great ways to learn more about various concepts if you need it!


- We use the word "column" for consistency with Pandas terminology.
  - You can also think of it as a dictionary key.



## Generating Purchase Data

We have a simple [**>>> Purchases Producer <<<**](producer.py) that generates a small static set of 
"purchases", which are simply dictionaries with various info about what was purchased by 
a customer during their visit. The data is keyed on customer ID.

An outgoing Kafka message looks something like:

```python
# ...
kafka_key: "CUSTOMER_ID_123"
kafka_value: {
      "First Name": "Jane",
      "Last Name": "Doe",
      "Email": "jdoe@mail.com",
      "Membership Type": "Gold",
      "Purchases": [
          {
              "Item ID": "abc123",
              "Price": 13.99,
              "Quantity": 12
          },
          {
              "Item ID": "def456",
              "Price": 12.59,
              "Quantity": 2
          },
      ]
  }
```


## Purchase Filtering Application


Now let's go over our [**>>> Purchase Filtering Application <<<**](application.py) line-by-line!


### Create Application

```python
import os
from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="purchase_summing",
    auto_offset_reset="earliest"
)
```

First, create the [Quix Streams Application](../../configuration.md), which is our constructor for everything! We provide it our connection settings, consumer group (ideally unique per Application), and where the consumer group should start from on our topic. 

NOTE: Once you are more familiar with Kafka, we recommend [learning more about auto_offset_reset](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls).


### Define Topics

```python
customer_purchases_topic = app.topic(name="customer_purchases")
customers_qualified_topic = app.topic(name="customers_coupon_qualified")
```

Next we define our input/output topics, named `customer_purchases` and `customers_coupon_qualified`, respectively. 

They each return [`Topic`](../../api-reference/topics.md) objects, used later on.

NOTE: the topics will automatically be created for you in Kafka when you run the application should they not exist.

### The StreamingDataFrame (SDF)

```python
sdf = app.dataframe(topic=customer_purchases_topic)
```

Now for the fun part: building our [StreamingDataFrame](../../processing.md#introduction-to-streamingdataframe), often shorthanded to "SDF".  

SDF allows manipulating the message value in a dataframe-like fashion using various operations.

After initializing, we continue re-assigning to the same `sdf` variable as we add operations.

(Also: notice that we pass our input `Topic` from the previous step to it.)

### Filtering Purchases

```python
def get_purchase_totals(items):
    return sum([i["Price"]*i["Quantity"] for i in items])

sdf = sdf[
    (sdf["Purchases"].apply(get_purchase_totals) * SALES_TAX >= 100.00)
    & (sdf["Membership Type"].isin(["Silver", "Gold"]))
]
```

We get started with a bang! 

Let's break it down step-by-step, as most of our
work is done here:

<br>

```python
# step A
sdf["Purchases"].apply(get_purchase_totals) * SALES_TAX >= 100.00
```

Here, we do an [`SDF.apply(F)`](../../processing.md#streamingdataframeapply) operation on a column (`F` should take your current 
message value as an argument, and return your new message value): 
our `F` here is `get_purchase_totals`.

Notice how you can still do basic operations with an `SDF.apply()` result, like multiplying it 
by our sales tax, and then finally doing an inequality check on the total (all of which
are SDF operations...more on that in a second).

<br>

```python
# step B
sdf["Membership Type"].isin(["Silver", "Gold"])
```

We additionally showcase one of our built-in column operations `.isin()`, a way for SDF to perform an 
`if x in y` check (SDF is declaratively defined, invalidating that approach).

**NOTE**: some operations (like `.isin()`) are only available when manipulating a column.

  - if you're unsure what's possible, autocomplete often covers you!

  - _ADVANCED_: [complete list of column operations](../../api-reference/dataframe.md#streamingseries).

<br>

```python
# "and" Steps A, B
(A) & (B)
```
Now we "and" these steps, which translates to your typical `A and B` 
(and returns a boolean).

A few notes around `&` (and): 

- It is considered an SDF operation.

- You MUST use `&` for and, `|` for or

- Your respective objects (i.e. `A`, `B`) must be wrapped in parentheses.

Ultimately, when executed, the result of `&` will be _boolean_. This is important for...

<br>

```python
# filter with "&" result
sdf = sdf[X]
```
Ultimately, this is a filtering operation: whenever `X` is an SDF operation(s) result, it acts like Pandas row filtering.

As such, SDF filtering interprets the SDF operation `&` _boolean_ result as follows:

- `True` -> continue processing this event

- `False` -> stop ALL further processing of this event (including produces!)

So, any events that don't satisfy these conditions will be filtered as desired!


### Adding a New Column

```python
def get_full_name(customer):
    return f'{customer["First Name"]} {customer["Last Name"]}'


sdf["Full Name"] = sdf.apply(get_full_name)
```

With filtering done, we now add a new column to the data that we need downstream.

This is basically a functional equivalent of adding a key to a dictionary.

```python
>>> {"Remove Me": "value", "Email": "cool email"}`
```

becomes

```python
>>> {"Remove Me": "value", "Email": "cool email", "Full Name": "cool name"}`
```

### Getting a Column Subset/Selection

```python
sdf = sdf[["Email", "Full Name"]]
```
We only need a couple fields to send downstream, so this is a convenient way to select
only a specific list of columns (AKA dictionary keys) from our data.

So 

```python
>>> {"Remove Me": "value", "Email": "cool email", "Full Name": "cool name", }`
```

becomes

```python
>>> {"Email": "cool email", "Full Name": "cool name"}`
```

NOTE: you cannot reference nested keys in this way.

### Producing the Result

```python
sdf = sdf.to_topic(customers_qualified_topic)
```

Finally, we produce our non-filtered results downstream via [`SDF.to_topic(T)`](../../processing.md#writing-data-to-kafka-topics), where `T`
is our previously defined `Topic` (not the topic name!).

NOTE: by default, our outgoing Kafka key is persisted from the input message. 
[You can alter it](../../processing.md#changing-message-key-before-producing), if needed.


## Try it yourself!

### 1. Run Kafka
First, have a running Kafka cluster. 

To conveniently follow along with this tutorial, just [run this simple one-liner](../tutorials-overview.md#running-kafka-locally).

### 2. Install Quix Streams
In your python environment, run `pip install quixstreams`

### 3. Run the Producer and Application
Just call `python producer.py` and `python application.py` in separate windows.

### 4. Check out the results!

...but wait, I don't see any message processing output...Is it working???

One thing to keep in mind is that the Quix Streams does not log/print any message processing
operations by default.

To get visual outputs around message processing, you can either:
- use [recommended way of printing/logging stuff](../../processing.md#debugging)
 
- use `DEBUG` mode via `Application(loglevel="DEBUG")`
  - WARNING: you should NOT run your applications in `DEBUG` mode in production.
