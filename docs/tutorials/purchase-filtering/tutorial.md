# Tutorial: Purchase Filtering

We will build a simple Purchase Filtering app to showcase some common Quix Streams 
dataframe-like operations with dictionary/JSON data (a format frequently used).

You'll learn how to:

- Ingest a non-Kafka data source
- Assign a value to a new column
- Use `SDF.apply()` with additional operations
- Filter with inequalities combined with and/or (`&`, `|`)
- Get a subset/selection of columns
- Create a Kafka topic 
- Produce results to a Kafka topic



## Outline of the Problem

Imagine we are a company who sells goods to members only: they can have a 
"Bronze", "Silver", or "Gold" membership.

We always send a coupon to our Silver and Gold members who spend at least $100 in one
visit.

We need an application that will filter for the applicable customers and send only the
necessary information downstream.



## Our Example

We will use a [Quix Streams `Source`](../../connectors/sources/README.md) to generate some mock purchase data to be 
processed by our new Purchase Filtering `Application`.


## Important Takeaways
 
The primary lesson: learning how you can use common pandas-like 
operations on dictionary/JSON data to perform various transformations
as if it were a dataframe.



## Before Getting Started

1. You will see links scattered throughout this tutorial.
    - Tutorial code links are marked **>>> LIKE THIS <<<** .
    - ***All other links provided are completely optional***. 
    - They are great ways to learn more about various concepts if you need it!

2. This tutorial uses a Quix Streams [`Source`](../../connectors/sources/README.md) rather than a Kafka [`Topic`]() to ingest data.
    - `Source` connectors enable reading data from a non-Kafka origin (typically to get it into Kafka). 
    - This approach circumvents users having to run a [producer](../../producer.md) alongside the `Application`.
    - A `Source` is easily replaced with an actual Kafka topic (just pass a `Topic` instead of a `Source`).

3. We use the word "column" for consistency with Pandas terminology.
    - You can also think of it as a dictionary key.



## Generating Purchase Data

Our [**>>> Purchase Filtering Application <<<**](tutorial_app.py) uses a `Source` called 
`PurchaseGenerator` that generates a small static set of "purchases", which are simply 
dictionaries with various info about what was purchased by a customer during their visit. 

The data is keyed on customer ID.

The incoming Kafka data looks something like:

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


Now let's go over the `setup_and_run_application()` portion of 
our [**>>> Purchase Filtering Application <<<**](tutorial_app.py) in detail!



### Create an Application

```python
import os
from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="purchase_summing",
    auto_offset_reset="earliest"
)
```

Create a [Quix Streams Application](../../configuration.md), which is our constructor for everything! 

We provide it our connection settings, consumer group (ideally unique per Application), 
and where the consumer group should start from on the (internal) `Source` topic.

!!! TIP

    Once you are more familiar with Kafka, we recommend 
    [learning more about auto_offset_reset](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls).



### Define Output Topics

`Application.topic()` returns [`Topic`](../../api-reference/topics.md) objects which are used by `StreamingDataFrame`.

Create one for each topic used by your `Application`.

!!! NOTE

    Any missing topics will be automatically created for you upon running an `Application`.

#### Our Topics
We have one output topic, named `customers_coupon_qualified`:

```python
customers_qualified_topic = app.topic(name="customers_coupon_qualified")
```



### The StreamingDataFrame (SDF)

```python
sdf = app.dataframe(topic=customer_purchases_topic)
```

Now for the fun part: building our [StreamingDataFrame](../../processing.md#introduction-to-streamingdataframe), often shorthanded to "SDF".  

SDF allows manipulating the message value in a dataframe-like fashion using various operations.

After initializing with either a `Topic` or `Source`, we continue reassigning to the 
same `sdf` variable as we add operations.

!!! NOTE

    A few `StreamingDataFrame` operations are 
    ["in-place"](../../advanced/dataframe-assignments.md#valid-in-place-operations), 
    like `.print()`.

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

!!! INFO 

    Some operations (like `.isin()`) are only available when manipulating a column.

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

!!! WARNING

    You cannot reference nested keys in this way.



### Producing the Result

```python
sdf = sdf.to_topic(customers_qualified_topic)
```

Finally, we produce our non-filtered results downstream via [`SDF.to_topic(T)`](../../processing.md#writing-data-to-kafka-topics), where `T`
is our previously defined `Topic` (not the topic name!).

!!! INFO

    By default, our outgoing Kafka key is persisted from the input message. 
    [You can alter it](../../processing.md#changing-message-key-before-producing), if needed.

### Running an Application

Running a `Source`-based `Application` requires calling `Application.run()` within a
`if __name__ == "__main__"` block.

#### Our Application Run Block 

Our entire `Application` (and all its spawned objects) resides within a 
`setup_and_run_application()` function, executed as required:

```python
if __name__ == "__main__":
    setup_and_run_application()
```



## Try it yourself!

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