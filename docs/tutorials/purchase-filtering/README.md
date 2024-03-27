# Example: Purchase Filtering

We will build a simple Purchase Filtering app to showcase some common Quix Streams 
dataframe-like operations with dictionary/JSON data (a format frequently used).

You'll learn how to:

- Create a topic
- Assign a value to a new column
- Use sdf.Apply() with additional operations
- Filter with inequalities combined with and/or ("&", "|")
- Get subset of columns (AKA slice or select)
- Produce resulting output to a topic



## 1. Outline of the Problem

Imagine we are a company who sells goods to members only: they can have a 
"Bronze", "Silver", or "Gold" membership.

We always send a coupon to our Silver and Gold members who spend at least $100 in one
visit.

We need an application that will filter for the applicable customers and send only the
necessary information downstream.

## 2. Our Example

We will use a simple producer to generate some mock purchase data, which our 
new Purchase Filtering application will then adjust and filter as required.

## 3. Main Takeaways
 
The primary lesson: learning how you can use common pandas-like 
operations on dictionary/JSON data to perform various transformations
as if it were a dataframe.

Another important thing to note that we use the word "column" for consistency
with the Pandas terminology, but you can also think of it as a dictionary key.

## 4. Generating Purchase Data

We have a standalone producer HERE [LINK] that generates a small static set of 
"purchases", which are simply dictionaries with various info about what was purchased by 
a customer during their visit. The data is keyed on customer ID.

An outgoing Kafka message looks something like:

```python
# ...
kafka_key: "CUSTOMER_ID_123"
kafka_value: 
    {
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


## 5. Purchase Filtering application


Here is what our application will look like in the end:

```python
import os

from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="purchase_filtering",
    auto_offset_reset="earliest",
)
customer_purchases_topic = app.topic(name="customer_purchases")
customers_qualified_topic = app.topic(name="customers_coupon_qualified")


def get_full_name(customer):
    return f'{customer["First Name"]} {customer["Last Name"]}'


def get_purchase_totals(transaction):
    return sum([t["Price"]*t["Quantity"] for t in transaction])


SALES_TAX = 1.10

sdf = app.dataframe(topic=customer_purchases_topic)
sdf = sdf[(sdf["Transaction"].apply(get_purchase_totals) * SALES_TAX >= 100.00) & (sdf["Membership Type"].isin(["Silver", "Gold"]))]
sdf["Full Name"] = sdf.apply(get_full_name)
sdf = sdf[["Full Name", "Email"]]
sdf = sdf.to_topic(customers_qualified_topic)


if __name__ == '__main__':
    app.run(sdf)
```

Let's go over it in detail:

### Create Application

```python
app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="purchase_summing",
    auto_offset_reset="earliest"
)
```

First, create the quixstreams Application, which is our constructor for everything! We provide it our connection settings, consumer group (ideally unique per Application), and where the consumer group should start from on our topic. 

You can learn more about auto_offset_reset HERE (link to article?), but TL;DR - "earliest" is generally recommended while learning.


### Define Topics

```python
customer_purchases_topic = app.topic(name="customer_purchases")
customers_qualified_topic = app.topic(name="customers_coupon_qualified")
```

Next we define our input/output topics, named customer_purchases and customers_coupon_qualified, respectively. 

These topics will automatically be created for you when you run the application should they not exist.


### The StreamingDataFrame (SDF)

```python
sdf = app.dataframe(topic=customer_purchases_topic)
```

Now for the fun part: building our StreamingDataFrame, often shorthanded to "SDF".  

We initialize it, and then continue re-assigning to the same variable ("sdf") as we add operations until we are finished with it.


### Adding a New Column

```python
def get_full_name(customer):
    return f'{customer["First Name"]} {customer["Last Name"]}'


sdf["Full Name"] = sdf.apply(get_full_name)
```

### SDF-based filtering

```python
def get_purchase_totals(transaction):
    return sum([t["Price"]*t["Quantity"] for t in transaction])

sdf = sdf[
    (sdf["Transaction"].apply(get_purchase_totals) * SALES_TAX >= 100.00)
    & (sdf["Membership Type"].isin(["Silver", "Gold"]))
]
```

We get started with a bang! 

Let's break it down step-by-step, as most of our
work is done here:

```python
# step A
sdf["Transaction"].apply(get_purchase_totals) * SALES_TAX >= 100.00
```

Here, we do a `.apply(F)` operation on a column value (F should take your data as an 
argument, and return some data). Our F here is "get_purchase_totals".

Notice how you can still do basic operations with an apply result, like multiplying it 
by our sales tax, and then finally doing an inequality check on the total (all of which
are SDF operations...more on that in a second).

```python
# step B
sdf["Membership Type"].isin(["Silver", "Gold"])
```

We additionally showcase our built-in `.isin` function, a way for SDF to perform an 
"`if x in y`" check (SDF is declaratively defined, invalidating that approach).

Be sure to check out our documentation HERE to see what other built-ins are available!


```python
# "and" Steps A, B
(A) & (B)
```
Now we "and" these steps, which translates to your typical "A and B" 
(and returns a boolean).

A couple things with "&" (and): 
- It is considered an SDF operation.
- You MUST use "&" for and, "|" for or
- Your respective objects (i.e. A, B) must be wrapped in parentheses.

Ultimately, when executed, the result of "&" will be boolean. This is imporant for...


```python
# filter with "&" result
sdf = sdf[X]
```
Ultimately, this is a filtering operation: whenever X is an SDF operation(s) result, it acts like Pandas row filtering.

As such, SDF filtering interprets the SDF operation "&" boolean result as follows:
- True -> continue processing this event
- False -> stop ALL further processing of this event (including produces!)

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

### Getting a Column Subset

```python
sdf = sdf[["Email", "Full Name"]]
```
We only need a couple fields to send downstream, so this is a convenient way to select
only a specific list of columns (aka dict keys) from our data.

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

Finally, we produce our non-filtered results downstream.

NOTE: by default, our outgoing Kafka key is persisted from the input message. Should
you need to change it, check out our documentation HERE.


## 6. Try it yourself!

### Run kafka
First, go ahead and get a kafka cluster running. To easily follow along with this example, just follow THESE (link) instructions.

### Install quixstreams
In your python environment, run `pip install quixstreams`

### Run the Producer and Application
Just call `python producer.py` and `python application.py` in separate windows.

### Check out the results!

...but wait, I don't see any message processing output...Is it working???

One thing to keep in mind is that the Quix Streams does not log/print any message processing
operations by default.

To get visual outputs around message processing, you can either:
- use recommended ways of printing/logging stuff HERE
- use DEBUG mode via `Application(loglevel="DEBUG")`
  - WARNING: you should NOT run your applications in "DEBUG" mode in production.
