# Example: Purchase Summing

We will build a simple Purchase Summing app to showcase some common Quix Streams 
dataframe-like operations with dictionary/JSON data (a format frequently used).

You'll learn how to:

- Create a topic
- Assign a value to a new column
- Pair sdf.Apply() with additional operations
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
new Purchase Summing application will then transform and filter as required.

## 3. Main Takeaways
 
The primary lesson in this example is learning how you can use common pandas-like 
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


## 5. Purchase Condenser application


Here is what our application will look like in the end:

```python
import os

from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="purchase_summing",
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

sdf = sdf[(sdf["Transaction"].apply(get_purchase_totals) * SALES_TAX >= 100.00) & (sdf["Membership Type"].isin(["Silver", "Gold"]))]
```

We get started with a bang! Basica Using SDF.apply(F), we can apply any custom
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
