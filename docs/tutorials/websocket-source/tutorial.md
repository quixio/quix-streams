# Tutorial: Websocket Source (Coinbase API)

This tutorial builds a custom `Source` connector named `CoinbaseSource` for ingesting 
ticker updates with the Coinbase Websocket API for processing them with a `StreamingDataFrame`. 

Specifically, it showcases how to use the Quix Streams connector framework to 
create and use a customized `Source` (there are also `Sink` connectors as well!).


## Outline of the Problem

We want to track of various Bitcoin prices for real-time analysis, but we need to get 
the data into Kafka first.


## Our Example
This example showcases:

1. Extending the Quix Streams `Source` class to read from the Coinbase Websocket API.
2. Using the new extension (`CoinbaseSource`).

## Before Getting Started

- You will see links scattered throughout this tutorial.
    - Tutorial code links are marked **>>> LIKE THIS <<<** .
    - ***All other links provided are completely optional***. 
    - They are great ways to learn more about various concepts if you need it!


## Creating `CoinbaseSource`

Let's take a look at [**>>> CoinbaseSource <<<**](tutorial_app.py) in detail to
understand what modifications to `Source` were necessary.

> [!NOTE] 
> Check out the [custom Source docs](../../connectors/sources/custom-sources.md) 
> for additional details around what can be adjusted.


### Setting up `.run()`

A `Source` requires defining a `.run()` method, which should perform
a data retrieval and produce loop (using `Source.serialize()` and `Source.produce()` methods) within a 
`while Source.running` block.

Lets take a look at `CoinbaseSource`'s `.run()` in detail.

#### Setting up the Connection

First, we set up the connection. 

This doesn't have to be done during `.run()`, but now the connection is only 
established when `Application.run()` is called.

```python
ws_conn = connect(self._url)
subscribe_payload = {
    "type": "subscribe",
    "channels": [
        {"name": "ticker", "product_ids": self._product_ids},
    ],
}
ws_conn.send(json.dumps(subscribe_payload))
```

#### Data retrieval loop

Now we set up the data retrieval loop contained within a `while self.running` block. 

This is so a shutdown from the `Application` level also gracefully exits this loop; the 
`Source` essentially stops if the `Source.run()` method is ever exited.

> [!NOTE] 
> Since no other teardown is required for websockets, nothing happens after the
> `while self.running` block.

Inside this block, records are retrieved, serialized (to `JSON`), and produced to an
underlying internal topic as close to its raw form as possible. 

> [!NOTE]
> The internal topic can be configured to accept other data serializations by
> overriding the `Source.default_topic()` method.

User-level manipulations of the data occur later using the `Application` that utilizes 
the `Source` (with a `StreamingDataFrame`).


## Using `CoinbaseSource`

Now that `CoinbaseSource` exists, anyone can use it to ingest data from Coinbase.

Of course, each user will have their own product ID's and transformations to apply.

### Defining the Source

First, set up the `CoinBaseSource` with our desired `product_ids`.

Be sure to provide a unique name since it affects the internal topic name.

```python
coinbase_source = CoinbaseSource(
    name="coinbase-source",
    url="wss://ws-feed-public.sandbox.exchange.coinbase.com",
    product_ids=["ETH-BTC"],
)
```

### Defining an Application

Now create the [Quix Streams Application](../../configuration.md), which is our constructor for everything! 

We provide it our connection settings, consumer group (ideally unique per Application), 
and where the consumer group should start from on our (internal) topic.

NOTE: Once you are more familiar with Kafka, we recommend [learning more about auto_offset_reset](https://www.quix.io/blog/kafka-auto-offset-reset-use-cases-and-pitfalls).

#### Our Application
```python
from quixstreams import Application
app = Application(
    broker_address="localhost:9092",  # your Kafka broker address here
    auto_offset_reset="earliest",
)
```

### Define Outbound Topics

Next we define any output topics.

`Application.topic()` returns [`Topic`](../../api-reference/topics.md) objects which are used by `StreamingDataFrame.to_topic()`.

> [!NOTE]
> The topics will automatically be created for you in Kafka when you run the application should they not exist.

#### Our Topic
Our output topic name is `price_updates`:

```python
price_updates_topic = app.topic("price_updates")
```

### The StreamingDataFrame (SDF)

Now for the fun part: building our [StreamingDataFrame](../../processing.md#introduction-to-streamingdataframe), often shorthanded to "SDF".  

SDF allows manipulating the message value in a dataframe-like fashion using various operations.

After initializing with either a `Topic` or `Source`, we continue re-assigning to the same `sdf` variable as we add operations.

> [!NOTE]
> A few `StreamingDataFrame` operations are 
> ["in-place"](../../advanced/dataframe-assignments.md#valid-in-place-operations), 
> like `.print()`.

#### Our SDF operations
First, we initialize our SDF with our `coinbase_source`.

Then, our SDF prints each record to the console, and then produces only 
the price and ticker name to our outgoing topic.

```python
sdf = app.dataframe(source=coinbase_source)
sdf.print()
sdf = sdf[['price', 'volume_24h']]
sdf.to_topic(price_updates_topic)
```

#### Example record
As an example, a record processed by our `StreamingDataframe` would print the following:
```python
{ 'value': { 'type': 'ticker',
             'sequence': 754296790,
             'product_id': 'ETH-BTC',
             'price': '0.00005',
             'open_24h': '0.00008',
             'volume_24h': '322206074.45925051',
             'low_24h': '0.00005',
             'high_24h': '0.00041',
             'volume_30d': '3131937035.46099349',
             'best_bid': '0.00001',
             'best_bid_size': '1000000000.00000000',
             'best_ask': '0.00006',
             'best_ask_size': '166668.66666667',
             'side': 'sell',
             'time': '2024-09-19T10:01:26.411029Z',
             'trade_id': 28157206,
             'last_size': '16666.86666667'}}
```

and then produce the following to topic `price_updates`:
```python
{
  'key': 'ETH-BTC', 
  'value': {'price': '0.00005', 'volume_24h': '322206074.45925051'}
}

```


### Running an Application

Running a `Source`-based `Application` requires calling `Application.run()` within a
`if __name__ == "__main__"` block.

#### Our Application Run Block 

Our entire `Application` (and all its spawned objects) resides within a 
`main()` function, executed as required:

```python
if __name__ == "__main__":
    main()
```

This `main()` setup is a personal choice: the only true requirement is `app.run()` being 
called inside the block.



## Try it yourself!

### 1. Run Kafka
First, have a running Kafka cluster. 

To conveniently follow along with this tutorial, just [run this simple one-liner](../README.md#running-kafka-locally).

### 2. Download files
- [requirements.txt](requirements.txt) 
- [tutorial_app.py](tutorial_app.py)

### 3. Install requirements
In your desired python environment, execute: `pip install -r requirements.txt`

### 4. Run the application
In your desired python environment, execute: `python tutorial_app.py`.

### 5. Check out the results!

You should see record print outs [like the example above](#example-record).