# Custom Websocket Source Example

In this example, we are going to create a custom websocket source to connect to the 
Coinbase Websocket API and process ticker updates with a Streaming DataFrame. 

See [main.py](./main.py) file for more details on how the Source is implemented.

Under the hood, the Application will start the source as a subprocess, and it will produce data to the intermediate Kafka topic for robust processing.

Streaming DataFrame will connect to this topic and process the data.

To find more info about Sources, please see the docs page [TODO: ADD THE PAGE URL]

## How to run

1. Ensure that you have some local Kafka or Redpanda broker to connect to.  
If not, you can start it locally using Docker and provided [docker-compose.yml](../docker-compose.yml) file.  
2. Open the [main.py](./main.py) file and adjust the Kafka connection settings   
in the Application class if necessary.
3. Install the [requirements](./requirements.txt) for the source: `pip install -r requirements.txt`
4. Go to `custom_websocket_source` directory and start the source: `python main.py`
5. Observe the output.  
You should see something like this in your console:

```commandline
[2024-09-19 12:08:25,971] [INFO] [quixstreams] : Starting the Application with the config: broker_address="{'bootstrap.servers': 'localhost:9092'}" consumer_group="quixstreams-default" auto_offset_reset="earliest" commit_interval=5.0s commit_every=0 processing_guarantee="at-least-once"
[2024-09-19 12:08:25,971] [INFO] [quixstreams] : Topics required for this application: "coinbase-source"
[2024-09-19 12:08:26,023] [INFO] [quixstreams] : Validating Kafka topics exist and are configured correctly...
[2024-09-19 12:08:26,032] [INFO] [quixstreams] : Kafka topics validation complete
[2024-09-19 12:08:26,032] [INFO] [quixstreams] : Waiting for incoming messages
[2024-09-19 12:08:29,059] [INFO] [quixstreams] : Starting source coinbase-source
[2024-09-19 12:08:29,310] [INFO] [coinbase-source] [8534] : Source started
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
