import json
import logging
from typing import List

from dateutil.parser import isoparse
from websockets import ConnectionClosedOK
from websockets.sync.client import connect

from quixstreams import Application
from quixstreams.sources import Source

logger = logging.getLogger(__name__)


class CoinbaseSource(Source):
    def __init__(
        self, name: str, url: str, product_ids: List[str], shutdown_timeout: float = 10
    ):
        """
        An example implementation of the custom websocket source
        that connects to Coinbase Websocket API
        and streams data to an intermediate Kafka topic.

        Coinbase API docs - https://docs.cdp.coinbase.com/exchange/docs/websocket-overview

        :param name: a unique name of the source.
            It is used as a part of the default topic name.
        :param url: Coinbase WS API URL.
        :param product_ids: a list of Product IDs to receive from the API
            (e.g., "ETH-BTC").
        :param shutdown_timeout: Time in seconds the application waits
            for the source to gracefully shutdown.
        """
        super().__init__(name=name, shutdown_timeout=shutdown_timeout)
        self._url = url
        self._product_ids = product_ids

    def run(self) -> None:
        """
        The main method of the source with the main logic.
        """

        # Open a websocket connection
        ws_conn = connect(self._url)

        # Subscribe to the updates for the given Product IDs
        subscribe_payload = {
            "type": "subscribe",
            "channels": [
                {"name": "ticker", "product_ids": self._product_ids},
            ],
        }
        ws_conn.send(json.dumps(subscribe_payload))

        # Start the reading loop
        while self.running:
            try:
                # Receive a message from the websocket
                msg = ws_conn.recv(timeout=1)
            except TimeoutError:
                continue
            except ConnectionClosedOK:
                logger.info("Websocket connection closed")
                break

            # Parse the message and send only "ticker" data to the topic
            msg_json = json.loads(msg)
            if msg_json["type"] != "ticker":
                continue

            key = msg_json["product_id"]
            dt = isoparse(msg_json["time"])
            timestamp = int(dt.timestamp() * 1000)

            # Serialize message data to bytes to produce to Kafka
            kafka_msg = self.serialize(key=key, value=msg_json, timestamp_ms=timestamp)

            # Produce a serialized message to the Kafka topic
            self.produce(
                value=kafka_msg.value,
                key=kafka_msg.key,
                timestamp=kafka_msg.timestamp,
            )


def main():
    # Configure the CoinbaseSource instance
    coinbase_source = CoinbaseSource(
        # Pick the unique name for the source instance.
        # It will be used as a part of the default topic name.
        name="coinbase-source",
        url="wss://ws-feed-public.sandbox.exchange.coinbase.com",
        product_ids=[
            "ETH-BTC",
        ],
    )

    # Initialize an Application with Kafka configuration
    app = Application(
        broker_address="localhost:9092",  # Specify your Kafka broker address here
        auto_offset_reset="earliest",
    )

    # Define a topic for producing transformed data
    price_updates_topic = app.topic("price_updates")

    # Connect the CoinbaseSource to a StreamingDataFrame
    sdf = app.dataframe(source=coinbase_source)

    # Print the incoming messages from the source
    sdf.print()

    # Select specific data columns and produce them to a topic
    sdf = sdf[["price", "volume_24h"]]
    sdf.to_topic(price_updates_topic)

    # Start the application
    app.run()


if __name__ == "__main__":
    main()
