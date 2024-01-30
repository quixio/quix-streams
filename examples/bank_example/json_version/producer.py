import json
import time
import uuid
from os import environ
from random import randint, random, choice

from dotenv import load_dotenv

from quixstreams.kafka import Producer
from quixstreams.models.topics import TopicManager, TopicAdmin

load_dotenv("./env_vars.env")

topic_manager = TopicManager(
    topic_admin=TopicAdmin(broker_address=environ["BROKER_ADDRESS"])
)
topic_name = topic_manager.topic(
    name="json__purchase_events",
    # "config" only needed if you wish to not use the defaults!
    config=topic_manager.topic_config(extra_config={"retention.ms": "3600000"}),
).name
topic_manager.create_all_topics()

retailers = [
    "Billy Bob's Shop",
    "Tasty Pete's Burgers",
    "Mal-Wart",
    "Bikey Bikes",
    "Board Game Grove",
    "Food Emporium",
]

# strings for key, value, and headers will be serialized to bytes by default
i = 0
with Producer(
    broker_address=environ["BROKER_ADDRESS"],
) as producer:
    while i < 10000:
        account = randint(0, 10)
        account_id = f"A{'0'*(10-len(str(account)))}{account}"
        value = {
            "account_id": account_id,
            "account_class": "Gold" if account >= 8 else "Silver",
            "transaction_amount": randint(-2500, -1),
            "transaction_source": choice(retailers),
        }
        print(f"Producing value {value}")
        producer.produce(
            topic=topic_name,
            headers=[("uuid", str(uuid.uuid4()))],  # a dict is also allowed here
            key=account_id,
            value=json.dumps(value),  # needs to be a string
        )
        i += 1
        time.sleep(random())
