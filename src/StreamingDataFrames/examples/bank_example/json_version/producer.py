import json
import time
import uuid
from os import environ
from random import randint, random, choice

from dotenv import load_dotenv

from quixstreams.kafka import Producer

load_dotenv("./env_vars.env")

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
    extra_config={"allow.auto.create.topics": "true"},
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
            topic="json__purchase_events",
            headers=[("uuid", str(uuid.uuid4()))],  # a dict is also allowed here
            key=account_id,
            value=json.dumps(value),  # needs to be a string
        )
        i += 1
        time.sleep(random())
