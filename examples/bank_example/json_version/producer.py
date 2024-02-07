import time
import uuid
from os import environ
from random import randint, random, choice

from dotenv import load_dotenv

from quixstreams import Application

load_dotenv("./env_vars.env")


app = Application(
    broker_address=environ["BROKER_ADDRESS"],
    consumer_group="ignore",
)
topic = app.topic(name="json__purchase_events", value_serializer="json")

retailers = [
    "Billy Bob's Shop",
    "Tasty Pete's Burgers",
    "Mal-Wart",
    "Bikey Bikes",
    "Board Game Grove",
    "Food Emporium",
]

i = 0
# app.get_producer() automatically creates any topics made via `app.topic`
with app.get_producer() as producer:
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
        # with current functionality, we need to manually serialize our data
        serialized = topic.serialize(
            key=account_id, value=value, headers={"uuid": str(uuid.uuid4())}
        )
        producer.produce(
            topic=topic.name,
            headers=serialized.headers,
            key=serialized.key,
            value=serialized.value,
        )
        i += 1
        time.sleep(random())
