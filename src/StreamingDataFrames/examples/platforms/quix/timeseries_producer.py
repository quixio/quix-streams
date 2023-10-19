import uuid
from random import randint, random, choice
from time import sleep

from dotenv import load_dotenv

from streamingdataframes.kafka import Producer
from streamingdataframes.models.serializers import (
    QuixTimeseriesSerializer,
    SerializationContext,
)
from streamingdataframes.models.topics import Topic, TopicCreationConfigs
from streamingdataframes.platforms.quix import QuixKafkaConfigsBuilder

load_dotenv("./platforms/quix/quix_vars.env")


# For non-"Application.Quix" platform producing, config is a bit manual right now
topic = "qts__purchase_events"
cfg_builder = QuixKafkaConfigsBuilder()
cfgs, topics, _ = cfg_builder.get_confluent_client_configs([topic])
topic = topics[0]
cfg_builder.create_topics(
    [Topic(name=topic, creation_configs=TopicCreationConfigs(1, 1))]
)
producer = Producer(broker_address=cfgs.pop("bootstrap.servers"), extra_config=cfgs)
serialize = QuixTimeseriesSerializer()


retailers = [
    "Billy Bob's Shop",
    "Tasty Pete's Burgers",
    "Mal-Wart",
    "Bikey Bikes",
    "Board Game Grove",
    "Food Emporium",
]


# strings for key and headers will be serialized to bytes by default
i = 0
try:
    while i < 10000:
        account = randint(0, 10)
        account_id = f"A{'0'*(10-len(str(account)))}{account}"
        headers = {**serialize.extra_headers, "uuid": str(uuid.uuid4())}
        value = {
            "account_id": account_id,
            "account_class": "Gold" if account >= 8 else "Silver",
            "transaction_amount": randint(-2500, -1),
            "transaction_source": choice(retailers),
        }
        print(f"Producing value {value}")
        producer.produce(
            topic=topic,
            headers=headers,
            key=account_id,
            value=serialize(
                value=value, ctx=SerializationContext(topic=topic, headers=headers)
            ),
        )
        i += 1
        sleep(random())
finally:
    producer.flush(10)
