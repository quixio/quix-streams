import time
import uuid
from random import randint, random, choice
from time import sleep

from dotenv import load_dotenv

from quixstreams.kafka import Producer
from quixstreams.models.serializers import (
    QuixTimeseriesSerializer,
    SerializationContext,
)
from quixstreams.platforms.quix import QuixKafkaConfigsBuilder, QuixTopicManager

load_dotenv("./bank_example/quix_platform_version/quix_vars.env")


# For non-"Application.Quix" platform producing, config is a bit manual right now
cfg_builder = QuixKafkaConfigsBuilder()
cfgs = cfg_builder.get_confluent_broker_config()
topic_manager = QuixTopicManager(quix_config_builder=cfg_builder)
topic_name = topic_manager.topic(
    name="qts__purchase_events",
    # "config" only needed if you wish to not use the defaults!
    config=topic_manager.topic_config(extra_config={"retention.ms": "3600000"}),
).name
topic_manager.create_all_topics()
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
with Producer(
    broker_address=cfgs.pop("bootstrap.servers"), extra_config=cfgs
) as producer:
    while i < 10000:
        account = randint(0, 10)
        account_id = f"A{'0'*(10-len(str(account)))}{account}"
        headers = {**serialize.extra_headers, "uuid": str(uuid.uuid4())}
        value = {
            "account_id": account_id,
            "account_class": "Gold" if account >= 8 else "Silver",
            "transaction_amount": randint(-2500, -1),
            "transaction_source": choice(retailers),
            "Timestamp": time.time_ns(),
        }
        print(f"Producing value {value}")
        producer.produce(
            topic=topic_name,
            headers=headers,
            key=account_id,
            value=serialize(
                value=value, ctx=SerializationContext(topic=topic_name, headers=headers)
            ),
        )
        i += 1
        sleep(random())
