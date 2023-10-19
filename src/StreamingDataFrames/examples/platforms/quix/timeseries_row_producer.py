# This is an alternative example of how to produce messages using QuixTimeSeries

import uuid
from random import randint, random, choice
from time import sleep

from dotenv import load_dotenv

from streamingdataframes.models.serializers import QuixTimeseriesSerializer
from streamingdataframes.models.timestamps import MessageTimestamp, TimestampType
from streamingdataframes.models.topics import Topic
from streamingdataframes.platforms.quix import QuixKafkaConfigsBuilder
from streamingdataframes.rowproducer import RowProducer, Row

load_dotenv("./platforms/quix/quix_vars.env")


# For non-"Application.Quix" platform producing, config is a bit manual right now
topic = "qts__purchase_events"
cfg_builder = QuixKafkaConfigsBuilder()
cfgs, topics, _ = cfg_builder.get_confluent_client_configs([topic])
topic = Topic(name=topics[0], value_serializer=QuixTimeseriesSerializer())
cfg_builder.create_topics([topic])
producer = RowProducer(broker_address=cfgs.pop("bootstrap.servers"), extra_config=cfgs)

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
        headers = {"uuid": str(uuid.uuid4())}
        value = {
            "account_id": account_id,
            "account_class": "Gold" if account >= 8 else "Silver",
            "transaction_amount": randint(-2500, -1),
            "transaction_source": choice(retailers),
        }
        print(f"Producing value {value}")
        producer.produce_row(
            row=Row(
                key=account_id,
                value=value,
                # the remaining arguments are irrelevant for producing but are required
                topic="ignore",
                partition=0,
                offset=0,
                size=0,
                timestamp=MessageTimestamp(1234567890, type=TimestampType(1)),
            ),
            topic=topic,
        )
        i += 1
        sleep(random())
finally:
    producer.flush(10)
