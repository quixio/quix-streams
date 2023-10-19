from os import environ

from dotenv import load_dotenv

from streamingdataframes import Application
from streamingdataframes.models.serializers import JSONSerializer, JSONDeserializer

load_dotenv("./env_vars.env")

app = Application(
    broker_address=environ["BROKER_ADDRESS"],
    consumer_group="json__purchase_notifier",
    auto_offset_reset="earliest",
    consumer_extra_config={"allow.auto.create.topics": "true"},
    producer_extra_config={"allow.auto.create.topics": "true"},
)
input_topic = app.topic("json__purchase_events", value_deserializer=JSONDeserializer())
output_topic = app.topic("json__user_notifications", value_serializer=JSONSerializer())


def uppercase_source(msg_value):
    msg_value["transaction_source"] = msg_value["transaction_source"].upper()
    return msg_value


# "Gold" members get realtime notifications about purchase events larger than $1000
sdf = app.dataframe(topics_in=[input_topic])
sdf = sdf[
    (sdf["account_class"] == "Gold")
    & (sdf["transaction_amount"].apply(lambda x: abs(x)) >= 1000)
]  # filter out any messages according to these conditionals
sdf = sdf[["account_id", "transaction_amount", "transaction_source"]]  # column subset
sdf = sdf.apply(uppercase_source)  # apply funcs must accept the message value as an arg
sdf["customer_notification"] = "A high cost purchase was attempted"  # add new column
sdf = sdf.apply(lambda val: print(f"Sending update: {val}"))  # easy way to print out
sdf.to_topic(output_topic)

app.run(sdf)
