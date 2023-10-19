# It is suggested to use the `Application.Quix` when interacting with the Quix platform

from dotenv import load_dotenv

from streamingdataframes import Application
from streamingdataframes.models.serializers import (
    QuixTimeseriesSerializer,
    QuixTimeseriesDeserializer,
)

# Reminder: the platform will have these values available by default so loading the
# environment would be unnecessary there.
load_dotenv("./platforms/quix/quix_vars.env")

# Quix app has an option to auto create topics
# Quix app does not require the broker being defined
app = Application.Quix(
    "qts__purchase_notifier", auto_create_topics=True, auto_offset_reset="earliest"
)
input_topic = app.topic(
    "qts__purchase_events", value_deserializer=QuixTimeseriesDeserializer()
)
output_topic = app.topic(
    "qts__user_notifications", value_serializer=QuixTimeseriesSerializer()
)


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
