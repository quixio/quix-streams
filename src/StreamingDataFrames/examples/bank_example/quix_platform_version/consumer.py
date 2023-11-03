"""
An application to process imaginary purchase transactions in real-time using Kafka
running on Quix platform.

In this application, we will simulate notifications for "Gold" accounts about
purchase events larger than $1000
"""

from dotenv import load_dotenv

from streamingdataframes import Application, MessageContext
from streamingdataframes.models.serializers import (
    QuixTimeseriesSerializer,
    QuixDeserializer,
)

# Reminder: the platform will have these values available by default so loading the
# environment would be unnecessary there.
load_dotenv("./bank_example/quix_platform_version/quix_vars.env")

# Define your application and settings
# Quix application is automatically configured to work with Quix platform
app = Application.Quix(
    "qts__purchase_notifier",
    auto_offset_reset="earliest",
    auto_create_topics=True,  # Quix app has an option to auto create topics
)

# Define an input topic with JSON deserializer
input_topic = app.topic("qts__purchase_events", value_deserializer=QuixDeserializer())

# Define an output topic with JSON dserializer
output_topic = app.topic(
    "qts__user_notifications", value_serializer=QuixTimeseriesSerializer()
)

# Create a StreamingDataFrame and start building your processing pipeline
sdf = app.dataframe(input_topic)


def uppercase_source(value: dict, ctx: MessageContext):
    """
    Upper-case field "transaction_source" for each processed message

    :param value: message value, a dictionary with all deserialized message data
    :param ctx: message context, it contains message metadata like key, topic, timestamp
        etc.

    :return: this function must either return None or a new dictionary
    """
    print(f'Processing message with key "{ctx.key}"')
    value["transaction_source"] = value["transaction_source"].upper()
    return value


# Filter only messages with "account_class" == "Gold" and "transaction_amount" >= 1000
sdf = sdf[
    (sdf["account_class"] == "Gold")
    & (sdf["transaction_amount"].apply(lambda x: abs(x)) >= 1000)
]

# Drop all fields except the ones we need
sdf = sdf[["account_id", "transaction_amount", "transaction_source"]]

# Transform field "transaction_source" to upper-case using a custom function
sdf = sdf.apply(uppercase_source)

# Add a new field with a notification text
sdf["customer_notification"] = "A high cost purchase was attempted"  # add new column

# Print the transformed message to the console
sdf = sdf.apply(lambda val: print(f"Sending update: {val}"))

# Send the message to the output topic
sdf.to_topic(output_topic)

if __name__ == "__main__":
    # Start message processing
    app.run(sdf)
