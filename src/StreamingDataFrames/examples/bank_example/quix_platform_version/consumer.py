"""
An application to process imaginary purchase transactions in real-time using Kafka
running on Quix platform.

In this application, we will simulate notifications for "Gold" accounts about
purchase events larger than $1000
"""

from dotenv import load_dotenv

from quixstreams import Application, MessageContext, State

# Reminder: the platform will have these values available by default so loading the
# environment would be unnecessary there.
load_dotenv("./bank_example/quix_platform_version/quix_vars.env")


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


def count_transactions(value: dict, ctx: MessageContext, state: State):
    """
    Track the number of transactions using persistent state

    :param value: message value
    :param ctx: message context with key, timestamp and other Kafka message metadata
    :param state: instance of State store
    """
    total = state.get("total_transactions", 0)
    total += 1
    state.set("total_transactions", total)
    value["total_transactions"] = total


# Define your application and settings
# Quix application is automatically configured to work with Quix platform
app = Application.Quix(
    "qts__purchase_notifier",
    auto_offset_reset="earliest",
    auto_create_topics=True,  # Quix app has an option to auto create topics
)

# Define an input topic with Quix deserializer
input_topic = app.topic("qts__purchase_events", value_deserializer="quix")

# Define an output topic with Quix Timeseries serializer
output_topic = app.topic("qts__user_notifications", value_serializer="quix_timeseries")

# Create a StreamingDataFrame and start building your processing pipeline
sdf = app.dataframe(input_topic)


# Filter only messages with "account_class" == "Gold" and "transaction_amount" >= 1000
sdf = sdf[
    (sdf["account_class"] == "Gold")
    & (sdf["transaction_amount"].apply(lambda x, ctx: abs(x)) >= 1000)
]

# Drop all fields except the ones we need
sdf = sdf[["account_id", "transaction_amount", "transaction_source"]]

# Update the total number of transactions in state
sdf = sdf.apply(count_transactions, stateful=True)

# Transform field "transaction_source" to upper-case using a custom function
sdf = sdf.apply(uppercase_source)

# Add a new field with a notification text
sdf["customer_notification"] = "A high cost purchase was attempted"

# Print the transformed message to the console
sdf = sdf.apply(lambda val, ctx: print(f"Sending update: {val}"))

# Send the message to the output topic
sdf.to_topic(output_topic)

if __name__ == "__main__":
    # Start message processing
    app.run(sdf)
