"""
An application to process imaginary purchase transactions in real-time using Kafka
running on Quix platform.

In this application, we will simulate notifications for "Gold" accounts about
purchase events larger than $1000
"""

from dotenv import load_dotenv

from quixstreams import Application, State


# Reminder: the platform will have these values available by default so loading the
# environment would be unnecessary there.
load_dotenv("./bank_example/quix_platform_version/quix_vars.env")


def count_transactions(value: dict, state: State):
    """
    Track the number of transactions using persistent state

    :param value: message value
    :param state: instance of State store
    """
    total = state.get("total_transactions", 0)
    total += 1
    state.set("total_transactions", total)
    return total


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
sdf = sdf[(sdf["account_class"] == "Gold") & (sdf["transaction_amount"].abs() >= 1000)]

# Drop all fields except the ones we need
sdf = sdf[["Timestamp", "account_id", "transaction_amount", "transaction_source"]]

# Update the total number of transactions in state and save result to the message
sdf["total_transactions"] = sdf.apply(count_transactions, stateful=True)

# Transform field "transaction_source" to upper-case using a custom function
sdf["transaction_source"] = sdf["transaction_source"].apply(lambda v: v.upper())

# Add a new field with a notification text
sdf["customer_notification"] = "A high cost purchase was attempted"

# Print the transformed message to the console
sdf = sdf.update(lambda val: print(f"Sending update: {val}"))

# Send the message to the output topic
sdf = sdf.to_topic(output_topic)

if __name__ == "__main__":
    # Start message processing
    app.run(sdf)
