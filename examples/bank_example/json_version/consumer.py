"""
An application to process imaginary purchase transactions in real-time using Kafka

In this application, we will simulate notifications for "Gold" accounts about
purchase events larger than $1000 and count them.
"""

from os import environ

from dotenv import load_dotenv

from quixstreams import Application, State, message_key

load_dotenv("./env_vars.env")


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


def uppercase_source(value: dict):
    """
    Upper-case field "transaction_source" for each processed message

    :param value: message value, a dictionary with all deserialized message data


    :return: this function must either return None or a new dictionary
    """
    print(f'Processing message with key "{message_key()}"')
    value["transaction_source"] = value["transaction_source"].upper()
    return value


# Define your application and settings
app = Application(
    broker_address=environ["BROKER_ADDRESS"],
    consumer_group="json__purchase_notifier",
    auto_offset_reset="earliest",
    consumer_extra_config={"allow.auto.create.topics": "true"},
    producer_extra_config={"allow.auto.create.topics": "true"},
    loglevel="DEBUG",
)

# Define an input topic with JSON deserializer
input_topic = app.topic("json__purchase_events", value_deserializer="json")

# Define an output topic with JSON serializer
output_topic = app.topic("json__user_notifications", value_serializer="json")

# Create a StreamingDataFrame and start building your processing pipeline
sdf = app.dataframe(input_topic)

# Filter only messages with "account_class" == "Gold" and "transaction_amount" >= 1000
sdf = sdf[(sdf["account_class"] == "Gold") & (sdf["transaction_amount"].abs() >= 1000)]

# Drop all fields except the ones we need
sdf = sdf[["account_id", "transaction_amount", "transaction_source"]]

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
