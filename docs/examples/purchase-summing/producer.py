import os
import time

from quixstreams import Application
from random import choice


_app = Application(broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"))
topic = _app.topic(name="product_reviews")

d = {
    "First Name": "Jane",
    "Last Name": "Doe",
    "Email": "jdoe@mail.com",
    "Membership Type": "Gold",
    "Purchases": [
        {"Item ID": "abc123", "Price": 13.99, "Quantity": 12},
        {"Item ID": "def456", "Price": 12.59, "Quantity": 2},
    ],
}

product_list = ["product_a", "product_b", "product_c"]


if __name__ == "__main__":
    with _app.get_producer() as producer:
        for review in review_list:
            event = topic.serialize(key=choice(product_list), value=review)
            print(f"producing review for {event.key}: {event.value}")
            producer.produce(key=event.key, value=event.value, topic=topic.name)
            time.sleep(0.5)  # just to make things easier to follow along
        print("Sent all product reviews.")
