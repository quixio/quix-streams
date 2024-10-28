import os
import time

from quixstreams import Application

_app = Application(broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"))
topic = _app.topic(name="customer_purchases")

purchases_data = [
    {
        "First Name": "Jane",
        "Last Name": "Doe",
        "Email": "jdoe@mail.com",
        "Membership Type": "Gold",
        "Purchases": [
            {"Item ID": "abc123", "Price": 13.99, "Quantity": 1},
            {"Item ID": "def456", "Price": 12.59, "Quantity": 2},
        ],
    },
    {
        "First Name": "Robbie",
        "Last Name": "McRobberson",
        "Email": "therob@mail.com",
        "Membership Type": "Bronze",
        "Purchases": [
            {"Item ID": "abc123", "Price": 13.99, "Quantity": 13},
        ],
    },
    {
        "First Name": "Howbout",
        "Last Name": "Dat",
        "Email": "cashmeoutside@mail.com",
        "Membership Type": "Silver",
        "Purchases": [
            {"Item ID": "abc123", "Price": 3.14, "Quantity": 7},
            {"Item ID": "xyz987", "Price": 7.77, "Quantity": 13},
        ],
    },
    {
        "First Name": "The",
        "Last Name": "Reaper",
        "Email": "plzdontfearme@mail.com",
        "Membership Type": "Gold",
        "Purchases": [
            {"Item ID": "xyz987", "Price": 7.77, "Quantity": 99},
        ],
    },
]


if __name__ == "__main__":
    with _app.get_producer() as producer:
        for cid, purchase_info in enumerate(purchases_data):
            event = topic.serialize(key=f"CUSTOMER_ID{cid}", value=purchase_info)
            print(f"producing review for {event.key}: {event.value}")
            producer.produce(key=event.key, value=event.value, topic=topic.name)
            time.sleep(1)  # just to make things easier to follow along
        print("Sent all customer purchases")
