import logging
import os
import time

from quixstreams.sources import Source

logger = logging.getLogger(__name__)


class PurchaseGenerator(Source):
    """
    What a Source is:
    A Quix Streams Source enables Applications to read data from something other than a Kafka topic.
    Basically, this generates JSON data that the StreamingDataFrame consumes directly.
    This simplifies the example by not having to run both a producer and Application.

    What it does:
    Generates "purchase events" based on the below list of `_purchase data`.
    """

    _purchases_data = [
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

    def __init__(self):
        super().__init__(name="customer-purchases")

    def run(self):
        for cid, purchase_info in enumerate(self._purchases_data):
            event = self.serialize(key=f"CUSTOMER_ID{cid}", value=purchase_info)
            logger.info(f"producing review for {event.key}: {event.value}")
            self.produce(key=event.key, value=event.value)
            time.sleep(1)  # just to make things easier to follow along
        logger.info("Sent all customer purchases")


def setup_and_run_application():
    """Group all Application-related code here for easy reading."""
    from quixstreams import Application

    app = Application(
        broker_address=os.getenv("BROKER_ADDRESS", "localhost:9092"),
        consumer_group="purchase_filtering",
        auto_offset_reset="earliest",
    )
    customers_qualified_topic = app.topic(name="customers_coupon_qualified")

    def get_full_name(customer):
        return f'{customer["First Name"]} {customer["Last Name"]}'

    def get_purchase_totals(items):
        return sum([i["Price"] * i["Quantity"] for i in items])

    sales_tax = 1.10

    # If reading from a Kafka topic, pass topic=<Topic> instead of a source
    sdf = app.dataframe(source=PurchaseGenerator())
    sdf = sdf[
        (sdf["Purchases"].apply(get_purchase_totals) * sales_tax >= 100.00)
        & (sdf["Membership Type"].isin(["Silver", "Gold"]))
    ]
    sdf["Full Name"] = sdf.apply(get_full_name)
    sdf = sdf[["Full Name", "Email"]]
    # .to_topic() does not require reassignment ("in-place" operation), but does no harm
    sdf = sdf.to_topic(customers_qualified_topic)

    app.run()


# This approach is necessary since we are using a Source
if __name__ == "__main__":
    setup_and_run_application()
