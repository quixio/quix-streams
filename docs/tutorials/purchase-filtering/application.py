import os

from quixstreams import Application

app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="purchase_filtering",
    auto_offset_reset="earliest",
)
customer_purchases_topic = app.topic(name="customer_purchases")
customers_qualified_topic = app.topic(name="customers_coupon_qualified")


def get_full_name(customer):
    return f'{customer["First Name"]} {customer["Last Name"]}'


def get_purchase_totals(transaction):
    return sum([t["Price"] * t["Quantity"] for t in transaction])


SALES_TAX = 1.10

sdf = app.dataframe(topic=customer_purchases_topic)
sdf = sdf[
    (sdf["Purchases"].apply(get_purchase_totals) * SALES_TAX >= 100.00)
    & (sdf["Membership Type"].isin(["Silver", "Gold"]))
]
sdf["Full Name"] = sdf.apply(get_full_name)
sdf = sdf[["Full Name", "Email"]]
sdf = sdf.to_topic(customers_qualified_topic)


if __name__ == "__main__":
    app.run(sdf)
