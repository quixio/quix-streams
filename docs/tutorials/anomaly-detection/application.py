import os

from quixstreams import Application
from quixstreams.context import message_key


app = Application(
    broker_address=os.environ.get("BROKER_ADDRESS", "localhost:9092"),
    consumer_group="temperature_alerter",
    auto_offset_reset="earliest",
)
temperature_readings_topic = app.topic(name="temperature_readings")
alerts_topic = app.topic(name="alerts")


def should_alert(window_value):
    if window_value >= 90:
        print(f"Alerting for MID {message_key()}: Average Temperature {window_value}")
        return True


sdf = app.dataframe(topic=temperature_readings_topic)
sdf = sdf.apply(lambda data: data["Temperature_C"])
sdf = sdf.hopping_window(duration_ms=5000, step_ms=1000).mean().current()
sdf = sdf.apply(lambda result: round(result["value"], 2)).filter(should_alert)
sdf = sdf.to_topic(alerts_topic)


if __name__ == "__main__":
    app.run(sdf)
