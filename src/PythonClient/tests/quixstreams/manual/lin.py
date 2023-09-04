from src.quixstreams import QuixStreamingClient
import time
import datetime
import math

# Quix injects credentials automatically to the client. Alternatively, you can always pass an SDK token manually as an argument.
client = QuixStreamingClient("sdk-a02c9a23857f4311a6626de81be1fb3e")

# Open the output topic where to write data out
topic_producer = client.get_topic_producer("hello-world-source")

stream = topic_producer.create_stream()
stream.timeseries \
            .buffer \
            .add_timestamp(datetime.datetime.utcnow()) \
            .add_value("Lat", math.sin(index / 100.0) + math.sin(index) / 5.0) \
            .add_value("Long", math.sin(index / 200.0) + math.sin(index) / 5.0) \
            .publish()