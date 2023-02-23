from src.quixstreams import QuixStreamingClient
import time
import datetime
import math

# Quix injects credentials automatically to the client. Alternatively, you can always pass an SDK token manually as an argument.
client = QuixStreamingClient("sdk-a02c9a23857f4311a6626de81be1fb3e")

# Open the output topic where to write data out
topic_producer = client.create_topic_producer("hello-world-source")

stream = topic_producer.create_stream()
stream.properties.name = "Hello World python stream"
stream.timeseries.add_definition("ParameterA").set_range(-1.2, 1.2)
stream.timeseries.buffer.time_span_in_milliseconds = 100

print("Sending values for 30 seconds.")

for index in range(0, 3000):
    stream.timeseries \
        .buffer \
        .add_timestamp(datetime.datetime.utcnow()) \
        .add_value("ParameterA", math.sin(index / 200.0) + math.sin(index) / 5.0) \
        .publish()
    time.sleep(0.01)

print("Closing stream")
stream.close()
