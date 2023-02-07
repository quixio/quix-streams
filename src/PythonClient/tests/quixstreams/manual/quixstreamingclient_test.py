from src.quixstreams import QuixStreamingClient
import time
import datetime
import math

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
InteropUtils.enable_debug()

# Quix injects credentials automatically to the client. Alternatively, you can always pass an SDK token manually as an argument.
client = QuixStreamingClient("sdk-sometoken")

# Open the output topic where to write data out
output_topic = client.open_output_topic("hello-world-source")

stream = output_topic.create_stream()
stream.properties.name = "Hello World python stream"
stream.parameters.add_definition("ParameterA").set_range(-1.2, 1.2)
stream.parameters.buffer.time_span_in_milliseconds = 100

print("Sending values for 30 seconds.")

for index in range(0, 3000):
    stream.parameters \
        .buffer \
        .add_timestamp(datetime.datetime.utcnow()) \
        .add_value("ParameterA", math.sin(index / 200.0) + math.sin(index) / 5.0) \
        .write()
    time.sleep(0.01)

print("Closing stream")
stream.close()