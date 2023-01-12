import src.quixstreams as qx
import time

client = qx.KafkaStreamingClient('127.0.0.1:9092', None)
output_topic = client.open_output_topic('transactions')

stream = output_topic.create_stream("Test stream")

stream.parameters.buffer.add_timestamp_nanoseconds(time.time_ns()) \
    .add_value("ParameterA", 5.5) \
    .write()

stream.parameters.buffer.flush()

print("Done")