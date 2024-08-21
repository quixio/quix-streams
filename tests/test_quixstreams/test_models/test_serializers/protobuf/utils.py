from google.protobuf.timestamp_pb2 import Timestamp


def create_timestamp(timestring: str) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromJsonString(timestring)
    return timestamp
