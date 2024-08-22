import base64

from google.protobuf.message import Message
from google.protobuf.timestamp_pb2 import Timestamp


def create_timestamp(timestring: str) -> Timestamp:
    timestamp = Timestamp()
    timestamp.FromJsonString(timestring)
    return timestamp


def get_schema_str(msg_type: Message) -> str:
    # This function follows the algorithm from:
    # confluent_kafka.schema_registry.protobuf:_schema_to_str
    serialized_protobuf = msg_type.DESCRIPTOR.file.serialized_pb
    return base64.standard_b64encode(serialized_protobuf).decode("ascii")
