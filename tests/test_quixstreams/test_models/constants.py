from quixstreams.models import SerializationContext

dummy_context = SerializationContext(topic="topic")

AVRO_TEST_SCHEMA = {
    "type": "record",
    "name": "testschema",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "id", "type": "int", "default": 0},
    ],
}

JSONSCHEMA_TEST_SCHEMA = {
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "id": {"type": "number"},
    },
    "required": ["name"],
}
