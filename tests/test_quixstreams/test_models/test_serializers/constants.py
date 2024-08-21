from quixstreams.models import MessageField, SerializationContext

DUMMY_CONTEXT = SerializationContext(topic="topic", field=MessageField.VALUE)

AVRO_TEST_SCHEMA = {
    "type": "record",
    "name": "testschema",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "id", "type": "int", "default": 0},
    ],
}

JSONSCHEMA_TEST_SCHEMA = {
    "title": "Test",
    "type": "object",
    "properties": {
        "name": {"type": "string"},
        "id": {"type": "number"},
    },
    "required": ["name"],
}
