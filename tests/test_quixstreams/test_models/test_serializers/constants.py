from quixstreams.models import MessageField, SerializationContext

DUMMY_CONTEXT = SerializationContext(topic="topic", field=MessageField.VALUE)

AVRO_TEST_SCHEMA = {
    "type": "record",
    "name": "Root",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "id", "type": "int", "default": 0},
        {
            "name": "nested",
            "type": {
                "type": "record",
                "name": "Nested",
                "fields": [
                    {"name": "id", "type": "int", "default": 0},
                ],
            },
            "default": {"id": 0},
        },
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
