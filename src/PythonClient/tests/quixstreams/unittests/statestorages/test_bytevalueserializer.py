import unittest

from quixstreams import StateValue
from quixstreams.statestorages.serializers.bytevalueserializer import ByteValueSerializer


class TestByteValueSerializer(unittest.TestCase):

    def test_boolean(self):
        data = StateValue(True)
        serialized = ByteValueSerializer.serialize(data)
        deserialized = ByteValueSerializer.deserialize(serialized)

        self.assertEqual(data.value, deserialized.value)

    def test_string(self):
        data = StateValue("123")
        serialized = ByteValueSerializer.serialize(data)
        deserialized = ByteValueSerializer.deserialize(serialized)

        self.assertEqual(data.value, deserialized.value)

    def test_long(self):
        data = StateValue(12)  # Python doesn't have a 'long' type, use 'int' instead
        serialized = ByteValueSerializer.serialize(data)
        deserialized = ByteValueSerializer.deserialize(serialized)

        self.assertEqual(data.value, deserialized.value)

    def test_binary(self):
        data = StateValue([1, 5, 78, 21])
        serialized = ByteValueSerializer.serialize(data)
        deserialized = ByteValueSerializer.deserialize(serialized)

        self.assertEqual(data.value, deserialized.value)

    def test_double(self):
        data = StateValue(1.57)
        serialized = ByteValueSerializer.serialize(data)
        deserialized = ByteValueSerializer.deserialize(serialized)

        self.assertEqual(data.value, deserialized.value)
