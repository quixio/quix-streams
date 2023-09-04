import unittest

from src.quixstreams import KafkaHeader


class KafkaHeaderTests(unittest.TestCase):

    def test_constructor_value_as_bytes(self):
        # Act
        key = "my_key"
        value_bytes = bytes("value_bytes", "utf-8")
        header = KafkaHeader(key, value_bytes)
        # Assert
        self.assertEqual(header.key, key)
        self.assertEqual(header.value, value_bytes)

    def test_constructor_value_as_bytearray(self):
        # Act
        key = "my_key"
        value_bytes = bytearray("value_bytes", "utf-8")
        header = KafkaHeader(key, value_bytes)
        # Assert
        self.assertEqual(header.key, key)
        self.assertEqual(header.value, value_bytes)

    def test_constructor_value_as_string(self):
        # Act
        key = "my_key"
        value = "value_bytes"
        header = KafkaHeader(key, value)
        # Assert
        self.assertEqual(header.key, key)
        self.assertEqual(header.get_value_as_str(), value)

    def test_constructor_net_object_properly_set(self):
        # Arrange
        key = "my_key"
        value_bytes = bytearray("value_bytes", "utf-8")
        header = KafkaHeader(key, value_bytes)
        # Act
        header2 = KafkaHeader(net_pointer=header.get_net_pointer())
        # Assert net object has it properly set
        self.assertEqual(header2.value, value_bytes)
        self.assertEqual(header2.key, key)
