import unittest

from src.quixstreams import KafkaMessage, KafkaHeader



class KafkaMessageTests(unittest.TestCase):

    # TODO test with null lists

    def test_constructor_value_only_bytes(self):
        # Act
        value_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        message = KafkaMessage(None, value_bytes)
        # Assert
        self.assertEqual(message.value, value_bytes)

    def test_constructor_value_only_bytearray(self):
        # Act
        value_bytes = bytearray("Test Quix Raw with bytes", "utf-8")
        message = KafkaMessage(None, value_bytes)
        # Assert
        self.assertEqual(message.value, value_bytes)

    def test_constructor_unsetprops_returnexpected(self):
        # Act
        value_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        message = KafkaMessage(None, value_bytes)
        # Assert
        self.assertNotEqual(message.value, None)
        self.assertEqual(message.key, None)
        self.assertEqual(message.headers, [])

    def test_constructor_with_key_bytes(self):
        # Act
        value_bytes = bytes("irrelevant", "utf-8")
        key_bytes = bytes("The Key", "utf-8")

        message = KafkaMessage(key_bytes, value_bytes)
        # Assert
        self.assertEqual(message.key, key_bytes)

    def test_constructor_with_key_bytearray(self):
        # Act
        value_bytes = bytes("irrelevant", "utf-8")
        key_bytes = bytearray("The Key", "utf-8")

        message = KafkaMessage(key_bytes, value_bytes)
        # Assert
        self.assertEqual(message.key, key_bytes)

    def test_constructor_with_headers(self):
        # Act
        value_bytes = bytes("irrelevant", "utf-8")
        headers = []
        headers.append(KafkaHeader(key="header1", value="value1"))
        headers.append(KafkaHeader(key="header2", value="value2"))
        message = KafkaMessage(None, value_bytes, headers)
        # Assert
        self.assertEqual(len(message.headers), 2)
        header1 = message.headers[0]
        self.assertEqual(header1.key, "header1")
        self.assertEqual(header1.get_value_as_str(), "value1")
        header2 = message.headers[1]
        self.assertEqual(header2.key, "header2")
        self.assertEqual(header2.get_value_as_str(), "value2")


    def test_constructor_net_object_properly_set(self):
        # Arrange
        value_bytes = bytes("the value", "utf-8")
        key_bytes = bytearray("The Key", "utf-8")
        headers = []
        headers.append(KafkaHeader(key="header1", value="value1"))
        headers.append(KafkaHeader(key="header2", value="value2"))
        message = KafkaMessage(key_bytes, value_bytes, headers)
        # Act
        message2 = KafkaMessage(net_pointer=message.get_net_pointer())
        # Assert net object has it properly set
        self.assertEqual(message2.value, value_bytes)
        self.assertEqual(message2.key, key_bytes)
        self.assertEqual(len(message2.headers), 2)
        header1 = message2.headers[0]
        self.assertEqual(header1.key, "header1")
        self.assertEqual(header1.get_value_as_str(), "value1")
        header2 = message2.headers[1]
        self.assertEqual(header2.key, "header2")
        self.assertEqual(header2.get_value_as_str(), "value2")
