import unittest

from src.quixstreams import KafkaMessage, KafkaHeader, KafkaTimestamp


class KafkaMessageTests(unittest.TestCase):

    def test_constructor_value_only_bytes(self):
        # Act
        value_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        message = KafkaMessage(None, value_bytes)
        # Assert
        self.assertEqual(value_bytes, message.value)

    def test_constructor_value_only_bytearray(self):
        # Act
        value_bytes = bytearray("Test Quix Raw with bytes", "utf-8")
        message = KafkaMessage(None, value_bytes)
        # Assert
        self.assertEqual(value_bytes, message.value)

    def test_constructor_unsetprops_returnexpected(self):
        # Act
        value_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        message = KafkaMessage(None, value_bytes)
        # Assert
        self.assertNotEqual(None, message.value)
        self.assertEqual(None, message.key)
        self.assertEqual([], message.headers)

    def test_constructor_with_key_bytes(self):
        # Act
        value_bytes = bytes("irrelevant", "utf-8")
        key_bytes = bytes("The Key", "utf-8")

        message = KafkaMessage(key_bytes, value_bytes)
        # Assert
        self.assertEqual(key_bytes, message.key)

    def test_constructor_with_key_bytearray(self):
        # Act
        value_bytes = bytes("irrelevant", "utf-8")
        key_bytes = bytearray("The Key", "utf-8")

        message = KafkaMessage(key_bytes, value_bytes)
        # Assert
        self.assertEqual(key_bytes, message.key)

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
        self.assertEqual("header1", header1.key)
        self.assertEqual("value1", header1.get_value_as_str())
        header2 = message.headers[1]
        self.assertEqual("header2", header2.key)
        self.assertEqual("value2", header2.get_value_as_str())

    def test_constructor_with_timestamp(self):
        # Act
        value_bytes = bytes("irrelevant", "utf-8")
        timestamp = KafkaTimestamp(unix_timestamp_ms=1000)

        message = KafkaMessage(None, value_bytes, timestamp=timestamp)
        # Assert
        self.assertEqual(message.timestamp, timestamp)


    def test_constructor_net_object_properly_set(self):
        # Arrange
        value_bytes = bytes("the value", "utf-8")
        key_bytes = bytearray("The Key", "utf-8")
        headers = []
        headers.append(KafkaHeader(key="header1", value="value1"))
        headers.append(KafkaHeader(key="header2", value="value2"))
        timestamp = KafkaTimestamp(unix_timestamp_ms=1000000000000)
        message = KafkaMessage(key_bytes, value_bytes, headers, timestamp=timestamp)
        # Act
        message2 = KafkaMessage(net_pointer=message.get_net_pointer())
        # Assert net object has it properly set
        self.assertEqual(value_bytes, message2.value)
        self.assertEqual(key_bytes, message2.key)
        self.assertEqual(len(message2.headers), 2)
        header1 = message2.headers[0]
        self.assertEqual("header1", header1.key)
        self.assertEqual("value1", header1.get_value_as_str())
        header2 = message2.headers[1]
        self.assertEqual("header2", header2.key)
        self.assertEqual("value2", header2.get_value_as_str())
        self.assertEqual(message.timestamp.unix_timestamp_ms, message2.timestamp.unix_timestamp_ms)

