import unittest
from datetime import datetime

from quixstreams import RawMessage

from src.quixstreams import EventData, App

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
InteropUtils.enable_debug()


class RawMessageTests(unittest.TestCase):

    # TODO test with null lists

    def test_constructor_with_bytes(self):
        # Act
        message_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        message = RawMessage(message_bytes)
        # Assert
        self.assertEqual(message.value, message_bytes)

    def test_constructor_with_bytearray(self):
        # Act
        message_bytes = bytearray("Test Quix Raw with bytes", "utf-8")
        message = RawMessage(message_bytes)
        # Assert
        self.assertEqual(message.value, message_bytes)

    def test_set_value(self):
        # Act
        message_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        raw_message = RawMessage(message_bytes)
        message = RawMessage(bytes('other', 'utf-8'))
        message.value = raw_message.value
        # Assert
        self.assertEqual(message.value, message_bytes)

    def test_set_key(self):
        # Act
        message_bytes = bytes("Test Quix Raw with bytes", "utf-8")
        message = RawMessage(message_bytes)
        message_key_bytes = bytes("Test key", "utf-8")
        message.key = bytes("Test key", "utf-8")
        # Assert
        self.assertEqual(message.key, message_key_bytes)