import unittest

from quixstreams import StateValue, StateType


class TestStateValue(unittest.TestCase):

    def test_long(self):
        state_value = StateValue(1)
        self.assertEqual(state_value.type, StateType.Long)
        self.assertEqual(state_value.value, 1)

    def test_bool(self):
        state_value = StateValue(True)
        self.assertEqual(state_value.type, StateType.Bool)
        self.assertEqual(state_value.value, True)

    def test_string(self):
        state_value = StateValue("TestStr312")
        self.assertEqual(state_value.type, StateType.String)
        self.assertEqual(state_value.value, "TestStr312")

    def test_double(self):
        state_value = StateValue(0.426)
        self.assertEqual(state_value.type, StateType.Double)
        self.assertEqual(state_value.value, 0.426)

    def test_binary(self):
        bytes_ = bytearray([0, 4, 6, 8, 1, 0, 43, 255, 0, 32])
        state_value = StateValue(bytes_)
        self.assertEqual(state_value.type, StateType.Binary)
        self.assertEqual(state_value.value, bytes_)

    def test_object_with_value(self):
        bytes_ = None
        state_value = StateValue(bytes_)
        self.assertEqual(state_value.type, StateType.Object)
        self.assertIsNone(state_value.value)
        self.assertTrue(state_value.is_null())

