import unittest

from quixstreams import InMemoryStorage, StateValue
from quixstreams.states.scalarstate import ScalarState


class TestScalarState(unittest.TestCase):

    def test_constructor_using_non_empty_state_should_load_state(self):
        storage = InMemoryStorage()
        storage.set(ScalarState.STORAGE_KEY, StateValue("whatever"))
        state = ScalarState(storage)

        self.assertEqual(state.value, "whatever")

    def test_set_value_should_change_value(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue("value")

        self.assertEqual(state.value.value, "value")

    def test_clear_value_should_set_to_null(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue("value")
        state.clear()

        self.assertIsNone(state.value)

    def test_flush_value_should_persist_changes_to_storage(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue("value")
        state.flush()

        self.assertEqual(storage.get(ScalarState.STORAGE_KEY), "value")

    def test_flush_clear_before_flush_should_clear_storage(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue("value")
        state.flush()
        state.clear()
        state.flush()

        self.assertFalse(storage.contains_key(ScalarState.STORAGE_KEY))

    def test_state_with_null_storage_should_throw_argument_null_exception(self):
        with self.assertRaises(ValueError):
            ScalarState(None)

    def test_reset_modified_should_reset_to_saved(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue("value")
        state.flush()

        state.value = StateValue("updatedValue")
        state.reset()

        self.assertEqual(state.value, "value")

    def test_update_with_null_byte_value_should_be_removed_from_state(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue({1, 2, 3})
        state.flush()

        state.value = StateValue(None)
        state.flush()

        self.assertFalse(storage.contains_key(ScalarState.STORAGE_KEY))

    def test_update_with_null_object_value_should_be_removed_from_state(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue([1, 2, 3])
        state.flush()

        state.value = StateValue(None)
        state.flush()

        self.assertFalse(storage.contains_key(ScalarState.STORAGE_KEY))

    def test_update_with_null_string_value_should_be_removed_from_state(self):
        storage = InMemoryStorage()
        state = ScalarState(storage)
        state.value = StateValue("something")
        state.flush()

        state.value = StateValue(None)
        state.flush()

        self.assertFalse(storage.contains_key(ScalarState.STORAGE_KEY))

