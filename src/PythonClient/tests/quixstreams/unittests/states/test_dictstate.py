import unittest
from quixstreams import InMemoryStorage, StateValue
from quixstreams.states.dictstate import DictState


class TestDictState(unittest.TestCase):

    def test_constructor_using_non_empty_state_should_load_state(self):
        storage = InMemoryStorage()
        storage.set("existing", StateValue("whatever"))
        state = DictState(storage)

        self.assertEqual(len(state), 1)
        self.assertEqual(state["existing"], "whatever")

    def test_add_state_value_should_increase_count(self):
        storage = InMemoryStorage()
        state = DictState(storage)

        state["key"] = StateValue("value")

        self.assertEqual(len(state), 1)

    def test_remove_state_value_should_decrease_count(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key"] = StateValue("value")

        del state["key"]

        self.assertEqual(len(state), 0)

    def test_clear_state_should_remove_all_items(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key1"] = StateValue("value1")
        state["key2"] = StateValue("value2")

        state.clear()

        self.assertEqual(len(state), 0)

    def test_flush_state_should_persist_changes_to_storage(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key1"] = StateValue("value1")
        state["key2"] = StateValue("value2")

        state.flush()

        self.assertEqual(storage.count(), 2)

    def test_flush_clear_before_flush_should_clear_storage(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key1"] = StateValue("value1")
        state["key2"] = StateValue("value2")
        state.flush()
        state.clear()

        state.flush()

        self.assertEqual(storage.count(), 0)

    def test_state_with_null_storage_should_throw_argument_null_exception(self):
        with self.assertRaises(ValueError):
            DictState(None)

    def test_contains_key_key_exists_should_return_true(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key"] = StateValue("value")

        self.assertTrue("key" in state)

    def test_contains_key_key_does_not_exist_should_return_false(self):
        storage = InMemoryStorage()
        state = DictState(storage)

        self.assertFalse("key" in state)

    def test_indexer_get_and_set_should_work_correctly(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key"] = StateValue("value")

        state["key"] = StateValue("updatedValue")
        state_value = state["key"]

        self.assertEqual(state_value.value, "updatedValue")

    def test_reset_modified_should_reset_to_saved(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key"] = StateValue("value")
        state.flush()

        state["key"] = StateValue("updatedValue")
        state.reset()

        value = state["key"]
        self.assertEqual(value, "value")

    def test_update_with_null_value_should_not_be_in_storage(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key"] = StateValue(bytearray([1, 2, 3]))
        state.flush()

        self.assertTrue(storage.contains_key("key"))

        state["key"] = StateValue(None)
        state.flush()

        self.assertFalse(storage.contains_key("key"))

    def test_update_with_null_should_not_be_in_storage(self):
        storage = InMemoryStorage()
        state = DictState(storage)
        state["key"] = StateValue(bytearray([1, 2, 3]))
        state.flush()

        self.assertTrue(storage.contains_key("key"))

        state["key"] = None
        state.flush()

        self.assertFalse(storage.contains_key("key"))
