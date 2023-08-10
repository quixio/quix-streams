import math
import unittest

from quixstreams.statestorages.statevalue import StateValue
from quixstreams.statestorages.inmemorystorage import InMemoryStorage


class InMemoryStorageTests(unittest.TestCase):

    @staticmethod
    def create_in_memory_storage():
        return InMemoryStorage()

    def test_setget_float(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            storage.set("Float_Pos", 12.51)
            storage.set("Float_Neg", -12.51)
            storage.set("Float_Nan", float('NaN'))
            storage.set("Float_Inf", float('Inf'))
            storage.set("Float_NegInf", -float('Inf'))

            # Assert
            self.assertEqual(12.51, storage.get("Float_Pos"))
            self.assertEqual(-12.51, storage.get("Float_Neg"))
            self.assertTrue(math.isnan(storage.get("Float_Nan")))
            self.assertEqual(float('Inf'), storage.get("Float_Inf"))
            self.assertEqual(-float('Inf'), storage.get("Float_NegInf"))
        finally:
            storage.clear()

    def test_setget_int(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            storage.set("Int_Pos", 9875)
            storage.set("Int_Neg", -9875)

            # Assert
            self.assertEqual(9875, storage.get("Int_Pos"))
            self.assertEqual(-9875, storage.get("Int_Neg"))
        finally:
            storage.clear()

    def test_setget_string(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            storage.set("Str_NonEmpty", "test")
            storage.set("Str_Empty", "")

            # Assert
            self.assertEqual("test", storage.get("Str_NonEmpty"))
            self.assertEqual("", storage.get("Str_Empty"))
        finally:
            storage.clear()

    def test_setget_bool(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            storage.set("Bool_True", True)
            storage.set("Bool_False", False)

            # Assert
            self.assertEqual(True, storage.get("Bool_True"))
            self.assertEqual(False, storage.get("Bool_False"))
        finally:
            storage.clear()

    def test_setget_binary(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            storage.set("Bytes_NonEmpty", bytes("Test-val-1", "utf-8"))
            storage.set("ByteArray_NonEmpty", bytearray("Test-val-1", "utf-8"))
            storage.set("Bytes_Empty", bytes())
            storage.set("ByteArray_Empty", bytearray())

            # Assert
            self.assertEqual(bytes("Test-val-1", "utf-8"), storage.get("Bytes_NonEmpty"))
            self.assertEqual(bytearray("Test-val-1", "utf-8"), storage.get("ByteArray_NonEmpty"))
            self.assertEqual(bytes(), storage.get("Bytes_Empty"))
            self.assertEqual(bytearray(), storage.get("ByteArray_Empty"))
        finally:
            storage.clear()

    def test_setget_none(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            storage.set("None", None)

            # Assert
            self.assertEqual(None, storage.get("None"))
        finally:
            storage.clear()

    def test_setget_object(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            dictionary = {"test": "me"}
            storage.set("Object_Dict", dictionary)

            # Assert
            self.assertEqual(dictionary, storage.get("Object_Dict"))
        finally:
            storage.clear()

    def test_setget_statevalue(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        try:
            # Act
            state_value = StateValue(3.12)
            storage.set("StateValue", state_value)

            # Assert
            res = storage.get("StateValue")
            self.assertIsNotNone(res)
            self.assertEqual(state_value.value, res)
        finally:
            storage.clear()

    def test_get_all_keys(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        storage.set("Key1", 12.51)
        storage.set("Key2", 12.51)

        # Act
        keys = storage.get_all_keys()

        # Assert
        self.assertIn("Key1", keys)
        self.assertIn("Key2", keys)
        self.assertEqual(2, len(keys))

    def test_clear(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        storage.set("Key1", 12.51)
        storage.set("Key2", 12.51)

        keys = storage.get_all_keys()
        self.assertEqual(2, len(keys))

        # Act
        storage.clear()

        # Assert
        keys = storage.get_all_keys()
        self.assertEqual(0, len(keys))
    
    def test_remove_key(self):
        # Arrange
        storage = InMemoryStorageTests.create_in_memory_storage()
        storage.clear()

        storage.set("Key_To_Remove", 12.51)
        self.assertTrue(storage.contains_key("Key_To_Remove"))

        # Act
        storage.remove("Key_To_Remove")

        # Assert
        self.assertFalse(storage.contains_key("Key_To_Remove"))
