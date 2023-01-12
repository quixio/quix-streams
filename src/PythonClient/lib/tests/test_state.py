import unittest
import asyncio
import time

from quixstreaming.state.localfilestorage import LocalFileStorage



class StateTest(unittest.TestCase):

    def test_state(self):
        # Arrange
        storage = LocalFileStorage()
        storage.clear()


        storage.set("KEY1", 12.51)
        storage.set("KEY2", "str")
        storage.set("KEY3", True)
        storage.set("KEY4", False)

        self.assertEqual(12.51, storage.get("KEY1"))
        self.assertEqual("str", storage.get("KEY2"))
        self.assertEqual(True, storage.get("KEY3"))
        self.assertEqual(False, storage.get("KEY4"))

    
    def test_rmkeys(self):

        # Arrange
        storage = LocalFileStorage()
        storage.clear()

        storage.set("KEY1", 12.51)
        storage.set("KEY2", "str")
        storage.set("KEY3", True)
        storage.set("KEY4", False)

        self.assertTrue(storage.containsKey("KEY1"))
        self.assertTrue(storage.containsKey("KEY2"))
        self.assertTrue(storage.containsKey("KEY3"))
        self.assertTrue(storage.containsKey("KEY4"))
        self.assertTrue(4, len(storage.getAllKeys()))

        storage.remove("KEY2")
        self.assertTrue(storage.containsKey("KEY1"))
        self.assertFalse(storage.containsKey("KEY2"))
        self.assertTrue(storage.containsKey("KEY3"))
        self.assertTrue(storage.containsKey("KEY4"))
        self.assertTrue(3, len(storage.getAllKeys()))

        storage.remove("KEY4")
        self.assertTrue(storage.containsKey("KEY1"))
        self.assertFalse(storage.containsKey("KEY2"))
        self.assertTrue(storage.containsKey("KEY3"))
        self.assertFalse(storage.containsKey("KEY4"))
        self.assertTrue(2, len(storage.getAllKeys()))

        storage.set("KEY2", "str2")
        self.assertTrue(storage.containsKey("KEY1"))
        self.assertTrue(storage.containsKey("KEY2"))
        self.assertTrue(storage.containsKey("KEY3"))
        self.assertFalse(storage.containsKey("KEY4"))
        self.assertTrue(3, len(storage.getAllKeys()))

        self.assertEqual(12.51, storage.get("KEY1"))
        self.assertEqual("str2", storage.get("KEY2"))
        self.assertEqual(True, storage.get("KEY3"))

    def test_binary_bytearray(self):

        # Arrange
        storage = LocalFileStorage()
        storage.clear()

        data = bytearray([ 2,3,5,7 ])
        storage.set("KEY_b1", data)

        self.assertTrue(storage.containsKey("KEY_b1"))

        retrieved = storage.get("KEY_b1")
        self.assertTrue(isinstance(retrieved, (bytes, bytearray)))
        

        storage.remove("KEY_b1")
        self.assertFalse(storage.containsKey("KEY_b1"))

    def test_binary_bytes(self):

        # Arrange
        storage = LocalFileStorage()
        storage.clear()

        data = bytes([ 2,3,5,7 ])
        storage.set("KEY_b1", data)

        self.assertTrue(storage.containsKey("KEY_b1"))

        retrieved = storage.get("KEY_b1")
        self.assertTrue(isinstance(retrieved, (bytes)))
        

        storage.remove("KEY_b1")
        self.assertFalse(storage.containsKey("KEY_b1"))



if __name__ == '__main__':
    unittest.main()
 