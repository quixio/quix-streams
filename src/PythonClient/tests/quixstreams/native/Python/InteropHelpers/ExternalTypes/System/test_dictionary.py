import ctypes
import unittest

from src.quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Dictionary import Dictionary as di

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
#InteropUtils.enable_debug()

class StreamingClientTests(unittest.TestCase):

    def test_read_write_string_pointers(self):
        testvals = {}
        testvals["alpha"] = ctypes.c_void_p(id({}))
        testvals["beta"] = ctypes.c_void_p(id({}))

        dic_uptr = di.WriteStringPointers(testvals)
        result = di.ReadStringPointers(dic_uptr)

        self.assertEqual(testvals.keys(), result.keys())
        self.assertEqual([ii.value for ii in testvals.values()], [ii.value for ii in result.values()]) # c_void_p doesn't compare well


    def test_read_write_string_long_array(self):
        testvals = {}
        testvals["alpha"] = [-6, 9, 0]
        testvals["beta"] = [1, 3, 8]

        dic_uptr = di.WriteStringLongsArray(testvals)
        result = di.ReadStringLongsArray(dic_uptr)

        self.assertEqual(testvals, result)

    def test_read_write_string_double_array(self):
        testvals = {}
        testvals["alpha"] = [-6.123, 9.92, 0.32]
        testvals["beta"] = [1.76, 3.89, 8.12]

        dic_uptr = di.WriteStringDoublesArray(testvals)
        result = di.ReadStringDoublesArray(dic_uptr)

        self.assertEqual(testvals, result)

    def test_read_write_string_nullable_double_array(self):
        testvals = {}
        testvals["alpha"] = [-6.123, None, 9.92, 0.32]
        testvals["beta"] = [1.76, 3.89, None, None]

        dic_uptr = di.WriteStringNullableDoublesArray(testvals)
        result = di.ReadStringNullableDoublesArray(dic_uptr)

        self.assertEqual(testvals, result)

    def test_read_write_string_string_array(self):
        testvals = {}
        testvals["alpha"] = ["apple", "lord", "phalanx", "horse", "apple"]
        testvals["beta"] = ["bear", "eat", "tower", "apple"]

        dic_uptr = di.WriteStringStringsArray(testvals)
        result = di.ReadStringStringsArray(dic_uptr)

        self.assertEqual(testvals, result)

    def test_read_write_string_string(self):
        testvals = {}
        testvals["alpha"] = "apple"
        testvals["beta"] = "bear"

        dic_uptr = di.WriteStringStrings(testvals)
        result = di.ReadStringStrings(dic_uptr)

        self.assertEqual(testvals, result)

    def test_read_write_string_bytes_array(self):
        testvals = {}
        testvals["alpha"] = [bytes("apple", 'utf-8'), bytes("lord", 'utf-8'), bytes("phalanx", 'utf-8'), bytes("horse", 'utf-8'), bytes("apple", 'utf-8')]
        testvals["beta"] = [bytes("bear", 'utf-8'), bytes("eat", 'utf-8'), bytes("tower", 'utf-8'), bytes("apple", 'utf-8')]

        dic_uptr = di.WriteStringBytesArray(testvals)
        result = di.ReadStringBytesArray(dic_uptr)

        self.assertEqual(testvals, result)
