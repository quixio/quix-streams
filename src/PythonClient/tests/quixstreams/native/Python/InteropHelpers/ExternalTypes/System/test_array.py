import ctypes
import unittest

from src.quixstreams.native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai

from src.quixstreams.native.Python.InteropHelpers.InteropUtils import InteropUtils
#InteropUtils.enable_debug()

class ArrayTests(unittest.TestCase):

    # TODO test with null lists

    def test_read_write_longs(self):
        test_vals = [9223372036854775807, 987987, -9223372036854775808]
        arr_uptr = ai.WriteLongs(test_vals)
        result = ai.ReadLongs(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_read_write_longs_array(self):
        test_vals = [[-3, -2, -1], [0, 1, 2], [9223372036854775807, -9223372036854775808]]
        arr_uptr = ai.WriteLongsArray(test_vals)
        result = ai.ReadLongsArray(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_read_write_strings(self):
        test_vals = ["a string", "another", "yetanother", "    ", None]
        arr_uptr = ai.WriteStrings(test_vals)
        result = ai.ReadStrings(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_read_write_strings_array(self):
        test_vals = [["a string", "another", "yetanother", "    ", None], ["other"]]
        arr_uptr = ai.WriteStringsArray(test_vals)
        result = ai.ReadStringsArray(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_read_write_doubles(self):
        test_vals = [3.1, 5.7, 8.99, 0, -4, -6.768]
        arr_uptr = ai.WriteDoubles(test_vals)
        result = ai.ReadDoubles(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_read_write_doubles_array(self):
        test_vals = [[3.1, 5.7, 8.99, 0, -4, -6.768], [-7, 6.3]]
        arr_uptr = ai.WriteDoublesArray(test_vals)
        result = ai.ReadDoublesArray(arr_uptr)

        self.assertEqual(test_vals, result)
    def test_write_read_bytes(self):
        test_vals = bytes("byte me", "utf-8")
        arr_uptr = ai.WriteBytes(test_vals)
        result = ai.ReadBytes(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_write_read_bytearray(self):
        test_vals = bytearray("byte me", "utf-8")
        arr_uptr = ai.WriteBytes(test_vals)
        result = ai.ReadBytes(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_read_write_byte_arrays(self):
        test_vals = [bytes("byte me", "utf-8"), bytearray("byte me 2", "utf-8")]
        arr_uptr = ai.WriteBytesArray(test_vals)
        result = ai.ReadBytesArray(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_write_read_references(self):
        test_vals = [ctypes.c_void_p(id({})), ctypes.c_void_p(id({}))]
        arr_uptr = ai.WritePointers(test_vals)
        result = ai.ReadPointers(arr_uptr)

        self.assertEqual([ii.value for ii in test_vals], [jj.value for jj in result])  # pointers don't compare well

    def test_write_read_references_array(self):
        test_vals = [[ctypes.c_void_p(id({})), ctypes.c_void_p(id({}))], [ctypes.c_void_p(id({})), ctypes.c_void_p(id({}))]]
        arr_uptr = ai.WritePointersArray(test_vals)
        result = ai.ReadPointersArray(arr_uptr)

        self.assertEqual([[jj.value for jj in ii] for ii in test_vals], [[jj.value for jj in ii] for ii in result])  # pointers don't compare well

    def test_read_write_nullable_doubles(self):
        test_vals = [3.1, 5.7, 8.99, 0, None, -4, -6.768]
        arr_uptr = ai.WriteNullableDoubles(test_vals)
        result = ai.ReadNullableDoubles(arr_uptr)

        self.assertEqual(test_vals, result)

    def test_read_write_nullable_doubles_array(self):
        test_vals = [[3.1, 5.7, 8.99, 0, None, -4, -6.768], [None, None, 9, -1, 4.5123]]
        arr_uptr = ai.WriteNullableDoublesArray(test_vals)
        result = ai.ReadNullableDoublesArray(arr_uptr)

        self.assertEqual(test_vals, result)