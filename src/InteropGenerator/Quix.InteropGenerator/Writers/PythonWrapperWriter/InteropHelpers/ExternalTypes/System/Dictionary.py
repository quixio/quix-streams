# ***********************GENERATED CODE WARNING************************
# This file is code generated, any modification you do will be lost the
# next time this file is regenerated.
# *********************************************************************

from typing import Any, Dict, List, Optional
from ctypes import c_void_p, c_int, c_long, POINTER, c_bool
import ctypes
from ...InteropUtils import InteropUtils
from .Array import Array


# noinspection PyPep8Naming
class Dictionary:

    # region helpers
    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_hptr_to_uptr")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_void_p

    @staticmethod
    def ReadAnyHPtrToUPtr(dictionary_hptr: c_void_p) -> c_void_p:
        """
        Read any dictionary that implements IEnumerable<KeyValuePair<,>>. Useful for dictionaries that do not implement
        IDictionary, such as ReadOnlyDictionary

        :param dictionary_hptr: Handler pointer to a dictionary
        :return: Pointer to array with elements [c_void_p, c_void_p] where first is the key array, 2nd is the value array
        """
        if dictionary_hptr is None:
            return None
        uptr = InteropUtils.invoke("dictionary_hptr_to_uptr", dictionary_hptr)
        return c_void_p(uptr) if uptr is not None else None
    
    @staticmethod
    def WriteBlittables(dictionary: Dict[any, any], key_converter, value_converter) -> c_void_p:
        """
         Writes dictionary into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        if dictionary is None:
            return None
        keys = key_converter(dictionary.keys())
        values = value_converter(dictionary.values())
        return Array.WritePointers([keys, values])

    @staticmethod
    def ReadBlittables(dict_uptr: c_void_p, key_converter, value_converter) -> c_void_p:
        """
         Read a pointer into a managed dictionary. The pointer must be to a structure [[keys],[values]], each array with a 4 byte length prefix
        """
        if dict_uptr is None:
            return None
        keys_and_vals = Array.ReadPointers(dict_uptr)
        keys = key_converter(keys_and_vals[0])
        values = value_converter(keys_and_vals[1])

        return dict(zip(keys, values))
    # endregion

    @staticmethod
    def WriteStringPointers(dictionary: Dict[str, c_void_p]) -> c_void_p:
        """
         Writes dictionary of [str, c_void_p] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        return Dictionary.WriteBlittables(dictionary, Array.WriteStrings, Array.WritePointers)

    @staticmethod
    def ReadStringPointers(dictionary_uptr: c_void_p, valuemapper=None) -> Dict[str, c_void_p]:
        """
         Writes dictionary of [str, c_void_p] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        if valuemapper is None:
            return Dictionary.ReadBlittables(dictionary_uptr, Array.ReadStrings, Array.ReadPointers)
        return Dictionary.ReadBlittables(dictionary_uptr, Array.ReadStrings, lambda v_uptr: [valuemapper(pointer) for pointer in Array.ReadPointers(v_uptr)])

    @staticmethod
    def WriteStringDoublesArray(dictionary: Dict[str, List[float]]) -> c_void_p:
        """
         Writes dictionary of [str, [float]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        return Dictionary.WriteBlittables(dictionary, Array.WriteStrings, Array.WriteDoublesArray)

    @staticmethod
    def ReadStringDoublesArray(dictionary: Dict[str, List[int]]) -> c_void_p:
        """
         Reads unmanaged memory at address, converting it to managed [str, float[]] dictionary
         """
        return Dictionary.ReadBlittables(dictionary, Array.ReadStrings, Array.ReadDoublesArray)

    @staticmethod
    def WriteStringLongsArray(dictionary: Dict[str, List[int]]) -> c_void_p:
        """
         Writes dictionary of [str, [int64]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        return Dictionary.WriteBlittables(dictionary, Array.WriteStrings, Array.WriteLongsArray)

    @staticmethod
    def ReadStringLongsArray(dictionary: Dict[str, List[int]]) -> c_void_p:
        """
         Reads unmanaged memory at address, converting it to managed [str, int64p[]] dictionary
         """
        return Dictionary.ReadBlittables(dictionary, Array.ReadStrings, Array.ReadLongsArray)

    @staticmethod
    def WriteStringStrings(dictionary: Dict[str, str]) -> c_void_p:
        """
         Writes dictionary of [str, str] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        return Dictionary.WriteBlittables(dictionary, Array.WriteStrings, Array.WriteStrings)

    @staticmethod
    def ReadStringStrings(dictionary: Dict[str, str]) -> c_void_p:
        """
         Reads unmanaged memory at address, converting it to managed [str, str] dictionary
         """
        return Dictionary.ReadBlittables(dictionary, Array.ReadStrings, Array.ReadStrings)

    @staticmethod
    def WriteStringStringsArray(dictionary: Dict[str, List[str]]) -> c_void_p:
        """
         Writes dictionary of [str, [str]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        return Dictionary.WriteBlittables(dictionary, Array.WriteStrings, Array.WriteStringsArray)

    @staticmethod
    def ReadStringStringsArray(dictionary: Dict[str, List[str]]) -> c_void_p:
        """
         Reads unmanaged memory at address, converting it to managed [str, str[]] dictionary
         """
        return Dictionary.ReadBlittables(dictionary, Array.ReadStrings, Array.ReadStringsArray)

    @staticmethod
    def WriteStringBytesArray(dictionary: Dict[str, List[bytes]]) -> c_void_p:
        """
         Writes dictionary of [str, [bytes]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        # special, because "bytes" is already a list of ubyte, so [bytes] as values is twice nested, but because we're dealing with all values it is [[bytes]],
        # essentially a triple nested ubyte list
        return Dictionary.WriteBlittables(dictionary, Array.WriteStrings, lambda dict_vals: Array.WritePointers([Array.WriteBytesArray(val) for val in dict_vals]))

    @staticmethod
    def ReadStringBytesArray(dictionary: Dict[str, List[bytes]]) -> c_void_p:
        """
         Reads unmanaged memory at address, converting it to managed [str, [bytes]] dictionary
         """
        # special, because "bytes" is already a list of ubyte, so [bytes] as values is twice nested, but because we're dealing with all values it is [[bytes]],
        # essentially a triple nested ubyte list
        return Dictionary.ReadBlittables(dictionary, Array.ReadStrings, lambda dict_vals: [Array.ReadBytesArray(val) for val in Array.ReadPointers(dict_vals)])

    @staticmethod
    def WriteStringNullableDoublesArray(dictionary: Dict[str, List[Optional[float]]]) -> c_void_p:
        """
         Writes dictionary of [str, [Optional[float]]] into unmanaged memory, returning a pointer with structure [[keys],[values]], each array with a 4 byte length prefix
         """
        return Dictionary.WriteBlittables(dictionary, Array.WriteStrings, Array.WriteNullableDoublesArray)

    @staticmethod
    def ReadStringNullableDoublesArray(dictionary: Dict[str, List[Optional[Optional[float]]]]) -> c_void_p:
        """
         Reads unmanaged memory at address, converting it to managed [str, float[]] dictionary
         """
        return Dictionary.ReadBlittables(dictionary, Array.ReadStrings, Array.ReadNullableDoublesArray)


    # region HPtr helpers, useful when must maintain a managed reference

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_constructor_string_string")
    interop_func.argtypes = []
    interop_func.restype = c_void_p
    @staticmethod
    def ConstructorForStringString() -> c_void_p:
        ptr = InteropUtils.invoke("dictionary_constructor_string_string")
        return c_void_p(ptr) if ptr is not None else None

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_clear")
    interop_func.argtypes = [c_void_p]
    @staticmethod
    def Clear(dictionary_hptr: c_void_p) -> None:
        InteropUtils.invoke("dictionary_clear", dictionary_hptr)

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_remove")
    interop_func.argtypes = [c_void_p, c_void_p]
    @staticmethod
    def Remove(dictionary_hptr: c_void_p, key_hptr: c_void_p) -> None:
        InteropUtils.invoke("dictionary_remove", dictionary_hptr, key_hptr)

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_get_count")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_int
    @staticmethod
    def GetCount(dictionary_hptr: c_void_p) -> int:
        return InteropUtils.invoke("dictionary_get_count", dictionary_hptr)

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_set_value")
    interop_func.argtypes = [c_void_p, c_void_p, c_void_p]
    @staticmethod
    def SetValue(dictionary_hptr: c_void_p, key_hptr: c_void_p, value_hptr: c_void_p) -> None:
        InteropUtils.invoke("dictionary_set_value", dictionary_hptr, key_hptr, value_hptr)


    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_get_value")
    interop_func.argtypes = [c_void_p, c_void_p]
    interop_func.restype = c_void_p
    @staticmethod
    def GetValue(dictionary_hptr: c_void_p, key_hptr: c_void_p) -> Any:
        ptr = InteropUtils.invoke("dictionary_get_value", dictionary_hptr, key_hptr)
        return c_void_p(ptr) if ptr is not None else None
        
    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_get_keys")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_void_p
    @staticmethod
    def GetKeys(dictionary_hptr: c_void_p) -> c_void_p:
        ptr = InteropUtils.invoke("dictionary_get_keys", dictionary_hptr)
        return c_void_p(ptr) if ptr is not None else None

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("dictionary_get_values")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_void_p
    @staticmethod
    def GetValues(dictionary_hptr: c_void_p) -> c_void_p:
        ptr = InteropUtils.invoke("dictionary_get_values", dictionary_hptr)
        return c_void_p(ptr) if ptr is not None else None                
    # endregion