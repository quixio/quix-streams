# ***********************GENERATED CODE WARNING************************
# This file is code generated, any modification you do will be lost the
# next time this file is regenerated.
# *********************************************************************

from typing import Any, Union, List, Callable, Optional
from ctypes import c_void_p
import ctypes
from ...InteropUtils import InteropUtils


# noinspection PyPep8Naming
class Array:
    _nullable_double = InteropUtils.create_mem_nullable(ctypes.c_double)

    # region helpers
    @staticmethod
    def ReadBlittables(array_uptr: ctypes.c_void_p, valuetype, valuemapper=None) -> []:
        """
        Reads blittable values starting from the array pointer using the specified type and mapper then frees the pointer
        
        :param array_uptr: Unmanaged memory pointer to the first element of the array.
        :param valuetype: Type of the value
        :param valuemapper: Conversion function for the value
        :return: The converted array to the given type using the mapper
        """
        if array_uptr is None:
            return None

        try:
            # c_void_p type is not getting correctly set for the object but others are
            if valuetype is c_void_p:
                if valuemapper is None:
                    valuemapper = lambda x: c_void_p(x)
                else:
                    original_valuemapper = valuemapper
                    valuemapper = lambda x: original_valuemapper(c_void_p(x) if x is not None else None)
    
            # first 4 bytes contains the length as int
            length = int(ctypes.cast(array_uptr, ctypes.POINTER(ctypes.c_int32)).contents.value)
            if length == 0:
                return []
    
            # Allocate an array which will be managed by python. We move the values from unmanaged memory to
            # the managed and free the unmanaged (see finally). Managed will be freed by python once no longer in use
            arrsize = ctypes.sizeof(valuetype) * length
            arr = bytes(arrsize)
            arr_ptr = ctypes.c_char_p(arr)
            ctypes.memmove(arr, array_uptr.value + 4, arrsize)
            ctypes_pointer = ctypes.cast(arr_ptr, ctypes.POINTER(valuetype))
            # print(f"ARR READ ({valuetype}): {bytes(arr).hex().upper()}")  # for testing
    
            if valuemapper == None:
                return [ctypes_pointer[i] for i in range(length)]
    
            vals = [valuemapper(ctypes_pointer[i]) for i in range(length)]
            return vals
        finally:
            InteropUtils.free_uptr(array_uptr)

    @staticmethod
    def WriteBlittables(blittables: [any], valuetype, valuemapper=None) -> c_void_p:
        """
        Writes a list of blittables (like int) into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """

        if blittables is None:
            return None

        blit_len = len(blittables)
        if blit_len == 0:
            buffer_uptr = InteropUtils.allocate_uptr(4)  # 4 bytes for int32 length
        else:

            if valuemapper is not None:
                blittables = [valuemapper(i) for i in blittables]

            arr = (valuetype * blit_len)(*blittables)
            # print(f"ARR WROTE ({valuetype}): {bytes(arr).hex().upper()}")  # left here for testing purposes

            arrsize = ctypes.sizeof(valuetype) * blit_len
            buffer_uptr = InteropUtils.allocate_uptr(4 + arrsize)  # 4 bytes for int32 length + whatever the rest needs

            ctypes.memmove(buffer_uptr.value + 4, arr, arrsize)
        size_bytes = ctypes.c_int32.from_address(buffer_uptr.value)
        size_bytes.value = blit_len
        return buffer_uptr

    @staticmethod
    def ReadArray(array_uptr: ctypes.c_void_p, valuemapper: Callable[[c_void_p], Any]) -> [[any]]:
        """
        Reads from unmanaged memory, returning list of any sets (any[][])
        """
        if array_uptr is None:
            return None
          
        return Array.ReadBlittables(array_uptr, ctypes.c_void_p, valuemapper)

    @staticmethod
    def WriteArray(values: [[any]], valuemapper: Callable[[Any], c_void_p]) -> c_void_p:
        """
        Writes an array of any sets (any[][]) into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if values is None:
            return None

        return Array.WriteBlittables(values, ctypes.c_void_p, valuemapper)

    @staticmethod
    def ReadNullables(array_uptr: ctypes.c_void_p, nullable_type) -> [Any]:
        """
        Parameters
        ----------

        array_ptr: c_void_p
            Pointer to .Net nullable array.

        nullable_type:
            nullable type created by InteropUtils.create_nullable

        Returns
        -------
        []:
           array of underlying type with possible None values
        """
        if array_uptr is None:
            return None

        nullables = Array.ReadBlittables(array_uptr, nullable_type)

        result = []
        for nullable in nullables:
            if nullable.HasValue:
                result.append(nullable.Value)
            else:
                result.append(None)
        return result
    # endregion

    # region long
    @staticmethod
    def ReadLongs(array_uptr: ctypes.c_void_p) -> [int]:
        """
        Reads from unmanaged memory, returning list of int64
        """
        if array_uptr is None:
            return None
          
        return Array.ReadBlittables(array_uptr, ctypes.c_int64)

    @staticmethod
    def ReadLongsArray(array_uptr: ctypes.c_void_p) -> [[int]]:
        """
        Reads from unmanaged memory, returning list of int64 lists
        """
        if array_uptr is None:
            return None

        return Array.ReadArray(array_uptr, Array.ReadLongs)

    @staticmethod
    def WriteLongs(longs: [int]) -> c_void_p:
        """
        Writes list of int64 into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        
        if longs is None:
            return None

        return Array.WriteBlittables(longs, ctypes.c_int64)

    @staticmethod
    def WriteLongsArray(longs_array: [[int]]) -> c_void_p:
        """
        Writes list of int64 lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if longs_array is None:
            return None

        return Array.WriteArray(longs_array, Array.WriteLongs)

    # endregion

    # region string
    @staticmethod
    def ReadStrings(array_uptr: ctypes.c_void_p) -> [str]:
        """
        Reads from unmanaged memory, returning list of str
        """
        if array_uptr is None:
            return None
          
        return Array.ReadBlittables(array_uptr, ctypes.c_void_p, lambda x: InteropUtils.uptr_to_utf8(x))

    @staticmethod
    def ReadStringsArray(array_uptr: ctypes.c_void_p) -> [[str]]:
        """
        Reads from unmanaged memory, returning list of str lists
        """
        if array_uptr is None:
            return None

        return Array.ReadArray(array_uptr, Array.ReadStrings)

    @staticmethod
    def WriteStrings(strings: [str]) -> c_void_p:
        """
        Writes list of str into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if strings is None:
            return None

        return Array.WriteBlittables(strings, ctypes.c_void_p, lambda x: InteropUtils.utf8_to_uptr(x))

    @staticmethod
    def WriteStringsArray(strings_array: [[str]]) -> c_void_p:
        """
        Writes list of str lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if strings_array is None:
            return None

        return Array.WriteArray(strings_array, Array.WriteStrings)
    # endregion

    # region double
    @staticmethod
    def ReadDoubles(array_uptr: ctypes.c_void_p) -> [float]:
        """
        Reads from unmanaged memory, returning list of double (float)
        """        
        if array_uptr is None:
            return None
          
        return Array.ReadBlittables(array_uptr, ctypes.c_double)

    @staticmethod
    def ReadDoublesArray(array_uptr: ctypes.c_void_p) -> [[float]]:
        """
        Reads from unmanaged memory, returning list of double (float) lists
        """
        if array_uptr is None:
            return None

        return Array.ReadArray(array_uptr, Array.ReadDoubles)

    @staticmethod
    def WriteDoubles(doubles: [float]) -> c_void_p:
        """
        Writes list of double (float) into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if doubles is None:
            return None

        return Array.WriteBlittables(doubles, ctypes.c_double)

    @staticmethod
    def WriteDoublesArray(doubles_array: [[float]]) -> c_void_p:
        """
        Writes list of double (float) lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if doubles_array is None:
            return None

        return Array.WriteArray(doubles_array, Array.WriteDoubles)
    # endregion

    # region references
    @staticmethod
    def ReadPointers(pointers: [c_void_p]) -> c_void_p:
        """
        Reads from unmanaged memory, returning list of pointers
        """
        if pointers is None:
            return None

        return Array.ReadBlittables(pointers, ctypes.c_void_p)

    @staticmethod
    def ReadPointersArray(array_uptr: ctypes.c_void_p) -> [[c_void_p]]:
        """
        Reads from unmanaged memory, returning list of pointer lists
        """
        if array_uptr is None:
            return None

        return Array.ReadArray(array_uptr, Array.ReadPointers)

    @staticmethod
    def WritePointers(pointers: [c_void_p]) -> c_void_p:
        """
        Writes list of pointer into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if pointers is None:
            return None

        return Array.WriteBlittables(pointers, ctypes.c_void_p)

    @staticmethod
    def WritePointersArray(pointers_array: [[c_void_p]]) -> c_void_p:
        """
        Writes list of pointer lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if pointers_array is None:
            return None

        return Array.WriteArray(pointers_array, Array.WritePointers)
    # endregion

    # region bytes
    @staticmethod
    def ReadBytes(array_uptr: ctypes.c_void_p) -> bytes:
        """
        Reads from unmanaged memory, returning bytes
        """
        if array_uptr is None:
            return None
          
        return bytes(Array.ReadBlittables(array_uptr, ctypes.c_ubyte))

    @staticmethod
    def ReadBytesArray(array_uptr: ctypes.c_void_p) -> [bytes]:
        """
        Reads from unmanaged memory, returning list of bytes
        """
        if array_uptr is None:
            return None
          
        return Array.ReadArray(array_uptr, Array.ReadBytes)

    @staticmethod
    def WriteBytes(bytes_value: Union[bytes, bytearray]) -> c_void_p:
        """
        Writes list of bytes into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if bytes_value is None:
            return None        

        return Array.WriteBlittables(bytes_value, ctypes.c_ubyte)

    @staticmethod
    def WriteBytesArray(bytes_array: Union[List[bytes], List[bytearray]]) -> c_void_p:
        """
        Writes list of bytes into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if bytes_array is None:
            return None
          
        return Array.WriteArray(bytes_array, Array.WriteBytes)
    # endregion

    # region nullable double
    @staticmethod
    def ReadNullableDoubles(array_uptr: ctypes.c_void_p) -> [Optional[float]]:
        """
        Reads from unmanaged memory, returning list of Optional[float]
        """
        if array_uptr is None:
            return None
          
        return Array.ReadNullables(array_uptr, Array._nullable_double)


    @staticmethod
    def ReadNullableDoublesArray(array_uptr: ctypes.c_void_p) -> [[Optional[float]]]:
        """
        Reads from unmanaged memory, returning list of Optional[float] lists
        """
        if array_uptr is None:
            return None

        return Array.ReadArray(array_uptr, Array.ReadNullableDoubles)


    @staticmethod
    def WriteNullableDoubles(nullable_doubles: [Optional[float]]) -> c_void_p:
        """
        Writes list of Optional[float] into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if nullable_doubles is None:
            return None

        return Array.WriteBlittables(nullable_doubles, Array._nullable_double, lambda x: Array._nullable_double(x))

    @staticmethod
    def WriteNullableDoublesArray(nullable_doubles_array: [[Optional[float]]]) -> c_void_p:
        """
        Writes list of int64 lists into unmanaged memory, returning a pointer to the array, where first 4 bytes is the length
        """
        if nullable_doubles_array is None:
            return None

        return Array.WriteArray(nullable_doubles_array, Array.WriteNullableDoubles)
    # endregion
