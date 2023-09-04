# ***********************GENERATED CODE WARNING************************
# This file is code generated, any modification you do will be lost the
# next time this file is regenerated.
# *********************************************************************

from typing import Any
from ctypes import c_void_p, c_int, c_long
import ctypes
from ...InteropUtils import InteropUtils
from .Array import Array


# noinspection PyPep8Naming
class Enumerable:

    interop_func = InteropUtils.get_function("enumerable_read_strings")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_void_p
    @staticmethod
    def ReadStrings(enumerable_hptr: ctypes.c_void_p) -> [str]:
        if enumerable_hptr is None:
            return None
          
        inter_array_uptr = InteropUtils.invoke("enumerable_read_strings", enumerable_hptr)
        inter_array_uptr = c_void_p(inter_array_uptr) if inter_array_uptr is not None else None
        return Array.ReadBlittables(inter_array_uptr, ctypes.c_void_p, lambda x: InteropUtils.ptr_to_utf8(x))

    interop_func = InteropUtils.get_function("enumerable_read_references")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_void_p
    @staticmethod
    def ReadReferences(enumerable_hptr: ctypes.c_void_p) -> [c_void_p]:
        if enumerable_hptr is None:
            return None
              
        inter_array_uptr = InteropUtils.invoke("enumerable_read_references", enumerable_hptr)
        inter_array_uptr = c_void_p(inter_array_uptr) if inter_array_uptr is not None else None
        return Array.ReadBlittables(inter_array_uptr, ctypes.c_void_p)


    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("enumerable_read_any")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_void_p
    @staticmethod
    def ReadAny(enumerable_hptr: c_void_p) -> c_void_p:
        """
        Read any object that implements IEnumerable. Useful for cases when other enumerable methods don't fulfill the
        the role in a more efficient manner
        :param enumerable_hptr: Handler pointer to an object 
        :return: Handle pointer to an array of values which depend on the underlying value types
        """
        if enumerable_hptr is None:
            return None
                  
        inter_array_uptr = InteropUtils.invoke("enumerable_read_any", enumerable_hptr)
        return c_void_p(inter_array_uptr) if inter_array_uptr is not None else None
