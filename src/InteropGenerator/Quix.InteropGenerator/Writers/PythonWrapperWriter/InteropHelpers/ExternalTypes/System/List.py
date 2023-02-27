# ***********************GENERATED CODE WARNING************************
# This file is code generated, any modification you do will be lost the
# next time this file is regenerated.
# *********************************************************************

from typing import Any
from ctypes import c_void_p, c_int, c_long, c_bool
import ctypes
from ...InteropUtils import InteropUtils
from .Array import Array
from .Collection import Collection


# noinspection PyPep8Naming
class List:

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("list_constructor_string")
    interop_func.argtypes = []
    interop_func.restype = c_void_p
    @staticmethod
    def ConstructorForString() -> c_void_p:
        ptr = InteropUtils.invoke("list_constructor_string")
        return c_void_p(ptr) if ptr is not None else None

    # ctypes function return type//parameter fix
    interop_func = InteropUtils.get_function("list_get_value")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_void_p
    @staticmethod
    def GetValue(list_hptr: c_void_p, index: c_int) -> c_void_p:
        ptr = InteropUtils.invoke("list_get_value", list_hptr, index)
        return c_void_p(ptr) if ptr is not None else None

    @staticmethod
    def GetCount(list_hptr: c_void_p) -> int:
        return Collection.GetCount(list_hptr)

    # ctypes function return type//parameter fix        
    interop_func = InteropUtils.get_function("list_contains")
    interop_func.argtypes = [c_void_p, c_void_p]
    interop_func.restype = c_bool
    @staticmethod
    def Contains(list_hptr: c_void_p, value_hptr: c_void_p) -> bool:
        return InteropUtils.invoke("list_contains", list_hptr, value_hptr)
        
    # ctypes function return type//parameter fix        
    interop_func = InteropUtils.get_function("list_set_at")
    interop_func.argtypes = [c_void_p, c_int, c_void_p]
    @staticmethod
    def SetAt(list_hptr: c_void_p, index: int, value_hptr: c_void_p) -> None:
        InteropUtils.invoke("list_set_at", list_hptr, index, value_hptr)
        
    # ctypes function return type//parameter fix        
    interop_func = InteropUtils.get_function("list_remove_at")
    interop_func.argtypes = [c_void_p, c_int]
    @staticmethod
    def RemoveAt(list_hptr: c_void_p, index: int) -> None:
        InteropUtils.invoke("list_remove_at", list_hptr, index)
        
    # ctypes function return type//parameter fix        
    interop_func = InteropUtils.get_function("list_remove")
    interop_func.argtypes = [c_void_p, c_void_p]
    @staticmethod
    def Remove(list_hptr: c_void_p, value_hptr: c_void_p) -> None:
        InteropUtils.invoke("list_remove", list_hptr, value_hptr)
        
     # ctypes function return type//parameter fix        
    interop_func = InteropUtils.get_function("list_add")
    interop_func.argtypes = [c_void_p, c_void_p]
    @staticmethod
    def Add(list_hptr: c_void_p, value_hptr: c_void_p) -> None:
        InteropUtils.invoke("list_add", list_hptr, value_hptr)

    # ctypes function return type//parameter fix        
    interop_func = InteropUtils.get_function("list_clear")
    interop_func.argtypes = [c_void_p]
    @staticmethod
    def Clear(list_hptr: c_void_p) -> None:
        InteropUtils.invoke("list_clear", list_hptr)
