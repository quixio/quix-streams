# ***********************GENERATED CODE WARNING************************
# This file is code generated, any modification you do will be lost the
# next time this file is regenerated.
# *********************************************************************

from typing import Any
from ctypes import c_void_p, c_int, c_long, c_bool
import ctypes
from ...InteropUtils import InteropUtils
from .Array import Array


# noinspection PyPep8Naming
class Collection:

    interop_func = InteropUtils.get_function("collection_get_count")
    interop_func.argtypes = [c_void_p]
    interop_func.restype = c_int
    @staticmethod
    def GetCount(list_hptr: c_void_p) -> int:
        return InteropUtils.invoke("collection_get_count", list_hptr)
