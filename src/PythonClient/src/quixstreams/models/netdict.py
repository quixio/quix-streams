import weakref
from typing import List, Any

from .netlist import NetReadOnlyList
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.InteropHelpers.ExternalTypes.System.Dictionary import Dictionary as di
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils


class ReadOnlyNetDict(object):
    """
    Experimental. Acts as a proxy between a .net dictionary and a python dict. Useful if .net dictionary is observable and reacts to changes
    """

    @staticmethod
    def _returnsame(val):
        return val

    def __init__(self, net_pointer, key_converter_to_python=None, key_converter_from_python=None,
                 val_converter_to_python=None, val_converter_from_python=None,
                 key_converter_to_python_list=None, val_converter_to_python_list=None,
                 self_dispose: bool = True):

        self._pointer = net_pointer
        self._key_converter_to_python = key_converter_to_python
        if self._key_converter_to_python is None:
            self._key_converter_to_python = ReadOnlyNetDict._returnsame

        self._key_converter_from_python = key_converter_from_python
        if self._key_converter_from_python is None:
            self._key_converter_from_python = ReadOnlyNetDict._returnsame

        self._key_converter_to_python_list = key_converter_to_python_list
        if self._key_converter_to_python_list is None:
            self._key_converter_to_python_list = lambda key_arr_ptr: NetReadOnlyList(key_arr_ptr,
                                                                                     converter_to_python=self._key_converter_to_python,
                                                                                     converter_from_python=self._key_converter_from_python)

        self._val_converter_to_python = val_converter_to_python
        if self._val_converter_to_python is None:
            self._val_converter_to_python = ReadOnlyNetDict._returnsame

        self._val_converter_from_python = val_converter_from_python
        if self._val_converter_from_python is None:
            self._val_converter_from_python = ReadOnlyNetDict._returnsame

        self._val_converter_to_python_list = val_converter_to_python_list
        if self._val_converter_to_python_list is None:
            self._val_converter_to_python_list = lambda val_arr_ptr: NetReadOnlyList(val_arr_ptr,
                                                                                     converter_to_python=self._val_converter_to_python,
                                                                                     converter_from_python=self._val_converter_from_python)

        if self_dispose:
            self._finalizer = weakref.finalize(self, self._finalizerfunc)
        else:
            self._finalizer = lambda: None

    def _finalizerfunc(self):
        self._finalizer.detach()
        InteropUtils.free_hptr(self._pointer)

    def dispose(self) -> None:
        self._finalizer()

    def _get_actual_key_from(self, key) -> Any:
        return self._key_converter_from_python(key)

    def _get_actual_value_from(self, value) -> Any:
        return self._val_converter_from_python(value)

    def _get_actual_key_to(self, key) -> Any:
        return self._key_converter_to_python(key)

    def _get_actual_value_to(self, value) -> Any:
        return self._val_converter_to_python(value)

    def __getitem__(self, key) -> Any:
        actual_key = self._get_actual_key_from(key)
        item = di.GetValue(self._pointer, actual_key)
        return self._get_actual_value_to(item)

    def __contains__(self, key) -> bool:
        actual_key = self._get_actual_key_from(key)
        return di.Contains(self._pointer, actual_key)

    def __str__(self) -> str:
        text = "{"
        for item in self.items():
            key = item[0]
            if isinstance(key, str):
                text += "'" + key + "'"
            else:
                text += str(key)
            text += ": "
            val = item[1]
            if isinstance(val, str):
                text += "'" + val + "'"
            else:
                text += str(val)
            text += ", "
        text = text.rstrip(", ")
        text += "}"
        return text

    def __len__(self) -> int:
        return di.GetCount(self._pointer)

    def keys(self) -> List:
        net_key_uptr = di.GetKeys(self._pointer)
        return self._key_converter_to_python_list(net_key_uptr)

    def values(self) -> List:
        net_value_ptr = di.GetValues(self._pointer)
        return self._val_converter_to_python_list(net_value_ptr)

    def __iter__(self):
        # requirement for enumerate(self), else the enumeration doesn't work as expected
        # better performance than iterating using __getitem__ and can be terminated without full materialization
        for key, value in zip(self.keys(), self.values()):
            yield key, value

    def items(self) -> List:
        item_list: list = []
        for index, (key, value) in enumerate(self):
            item_list.append((key, value))
        return item_list

    def get_net_pointer(self):
        return self._pointer


class NetDict(ReadOnlyNetDict):
    """
        Experimental. Acts as a proxy between a .net dictionary and a python list.
    """

    def __init__(self, net_pointer, key_converter_to_python=None, key_converter_from_python=None,
                 val_converter_to_python=None, val_converter_from_python=None,
                 key_converter_to_python_list=None, val_converter_to_python_list=None,
                 self_dispose: bool = True):

        super().__init__(net_pointer=net_pointer,
                         key_converter_to_python=key_converter_to_python,
                         key_converter_from_python=key_converter_from_python,
                         val_converter_to_python=val_converter_to_python,
                         val_converter_from_python=val_converter_from_python,
                         key_converter_to_python_list=key_converter_to_python_list,
                         val_converter_to_python_list=val_converter_to_python_list,
                         self_dispose=self_dispose)

    @staticmethod
    def constructor_for_string_string(net_pointer=None):
        """
        Creates an empty dotnet list for strings if no pointer provided, else wraps in NetDict with string converters
        """
        if net_pointer is None:
            net_pointer = di.ConstructorForStringString()

        return NetDict(net_pointer=net_pointer,
                       key_converter_to_python=InteropUtils.ptr_to_utf8,
                       key_converter_from_python=InteropUtils.utf8_to_ptr,
                       val_converter_to_python=InteropUtils.ptr_to_utf8,
                       val_converter_from_python=InteropUtils.utf8_to_ptr,
                       key_converter_to_python_list=ai.ReadStrings,
                       val_converter_to_python_list=ai.ReadStrings)

    def __setitem__(self, key, value):
        actual_key = self._get_actual_key_from(key)
        actual_value = self._get_actual_value_from(value)
        di.SetValue(self._pointer, actual_key, actual_value)

    def __delitem__(self, key):
        if type(key) is slice:
            raise Exception("Slice is not currently supported")  # ? TODO
        actual_key = self._get_actual_key_from(key)
        di.Remove(self._pointer, actual_key)

    def update(self, key, item):
        self[key] = item

    def clear(self):
        di.Clear(self._pointer)

    def pop(self, key):
        val = self[key]
        del self[key]
        return val

    def setdefault(self, key, default):
        if key in self:
            return self[key]

        self[key] = default
        return default
