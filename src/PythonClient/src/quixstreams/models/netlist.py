import weakref

from ..native.Python.InteropHelpers.ExternalTypes.System.List import List as li
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils

class NetReadOnlyList(object):
    """
    Experimental. Acts as a proxy between a .net collection and a python list. Useful if .net collection is observable and reacts to changes
    """

    @staticmethod
    def _returnsame(val):
        return val

    def __init__(self, net_pointer, converter_to_python=None, converter_from_python=None):
        self._pointer = net_pointer
        self._converter_to_python = converter_to_python
        if self._converter_to_python is None:
            self._converter_to_python = NetReadOnlyList._returnsame

        self._converter_from_python = converter_from_python
        if self._converter_from_python is None:
            self._converter_from_python = NetReadOnlyList._returnsame

        self._finalizer = weakref.finalize(self, self._finalizerfunc)

    def _finalizerfunc(self):
        self._finalizer.detach()
        InteropUtils.free_hptr(self._pointer)

    def dispose(self):
        self._finalizer()

    def _get_actual_from(self, value):
        return self._converter_from_python(value)

    def _get_actual_to(self, value):
        return self._converter_to_python(value)

    def __iter__(self):
        # better performance than iterating using __getitem__ and can be terminated without full materialization
        count = self.count()
        for i in range(count):
            item = li.GetValue(self._pointer, i)
            yield self._get_actual_to(item)

    def __getitem__(self, key):
        if key >= self.count():
            raise IndexError('list index out of range')
        item = li.GetValue(self._pointer, key)
        return self._get_actual_to(item)

    def __contains__(self, item):
        actual_item = self._get_actual_from(item)
        return li.Contains(self._pointer, actual_item)

    def __str__(self):
        text = "["
        for item in self:
            if isinstance(item, str):
                text += "('" + item + "'), "
            else:
                text += "(" + str(item) + "), "
            text += "(" + item + "), "
        text = text.rstrip(", ")
        text += "]"
        return text

    def __len__(self):
        return self.count()

    def count(self):
        return li.GetCount(self._pointer)

    def get_net_pointer(self):
        return self._pointer


class NetList(NetReadOnlyList):
    """
    Experimental. Acts as a proxy between a .net collection and a python list. Useful if .net collection is observable and reacts to changes
    """

    def __init__(self, net_pointer, converter_to_python=None, converter_from_python=None):
        NetReadOnlyList.__init__(self, net_pointer, converter_to_python, converter_from_python)

    @staticmethod
    def constructor_for_string(net_pointer=None):
        """
        Creates an empty dotnet list for strings  if no pointer provided, else wraps in NetDict with string converters
        """

        if net_pointer is None:
            net_pointer = li.ConstructorForString()

        return NetList(net_pointer=net_pointer,
                       converter_to_python=InteropUtils.ptr_to_utf8,
                       converter_from_python=InteropUtils.utf8_to_ptr)

    def __setitem__(self, key, value):
        actual_value = self._get_actual_from(value)
        li.SetAt(self._pointer, key, actual_value)

    def __delitem__(self, key):
        if type(key) is slice:
            raise Exception("Slice is not currently supported")  # TODO
        li.RemoveAt(self._pointer, key)

    def append(self, item):
        actual_item = self._get_actual_from(item)
        li.Add(self._pointer, actual_item)

    def remove(self, item):
        actual_item = self._get_actual_from(item)
        li.Remove(self._pointer, actual_item)

    def clear(self):
        li.Clear(self._pointer)
