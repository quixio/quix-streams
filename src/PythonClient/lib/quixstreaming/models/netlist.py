import sys

class NetReadonlyList(object):
    """
        Experimental. Acts as a proxy between a .net collection and a python list. Useful if .net collection is observable and reacts to changes
    """

    def __init__(self, net_list, converter_to_python=None, converter_from_python=None):
        self._wrapped = net_list
        self._converter_to_python = converter_to_python
        self._converter_from_python = converter_from_python

    def _get_actual_from(self, value):
        if self._converter_from_python is None:
            return value
        return self._converter_from_python(value)

    def _get_actual_to(self, value):
        if self._converter_to_python is None:
            return value
        return self._converter_to_python(value)

    def __getitem__(self, key):
        if key >= self.count():
            raise IndexError('list index out of range')
        item = self._wrapped[key]
        return self._get_actual_to(item)

    def __contains__(self, item):
        actual_item = self._get_actual_from(item)
        return self._wrapped.Contains(actual_item)

    def __str__(self):
        text = "["
        for item in self.items():
            text += "(" + item + "), "
        text = text.rstrip(", ")
        text += "]"
        return text

    def __len__(self):
        return self.count()

    def count(self):
        return self._wrapped.Count


class NetList(NetReadonlyList):
    """
        Experimental. Acts as a proxy between a .net collection and a python list. Useful if .net collection is observable and reacts to changes
    """

    def __init__(self, net_list, converter_to_python=None, converter_from_python=None):
        NetReadonlyList.__init__(self, net_list, converter_to_python, converter_from_python)

    def __setitem__(self, key, value):
        actual_value = self._get_actual_from(value)
        self._wrapped[key] = actual_value

    def __delitem__(self, key):
        self._wrapped.RemoveAt(key)

    def append(self, item):
        actual_item = self._get_actual_from(item)
        self._wrapped.Add(actual_item)

    def remove(self, item):
        actual_item = self._get_actual_from(item)
        self._wrapped.Remove(actual_item)

    def clear(self):
        self._wrapped.Clear()

