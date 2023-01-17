class NetDict(object):
    """
        Experimental. Acts as a proxy between a .net dictionary and a python list.
    """

    def __init__(self, net_list):
        self.__wrapped = net_list

    def __getitem__(self, key):
        return self.__wrapped[key]

    def __setitem__(self, key, value):
        self.__wrapped[key] = value

    def __delitem__(self, key):
        self.__wrapped.RemoveAt(key)

    def __contains__(self, item):
        return self.__wrapped.Contains(item)

    def __str__(self):
        text = "["
        for item in self.items():
            text += "(" + item[0] + ", " + item[1] + "), "
        text = text.rstrip(", ")
        text += "]"
        return text

    def update(self, key, item):
        self.__wrapped[key] = item

    def keys(self, item):
        return self.__wrapped.Keys

    def values(self):
        return self.__wrapped.Values

    def clear(self):
        self.__wrapped.Clear()

    def pop(self, key):
        self.__wrapped.Remove(key)

    def items(self):
        item_list: list = []
        for pair in self.__wrapped:
            item_list.append((pair.Key, pair.Value))
        return item_list

    def setdefault(self, key, default):
        if self.__wrapped.ContainsKey(key):
            return self._wrapped[key]
        self._wrapped[key] = default
        return default

class ReadOnlyNetDict(object):
    """
        Experimental. Acts as a proxy between a .net dictionary and a python list. Useful if .net dictionary is observable and reacts to changes
    """

    def __init__(self, net_list, key_converter_to_python=None, key_converter_from_python=None,
                 val_converter_to_python=None, val_converter_from_python=None):
        self.__wrapped = net_list
        self.__key_converter_to_python = key_converter_to_python
        self.__key_converter_from_python = key_converter_from_python
        self.__val_converter_to_python = val_converter_to_python
        self.__val_converter_from_python = val_converter_from_python

    def __get_actual_key_from(self, key):
        if self.__key_converter_from_python is None:
            return key
        return self.__key_converter_from_python(key)

    def __get_actual_value_from(self, value):
        if self.__val_converter_from_python is None:
            return value
        return self.__val_converter_from_python(value)

    def __get_actual_key_to(self, key):
        if self.__key_converter_to_python is None:
            return key
        return self.__key_converter_to_python(key)

    def __get_actual_value_to(self, value):
        if self.__val_converter_to_python is None:
            return value
        return self.__val_converter_to_python(value)

    def __getitem__(self, key):
        actual_key = self.__get_actual_key_from(key)
        item = self.__wrapped[actual_key]
        return self.__get_actual_value_to(item)

    def __contains__(self, item):
        actual_item = self.__get_actual_value_from(item)
        return self.__wrapped.Contains(actual_item)

    def __str__(self):
        text = "{"
        for item in self.items():
            key = item[0]
            if isinstance(key, str):
                text += "'" + key + "'"
            else:
                text += key
            text += ": "
            val = item[1]
            if isinstance(val, str):
                text += "'" + val + "'"
            else:
                text += val
            text += ", "
        text = text.rstrip(", ")
        text += "}"
        return text

    def __len__(self):
        return self.__wrapped.Count

    def keys(self, item):
        if self.__key_converter_to_python is None:
            return self.__wrapped.Keys
        item_list: list = []
        for key in self.__wrapped.Keys:
            item_list.append(self.__key_converter_to_python(key))
        return item_list

    def values(self):
        if self.__value_converter_to is None:
            return self.__wrapped.Values
        item_list: list = []
        for val in self.__wrapped.Values:
            item_list.append(self.__key_converter_to_python(val))
        return item_list

    def items(self):
        item_list: list = []
        for pair in self.__wrapped:
            actual_key = self.__get_actual_key_to(pair.Key)
            actual_val = self.__get_actual_value_to(pair.Value)
            item_list.append((actual_key, actual_val))
        return item_list
