import System
from datetime import datetime, timedelta, timezone


class NetToPythonConverter:

    @staticmethod
    def convert_datetime(value: System.DateTime) -> datetime:
        if value is None:
            return None
        utc_value = value.ToUniversalTime()
        base = datetime(1, 1, 1, tzinfo=timezone.utc)
        ticks = utc_value.Ticks
        elapsed = timedelta(microseconds=ticks/10)
        return base + elapsed

    @staticmethod
    def convert_timespan(value: System.TimeSpan) -> timedelta:
        if value is None:
            return None
        elapsed = value.TotalSeconds
        return timedelta(seconds=elapsed)

    @staticmethod
    def convert_array(net_array, convertValue=None) -> list:
        return NetToPythonConverter.convert_list(net_array, convertValue)

    @staticmethod
    def convert_list(net_list, convertValue=None) -> list:
        if net_list is None:
            return None
        if convertValue is None:
            convertValue=NetToPythonConverter.convert_type

        if isinstance(net_list, System.Array[System.Byte]):
            return bytes(net_list)
        items = []
        for i in net_list:
            items.append(convertValue(i))
        return items

    @staticmethod
    def convert_type(item):
        if item is None:
            return None
        if isinstance(item, System.DateTime):
            return NetToPythonConverter.convert_datetime(item)
        if isinstance(item, System.TimeSpan):
            return NetToPythonConverter.convert_timespan(item)
        if isinstance(item, list) or isinstance(item, System.Array):
            return NetToPythonConverter.convert_list(item)
        if isinstance(item, dict) or isinstance(item, System.Collections.IDictionary):
            return NetToPythonConverter.convert_dict(item)
        return item

    @staticmethod
    def convert_dict(net_dict, convertValue=None):
        if net_dict is None:
            return None
        if convertValue is None:
            convertValue = NetToPythonConverter.convert_type
        dictionary = {}
        for tuple in net_dict:
            key = tuple.Key
            val = tuple.Value
            key_conv = NetToPythonConverter.convert_type(key)
            val_conv = convertValue(val)
            dictionary[key_conv] = val_conv
        return dictionary
