from typing import Union, Dict
import ctypes

from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array
from ..native.Python.QuixSdkStreaming.Models.StreamWriter.TimeseriesDataBuilder import TimeseriesDataBuilder as tsdbi
from ..helpers.nativedecorator import nativedecorator


@nativedecorator
class TimeseriesDataBuilder(object):
    """
        Builder for creating timeseries data packages for StreamPropertiesWriter
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of TimeseriesDataBuilder.

            Parameters:

            net_pointer: Pointer to an instance of a .net TimeseriesDataBuilder.
        """

        if net_pointer is None:
            raise Exception("TimeseriesDataBuilder is none")

        self._interop = tsdbi(net_pointer)
        self._entered = False

    def __enter__(self):
        self._entered = True

    def add_value(self, parameter_id: str, value: Union[str, float, int, bytes, bytearray]) -> 'TimeseriesDataBuilder':
        """
        Adds new parameter value at the time the builder is created for
        :param parameter_id: The id of the parameter to set the value for
        :param value: the value as string or float
        :return: The builder
        """

        val_type = type(value)
        if val_type is int:
            value = float(value)
            val_type = float
        elif val_type is bytearray:
            value = bytes(value)
            val_type = bytes

        if val_type is float:
            new = tsdbi(self._interop.AddValue(parameter_id, value))
            if new != self._interop:
                self._interop.dispose_ptr__()
                self._interop = new
        elif val_type is str:
            new = tsdbi(self._interop.AddValue2(parameter_id, value))
            if new != self._interop:
                self._interop.dispose_ptr__()
                self._interop = new
        elif val_type is bytes:
            arr_ptr = Array.WriteBytes(value)
            new = tsdbi(self._interop.AddValue3(parameter_id, arr_ptr))
            if new != self._interop:
                self._interop.dispose_ptr__()
                self._interop = new
        else:
            raise Exception("Invalid type " + str(val_type) + " passed as parameter value.")
        return self

    def add_tag(self, tag_id: str, value: str) -> 'TimeseriesDataBuilder':
        """
        Adds tag value for the values. If
        :param tag_id: The id of the tag
        :param value: The value of the tag
        :return: The builder
        """
        new = tsdbi(self._interop.AddTag(tag_id, value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'EventDataBuilder':
        """
            Copies the tags from the specified dictionary. Conflicting tags will be overwritten
            :param tags: The tags to add

        :return: EventDataBuilder
        """

        if tags is None:
            return self
        # TODO
        prep_tags_dict = System.Collections.Generic.List[System.Collections.Generic.KeyValuePair[str, str]]([])
        for key, val in tags.items():
            prep_tags_dict.Add(System.Collections.Generic.KeyValuePair[str, str](key, val))
        self.__wrapped.AddTags(prep_tags_dict)
        return self

    def write(self):
        """
        Writes the values to the StreamParametersWriter buffer. See StreamParametersWriter buffer settings for more information when the values are sent to the broker
        """

        self._interop.Write()

        if not self._entered:
            self.dispose()
