import ctypes
from datetime import datetime, timedelta
from typing import Union, Dict

from ..helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ..helpers.nativedecorator import nativedecorator
from ..models.parametervalue import ParameterValue, ParameterValueType
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array as ai
from ..native.Python.InteropHelpers.ExternalTypes.System.Dictionary import Dictionary as di
from ..native.Python.InteropHelpers.InteropUtils import InteropUtils as iu
from ..native.Python.QuixStreamsStreaming.Models.TimeseriesDataTimestamp import TimeseriesDataTimestamp as tsdti


@nativedecorator
class TimeseriesDataTimestamp:
    """
    Represents a single point in time with parameter values and tags attached to that time.
    """

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TimeseriesDataTimestamp.

        Args:
            net_pointer: Pointer to an instance of a .net TimeseriesDataTimestamp.
        """
        if net_pointer is None:
            raise Exception("TimeseriesDataTimestamp constructor should not be invoked without a .net pointer")

        self._interop = tsdti(net_pointer)
        self._parameters = None  # to cache whatever is read from .net
        self._tags = None  # to cache whatever is read from .net

    def _finalizerfunc(self):
        self._clear_parameters()

    def _clear_parameters(self):
        if self._parameters is None:
            return
        [pval.dispose() for (pname, pval) in self._parameters.items()]
        self._parameters = None

    def __str__(self):
        text = "Time:" + str(self.timestamp_nanoseconds)
        text += "\r\n  Tags: " + str(self.tags)
        text += "\r\n  Params:"
        for param_id, param_val in self.parameters.items():
            if param_val.type == ParameterValueType.Numeric:
                text += "\r\n    " + str(param_id) + ": " + str(param_val.numeric_value)
                continue
            if param_val.type == ParameterValueType.String:
                text += "\r\n    " + str(param_id) + ": " + str(param_val.string_value)
                continue
            if param_val.type == ParameterValueType.Binary:
                text += "\r\n    " + str(param_id) + ": byte[" + str(len(param_val.binary_value)) + "]"
                continue
            if param_val.type == ParameterValueType.Empty:
                text += "\r\n    " + str(param_id) + ": Empty"
                continue
            text += "\r\n    " + str(param_id) + ": ???"
        return text

    @property
    def parameters(self) -> Dict[str, ParameterValue]:
        """
        Gets the parameter values for the timestamp as a dictionary. If a key is not found, returns an empty ParameterValue.

        Returns:
            Dict[str, ParameterValue]: A dictionary with parameter id as key and ParameterValue as value.
        """

        if self._parameters is None:
            def _value_converter_to_python(net_hptr: ctypes.c_void_p):
                if net_hptr is None:
                    return None
                return ParameterValue(net_hptr)

            parameters_hptr = self._interop.get_Parameters()
            try:
                parameters_uptr = di.ReadAnyHPtrToUPtr(parameters_hptr)
                self._parameters = di.ReadStringPointers(parameters_uptr, _value_converter_to_python)
            finally:
                iu.free_hptr(parameters_hptr)

        return self._parameters

    @property
    def tags(self) -> Dict[str, str]:
        """
        Gets the tags for the timestamp as a dictionary.

        Returns:
            Dict[str, str]: A dictionary with tag id as key and tag value as value.
        """

        if self._tags is None:
            try:
                tags_hptr = self._interop.get_Tags()
                if tags_hptr is None:
                    self._tags = {}
                else:
                    tags_uptr = di.ReadAnyHPtrToUPtr(tags_hptr)
                    self._tags = di.ReadStringStrings(tags_uptr)
            finally:
                iu.free_hptr(tags_hptr)

        return self._tags

    @property
    def timestamp_nanoseconds(self) -> int:
        """
        Gets the timestamp in nanoseconds.

        Returns:
            int: The timestamp in nanoseconds.
        """

        return self._interop.get_TimestampNanoseconds()

    @property
    def timestamp_milliseconds(self) -> int:
        """
        Gets the timestamp in milliseconds.

        Returns:
            int: The timestamp in milliseconds.
        """

        return self._interop.get_TimestampMilliseconds()

    @property
    def timestamp(self) -> datetime:
        """
        Gets the timestamp in datetime format.

        Returns:
            datetime: The timestamp in datetime format.
        """
        return dtc.datetime_to_python(self._interop.get_Timestamp())

    @property
    def timestamp_as_time_span(self) -> timedelta:
        """
        Gets the timestamp in timespan format.

        Returns:
            timedelta: The timestamp in timespan format.
        """
        return dtc.timespan_to_python(self._interop.get_TimestampAsTimeSpan())

    def add_value(self, parameter_id: str, value: Union[float, str, int, bytearray, bytes]) -> 'TimeseriesDataTimestamp':
        """
        Adds a new value for the specified parameter.

        Args:
            parameter_id: The parameter id to add the value for.
            value: The value to add. Can be float, string, int, bytearray, or bytes.

        Returns:
            TimeseriesDataTimestamp: The updated TimeseriesDataTimestamp instance.
        """

        if type(value) is int:
            value = float(value)

        val_type = type(value)
        if val_type is float:
            new = tsdti(self._interop.AddValue(parameter_id, value))
            if new != self._interop:
                self._interop.dispose_ptr__()
                self._interop = new
        elif val_type is str:
            new = tsdti(self._interop.AddValue2(parameter_id, value))
            if new != self._interop:
                self._interop.dispose_ptr__()
                self._interop = new
        elif val_type is bytearray or val_type is bytes:
            uptr = ai.WriteBytes(value)
            new = tsdti(self._interop.AddValue3(parameter_id, uptr))
            if new != self._interop:
                self._interop.dispose_ptr__()
                self._interop = new
        else:
            raise Exception("Invalid type " + str(val_type) + " passed as parameter value.")
        self._clear_parameters()  # to cause re-read from underlying if necessary

        return self

    def remove_value(self, parameter_id: str) -> 'TimeseriesDataTimestamp':
        """
        Removes the value for the specified parameter.

        Args:
            parameter_id: The parameter id to remove the value for.

        Returns:
            TimeseriesDataTimestamp: The updated TimeseriesDataTimestamp instance.
        """

        new = tsdti(self._interop.RemoveValue(parameter_id))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new

        self._clear_parameters()  # to cause re-read from underlying if necessary
        return self

    def add_tag(self, tag_id: str, tag_value: str) -> 'TimeseriesDataTimestamp':
        """
        Adds a tag to the timestamp.

        Args:
            tag_id: The id of the tag to add.
            tag_value: The value of the tag to add.

        Returns:
            TimeseriesDataTimestamp: The updated TimeseriesDataTimestamp instance.
        """
        tags = self.tags  # force evaluation of the local cache
        tags[tag_id] = tag_value
        new = tsdti(self._interop.AddTag(tag_id, tag_value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def remove_tag(self, tag_id: str) -> 'TimeseriesDataTimestamp':
        """
        Removes a tag from the timestamp.
        Args:
            tag_id: The id of the tag to remove.

        Returns:
            TimeseriesDataTimestamp: The updated TimeseriesDataTimestamp instance.
    """
        tags = self.tags  # force evaluation of the local cache
        tags.pop(tag_id)
        new = tsdti(self._interop.RemoveTag(tag_id))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'TimeseriesDataTimestamp':
        """
        Copies the tags from the specified dictionary. Conflicting tags will be overwritten.

        Args:
            tags: The dictionary of tags to add, with tag id as key and tag value as value.

        Returns:
            TimeseriesDataTimestamp: The updated TimeseriesDataTimestamp instance.
        """

        if tags is None:
            return self

        for key, val in tags.items():
            self.add_tag(key, val)  # TODO use the bulk add self._interop.AddTags()

        return self

    def get_net_pointer(self) -> ctypes.c_void_p:
        """
        Gets the .net pointer of the TimeseriesDataTimestamp instance.

        Returns:
            ctypes.c_void_p: The .net pointer of the TimeseriesDataTimestamp instance.
        """
        return self._interop.get_interop_ptr__()
