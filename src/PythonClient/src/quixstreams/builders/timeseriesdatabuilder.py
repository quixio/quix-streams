import ctypes
from typing import Union, Dict

from ..helpers.nativedecorator import nativedecorator
from ..native.Python.InteropHelpers.ExternalTypes.System.Array import Array
from ..native.Python.QuixStreamsStreaming.Models.StreamProducer.TimeseriesDataBuilder import TimeseriesDataBuilder as tsdbi


@nativedecorator
class TimeseriesDataBuilder(object):
    """Builder for managing TimeseriesDataTimestamp instances on TimeseriesBufferProducer."""

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
        Initializes a new instance of TimeseriesDataBuilder.

        Args:
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
        Adds new parameter value at the time the builder is created for.

        Args:
            parameter_id: The id of the parameter to set the value for.
            value: The value of type string, float, int, bytes, or bytearray.

        Returns:
            The builder.
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
        Adds a tag to the values.

        Args:
            tag_id: The id of the tag.
            value: The value of the tag.

        Returns:
            The builder.
        """

        new = tsdbi(self._interop.AddTag(tag_id, value))
        if new != self._interop:
            self._interop.dispose_ptr__()
            self._interop = new
        return self

    def add_tags(self, tags: Dict[str, str]) -> 'TimeseriesDataBuilder':
        """
        Copies the tags from the specified dictionary. Conflicting tags will be overwritten.

        Args:
            tags: The tags to add.

        Returns:
            The builder.
        """

        if tags is None:
            return self

        for key, val in tags.items():
            self.add_tag(key, val)  # TODO use the bulk add self._interop.AddTags()
        return self

    def publish(self):
        """Publish the values."""

        self._interop.Publish()

        if not self._entered:
            self.dispose()
