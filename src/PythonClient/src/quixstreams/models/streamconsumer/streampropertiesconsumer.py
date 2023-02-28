import ctypes
import traceback
from datetime import datetime
from typing import Dict, List, Callable

from ..netdict import NetDict
from ..netlist import NetList
from ...helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ...helpers.nativedecorator import nativedecorator
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesChangedEventArgs import StreamPropertiesChangedEventArgs
from ...native.Python.QuixStreamsStreaming.Models.StreamConsumer.StreamPropertiesConsumer import StreamPropertiesConsumer as spci


@nativedecorator
class StreamPropertiesConsumer(object):

    def __init__(self, stream_consumer: 'StreamConsumer', net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamPropertiesConsumer.
            NOTE: Do not initialize this class manually, use StreamConsumer.properties to access an instance of it

            Parameters:

            stream_consumer: The stream the buffer is created for
            net_pointer: Pointer to an instance of a .net StreamPropertiesConsumer
        """
        if net_pointer is None:
            raise Exception("StreamPropertiesConsumer is none")

        self._interop = spci(net_pointer)
        self._stream_consumer = stream_consumer

        # define events and their ref holder
        self._on_changed = None
        self._on_changed_ref = None  # keeping reference to avoid GC

        self._metadata = None
        self._parents = None

    def _finalizerfunc(self):
        if self._metadata is not None:
            self._metadata.dispose()
        if self._parents is not None:
            self._parents.dispose()
        self._on_changed_dispose()

    # region on_changed
    @property
    def on_changed(self) -> Callable[['StreamConsumer'], None]:
        """
        Gets the handler for when the stream properties changed. First parameter is the stream it is invoked for.
        """
        return self._on_changed

    @on_changed.setter
    def on_changed(self, value: Callable[['StreamConsumer'], None]) -> None:
        """
        Sets the handler for when the stream properties changed. First parameter is the stream it is invoked for.
        """
        self._on_changed = value
        if self._on_changed_ref is None:
            self._on_changed_ref = self._interop.add_OnChanged(self._on_changed_wrapper)

    def _on_changed_wrapper(self, sender_hptr, args_hptr):
        # To avoid unnecessary overhead and complication, we're using the instances we already have
        try:
            with (args := StreamPropertiesChangedEventArgs(args_hptr)):
                self._on_changed(self._stream_consumer)
            InteropUtils.free_hptr(sender_hptr)
        except:
            traceback.print_exc()

    def _on_changed_dispose(self):
        if self._on_changed_ref is not None:
            self._interop.remove_OnChanged(self._on_changed_ref)
            self._on_changed_ref = None

    # endregion on_changed

    @property
    def name(self) -> str:
        """ Gets the name of the stream """
        return self._interop.get_Name()

    @property
    def location(self) -> str:
        """ Gets the location of the stream """
        return self._interop.get_Location()

    @property
    def time_of_recording(self) -> datetime:
        """ Gets the datetime of the recording """

        ptr = self._interop.get_TimeOfRecording()
        value = dtc.datetime_to_python(ptr)
        return value

    @property
    def metadata(self) -> Dict[str, str]:
        """ Gets the metadata of the stream """
        if self._metadata is None:
            ptr = self._interop.get_Metadata()
            self._metadata = NetDict.constructor_for_string_string(ptr)
        return self._metadata

    @property
    def parents(self) -> List[str]:
        """Gets The ids of streams this stream is derived from"""
        if self._parents is None:
            list_ptr = self._interop.get_Parents()
            self._parents = NetList.constructor_for_string(list_ptr)
        return self._parents

    def get_net_pointer(self) -> ctypes.c_void_p:
        return self._interop.get_interop_ptr__()
