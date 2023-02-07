from typing import Dict, List

from ...eventhook import EventHook
from datetime import datetime

from ...native.Python.QuixSdkStreaming.Models.StreamReader.StreamPropertiesReader import StreamPropertiesReader as spri
import ctypes
from ...helpers.dotnet.datetimeconverter import DateTimeConverter as dtc
from ...native.Python.InteropHelpers.ExternalTypes.System.Enumerable import Enumerable as ei
from ..netdict import NetDict
from ..netlist import NetList
from ...native.Python.InteropHelpers.InteropUtils import InteropUtils
from ...helpers.nativedecorator import nativedecorator


@nativedecorator
class StreamPropertiesReader(object):

    def __init__(self, net_pointer: ctypes.c_void_p):
        """
            Initializes a new instance of StreamPropertiesReader.
            NOTE: Do not initialize this class manually, use StreamReader.properties to access an instance of it

            Parameters:

            net_pointer: Pointer to an instance of a .net StreamPropertiesReader
        """
        if net_pointer is None:
            raise Exception("StreamPropertiesReader is none")

        self._cfuncrefs = []  # exists to hold onto the references created by the interop layer to avoid GC'ing them
        self._interop = spri(net_pointer)

        def _on_changed_net_handler():
            self.on_changed.fire()

        def _on_first_changed_sub():
            ref = self._interop.add_OnChanged(_on_changed_net_handler)
            self._cfuncrefs.append(ref)

        def _on_last_changed_unsub():
            # TODO do unsign with previous handler
            self._interop.remove_OnChanged(_on_changed_net_handler)


        self.on_changed = EventHook(_on_first_changed_sub, _on_last_changed_unsub, name="StreamPropertiesReader.on_changed")
        """
        Raised when stream properties changed. This is an event without parameters. Interrogate the class raising
        it to get new state
        """

        self._metadata = None
        self._parents = None

    def _finalizerfunc(self):
        if self._metadata is not None:
            InteropUtils.free_hptr(self._metadata.get_net_pointer())
        if self._parents is not None:
            InteropUtils.free_hptr(self._parents.get_net_pointer())
        self._cfuncrefs = None

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
